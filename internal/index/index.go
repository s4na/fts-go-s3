// Package index is an experimental immutable, hash-sharded inverted index.
package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
	"github.com/s4na/fts-go-s3/internal/tokenize"
)

type Index struct {
	S3     *s3lite.Client
	Bucket string
	Prefix string
}

type Manifest struct {
	Shards    int      `json:"shards"`
	Documents []string `json:"documents"`
}

func (i Index) object(ctx context.Context, method, key string, data []byte) (s3lite.Result, error) {
	r, err := i.S3.Request(ctx, method, i.Bucket, strings.Trim(i.Prefix, "/")+"/"+key, nil, nil, data)
	if err != nil {
		return r, err
	}
	if r.Status < 200 || r.Status >= 300 {
		return r, fmt.Errorf("%s %s: HTTP %d: %s", method, key, r.Status, r.Body)
	}
	return r, nil
}

func shard(term string, count int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(term))
	return int(h.Sum64() % uint64(count))
}

func terms(text string) []string {
	text = tokenize.Normalize(text)
	seen := make(map[string]bool)
	for _, term := range (tokenize.Bigram{}).Tokens(text) {
		seen[term] = true
	}
	for _, r := range text {
		seen[string(r)] = true
	}
	result := make([]string, 0, len(seen))
	for term := range seen {
		result = append(result, term)
	}
	return result
}

func putVarint(w *bytes.Buffer, n uint64) {
	var buf [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buf[:], n)
	_, _ = w.Write(buf[:length])
}

// encode writes a sorted term dictionary followed by delta encoded document IDs.
func encode(postings map[string][]uint64) []byte {
	var out bytes.Buffer
	keys := make([]string, 0, len(postings))
	for key := range postings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	putVarint(&out, uint64(len(keys)))
	for _, key := range keys {
		putVarint(&out, uint64(len(key)))
		_, _ = out.WriteString(key)
		ids := postings[key]
		putVarint(&out, uint64(len(ids)))
		var previous uint64
		for _, id := range ids {
			putVarint(&out, id-previous)
			previous = id
		}
	}
	return out.Bytes()
}

func decode(data []byte) (map[string][]uint64, error) {
	reader := bytes.NewReader(data)
	count, err := binary.ReadUvarint(reader)
	if err != nil || count > uint64(len(data)) {
		return nil, fmt.Errorf("invalid dictionary size")
	}
	out := make(map[string][]uint64)
	for n := uint64(0); n < count; n++ {
		size, err := binary.ReadUvarint(reader)
		if err != nil || size > uint64(reader.Len()) {
			return nil, fmt.Errorf("invalid term length")
		}
		key := make([]byte, int(size))
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		length, err := binary.ReadUvarint(reader)
		if err != nil || length > uint64(reader.Len()) {
			return nil, fmt.Errorf("invalid posting length")
		}
		ids := make([]uint64, int(length))
		var previous uint64
		for j := range ids {
			delta, err := binary.ReadUvarint(reader)
			if err != nil || previous+delta < previous {
				return nil, fmt.Errorf("invalid posting")
			}
			previous += delta
			ids[j] = previous
		}
		out[string(key)] = ids
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("trailing bytes in shard")
	}
	return out, nil
}

// Build writes shards first and publishes the manifest last. The prefix must be
// fresh: later PRs add generations and conditional manifest writes.
func (i Index) Build(ctx context.Context, docs []oracle.Document, count int) (Manifest, error) {
	if count < 1 {
		return Manifest{}, fmt.Errorf("shards must be positive")
	}
	shards := make([]map[string][]uint64, count)
	manifest := Manifest{Shards: count, Documents: make([]string, 0, len(docs))}
	seen := make(map[string]bool)
	for index, doc := range docs {
		if doc.ID == "" || seen[doc.ID] {
			return Manifest{}, fmt.Errorf("empty or duplicate document ID %q", doc.ID)
		}
		seen[doc.ID] = true
		manifest.Documents = append(manifest.Documents, doc.ID)
		for _, term := range terms(doc.Title + "\n" + doc.Text) {
			n := shard(term, count)
			if shards[n] == nil {
				shards[n] = make(map[string][]uint64)
			}
			shards[n][term] = append(shards[n][term], uint64(index))
		}
	}
	for n, postings := range shards {
		if postings == nil {
			continue
		}
		if _, err := i.object(ctx, http.MethodPut, fmt.Sprintf("shards/%d", n), encode(postings)); err != nil {
			return Manifest{}, err
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, err
	}
	_, err = i.object(ctx, http.MethodPut, "manifest.json", data)
	return manifest, err
}

func (i Index) Manifest(ctx context.Context) (Manifest, error) {
	r, err := i.object(ctx, http.MethodGet, "manifest.json", nil)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(r.Body, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Shards < 1 {
		return Manifest{}, fmt.Errorf("invalid manifest shard count")
	}
	return manifest, nil
}

// Search loads each needed shard at most once and intersects postings.
// Passing a manifest caches it; nil fetches it afresh.
func (i Index) Search(ctx context.Context, query string, cached *Manifest) ([]string, error) {
	queryTerms := (tokenize.Bigram{}).Tokens(query)
	if len(queryTerms) == 0 {
		return nil, nil
	}
	var manifest Manifest
	var err error
	if cached == nil {
		manifest, err = i.Manifest(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		manifest = *cached
	}
	if manifest.Shards < 1 {
		return nil, fmt.Errorf("invalid manifest shard count")
	}
	loaded := make(map[int]map[string][]uint64)
	var candidates map[uint64]bool
	for _, term := range queryTerms {
		n := shard(term, manifest.Shards)
		postings, ok := loaded[n]
		if !ok {
			r, err := i.S3.Request(ctx, http.MethodGet, i.Bucket, strings.Trim(i.Prefix, "/")+fmt.Sprintf("/shards/%d", n), nil, nil, nil)
			if err != nil {
				return nil, err
			}
			if r.Status == http.StatusNotFound {
				return nil, nil
			}
			if r.Status != http.StatusOK {
				return nil, fmt.Errorf("get shard: HTTP %d", r.Status)
			}
			postings, err = decode(r.Body)
			if err != nil {
				return nil, err
			}
			loaded[n] = postings
		}
		current := make(map[uint64]bool)
		for _, id := range postings[term] {
			if candidates == nil || candidates[id] {
				current[id] = true
			}
		}
		candidates = current
		if len(candidates) == 0 {
			return nil, nil
		}
	}
	out := make([]string, 0, len(candidates))
	for id := range candidates {
		if id >= uint64(len(manifest.Documents)) {
			return nil, fmt.Errorf("posting references unknown document %d", id)
		}
		out = append(out, manifest.Documents[id])
	}
	sort.Strings(out)
	return out, nil
}
