// Package index is an experimental immutable, hash-sharded inverted index.
package index

import (
	"bytes"
	"context"
	"crypto/rand"
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
	Active    []bool   `json:"active"`
	Segments  []string `json:"segments"`
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
	seen := make(map[string]bool)
	for _, term := range (tokenize.Bigram{}).Tokens(text) {
		seen[term] = true
	}
	for _, r := range tokenize.Normalize(text) {
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
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Manifest{}, err
	}
	segment := fmt.Sprintf("%x", random[:])
	shards := make([]map[string][]uint64, count)
	manifest := Manifest{Shards: count, Documents: make([]string, 0, len(docs)), Segments: []string{segment}}
	seen := make(map[string]bool)
	for index, doc := range docs {
		if doc.ID == "" || seen[doc.ID] {
			return Manifest{}, fmt.Errorf("empty or duplicate document ID %q", doc.ID)
		}
		seen[doc.ID] = true
		manifest.Documents = append(manifest.Documents, doc.ID)
		manifest.Active = append(manifest.Active, true)
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
		if _, err := i.object(ctx, http.MethodPut, fmt.Sprintf("segments/%s/shards/%d", segment, n), encode(postings)); err != nil {
			return Manifest{}, err
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, err
	}
	key := strings.Trim(i.Prefix, "/") + "/manifest.json"
	r, err := i.S3.Request(ctx, http.MethodPut, i.Bucket, key, nil, http.Header{"If-None-Match": {"*"}}, data)
	if err != nil {
		return Manifest{}, err
	}
	if r.Status == http.StatusPreconditionFailed || r.Status == http.StatusConflict {
		return Manifest{}, ErrConflict
	}
	if r.Status < 200 || r.Status >= 300 {
		return Manifest{}, fmt.Errorf("publish manifest: HTTP %d: %s", r.Status, r.Body)
	}
	return manifest, nil
}

func (i Index) Manifest(ctx context.Context) (Manifest, error) {
	manifest, _, err := i.readManifest(ctx)
	return manifest, err
}

func (i Index) readManifest(ctx context.Context) (Manifest, string, error) {
	r, err := i.object(ctx, http.MethodGet, "manifest.json", nil)
	if err != nil {
		return Manifest{}, "", err
	}
	var manifest Manifest
	if err := json.Unmarshal(r.Body, &manifest); err != nil {
		return Manifest{}, "", err
	}
	if manifest.Shards < 1 || len(manifest.Documents) != len(manifest.Active) {
		return Manifest{}, "", fmt.Errorf("invalid manifest")
	}
	if r.Header.Get("ETag") == "" {
		return Manifest{}, "", fmt.Errorf("manifest has no ETag")
	}
	return manifest, r.Header.Get("ETag"), nil
}

// Update writes a fresh immutable segment and publishes a manifest with CAS.
// A nil document deletes id. Conflicts return ErrConflict; callers may retry
// after reloading the manifest. Unpublished segment objects are left for GC.
var ErrConflict = fmt.Errorf("manifest changed concurrently")

func (i Index) Update(ctx context.Context, id string, doc *oracle.Document) error {
	return i.Apply(ctx, []Change{{ID: id, Document: doc}})
}

type Change struct {
	ID       string
	Document *oracle.Document
}

// Apply atomically publishes a batch of distinct additions, replacements, and deletions.
func (i Index) Apply(ctx context.Context, changes []Change) error {
	if len(changes) == 0 {
		return fmt.Errorf("empty update")
	}
	manifest, etag, err := i.readManifest(ctx)
	if err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	segment := fmt.Sprintf("%x", random[:])
	postings := make(map[int]map[string][]uint64)
	seen := make(map[string]bool)
	for _, change := range changes {
		id, doc := change.ID, change.Document
		if id == "" || seen[id] || doc != nil && doc.ID != id {
			return fmt.Errorf("invalid or duplicate document ID %q", id)
		}
		seen[id] = true
		found := false
		for n, key := range manifest.Documents {
			if key == id && manifest.Active[n] {
				manifest.Active[n] = false
				found = true
			}
		}
		if doc == nil && !found {
			return fmt.Errorf("document %q does not exist", id)
		}
		if doc == nil {
			continue
		}
		docID := uint64(len(manifest.Documents))
		for _, term := range terms(doc.Title + "\n" + doc.Text) {
			n := shard(term, manifest.Shards)
			if postings[n] == nil {
				postings[n] = make(map[string][]uint64)
			}
			postings[n][term] = append(postings[n][term], docID)
		}
		manifest.Documents = append(manifest.Documents, id)
		manifest.Active = append(manifest.Active, true)
	}
	for n, posting := range postings {
		if _, err := i.object(ctx, http.MethodPut, fmt.Sprintf("segments/%s/shards/%d", segment, n), encode(posting)); err != nil {
			return err
		}
	}
	if len(postings) > 0 {
		manifest.Segments = append(manifest.Segments, segment)
	}
	return i.publish(ctx, manifest, etag)
}

func (i Index) publish(ctx context.Context, manifest Manifest, etag string) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	key := strings.Trim(i.Prefix, "/") + "/manifest.json"
	r, err := i.S3.Request(ctx, http.MethodPut, i.Bucket, key, nil, http.Header{"If-Match": {etag}}, data)
	if err != nil {
		return err
	}
	if r.Status == http.StatusPreconditionFailed || r.Status == http.StatusConflict {
		return ErrConflict
	}
	if r.Status < 200 || r.Status >= 300 {
		return fmt.Errorf("publish manifest: HTTP %d: %s", r.Status, r.Body)
	}
	return nil
}

// Compact writes a new immutable segment and atomically publishes it. It keeps
// old objects: readers with an older cached manifest may still need them.
func (i Index) Compact(ctx context.Context) error {
	manifest, etag, err := i.readManifest(ctx)
	if err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	segment := fmt.Sprintf("%x", random[:])
	for n := 0; n < manifest.Shards; n++ {
		merged := make(map[string][]uint64)
		for _, previous := range manifest.Segments {
			key := strings.Trim(i.Prefix, "/") + fmt.Sprintf("/segments/%s/shards/%d", previous, n)
			r, err := i.S3.Request(ctx, http.MethodGet, i.Bucket, key, nil, nil, nil)
			if err != nil {
				return err
			}
			if r.Status == http.StatusNotFound {
				continue
			}
			if r.Status != http.StatusOK {
				return fmt.Errorf("read segment: HTTP %d", r.Status)
			}
			postings, err := decode(r.Body)
			if err != nil {
				return err
			}
			for term, ids := range postings {
				for _, id := range ids {
					if id >= uint64(len(manifest.Active)) {
						return fmt.Errorf("posting references unknown document %d", id)
					}
					if manifest.Active[id] {
						merged[term] = append(merged[term], id)
					}
				}
			}
		}
		if len(merged) == 0 {
			continue
		}
		for _, ids := range merged {
			sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		}
		if _, err := i.object(ctx, http.MethodPut, fmt.Sprintf("segments/%s/shards/%d", segment, n), encode(merged)); err != nil {
			return err
		}
	}
	manifest.Segments = []string{segment}
	return i.publish(ctx, manifest, etag)
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
	if manifest.Shards < 1 || len(manifest.Active) != len(manifest.Documents) {
		return nil, fmt.Errorf("invalid manifest")
	}
	var out []string
	for _, segment := range manifest.Segments {
		ids, err := i.searchSegment(ctx, segment, queryTerms, manifest.Shards)
		if err != nil {
			return nil, err
		}
		for id := range ids {
			if id >= uint64(len(manifest.Documents)) {
				return nil, fmt.Errorf("posting references unknown document %d", id)
			}
			if manifest.Active[id] {
				out = append(out, manifest.Documents[id])
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (i Index) searchSegment(ctx context.Context, segment string, queryTerms []string, shardCount int) (map[uint64]bool, error) {
	loaded := make(map[int]map[string][]uint64)
	var candidates map[uint64]bool
	for _, term := range queryTerms {
		n := shard(term, shardCount)
		postings, ok := loaded[n]
		if !ok {
			r, err := i.S3.Request(ctx, http.MethodGet, i.Bucket, strings.Trim(i.Prefix, "/")+fmt.Sprintf("/segments/%s/shards/%d", segment, n), nil, nil, nil)
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
	return candidates, nil
}
