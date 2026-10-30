package zenbun

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const manifestVersion = 1

// Index stores immutable segments under a prefix, separate from source documents.
type Index struct {
	store   Store
	prefix  string
	cacheMu sync.Mutex
	cache   map[string]*segmentReader
}

// Open configures an index. It does not create an object until the first write.
func Open(store Store, prefix string) (*Index, error) {
	if store == nil || prefix == "" || !utf8.ValidString(prefix) {
		return nil, errors.New("zenbun: store and UTF-8 prefix are required")
	}
	return &Index{store: store, prefix: strings.TrimRight(prefix, "/") + "/"}, nil
}

// segment caches only immutable sparse indexes, never document or posting data.
func (i *Index) segment(ctx context.Context, ref segmentRef) (*segmentReader, error) {
	i.cacheMu.Lock()
	reader := i.cache[ref.Key]
	i.cacheMu.Unlock()
	if reader != nil {
		if reader.ref.Size != ref.Size || reader.ref.Documents != ref.Documents {
			return nil, errors.New("zenbun: inconsistent segment reference")
		}
		return reader, nil
	}
	reader, err := openSegment(ctx, i.store, ref)
	if err != nil {
		return nil, err
	}
	i.cacheMu.Lock()
	if i.cache == nil || len(i.cache) >= 1024 {
		i.cache = make(map[string]*segmentReader)
	}
	i.cache[ref.Key] = reader
	i.cacheMu.Unlock()
	return reader, nil
}

type segmentRef struct {
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	Documents int    `json:"documents"`
}

type manifest struct {
	Version    int          `json:"version"`
	Generation string       `json:"generation"`
	Segments   []segmentRef `json:"segments"`
}

func randomID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func (i *Index) loadManifest(ctx context.Context) (manifest, string, error) {
	obj, err := i.store.Get(ctx, i.prefix+"manifest", nil)
	if errors.Is(err, ErrNotFound) {
		return manifest{Version: manifestVersion}, "", nil
	}
	if err != nil {
		return manifest{}, "", err
	}
	var m manifest
	if len(obj.Data) > maxMetadataSize || json.Unmarshal(obj.Data, &m) != nil {
		return manifest{}, "", errors.New("zenbun: invalid manifest")
	}
	if m.Version != manifestVersion {
		return manifest{}, "", errors.New("zenbun: unsupported manifest version")
	}
	seen := make(map[string]bool)
	for _, ref := range m.Segments {
		if !strings.HasPrefix(ref.Key, i.prefix+"segments/") || strings.Contains(strings.TrimPrefix(ref.Key, i.prefix+"segments/"), "/") || ref.Size < footerSize || ref.Documents < 1 || seen[ref.Key] {
			return manifest{}, "", errors.New("zenbun: invalid segment reference")
		}
		seen[ref.Key] = true
	}
	if obj.ETag == "" {
		return manifest{}, "", errors.New("zenbun: manifest has no ETag")
	}
	return m, obj.ETag, nil
}

func (i *Index) publish(ctx context.Context, m manifest, etag string) error {
	id, err := randomID()
	if err != nil {
		return err
	}
	m.Version = manifestVersion
	m.Generation = id
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(data) > maxMetadataSize {
		return errors.New("zenbun: manifest exceeds size limit")
	}
	options := PutOptions{IfNoneMatch: etag == "", IfMatch: etag}
	_, err = i.store.Put(ctx, i.prefix+"manifest", data, options)
	return err
}

func (i *Index) writeSegment(ctx context.Context, docs map[string]indexedDocument) (segmentRef, error) {
	data, err := encodeSegment(docs)
	if err != nil {
		return segmentRef{}, err
	}
	id, err := randomID()
	if err != nil {
		return segmentRef{}, err
	}
	key := i.prefix + "segments/" + id
	if _, err := i.store.Put(ctx, key, data, PutOptions{IfNoneMatch: true}); err != nil {
		return segmentRef{}, err
	}
	return segmentRef{Key: key, Size: int64(len(data)), Documents: len(docs)}, nil
}

// Change adds or replaces a document, or deletes it when Delete is true.
// ETag is optional source metadata used by Sync.
type Change struct {
	Key    string
	Data   []byte
	Delete bool
	ETag   string
}

func validDocument(key string, data []byte) bool {
	return key != "" && utf8.ValidString(key) && utf8.Valid(data)
}

// Apply atomically publishes a batch. ErrConflict means no batch changes were
// published; callers can reload state and retry. Duplicate keys are rejected.
func (i *Index) Apply(ctx context.Context, changes []Change) error {
	if len(changes) == 0 {
		return nil
	}
	docs := make(map[string]indexedDocument, len(changes))
	for _, change := range changes {
		if change.Key == "" || !utf8.ValidString(change.Key) || !change.Delete && !utf8.Valid(change.Data) {
			return errors.New("zenbun: document key and content must be valid UTF-8")
		}
		if _, exists := docs[change.Key]; exists {
			return errors.New("zenbun: duplicate key in batch")
		}
		doc := indexedDocument{Deleted: change.Delete, ETag: change.ETag}
		if !change.Delete {
			doc.Terms = terms(string(change.Data))
		}
		docs[change.Key] = doc
	}
	m, etag, err := i.loadManifest(ctx)
	if err != nil {
		return err
	}
	ref, err := i.writeSegment(ctx, docs)
	if err != nil {
		return err
	}
	m.Segments = append(m.Segments, ref)
	return i.publish(ctx, m, etag)
}

// RetryConflict retries a complete operation on ErrConflict. The operation must
// reload the current state on each call. Other errors are returned unchanged.
func RetryConflict(ctx context.Context, attempts int, operation func(context.Context) error) error {
	if attempts < 1 || operation == nil {
		return errors.New("zenbun: retry requires an operation and positive attempts")
	}
	for n := 0; n < attempts; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx)
		if !errors.Is(err, ErrConflict) || n+1 == attempts {
			return err
		}
		timer := time.NewTimer(time.Duration(1<<min(n, 6)) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrConflict
}

func decodeList(data []byte) ([]string, error) {
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, errors.New("zenbun: invalid segment list")
	}
	for n, key := range keys {
		if !utf8.ValidString(key) || n > 0 && keys[n-1] >= key {
			return nil, errors.New("zenbun: invalid segment list order")
		}
	}
	return keys, nil
}

// decodePosting resolves version 2 document IDs through the segment's sorted
// document table. The table is read at most once per segment and search.
func (reader *segmentReader) decodePosting(ctx context.Context, data []byte, documentKeys *[]string) ([]string, error) {
	if reader.version == legacySegmentVersion {
		return decodeList(data)
	}
	if *documentKeys == nil {
		table, found, err := reader.lookup(ctx, "a")
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("zenbun: missing segment document list")
		}
		keys, err := decodeList(table)
		if err != nil || len(keys) != reader.ref.Documents {
			return nil, errors.New("zenbun: invalid segment document table")
		}
		*documentKeys = keys
	}
	keys := make([]string, 0)
	var id uint64
	for len(data) > 0 {
		delta, n := binary.Uvarint(data)
		if n <= 0 || delta == 0 || delta > uint64(len(*documentKeys))-id {
			return nil, errors.New("zenbun: invalid segment posting")
		}
		id += delta
		keys = append(keys, (*documentKeys)[id-1])
		data = data[n:]
	}
	if len(keys) == 0 {
		return nil, errors.New("zenbun: empty segment posting")
	}
	return keys, nil
}

func readDocument(ctx context.Context, reader *segmentReader, key string) (indexedDocument, bool, error) {
	data, found, err := reader.lookup(ctx, "k"+key)
	if err != nil || !found {
		return indexedDocument{}, found, err
	}
	var doc indexedDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return indexedDocument{}, false, errors.New("zenbun: invalid document record")
	}
	return doc, true, nil
}

// Search returns candidate document keys. It may include false positives;
// callers should read and verify source documents when exact matches matter.
// Empty queries are rejected; matching uses NFKC, lowercase and character grams.
func (i *Index) Search(ctx context.Context, query string) ([]string, error) {
	if !utf8.ValidString(query) || query == "" {
		return nil, errors.New("zenbun: query must be nonempty UTF-8")
	}
	all := terms(query)
	var queryTerms []string
	for _, term := range all {
		if strings.HasPrefix(term, "b") {
			queryTerms = append(queryTerms, term)
		}
	}
	if len(queryTerms) == 0 {
		queryTerms = all
	}
	m, _, err := i.loadManifest(ctx)
	if err != nil {
		return nil, err
	}
	readers := make([]*segmentReader, len(m.Segments))
	for n, ref := range m.Segments {
		readers[n], err = i.segment(ctx, ref)
		if err != nil {
			return nil, err
		}
	}
	result := make(map[string]bool)
	for n := len(readers) - 1; n >= 0; n-- {
		var candidates map[string]bool
		var documentKeys []string
		for _, term := range queryTerms {
			data, found, err := readers[n].lookup(ctx, "t"+term)
			if err != nil {
				return nil, err
			}
			if !found {
				candidates = nil
				break
			}
			keys, err := readers[n].decodePosting(ctx, data, &documentKeys)
			if err != nil {
				return nil, err
			}
			if candidates == nil {
				candidates = make(map[string]bool, len(keys))
				for _, key := range keys {
					candidates[key] = true
				}
			} else {
				present := make(map[string]bool, len(keys))
				for _, key := range keys {
					present[key] = true
				}
				for key := range candidates {
					if !present[key] {
						delete(candidates, key)
					}
				}
			}
		}
		for key := range candidates {
			shadowed := false
			for newer := n + 1; newer < len(readers); newer++ {
				_, found, err := readDocument(ctx, readers[newer], key)
				if err != nil {
					return nil, err
				}
				if found {
					shadowed = true
					break
				}
			}
			if !shadowed {
				result[key] = true
			}
		}
	}
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func segmentDocuments(ctx context.Context, reader *segmentReader) (map[string]indexedDocument, error) {
	data, found, err := reader.lookup(ctx, "a")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("zenbun: missing segment document list")
	}
	keys, err := decodeList(data)
	if err != nil {
		return nil, err
	}
	if len(keys) != reader.ref.Documents {
		return nil, errors.New("zenbun: invalid segment document count")
	}
	docs := make(map[string]indexedDocument, len(keys))
	for _, key := range keys {
		doc, found, err := readDocument(ctx, reader, key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("zenbun: missing document %q", key)
		}
		docs[key] = doc
	}
	return docs, nil
}

// Compact merges one adjacent pair of similarly sized segments. It preserves
// update order and tombstones; subsequent calls merge additional pairs.
func (i *Index) Compact(ctx context.Context) (bool, error) {
	m, etag, err := i.loadManifest(ctx)
	if err != nil {
		return false, err
	}
	at := -1
	for n := 0; n+1 < len(m.Segments); n++ {
		a, b := m.Segments[n].Size, m.Segments[n+1].Size
		if a <= b && b/8 <= a || b < a && a/8 <= b {
			at = n
			break
		}
	}
	// Highly unequal alternating segment sizes may leave no comparable
	// neighbours. Bound that backlog by merging the least expensive adjacent
	// pair when it grows beyond the normal size tiers.
	if at < 0 && len(m.Segments) > 16 {
		best := ^uint64(0)
		for n := 0; n+1 < len(m.Segments); n++ {
			cost := uint64(m.Segments[n].Size) + uint64(m.Segments[n+1].Size)
			if cost < best {
				at, best = n, cost
			}
		}
	}
	if at < 0 {
		return false, nil
	}
	docs := make(map[string]indexedDocument)
	for _, ref := range m.Segments[at : at+2] {
		reader, err := i.segment(ctx, ref)
		if err != nil {
			return false, err
		}
		part, err := segmentDocuments(ctx, reader)
		if err != nil {
			return false, err
		}
		for key, doc := range part {
			docs[key] = doc
		}
	}
	// At the oldest edge there is no earlier version to suppress, so those
	// tombstones can be removed. Elsewhere they must survive the merge.
	if at == 0 {
		for key, doc := range docs {
			if doc.Deleted {
				delete(docs, key)
			}
		}
	}
	segments := make([]segmentRef, 0, len(m.Segments)-1)
	segments = append(segments, m.Segments[:at]...)
	if len(docs) != 0 {
		ref, err := i.writeSegment(ctx, docs)
		if err != nil {
			return false, err
		}
		segments = append(segments, ref)
	}
	segments = append(segments, m.Segments[at+2:]...)
	m.Segments = segments
	if err := i.publish(ctx, m, etag); err != nil {
		return false, err
	}
	return true, nil
}

// Reclaim discovers unreferenced segments and deletes them only after a later
// call observes a retirement marker older than grace. Grace must exceed the
// maximum reader duration plus clock skew and storage-listing delay.
// The conditional manifest barrier prevents stale writers from publishing
// objects discovered during reclamation. Storage must provide strong listing
// and conditional-write consistency.
func (i *Index) Reclaim(ctx context.Context, grace time.Duration) (int, error) {
	if grace <= 0 {
		return 0, errors.New("zenbun: reclaim grace must be positive")
	}
	m, etag, err := i.loadManifest(ctx)
	if err != nil {
		return 0, err
	}
	// Discover before the barrier. A writer that has already created an object
	// must publish before our CAS (which makes it fail), or its own stale CAS
	// will fail. Writers observing the barrier cannot appear in this listing.
	objects, err := i.store.List(ctx, i.prefix+"segments/")
	if err != nil {
		return 0, err
	}
	if err := i.publish(ctx, m, etag); err != nil {
		return 0, err
	}
	markers, err := i.store.List(ctx, i.prefix+"retired/")
	if err != nil {
		return 0, err
	}
	live := make(map[string]bool, len(m.Segments))
	for _, ref := range m.Segments {
		live[ref.Key] = true
	}
	markerByKey := make(map[string]ObjectInfo, len(markers))
	for _, marker := range markers {
		markerByKey[marker.Key] = marker
	}
	present := make(map[string]bool, len(objects))
	cutoff := time.Now().Add(-grace)
	removed := 0
	for _, obj := range objects {
		id := strings.TrimPrefix(obj.Key, i.prefix+"segments/")
		if id == "" || strings.Contains(id, "/") {
			continue
		}
		markerKey := i.prefix + "retired/" + id
		present[markerKey] = true
		if live[obj.Key] {
			continue
		}
		marker, exists := markerByKey[markerKey]
		if !exists {
			_, err := i.store.Put(ctx, markerKey, []byte(obj.Key), PutOptions{IfNoneMatch: true})
			if err != nil && !errors.Is(err, ErrConflict) {
				return removed, err
			}
			continue
		}
		if !marker.LastModified.IsZero() && marker.LastModified.Before(cutoff) {
			if err := i.store.Delete(ctx, obj.Key); err != nil {
				return removed, err
			}
			removed++
			if err := i.store.Delete(ctx, markerKey); err != nil {
				return removed, err
			}
		}
	}
	for _, marker := range markers {
		if !present[marker.Key] && !marker.LastModified.IsZero() && marker.LastModified.Before(cutoff) {
			if err := i.store.Delete(ctx, marker.Key); err != nil {
				return removed, err
			}
		}
	}
	return removed, nil
}
