package zenbun

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	manifestVersion = 1
	// maxCachedSegments bounds the segment readers an Index keeps; the cache
	// is cleared when it fills.
	maxCachedSegments = 1024
)

// Index stores immutable segments under a prefix, separate from source documents.
//
// An Index is safe for concurrent use by multiple goroutines, and several
// Index values, in one or many processes, may share a prefix: writers publish
// through a conditional manifest write and see ErrConflict when they lose a
// race. Use RetryConflict to retry a whole operation after a conflict.
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
	if i.cache == nil || len(i.cache) >= maxCachedSegments {
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
	_, err := i.publishWithETag(ctx, m, etag)
	return err
}

func (i *Index) publishWithETag(ctx context.Context, m manifest, etag string) (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	m.Version = manifestVersion
	m.Generation = id
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if len(data) > maxMetadataSize {
		return "", errors.New("zenbun: manifest exceeds size limit")
	}
	options := PutOptions{IfNoneMatch: etag == "", IfMatch: etag}
	return i.store.Put(ctx, i.prefix+"manifest", data, options)
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
		data := change.Data
		if change.Delete {
			data = nil // A deletion has no content to check.
		}
		if !validDocument(change.Key, data) {
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
	for n := range attempts {
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

// decodeDocumentBlock validates one document-table block. Blocks are records
// whose first key matches the index.
func (r *segmentReader) decodeDocumentBlock(block int, data []byte) ([]documentRecord, error) {
	expected := min(documentBlockSize, r.ref.Documents-block*documentBlockSize)
	invalid := errors.New("zenbun: invalid segment document block")
	if expected <= 0 {
		return nil, invalid
	}
	var records []documentRecord
	if err := json.Unmarshal(data, &records); err != nil || len(records) != expected || block >= len(r.docFirst) || records[0].Key != r.docFirst[block] {
		return nil, invalid
	}
	for n, record := range records {
		if !utf8.ValidString(record.Key) || !utf8.ValidString(record.ETag) || n > 0 && records[n-1].Key >= record.Key {
			return nil, invalid
		}
	}
	if block+1 < len(r.docFirst) && records[len(records)-1].Key >= r.docFirst[block+1] {
		return nil, invalid
	}
	return records, nil
}

// documentBlock reads only IDs in the requested fixed-size block. Cached
// entries are bounded by their encoded byte size per immutable segment.
func (r *segmentReader) documentBlock(ctx context.Context, block int) ([]documentRecord, error) {
	r.docMu.Lock()
	cached := r.docBlocks[block]
	r.docMu.Unlock()
	if cached != nil {
		return cached, nil
	}
	data, found, err := r.lookup(ctx, fmt.Sprintf("d%08x", block))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("zenbun: missing segment document block")
	}
	keys, err := r.decodeDocumentBlock(block, data)
	if err != nil {
		return nil, err
	}
	return r.cacheDocumentBlock(block, keys, len(data)), nil
}

// cacheDocumentBlock stores records decoded from size bytes unless the cache
// is full. It returns the cached records if another caller stored them first.
func (r *segmentReader) cacheDocumentBlock(block int, records []documentRecord, size int) []documentRecord {
	r.docMu.Lock()
	defer r.docMu.Unlock()
	if cached := r.docBlocks[block]; cached != nil {
		return cached
	}
	if size <= maxDocumentCacheBytes-r.docBytes {
		if r.docBlocks == nil {
			r.docBlocks = make(map[int][]documentRecord)
		}
		r.docBlocks[block] = records
		r.docBytes += size
	}
	return records
}

// documentBlockBatch coalesces adjacent document blocks while bounding each read.
// A single block may exceed the batch target but remains limited by maxMetadataSize.
// In v3 a dictionary block may sit between two document blocks; the gap is
// read along with them.
func (r *segmentReader) documentBlockBatch(ctx context.Context, first, last int) ([][]documentRecord, error) {
	if first == last {
		keys, err := r.documentBlock(ctx, first)
		return [][]documentRecord{keys}, err
	}
	blocks := make([][]documentRecord, last-first+1)
	r.docMu.Lock()
	allCached := true
	for n := first; n <= last; n++ {
		blocks[n-first] = r.docBlocks[n]
		allCached = allCached && blocks[n-first] != nil
	}
	r.docMu.Unlock()
	if allCached {
		return blocks, nil
	}

	entries := make([]dictionaryEntry, 0, last-first+1)
	dictionaries := make(map[int][]dictionaryEntry)
	for block := first; block <= last; block++ {
		term := fmt.Sprintf("d%08x", block)
		index := sort.Search(len(r.blocks), func(i int) bool { return r.blocks[i].First > term }) - 1
		if index < 0 {
			return nil, errors.New("zenbun: missing segment document block")
		}
		dictionary, ok := dictionaries[index]
		if !ok {
			var err error
			dictionary, err = r.dictionary(ctx, index)
			if err != nil {
				return nil, err
			}
			dictionaries[index] = dictionary
		}
		i := sort.Search(len(dictionary), func(i int) bool { return dictionary[i].Term >= term })
		if i == len(dictionary) || dictionary[i].Term != term {
			return nil, errors.New("zenbun: missing segment document block")
		}
		entries = append(entries, dictionary[i])
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Offset < entries[i-1].Offset+entries[i-1].Length {
			return nil, errors.New("zenbun: unordered segment document blocks")
		}
	}
	start := entries[0].Offset
	end := entries[len(entries)-1].Offset + entries[len(entries)-1].Length
	const maxBatchBytes = 1 << 20
	if end-start > maxBatchBytes {
		mid := first + (last-first)/2
		left, err := r.documentBlockBatch(ctx, first, mid)
		if err != nil {
			return nil, err
		}
		right, err := r.documentBlockBatch(ctx, mid+1, last)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	}
	data, err := readRange(ctx, r.store, r.ref.Key, start, end-start)
	if err != nil {
		return nil, err
	}
	for n, entry := range entries {
		value := data[entry.Offset-start : entry.Offset-start+entry.Length]
		keys, err := r.decodeDocumentBlock(first+n, value)
		if err != nil {
			return nil, err
		}
		blocks[n] = r.cacheDocumentBlock(first+n, keys, len(value))
	}
	return blocks, nil
}

// validatePosting checks delta-encoded IDs, including overflow and truncation,
// and calls fn with each ID in increasing order.
func validatePosting(data []byte, documents int, fn func(uint64)) error {
	if len(data) == 0 || documents < 1 {
		return errors.New("zenbun: empty segment posting")
	}
	var id uint64
	for remaining := data; len(remaining) > 0; {
		delta, n := binary.Uvarint(remaining)
		if n <= 0 || delta == 0 || delta > uint64(documents)-id {
			return errors.New("zenbun: invalid segment posting")
		}
		id += delta
		remaining = remaining[n:]
		if fn != nil {
			fn(id)
		}
	}
	return nil
}

// decodePosting resolves document IDs without reading the complete key table.
func (r *segmentReader) decodePosting(ctx context.Context, data []byte) ([]string, error) {
	// Validate all IDs before loading any blocks so malformed entries fail
	// without partial results.
	if err := validatePosting(data, r.ref.Documents, nil); err != nil {
		return nil, err
	}
	var keys []string
	var id uint64
	for len(data) > 0 {
		first := -1
		last := -1
		remaining := data
		nextID := id
		for len(remaining) > 0 {
			delta, n := binary.Uvarint(remaining)
			block := int((nextID + delta - 1) / documentBlockSize)
			if first < 0 {
				first = block
			}
			if last >= 0 && (block > last+1 || block-first >= documentBatchBlocks) {
				break
			}
			last = block
			nextID += delta
			remaining = remaining[n:]
		}
		blocks, err := r.documentBlockBatch(ctx, first, last)
		if err != nil {
			return nil, err
		}
		consumed := len(data) - len(remaining)
		for processed := data[:consumed]; len(processed) > 0; {
			delta, n := binary.Uvarint(processed)
			id += delta
			block := int((id - 1) / documentBlockSize)
			keys = append(keys, blocks[block-first][(id-1)%documentBlockSize].Key)
			processed = processed[n:]
		}
		data = remaining
	}
	return keys, nil
}

// hasDocument reports whether the segment holds a record, live or deleted,
// for key. It answers from the document table without reading records.
func (r *segmentReader) hasDocument(ctx context.Context, key string) (bool, error) {
	if !bloomContains(r.bloom, key) {
		return false, nil
	}
	block := sort.Search(len(r.docFirst), func(i int) bool { return r.docFirst[i] > key }) - 1
	if block < 0 {
		return false, nil
	}
	records, err := r.documentBlock(ctx, block)
	if err != nil {
		return false, err
	}
	i := sort.Search(len(records), func(i int) bool { return records[i].Key >= key })
	return i < len(records) && records[i].Key == key, nil
}

// Search returns candidate document keys. It may include false positives;
// callers should read and verify source documents when exact matches matter.
// The query must be nonempty UTF-8 of at most 256 bytes, the same word limit
// as SearchQuery; matching uses NFKC, lowercase and character grams.
func (i *Index) Search(ctx context.Context, query string) ([]string, error) {
	if query == "" || len(query) > maxQueryWordBytes || !utf8.ValidString(query) {
		return nil, fmt.Errorf("zenbun: query must be nonempty UTF-8 up to %d bytes", maxQueryWordBytes)
	}
	return i.searchQuery(ctx, Query{All: [][]string{{query}}})
}

// SearchQuery returns candidate keys for a structured query in key order. Like
// Search it may include false positives, and None is not applied: the index
// cannot prove that a document lacks a word. Use Find to verify bodies.
func (i *Index) SearchQuery(ctx context.Context, q Query) ([]string, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	return i.searchQuery(ctx, q)
}

func (i *Index) searchQuery(ctx context.Context, q Query) ([]string, error) {
	groups := q.indexGroups()
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
	// Different immutable segments can be searched independently. Keep the
	// number of concurrent reads bounded even when a manifest grows large.
	const maxSearchWorkers = 8
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, len(readers))
	for n := range readers {
		jobs <- n
	}
	close(jobs)
	matches := make([][]string, len(readers))
	var wg sync.WaitGroup
	var firstError error
	var errorOnce sync.Once
	for worker := 0; worker < min(maxSearchWorkers, len(readers)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				if workCtx.Err() != nil {
					return
				}
				keys, err := searchSegment(workCtx, readers, n, groups, q.KeyPrefix)
				if err != nil {
					errorOnce.Do(func() { firstError = err; cancel() })
					return
				}
				matches[n] = keys
			}
		}()
	}
	wg.Wait()
	if firstError != nil {
		return nil, firstError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	for _, keys := range matches {
		for _, key := range keys {
			result[key] = true
		}
	}
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

// searchSegment intersects groups; each group is the union of its words, and
// each word requires every one of its index terms.
func searchSegment(ctx context.Context, readers []*segmentReader, n int, groups [][][]string, keyPrefix string) ([]string, error) {
	var candidates map[string]bool
	for _, group := range groups {
		union := make(map[string]bool)
		for _, wordTerms := range group {
			keys, err := segmentPostings(ctx, readers[n], wordTerms, keyPrefix)
			if err != nil {
				return nil, err
			}
			for key := range keys {
				if candidates == nil || candidates[key] {
					union[key] = true
				}
			}
		}
		candidates = union
		if len(candidates) == 0 {
			return nil, nil
		}
	}
	result := make([]string, 0, len(candidates))
	for key := range candidates {
		shadowed := false
		for newer := n + 1; newer < len(readers); newer++ {
			found, err := readers[newer].hasDocument(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				shadowed = true
				break
			}
		}
		if !shadowed {
			result = append(result, key)
		}
	}
	return result, nil
}

// segmentPostings returns keys under keyPrefix whose postings contain every term.
func segmentPostings(ctx context.Context, reader *segmentReader, queryTerms []string, keyPrefix string) (map[string]bool, error) {
	var candidates map[string]bool
	for _, term := range queryTerms {
		data, found, err := reader.lookup(ctx, "t"+term)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		keys, err := reader.decodePosting(ctx, data)
		if err != nil {
			return nil, err
		}
		if candidates == nil {
			candidates = make(map[string]bool, len(keys))
			for _, key := range keys {
				if strings.HasPrefix(key, keyPrefix) {
					candidates[key] = true
				}
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
		if len(candidates) == 0 {
			return nil, nil
		}
	}
	return candidates, nil
}

// segmentSnapshot presents the document and metadata ranges without copying
// them into a second in-memory store. The postings gap is deliberately absent.
type segmentSnapshot struct {
	Store
	key              string
	head, tail       []byte
	tailOffset, size int64
}

func (s *segmentSnapshot) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if key != s.key || r == nil || r.Start < 0 || r.End <= r.Start || r.End > s.size {
		return Object{}, ErrInvalidRange
	}
	if r.End <= int64(len(s.head)) {
		return Object{Data: s.head[r.Start:r.End]}, nil
	}
	if r.Start >= s.tailOffset {
		return Object{Data: s.tail[r.Start-s.tailOffset : r.End-s.tailOffset]}, nil
	}
	return Object{}, io.ErrUnexpectedEOF
}

// segmentDocumentTable reads the document table, which holds deletion state
// and ETags but no terms, in batched ranges.
func segmentDocumentTable(ctx context.Context, reader *segmentReader) (map[string]indexedDocument, error) {
	docs := make(map[string]indexedDocument, reader.ref.Documents)
	count := len(reader.docFirst)
	for first := 0; first < count; first += documentBatchBlocks {
		blocks, err := reader.documentBlockBatch(ctx, first, min(first+documentBatchBlocks, count)-1)
		if err != nil {
			return nil, err
		}
		for _, block := range blocks {
			for _, record := range block {
				docs[record.Key] = indexedDocument{Deleted: record.Deleted, ETag: record.ETag}
			}
		}
	}
	if len(docs) != reader.ref.Documents {
		return nil, errors.New("zenbun: invalid segment document count")
	}
	return docs, nil
}

// segmentContents rebuilds each v3 document's terms from the postings, since
// v3 records do not store them. It reads the segment once; the result is as
// large as the segment's postings. Issue #66 replaces this with a streaming
// merge of postings.
func segmentContents(ctx context.Context, reader *segmentReader) (map[string]indexedDocument, error) {
	data, err := readRange(ctx, reader.store, reader.ref.Key, 0, reader.ref.Size)
	if err != nil {
		return nil, err
	}
	snapshot := &segmentSnapshot{Store: reader.store, key: reader.ref.Key, head: data, tailOffset: reader.ref.Size, size: reader.ref.Size}
	reader, err = openSegment(ctx, snapshot, reader.ref)
	if err != nil {
		return nil, err
	}
	records := make([]documentRecord, 0, reader.ref.Documents)
	for block := range reader.docFirst {
		part, err := reader.documentBlock(ctx, block)
		if err != nil {
			return nil, err
		}
		records = append(records, part...)
	}
	if len(records) != reader.ref.Documents {
		return nil, errors.New("zenbun: invalid segment document count")
	}
	termsByID := make([][]string, len(records))
	for block := range reader.blocks {
		entries, err := reader.dictionary(ctx, block)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Term, "t") {
				continue
			}
			term := entry.Term[1:]
			err := validatePosting(data[entry.Offset:entry.Offset+entry.Length], len(records), func(id uint64) {
				termsByID[id-1] = append(termsByID[id-1], term)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	docs := make(map[string]indexedDocument, len(records))
	for n, record := range records {
		docs[record.Key] = indexedDocument{Deleted: record.Deleted, ETag: record.ETag, Terms: termsByID[n]}
	}
	return docs, nil
}

// Compact merges one adjacent pair of similarly sized segments. It preserves
// update order and tombstones; subsequent calls merge additional pairs. It
// reports whether a pair was merged; false means no pair needs merging.
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
		part, err := segmentContents(ctx, reader)
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
// and conditional-write consistency. It returns the number of segments
// deleted by this call.
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
