package zenbun

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// segmentVersion is the format written by this library. Versions 1 and 2
// remain readable and are replaced by version 3 when compacted.
const segmentVersion = 3
const v2SegmentVersion = 2
const legacySegmentVersion = 1
const footerSize = 32
const dictionaryBlockSize = 64
const maxCachedDictionaryBytes = 128 << 10
const maxMetadataSize = 64 << 20
const documentBlockSize = 256
const maxDocumentCacheBytes = 64 << 10

var segmentMagic = [8]byte{'Z', 'E', 'N', 'B', 'U', 'N', '0', '1'}

type indexedDocument struct {
	Deleted bool     `json:"deleted,omitempty"`
	Terms   []string `json:"terms,omitempty"`
	ETag    string   `json:"etag,omitempty"`
}

type dictionaryEntry struct {
	Term   string `json:"term"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

type blockIndex struct {
	First  string `json:"first"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// documentRecord is one row of the v3 document table. Its position in key
// order, starting at 1, is the document ID used by postings.
type documentRecord struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted,omitempty"`
	ETag    string `json:"etag,omitempty"`
}

type segmentIndex struct {
	DocBlockSize int          `json:"doc_block_size,omitempty"`
	DocFirst     []string     `json:"doc_first,omitempty"`
	Blocks       []blockIndex `json:"blocks"`
	Bloom        []byte       `json:"bloom"`
	BloomHash    string       `json:"bloom_hash"`
}

func bloomPositions(key string, size int) [3]uint64 {
	hash := sha256.Sum256([]byte(key))
	bits := uint64(size) * 8
	return [3]uint64{
		binary.BigEndian.Uint64(hash[0:8]) % bits,
		binary.BigEndian.Uint64(hash[8:16]) % bits,
		binary.BigEndian.Uint64(hash[16:24]) % bits,
	}
}

func bloomAdd(bloom []byte, key string) {
	for _, bit := range bloomPositions(key, len(bloom)) {
		bloom[bit/8] |= 1 << (bit % 8)
	}
}

func bloomContains(bloom []byte, key string) bool {
	for _, bit := range bloomPositions(key, len(bloom)) {
		if bloom[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func terms(text string) []string {
	runes := []rune(strings.ToLower(norm.NFKC.String(text)))
	set := make(map[string]struct{})
	for i, r := range runes {
		set["u"+string(r)] = struct{}{}
		if i+1 < len(runes) {
			set["b"+string(runes[i:i+2])] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for term := range set {
		out = append(out, term)
	}
	sort.Strings(out)
	return out
}

// A segment contains a sorted document table, posting lists, dictionary blocks,
// a sparse index and a fixed-size footer. Version 2 and later store
// delta-encoded IDs; version 3 drops the per-document term lists.
func encodeSegment(docs map[string]indexedDocument) ([]byte, error) {
	return encodeSegmentVersion(docs, segmentVersion)
}

func encodeSegmentVersion(docs map[string]indexedDocument, version uint32) ([]byte, error) {
	if version == segmentVersion {
		return encodeSegmentV3(docs)
	}
	if version != legacySegmentVersion && version != v2SegmentVersion {
		return nil, errors.New("zenbun: unsupported segment version")
	}
	entries := make(map[string][]byte)
	keys := make([]string, 0, len(docs))
	posting := make(map[string][]uint64)
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for n, key := range keys {
		doc := docs[key]
		data, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		entries["k"+key] = data
		if !doc.Deleted {
			for _, term := range doc.Terms {
				posting[term] = append(posting[term], uint64(n+1))
			}
		}
	}
	list, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	entries["a"] = list
	if version == v2SegmentVersion {
		for start := 0; start < len(keys); start += documentBlockSize {
			block, err := json.Marshal(keys[start:min(start+documentBlockSize, len(keys))])
			if err != nil {
				return nil, err
			}
			entries[fmt.Sprintf("d%08x", start/documentBlockSize)] = block
		}
	}
	for term, ids := range posting {
		var data []byte
		if version == legacySegmentVersion {
			legacyKeys := make([]string, 0, len(ids))
			for _, id := range ids {
				legacyKeys = append(legacyKeys, keys[id-1])
			}
			data, err = json.Marshal(legacyKeys)
			if err != nil {
				return nil, err
			}
		} else {
			var previous uint64
			for _, id := range ids {
				data = binary.AppendUvarint(data, id-previous)
				previous = id
			}
		}
		entries["t"+term] = data
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var out bytes.Buffer
	dict := make([]dictionaryEntry, 0, len(names))
	for _, name := range names {
		data := entries[name]
		if len(data) > maxMetadataSize {
			return nil, errors.New("zenbun: segment entry exceeds size limit")
		}
		dict = append(dict, dictionaryEntry{Term: name, Offset: int64(out.Len()), Length: int64(len(data))})
		_, _ = out.Write(data)
	}
	var blocks []blockIndex
	for start := 0; start < len(dict); start += dictionaryBlockSize {
		end := start + dictionaryBlockSize
		if end > len(dict) {
			end = len(dict)
		}
		data, err := json.Marshal(dict[start:end])
		if err != nil {
			return nil, err
		}
		if len(data) > maxMetadataSize {
			return nil, errors.New("zenbun: dictionary block exceeds size limit")
		}
		blocks = append(blocks, blockIndex{First: dict[start].Term, Offset: int64(out.Len()), Length: int64(len(data))})
		_, _ = out.Write(data)
	}
	bloom := make([]byte, max(8, len(keys)*2))
	for _, key := range keys {
		bloomAdd(bloom, key)
	}
	sum := sha256.Sum256(bloom)
	indexData := segmentIndex{Blocks: blocks, Bloom: bloom, BloomHash: hex.EncodeToString(sum[:])}
	if version == v2SegmentVersion {
		indexData.DocBlockSize = documentBlockSize
	}
	index, err := json.Marshal(indexData)
	if err != nil {
		return nil, err
	}
	if len(index) > maxMetadataSize {
		return nil, errors.New("zenbun: segment index exceeds size limit")
	}
	indexOffset := out.Len()
	_, _ = out.Write(index)
	var footer [footerSize]byte
	copy(footer[:8], segmentMagic[:])
	binary.BigEndian.PutUint32(footer[8:12], version)
	binary.BigEndian.PutUint64(footer[16:24], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[24:32], uint64(len(index)))
	_, _ = out.Write(footer[:])
	return out.Bytes(), nil
}

// encodeSegmentV3 inverts documents into postings and streams them through a
// segmentWriter. The inversion is the caller's batch; the writer itself keeps
// only the sparse indexes, the Bloom filter and one block of each kind.
func encodeSegmentV3(docs map[string]indexedDocument) ([]byte, error) {
	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	w := newSegmentWriter(&out, len(keys))
	posting := make(map[string][]uint64)
	for n, key := range keys {
		doc := docs[key]
		if err := w.addDocument(documentRecord{Key: key, Deleted: doc.Deleted, ETag: doc.ETag}); err != nil {
			return nil, err
		}
		if !doc.Deleted {
			for _, term := range doc.Terms {
				posting[term] = append(posting[term], uint64(n+1))
			}
		}
	}
	names := make([]string, 0, len(posting))
	for term := range posting {
		names = append(names, term)
	}
	sort.Strings(names)
	for _, term := range names {
		if err := w.addPosting(term, posting[term]); err != nil {
			return nil, err
		}
	}
	if err := w.finish(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// segmentWriter writes a v3 segment in one pass. Documents must be added in
// strictly increasing key order, then postings in strictly increasing term
// order. Each dictionary block is written right after its 64 entries, so the
// writer never holds the whole dictionary.
type segmentWriter struct {
	w         io.Writer
	offset    int64
	documents int
	written   int
	docBlock  []documentRecord
	docFirst  []string
	lastKey   string
	postings  bool
	lastTerm  string
	pending   []dictionaryEntry
	blocks    []blockIndex
	bloom     []byte
	err       error
}

func newSegmentWriter(w io.Writer, documents int) *segmentWriter {
	return &segmentWriter{w: w, documents: documents, bloom: make([]byte, max(8, documents*2))}
}

func (s *segmentWriter) write(data []byte) {
	if s.err != nil {
		return
	}
	n, err := s.w.Write(data)
	s.offset += int64(n)
	s.err = err
}

func (s *segmentWriter) writeEntry(name string, data []byte) {
	if s.err != nil {
		return
	}
	if len(data) == 0 || len(data) > maxMetadataSize {
		s.err = errors.New("zenbun: segment entry exceeds size limit")
		return
	}
	s.pending = append(s.pending, dictionaryEntry{Term: name, Offset: s.offset, Length: int64(len(data))})
	s.write(data)
	if len(s.pending) == dictionaryBlockSize {
		s.flushDictionary()
	}
}

func (s *segmentWriter) flushDictionary() {
	if s.err != nil || len(s.pending) == 0 {
		return
	}
	data, err := json.Marshal(s.pending)
	if err != nil {
		s.err = err
		return
	}
	if len(data) > maxMetadataSize {
		s.err = errors.New("zenbun: dictionary block exceeds size limit")
		return
	}
	s.blocks = append(s.blocks, blockIndex{First: s.pending[0].Term, Offset: s.offset, Length: int64(len(data))})
	s.write(data)
	s.pending = s.pending[:0]
}

func (s *segmentWriter) flushDocuments() {
	if s.err != nil || len(s.docBlock) == 0 {
		return
	}
	data, err := json.Marshal(s.docBlock)
	if err != nil {
		s.err = err
		return
	}
	block := (s.written - 1) / documentBlockSize
	s.docFirst = append(s.docFirst, s.docBlock[0].Key)
	s.writeEntry(fmt.Sprintf("d%08x", block), data)
	s.docBlock = s.docBlock[:0]
}

func (s *segmentWriter) addDocument(record documentRecord) error {
	if s.err != nil {
		return s.err
	}
	if s.postings || s.written == s.documents || !utf8.ValidString(record.Key) || s.written > 0 && record.Key <= s.lastKey {
		return errors.New("zenbun: documents must be unique, sorted and precede postings")
	}
	s.written++
	s.lastKey = record.Key
	bloomAdd(s.bloom, record.Key)
	s.docBlock = append(s.docBlock, record)
	if len(s.docBlock) == documentBlockSize {
		s.flushDocuments()
	}
	return s.err
}

// addPosting writes the sorted, 1-based document IDs that contain term.
func (s *segmentWriter) addPosting(term string, ids []uint64) error {
	if s.err != nil {
		return s.err
	}
	if !s.postings {
		if s.written != s.documents {
			return errors.New("zenbun: postings must follow every document")
		}
		s.flushDocuments()
		s.postings = true
	} else if term <= s.lastTerm {
		return errors.New("zenbun: postings must be sorted by term")
	}
	if !utf8.ValidString(term) || len(ids) == 0 {
		return errors.New("zenbun: invalid posting")
	}
	s.lastTerm = term
	var data []byte
	var previous uint64
	for _, id := range ids {
		if id <= previous || id > uint64(s.documents) {
			return errors.New("zenbun: posting IDs must be sorted document IDs")
		}
		data = binary.AppendUvarint(data, id-previous)
		previous = id
	}
	s.writeEntry("t"+term, data)
	return s.err
}

func (s *segmentWriter) finish() error {
	if s.err != nil {
		return s.err
	}
	if s.documents == 0 || s.written != s.documents {
		return errors.New("zenbun: segment document count mismatch")
	}
	s.flushDocuments()
	s.flushDictionary()
	if s.err != nil {
		return s.err
	}
	sum := sha256.Sum256(s.bloom)
	index, err := json.Marshal(segmentIndex{DocBlockSize: documentBlockSize, DocFirst: s.docFirst, Blocks: s.blocks, Bloom: s.bloom, BloomHash: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if len(index) > maxMetadataSize {
		return errors.New("zenbun: segment index exceeds size limit")
	}
	indexOffset := s.offset
	s.write(index)
	var footer [footerSize]byte
	copy(footer[:8], segmentMagic[:])
	binary.BigEndian.PutUint32(footer[8:12], segmentVersion)
	binary.BigEndian.PutUint64(footer[16:24], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[24:32], uint64(len(index)))
	s.write(footer[:])
	return s.err
}

func validExtent(offset, length, limit int64) bool {
	return offset >= 0 && length > 0 && offset <= limit && length <= limit-offset
}

func parseFooter(footer []byte, size int64) (int64, int64, error) {
	if len(footer) != footerSize || !bytes.Equal(footer[:8], segmentMagic[:]) {
		return 0, 0, errors.New("zenbun: invalid segment footer")
	}
	if v := binary.BigEndian.Uint32(footer[8:12]); v < legacySegmentVersion || v > segmentVersion {
		return 0, 0, errors.New("zenbun: unsupported segment version")
	}
	off, length := binary.BigEndian.Uint64(footer[16:24]), binary.BigEndian.Uint64(footer[24:32])
	if off > uint64(^uint64(0)>>1) || length > maxMetadataSize || !validExtent(int64(off), int64(length), size-footerSize) || int64(off)+int64(length) != size-footerSize {
		return 0, 0, errors.New("zenbun: invalid segment index extent")
	}
	return int64(off), int64(length), nil
}

func readRange(ctx context.Context, store Store, key string, start, length int64) ([]byte, error) {
	if !validExtent(start, length, int64(^uint64(0)>>1)) {
		return nil, errors.New("zenbun: invalid segment extent")
	}
	obj, err := store.Get(ctx, key, &ByteRange{Start: start, End: start + length})
	if err != nil {
		return nil, err
	}
	if int64(len(obj.Data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	return obj.Data, nil
}

type segmentReader struct {
	dictMu      sync.Mutex
	dictCache   map[int][]dictionaryEntry
	dictBytes   int
	docMu       sync.Mutex
	docBlocks   map[int][]documentRecord
	docBytes    int
	version     uint32
	store       Store
	ref         segmentRef
	blocks      []blockIndex
	docFirst    []string
	bloom       []byte
	indexOffset int64
}

func openSegment(ctx context.Context, store Store, ref segmentRef) (*segmentReader, error) {
	if ref.Size < footerSize {
		return nil, errors.New("zenbun: short segment")
	}
	footer, err := readRange(ctx, store, ref.Key, ref.Size-footerSize, footerSize)
	if err != nil {
		return nil, err
	}
	offset, length, err := parseFooter(footer, ref.Size)
	if err != nil {
		return nil, err
	}
	data, err := readRange(ctx, store, ref.Key, offset, length)
	if err != nil {
		return nil, err
	}
	var index segmentIndex
	if err := json.Unmarshal(data, &index); err != nil || len(index.Blocks) == 0 || len(index.Bloom) < 8 {
		return nil, errors.New("zenbun: invalid segment index")
	}
	sum := sha256.Sum256(index.Bloom)
	if hex.EncodeToString(sum[:]) != index.BloomHash {
		return nil, errors.New("zenbun: invalid segment bloom checksum")
	}
	version := binary.BigEndian.Uint32(footer[8:12])
	if version >= v2SegmentVersion && index.DocBlockSize != documentBlockSize || version == legacySegmentVersion && index.DocBlockSize != 0 {
		return nil, errors.New("zenbun: unsupported segment document block size")
	}
	if version == segmentVersion {
		if ref.Documents < 1 || len(index.DocFirst) != (ref.Documents+documentBlockSize-1)/documentBlockSize {
			return nil, errors.New("zenbun: invalid segment document table")
		}
		for i, key := range index.DocFirst {
			if !utf8.ValidString(key) || i > 0 && index.DocFirst[i-1] >= key {
				return nil, errors.New("zenbun: invalid segment document table")
			}
		}
	} else if len(index.DocFirst) != 0 {
		return nil, errors.New("zenbun: unexpected segment document table")
	}
	blocks := index.Blocks
	for i, block := range blocks {
		if !utf8.ValidString(block.First) || !validExtent(block.Offset, block.Length, offset) || block.Length > maxMetadataSize || i > 0 && blocks[i-1].First >= block.First {
			return nil, errors.New("zenbun: invalid dictionary block")
		}
	}
	return &segmentReader{version: version, store: store, ref: ref, blocks: blocks, docFirst: index.DocFirst, bloom: index.Bloom, indexOffset: offset}, nil
}

func (s *segmentReader) lookup(ctx context.Context, term string) ([]byte, bool, error) {
	if strings.HasPrefix(term, "k") && !bloomContains(s.bloom, term[1:]) {
		return nil, false, nil
	}
	index := sort.Search(len(s.blocks), func(i int) bool { return s.blocks[i].First > term }) - 1
	if index < 0 {
		return nil, false, nil
	}
	entries, err := s.dictionary(ctx, index)
	if err != nil {
		return nil, false, err
	}
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Term >= term })
	if i == len(entries) || entries[i].Term != term {
		return nil, false, nil
	}
	entry := entries[i]
	data, err := readRange(ctx, s.store, s.ref.Key, entry.Offset, entry.Length)
	if err != nil {
		return nil, false, fmt.Errorf("zenbun: read segment entry: %w", err)
	}
	return data, true, nil
}

// dictionary caches validated immutable blocks up to a bounded JSON byte size per reader.
func (s *segmentReader) dictionary(ctx context.Context, index int) ([]dictionaryEntry, error) {
	s.dictMu.Lock()
	entries, found := s.dictCache[index]
	s.dictMu.Unlock()
	if found {
		return entries, nil
	}
	block := s.blocks[index]
	data, err := readRange(ctx, s.store, s.ref.Key, block.Offset, block.Length)
	if err != nil {
		return nil, err
	}
	entries = nil
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) == 0 || entries[0].Term != block.First {
		return nil, errors.New("zenbun: invalid dictionary")
	}
	for i, entry := range entries {
		if !utf8.ValidString(entry.Term) || !validExtent(entry.Offset, entry.Length, s.indexOffset) || entry.Length > maxMetadataSize || i > 0 && entries[i-1].Term >= entry.Term {
			return nil, errors.New("zenbun: invalid dictionary entry")
		}
	}
	s.dictMu.Lock()
	defer s.dictMu.Unlock()
	// Another caller may have fetched this block while the lock was released.
	if cached, found := s.dictCache[index]; found {
		return cached, nil
	}
	if len(data) <= maxCachedDictionaryBytes-s.dictBytes {
		if s.dictCache == nil {
			s.dictCache = make(map[int][]dictionaryEntry)
		}
		s.dictCache[index] = entries
		s.dictBytes += len(data)
	}
	return entries, nil
}
