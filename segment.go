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

const segmentVersion = 2
const legacySegmentVersion = 1
const footerSize = 32
const dictionaryBlockSize = 64
const maxMetadataSize = 64 << 20
const maxCachedDictionaryBytes = 128 << 10

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

type segmentIndex struct {
	Blocks    []blockIndex `json:"blocks"`
	Bloom     []byte       `json:"bloom"`
	BloomHash string       `json:"bloom_hash"`
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
// a sparse index and a fixed-size footer. Version 2 stores delta-encoded IDs.
func encodeSegment(docs map[string]indexedDocument) ([]byte, error) {
	return encodeSegmentVersion(docs, segmentVersion)
}

func encodeSegmentVersion(docs map[string]indexedDocument, version uint32) ([]byte, error) {
	if version != legacySegmentVersion && version != segmentVersion {
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
	index, err := json.Marshal(segmentIndex{Blocks: blocks, Bloom: bloom, BloomHash: hex.EncodeToString(sum[:])})
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

func validExtent(offset, length, limit int64) bool {
	return offset >= 0 && length > 0 && offset <= limit && length <= limit-offset
}

func parseFooter(footer []byte, size int64) (int64, int64, error) {
	if len(footer) != footerSize || !bytes.Equal(footer[:8], segmentMagic[:]) {
		return 0, 0, errors.New("zenbun: invalid segment footer")
	}
	if v := binary.BigEndian.Uint32(footer[8:12]); v != legacySegmentVersion && v != segmentVersion {
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
	docKeys     []string
	version     uint32
	store       Store
	ref         segmentRef
	blocks      []blockIndex
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
	blocks := index.Blocks
	for i, block := range blocks {
		if !utf8.ValidString(block.First) || !validExtent(block.Offset, block.Length, offset) || block.Length > maxMetadataSize || i > 0 && blocks[i-1].First >= block.First {
			return nil, errors.New("zenbun: invalid dictionary block")
		}
	}
	return &segmentReader{version: binary.BigEndian.Uint32(footer[8:12]), store: store, ref: ref, blocks: blocks, bloom: index.Bloom, indexOffset: offset}, nil
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

// dictionary caches validated immutable blocks up to a bounded size per reader.
func (s *segmentReader) dictionary(ctx context.Context, index int) ([]dictionaryEntry, error) {
	s.dictMu.Lock()
	defer s.dictMu.Unlock()
	if entries, found := s.dictCache[index]; found {
		return entries, nil
	}
	block := s.blocks[index]
	data, err := readRange(ctx, s.store, s.ref.Key, block.Offset, block.Length)
	if err != nil {
		return nil, err
	}
	var entries []dictionaryEntry
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) == 0 || entries[0].Term != block.First {
		return nil, errors.New("zenbun: invalid dictionary")
	}
	for i, entry := range entries {
		if !utf8.ValidString(entry.Term) || !validExtent(entry.Offset, entry.Length, s.indexOffset) || entry.Length > maxMetadataSize || i > 0 && entries[i-1].Term >= entry.Term {
			return nil, errors.New("zenbun: invalid dictionary entry")
		}
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
