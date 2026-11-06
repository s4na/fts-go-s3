package zenbun

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func openEncoded(t *testing.T, data []byte, documents int) *segmentReader {
	t.Helper()
	store := &MemoryStore{}
	ctx := context.Background()
	if _, err := store.Put(ctx, "segment", data, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	reader, err := openSegment(ctx, store, segmentRef{Key: "segment", Size: int64(len(data)), Documents: documents})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

// TestVersionThreeRoundTrip covers more than one document block, so the
// document table has dictionary blocks between its blocks.
func TestVersionThreeRoundTrip(t *testing.T) {
	ctx := context.Background()
	const count = documentBlockSize*2 + 3
	docs := make(map[string]indexedDocument, count)
	for n := 0; n < count; n++ {
		key := fmt.Sprintf("docs/%05d", n)
		switch {
		case n%7 == 0:
			docs[key] = indexedDocument{Deleted: true, ETag: fmt.Sprintf("e%d", n)}
		case n%2 == 0:
			docs[key] = indexedDocument{Terms: terms(fmt.Sprintf("猫%d", n%10)), ETag: fmt.Sprintf("e%d", n)}
		default:
			docs[key] = indexedDocument{Terms: terms(fmt.Sprintf("犬%d", n%10))}
		}
	}
	data, err := encodeSegment(docs)
	if err != nil {
		t.Fatal(err)
	}
	reader := openEncoded(t, data, count)
	if len(reader.docFirst) != 3 {
		t.Fatalf("doc_first=%d", len(reader.docFirst))
	}
	for _, name := range []string{"a", "kdocs/00000"} {
		if _, found, err := reader.lookup(ctx, name); err != nil || found {
			t.Fatalf("v3 must not write %q: found=%t err=%v", name, found, err)
		}
	}
	for _, key := range []string{"docs/00000", "docs/00256", "docs/00514", "docs/00001"} {
		if found, err := reader.hasDocument(ctx, key); err != nil || !found {
			t.Fatalf("hasDocument(%q) = %t, %v", key, found, err)
		}
	}
	for _, key := range []string{"", "docs/", "docs/00515", "zzz"} {
		if found, err := reader.hasDocument(ctx, key); err != nil || found {
			t.Fatalf("hasDocument(%q) = %t, %v", key, found, err)
		}
	}
	state, err := segmentDocuments(ctx, reader, false)
	if err != nil {
		t.Fatal(err)
	}
	full, err := segmentDocuments(ctx, openEncoded(t, data, count), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != count || len(full) != count {
		t.Fatalf("documents: state=%d full=%d want %d", len(state), len(full), count)
	}
	for key, want := range docs {
		got := full[key]
		if want.Deleted {
			want.Terms = nil
		}
		if len(want.Terms) == 0 {
			want.Terms = nil
		}
		sort.Strings(got.Terms)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("full[%q] = %+v; want %+v", key, got, want)
		}
		if s := state[key]; s.Terms != nil || s.Deleted != want.Deleted || s.ETag != want.ETag {
			t.Fatalf("state[%q] = %+v", key, s)
		}
	}
	data, found, err := reader.lookup(ctx, "t"+terms("猫2")[0])
	if err != nil || !found {
		t.Fatalf("posting: %v %v", found, err)
	}
	keys, err := reader.decodePosting(ctx, data)
	if err != nil || len(keys) == 0 {
		t.Fatalf("decode posting: %v %v", keys, err)
	}
}

func TestSegmentWriterRejectsInvalidOrder(t *testing.T) {
	tests := map[string]func(w *segmentWriter) error{
		"unsorted documents": func(w *segmentWriter) error {
			if err := w.addDocument(documentRecord{Key: "b"}); err != nil {
				return nil
			}
			return w.addDocument(documentRecord{Key: "a"})
		},
		"duplicate document": func(w *segmentWriter) error {
			if err := w.addDocument(documentRecord{Key: "a"}); err != nil {
				return nil
			}
			return w.addDocument(documentRecord{Key: "a"})
		},
		"too many documents": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			_ = w.addDocument(documentRecord{Key: "b"})
			return w.addDocument(documentRecord{Key: "c"})
		},
		"posting before documents": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			return w.addPosting("ux", []uint64{1})
		},
		"unsorted postings": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			_ = w.addDocument(documentRecord{Key: "b"})
			if err := w.addPosting("uy", []uint64{1}); err != nil {
				return nil
			}
			return w.addPosting("ux", []uint64{1})
		},
		"unsorted IDs": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			_ = w.addDocument(documentRecord{Key: "b"})
			return w.addPosting("ux", []uint64{2, 1})
		},
		"ID out of range": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			_ = w.addDocument(documentRecord{Key: "b"})
			return w.addPosting("ux", []uint64{3})
		},
		"empty posting": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			_ = w.addDocument(documentRecord{Key: "b"})
			return w.addPosting("ux", nil)
		},
		"missing documents": func(w *segmentWriter) error {
			_ = w.addDocument(documentRecord{Key: "a"})
			return w.finish()
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(newSegmentWriter(&bytes.Buffer{}, 2)); err == nil {
				t.Fatal("writer accepted invalid input")
			}
		})
	}
	if err := newSegmentWriter(&bytes.Buffer{}, 0).finish(); err == nil {
		t.Fatal("writer accepted an empty segment")
	}
}

func TestVersionThreeRejectsBadDocumentTable(t *testing.T) {
	ctx := context.Background()
	data, err := encodeSegment(map[string]indexedDocument{"a": {Terms: terms("猫")}, "b": {Terms: terms("犬")}})
	if err != nil {
		t.Fatal(err)
	}
	store := &MemoryStore{}
	if _, err := store.Put(ctx, "segment", data, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, documents := range []int{0, documentBlockSize + 1} {
		if _, err := openSegment(ctx, store, segmentRef{Key: "segment", Size: int64(len(data)), Documents: documents}); err == nil {
			t.Fatalf("accepted document count %d", documents)
		}
	}
	reader := openEncoded(t, data, 2)
	for _, block := range []string{`[{"key":"b"},{"key":"a"}]`, `[{"key":"x"},{"key":"y"}]`, `[{"key":"a"}]`, `["a","b"]`} {
		if _, err := reader.decodeDocumentBlock(0, []byte(block)); err == nil {
			t.Fatalf("accepted document block %s", block)
		}
	}
}
