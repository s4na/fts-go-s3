package zenbun

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReadLinesFromFindSnippet(t *testing.T) {
	ctx := context.Background()
	index, store := newQueryIndex(t, map[string]string{
		"docs/a": "一行目\r\n二行目\r\n猫の行\r\n四行目\r\n五行目",
	})
	found, err := index.Find(ctx, store, Query{All: [][]string{{"猫"}}}, FindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	line := found.Documents[0].Snippets[0].Line
	result, err := index.Read(ctx, store, "docs/a", ReadOptions{Start: line - 1, End: line + 1})
	want := ReadResult{Key: "docs/a", Start: 2, Text: "二行目\n猫の行\n四行目", Lines: 5}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("read around snippet:\n got %+v %v\nwant %+v", result, err, want)
	}
	// End beyond the document and End 0 both read to the last line.
	for _, end := range []int{0, 99} {
		result, err = index.Read(ctx, store, "docs/a", ReadOptions{Start: 4, End: end})
		if err != nil || result.Text != "四行目\n五行目" || result.Truncated {
			t.Fatalf("read to end %d: %+v %v", end, result, err)
		}
	}
}

func TestReadContinuesAfterMaxBytes(t *testing.T) {
	ctx := context.Background()
	index, store := newQueryIndex(t, map[string]string{
		"docs/a": "あい\nうえ\nお" + strings.Repeat("か", 10) + "\nき",
	})
	// Each of the first two lines is 6 bytes; the line break makes 13.
	result, err := index.Read(ctx, store, "docs/a", ReadOptions{Start: 1, MaxBytes: 12})
	want := ReadResult{Key: "docs/a", Start: 1, Text: "あい", Lines: 4, Truncated: true, Next: 2}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("first page:\n got %+v %v\nwant %+v", result, err, want)
	}
	// A line longer than MaxBytes is cut at a character boundary.
	result, err = index.Read(ctx, store, "docs/a", ReadOptions{Start: 3, MaxBytes: 7})
	want = ReadResult{Key: "docs/a", Start: 3, Text: "おか", Lines: 4, Truncated: true, Next: 4}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("long line:\n got %+v %v\nwant %+v", result, err, want)
	}
	// Cutting the last requested line truncates the text but has no next line.
	result, err = index.Read(ctx, store, "docs/a", ReadOptions{Start: 3, End: 3, MaxBytes: 7})
	if err != nil || result.Text != "おか" || !result.Truncated || result.Next != 0 {
		t.Fatalf("long last line: %+v %v", result, err)
	}
}

func TestReadRejectsInvalidRanges(t *testing.T) {
	ctx := context.Background()
	index, store := newQueryIndex(t, map[string]string{"docs/a": "1\n2\n3"})
	for _, options := range []ReadOptions{{Start: 0}, {Start: 4}, {Start: 3, End: 2}, {Start: 1, MaxBytes: -1}} {
		if _, err := index.Read(ctx, store, "docs/a", options); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
	if _, err := index.Read(ctx, store, "docs/missing", ReadOptions{Start: 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing document: %v", err)
	}
	if _, err := index.Read(ctx, nil, "docs/a", ReadOptions{Start: 1}); err == nil {
		t.Fatal("accepted a nil source")
	}
}

func TestMirrorRead(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewMirror(index, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "user/1", Data: []byte("一\n二\n三")}}); err != nil {
		t.Fatal(err)
	}
	result, err := mirror.Read(ctx, "user/1", ReadOptions{Start: 2})
	if err != nil || result.Text != "二\n三" || result.Lines != 3 {
		t.Fatalf("mirror read: %+v %v", result, err)
	}
	for _, key := range []string{"", "../index/manifest", "user/./1"} {
		if _, err := mirror.Read(ctx, key, ReadOptions{Start: 1}); err == nil {
			t.Fatalf("accepted document ID %q", key)
		}
	}
	if _, err := mirror.Read(ctx, "user/2", ReadOptions{Start: 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing copy: %v", err)
	}
}
