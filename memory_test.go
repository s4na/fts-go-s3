package zenbun

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	var s MemoryStore
	input := []byte("abcdef")
	etag, err := s.Put(ctx, "docs/a", input, PutOptions{IfNoneMatch: true})
	if err != nil || etag == "" {
		t.Fatalf("put: %q, %v", etag, err)
	}
	input[1] = 'X'
	got, err := s.Get(ctx, "docs/a", &ByteRange{Start: 1, End: 4})
	if err != nil || string(got.Data) != "bcd" || got.ETag != etag {
		t.Fatalf("range: %+v, %v", got, err)
	}
	got.Data[0] = 'X'
	got, err = s.Get(ctx, "docs/a", nil)
	if err != nil || string(got.Data) != "abcdef" {
		t.Fatalf("full read: %+v, %v", got, err)
	}
	if _, err := s.Get(ctx, "docs/a", &ByteRange{Start: 0, End: 7}); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("invalid range: %v", err)
	}
	if _, err := s.Put(ctx, "docs/a", nil, PutOptions{IfNoneMatch: true}); !errors.Is(err, ErrConflict) {
		t.Fatalf("create conflict: %v", err)
	}
	if _, err := s.Put(ctx, "docs/a", nil, PutOptions{IfMatch: "stale"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("update conflict: %v", err)
	}
	if _, err := s.Put(ctx, "docs/a", []byte("updated"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatalf("conditional update: %v", err)
	}
	if _, err := s.Put(ctx, "docs/b", []byte("b"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	list, err := s.List(ctx, "docs/")
	if err != nil || len(list) != 2 || list[0].Key != "docs/a" || list[0].Size != 7 || list[1].Key != "docs/b" {
		t.Fatalf("list: %+v, %v", list, err)
	}
	if err := s.Delete(ctx, "docs/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "docs/a", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted read: %v", err)
	}
	if err := s.Delete(ctx, "docs/a"); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
}

func TestMemoryStoreConcurrentConditionalWrites(t *testing.T) {
	ctx := context.Background()
	var s MemoryStore
	etag, err := s.Put(ctx, "manifest", []byte("initial"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, value := range []string{"first", "second"} {
		wg.Add(1)
		go func(value string) {
			defer wg.Done()
			_, err := s.Put(ctx, "manifest", []byte(value), PutOptions{IfMatch: etag})
			results <- err
		}(value)
	}
	wg.Wait()
	close(results)
	var success, conflicts int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", success, conflicts)
	}
}

func TestMemoryStoreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var s MemoryStore
	if _, err := s.Put(ctx, "key", nil, PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("put: %v", err)
	}
	if _, err := s.Get(ctx, "key", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("get: %v", err)
	}
	if _, err := s.List(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("list: %v", err)
	}
	if err := s.Delete(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete: %v", err)
	}
}
