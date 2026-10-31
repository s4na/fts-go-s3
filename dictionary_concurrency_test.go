package zenbun

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type dictionaryBarrierStore struct {
	Store
	entered chan struct{}
	release chan struct{}
}

func (s *dictionaryBarrierStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return s.Store.Get(ctx, key, r)
	case <-ctx.Done():
		return Object{}, ctx.Err()
	}
}

// Different dictionary blocks in one segment must be able to read in parallel.
func TestDictionaryReadsDifferentBlocksConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	docs := make(map[string]indexedDocument)
	for n := 0; n < 100; n++ {
		docs[fmt.Sprintf("document-%03d", n)] = indexedDocument{Terms: []string{fmt.Sprintf("u%03d", n)}}
	}
	ref, err := index.writeSegment(ctx, docs)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openSegment(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.blocks) < 2 {
		t.Fatal("test fixture requires at least two dictionary blocks")
	}
	barrier := &dictionaryBarrierStore{Store: store, entered: make(chan struct{}, 2), release: make(chan struct{})}
	reader.store = barrier
	done := make(chan error, 2)
	for block := 0; block < 2; block++ {
		go func(block int) {
			_, err := reader.dictionary(ctx, block)
			done <- err
		}(block)
	}
	for n := 0; n < 2; n++ {
		select {
		case <-barrier.entered:
		case <-ctx.Done():
			t.Fatal("dictionary reads were serialized while waiting for storage")
		}
	}
	close(barrier.release)
	for n := 0; n < 2; n++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
