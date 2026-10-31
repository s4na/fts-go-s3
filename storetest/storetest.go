// Package storetest checks that a zenbun.Store adapter satisfies the contract
// zenbun relies on. Adapter tests call Run with an empty bucket or namespace.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
)

// Run exercises store with conditional writes, ranges, listing, an index and
// a mirror. The store must start without objects under "docs/", "index/",
// "mirror-index/" and "mirror-documents/".
func Run(t *testing.T, store zenbun.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const key = "docs/日本語 +.txt"

	if _, err := store.Get(ctx, key, nil); !errors.Is(err, zenbun.ErrNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	if _, err := store.Put(ctx, "docs/missing", []byte("x"), zenbun.PutOptions{IfMatch: "\"missing\""}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("missing update conflict: %v", err)
	}
	etag, err := store.Put(ctx, key, []byte("日本語と猫"), zenbun.PutOptions{IfNoneMatch: true})
	if err != nil || etag == "" {
		t.Fatalf("create: %q %v", etag, err)
	}
	if _, err := store.Put(ctx, key, nil, zenbun.PutOptions{IfNoneMatch: true}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("create conflict: %v", err)
	}
	if _, err := store.Put(ctx, key, nil, zenbun.PutOptions{IfMatch: "\"stale\""}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("update conflict: %v", err)
	}
	obj, err := store.Get(ctx, key, nil)
	if err != nil || string(obj.Data) != "日本語と猫" || obj.ETag != etag {
		t.Fatalf("get: %+v %v", obj, err)
	}
	obj, err = store.Get(ctx, key, &zenbun.ByteRange{Start: 0, End: 3})
	if err != nil || string(obj.Data) != "日" || obj.ETag != etag {
		t.Fatalf("range: %+v %v", obj, err)
	}
	if _, err := store.Get(ctx, key, &zenbun.ByteRange{Start: 0, End: 1 << 20}); !errors.Is(err, zenbun.ErrInvalidRange) {
		t.Fatalf("range past end: %v", err)
	}
	if _, err := store.Put(ctx, "docs/a", []byte("a"), zenbun.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	objects, err := store.List(ctx, "docs/")
	if err != nil || len(objects) != 2 || objects[0].Key != "docs/a" || objects[1].Key != key || objects[1].ETag != etag || objects[1].Size != int64(len("日本語と猫")) {
		t.Fatalf("list: %+v %v", objects, err)
	}
	if err := store.Delete(ctx, "docs/a"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "docs/a"); err != nil && !errors.Is(err, zenbun.ErrNotFound) {
		t.Fatalf("delete missing: %v", err)
	}

	index, err := zenbun.Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"日本", "猫"} {
		keys, err := index.Search(ctx, query)
		if err != nil || len(keys) != 1 || keys[0] != key {
			t.Fatalf("search %q: %v %v", query, keys, err)
		}
	}
	updated, err := store.Put(ctx, key, []byte("犬"), zenbun.PutOptions{IfMatch: etag})
	if err != nil || updated == etag {
		t.Fatalf("update: %q %v", updated, err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Search(ctx, "犬")
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("search after update: %v %v", keys, err)
	}
	keys, err = index.Search(ctx, "猫")
	if err != nil || len(keys) != 0 {
		t.Fatalf("search for old contents: %v %v", keys, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	keys, err = index.Search(ctx, "犬")
	if err != nil || len(keys) != 0 {
		t.Fatalf("delete: %v %v", keys, err)
	}

	mirrorIndex, err := zenbun.Open(store, "mirror-index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := zenbun.NewMirror(mirrorIndex, store, "mirror-documents/")
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []zenbun.Change{{Key: "文書/1", Data: []byte("日本語の猫")}}); err != nil {
		t.Fatal(err)
	}
	matches, err := mirror.Search(ctx, "日本語")
	if err != nil || len(matches) != 1 || matches[0].Key != "文書/1" || string(matches[0].Data) != "日本語の猫" {
		t.Fatalf("mirror search: %+v %v", matches, err)
	}
	if err := mirror.Apply(ctx, []zenbun.Change{{Key: "文書/1", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	matches, err = mirror.Search(ctx, "日本語")
	if err != nil || len(matches) != 0 {
		t.Fatalf("mirror delete: %+v %v", matches, err)
	}
}
