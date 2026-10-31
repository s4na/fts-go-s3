package s3store

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
)

func TestS3EndToEnd(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := NewS3Store(S3Config{Endpoint: endpoint, Bucket: "zenbun-test", AccessKey: "testaccess", SecretKey: "testsecret"})
	if err != nil {
		t.Fatal(err)
	}
	status, _, _, err := store.request(ctx, http.MethodPut, "", nil, nil, nil, false)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("create bucket: %d %v", status, err)
	}
	if _, err := store.Put(ctx, "missing", []byte("x"), zenbun.PutOptions{IfMatch: "\"missing\""}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("missing update conflict: %v", err)
	}
	etag, err := store.Put(ctx, "docs/日本語 +.txt", []byte("日本語と猫"), zenbun.PutOptions{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "docs/日本語 +.txt", nil, zenbun.PutOptions{IfNoneMatch: true}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("create conflict: %v", err)
	}
	if _, err := store.Put(ctx, "docs/日本語 +.txt", nil, zenbun.PutOptions{IfMatch: "\"stale\""}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("update conflict: %v", err)
	}
	obj, err := store.Get(ctx, "docs/日本語 +.txt", &zenbun.ByteRange{Start: 0, End: 3})
	if err != nil || string(obj.Data) != "日" || obj.ETag != etag {
		t.Fatalf("range: %+v %v", obj, err)
	}
	objects, err := store.List(ctx, "docs/")
	if err != nil || len(objects) != 1 || objects[0].Key != "docs/日本語 +.txt" {
		t.Fatalf("list: %+v %v", objects, err)
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
		if err != nil || len(keys) != 1 || keys[0] != "docs/日本語 +.txt" {
			t.Fatalf("search %q: %v %v", query, keys, err)
		}
	}
	if _, err := store.Put(ctx, "docs/日本語 +.txt", []byte("犬"), zenbun.PutOptions{IfMatch: etag}); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Search(ctx, "犬")
	if err != nil || len(keys) != 1 || keys[0] != "docs/日本語 +.txt" {
		t.Fatalf("search after update: %v %v", keys, err)
	}
	keys, err = index.Search(ctx, "猫")
	if err != nil || len(keys) != 0 {
		t.Fatalf("search for old contents: %v %v", keys, err)
	}
	if err := store.Delete(ctx, "docs/日本語 +.txt"); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	keys, err = index.Search(ctx, "犬")
	if err != nil || len(keys) != 0 {
		t.Fatalf("delete: %v %v", keys, err)
	}
}
