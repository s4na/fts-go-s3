package zenbun

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"
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
	if _, err := store.Put(ctx, "missing", []byte("x"), PutOptions{IfMatch: "\"missing\""}); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing update conflict: %v", err)
	}
	etag, err := store.Put(ctx, "docs/日本語 +.txt", []byte("日本語と猫"), PutOptions{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "docs/日本語 +.txt", nil, PutOptions{IfNoneMatch: true}); !errors.Is(err, ErrConflict) {
		t.Fatalf("create conflict: %v", err)
	}
	if _, err := store.Put(ctx, "docs/日本語 +.txt", nil, PutOptions{IfMatch: "\"stale\""}); !errors.Is(err, ErrConflict) {
		t.Fatalf("update conflict: %v", err)
	}
	obj, err := store.Get(ctx, "docs/日本語 +.txt", &ByteRange{Start: 0, End: 3})
	if err != nil || string(obj.Data) != "日" || obj.ETag != etag {
		t.Fatalf("range: %+v %v", obj, err)
	}
	objects, err := store.List(ctx, "docs/")
	if err != nil || len(objects) != 1 || objects[0].Key != "docs/日本語 +.txt" {
		t.Fatalf("list: %+v %v", objects, err)
	}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	assertOracle(t, index, map[string]string{"docs/日本語 +.txt": "日本語と猫"}, []string{"日本", "猫"})
	if _, err := store.Put(ctx, "docs/日本語 +.txt", []byte("犬"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	assertOracle(t, index, map[string]string{"docs/日本語 +.txt": "犬"}, []string{"犬", "猫"})
	if err := store.Delete(ctx, "docs/日本語 +.txt"); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Search(ctx, "犬")
	if err != nil || len(keys) != 0 {
		t.Fatalf("delete: %v %v", keys, err)
	}
}
