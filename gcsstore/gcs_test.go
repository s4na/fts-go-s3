package gcsstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	zenbun "github.com/s4na/fts-go-s3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestGCSAddressingAuthAndRetries(t *testing.T) {
	attempts := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if req.URL.Host != "example.test" || req.URL.EscapedPath() != "/base/storage/v1/b/bucket/o/a%20%2B%2F%E6%97%A5" || req.URL.Query().Get("alt") != "media" {
			t.Errorf("address: %s", req.URL.String())
		}
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("missing bearer token")
		}
		status := http.StatusServiceUnavailable
		body := ""
		if attempts == 3 {
			status = http.StatusOK
			body = "ok"
		}
		return &http.Response{StatusCode: status, Header: http.Header{"X-Goog-Generation": {"42"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	store, err := NewGCSStore(GCSConfig{Endpoint: "https://example.test/base", Bucket: "bucket", AccessToken: "token", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	obj, err := store.Get(context.Background(), "a +/日", nil)
	if err != nil || string(obj.Data) != "ok" || obj.ETag != "42" || attempts != 3 {
		t.Fatalf("read retry: %+v %v attempts=%d", obj, err, attempts)
	}
}

func TestGCSConditionalPut(t *testing.T) {
	var query string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		query = req.URL.RawQuery
		if req.Method != http.MethodPost || req.URL.EscapedPath() != "/upload/storage/v1/b/bucket/o" {
			t.Errorf("address: %s %s", req.Method, req.URL.String())
		}
		if req.URL.Query().Get("ifGenerationMatch") == "7" {
			return &http.Response{StatusCode: http.StatusPreconditionFailed, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"name":"a","generation":"8"}`)), Request: req}, nil
	})
	store, err := NewGCSStore(GCSConfig{Bucket: "bucket", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	etag, err := store.Put(ctx, "a b", []byte("x"), zenbun.PutOptions{IfNoneMatch: true})
	if err != nil || etag != "8" || !strings.Contains(query, "ifGenerationMatch=0") || !strings.Contains(query, "name=a%20b") {
		t.Fatalf("create: %q %v query=%s", etag, err, query)
	}
	if _, err := store.Put(ctx, "a", nil, zenbun.PutOptions{IfMatch: "7"}); !errors.Is(err, zenbun.ErrConflict) {
		t.Fatalf("stale generation: %v", err)
	}
	query = ""
	if _, err := store.Put(ctx, "a", nil, zenbun.PutOptions{IfMatch: "\"etag\""}); !errors.Is(err, zenbun.ErrConflict) || query != "" {
		t.Fatalf("non-generation ETag: %v query=%s", err, query)
	}
}

func TestGCSRequiresCredentials(t *testing.T) {
	t.Setenv("GOOGLE_OAUTH_ACCESS_TOKEN", "")
	if _, err := NewGCSStore(GCSConfig{Bucket: "bucket"}); err == nil {
		t.Fatal("missing credentials accepted")
	}
	t.Setenv("GOOGLE_OAUTH_ACCESS_TOKEN", "env-token")
	store, err := NewGCSStore(GCSConfig{Bucket: "bucket"})
	if err != nil || store.config.AccessToken != "env-token" || store.endpoint.String() != DefaultEndpoint {
		t.Fatalf("environment token: %+v %v", store, err)
	}
}
