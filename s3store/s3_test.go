package s3store

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	zenbun "github.com/s4na/zenbun"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestS3AddressingSigningAndRetries(t *testing.T) {
	attempts := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if req.URL.Host != "bucket.example.test" || req.URL.EscapedPath() != "/base/a%20%2B/%E6%97%A5" {
			t.Errorf("address: %s", req.URL.String())
		}
		if req.Header.Get("X-Amz-Security-Token") != "session" || !strings.Contains(req.Header.Get("Authorization"), "Credential=access/") {
			t.Errorf("missing signed credentials")
		}
		status := http.StatusServiceUnavailable
		body := ""
		if req.Method == http.MethodGet && attempts == 3 {
			status = http.StatusOK
			body = "ok"
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Etag": {"\"etag\""}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	store, err := New(Config{Endpoint: "https://example.test/base", Bucket: "bucket", AccessKey: "access", SecretKey: "secret", SessionToken: "session", VirtualHost: true, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	obj, err := store.Get(context.Background(), "a +/日", nil)
	if err != nil || string(obj.Data) != "ok" || attempts != 3 {
		t.Fatalf("read retry: %+v %v attempts=%d", obj, err, attempts)
	}
	attempts = 0
	_, err = store.Put(context.Background(), "a +/日", []byte("x"), zenbun.PutOptions{IfNoneMatch: true})
	var response *Error
	if !errors.As(err, &response) || response.StatusCode != http.StatusServiceUnavailable || attempts != 1 {
		t.Fatalf("conditional put: %v attempts=%d", err, attempts)
	}
}

func TestS3MissingBucketIsNotObjectNotFound(t *testing.T) {
	const noBucket = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>`
	const noKey = `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`
	body := noBucket
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	store, err := New(Config{Endpoint: "https://example.test", Bucket: "bucket", AccessKey: "a", SecretKey: "s", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	check := func(name string, err error, wantMissingObject bool) {
		t.Helper()
		var s3err *Error
		if wantMissingObject {
			if !errors.Is(err, zenbun.ErrNotFound) && !errors.Is(err, zenbun.ErrConflict) && err != nil {
				t.Errorf("%s: %v", name, err)
			}
			return
		}
		if errors.Is(err, zenbun.ErrNotFound) || errors.Is(err, zenbun.ErrConflict) || !errors.As(err, &s3err) || s3err.Code != "NoSuchBucket" || !strings.Contains(err.Error(), "NoSuchBucket") {
			t.Errorf("%s: want Error NoSuchBucket, got %v", name, err)
		}
	}
	_, err = store.Get(ctx, "k", nil)
	check("get", err, false)
	_, err = store.Put(ctx, "k", nil, zenbun.PutOptions{IfMatch: "\"e\""})
	check("put if-match", err, false)
	_, err = store.List(ctx, "")
	check("list", err, false)
	check("delete", store.Delete(ctx, "k"), false)

	body = noKey
	if _, err := store.Get(ctx, "k", nil); !errors.Is(err, zenbun.ErrNotFound) {
		t.Errorf("missing key: %v", err)
	}
	if _, err := store.Put(ctx, "k", nil, zenbun.PutOptions{IfMatch: "\"e\""}); !errors.Is(err, zenbun.ErrConflict) {
		t.Errorf("missing key put: %v", err)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Errorf("missing key delete: %v", err)
	}
	body = "" // unidentifiable 404 keeps the previous behavior
	if _, err := store.Get(ctx, "k", nil); !errors.Is(err, zenbun.ErrNotFound) {
		t.Errorf("empty 404: %v", err)
	}
}
