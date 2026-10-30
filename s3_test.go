package zenbun

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
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
	store, err := NewS3Store(S3Config{Endpoint: "https://example.test/base", Bucket: "bucket", AccessKey: "access", SecretKey: "secret", SessionToken: "session", VirtualHost: true, HTTPClient: &http.Client{Transport: transport}})
	if err != nil { t.Fatal(err) }
	obj, err := store.Get(context.Background(), "a +/日", nil)
	if err != nil || string(obj.Data) != "ok" || attempts != 3 { t.Fatalf("read retry: %+v %v attempts=%d", obj, err, attempts) }
	attempts = 0
	_, err = store.Put(context.Background(), "a +/日", []byte("x"), PutOptions{IfNoneMatch: true})
	var response *S3Error
	if !errors.As(err, &response) || response.StatusCode != http.StatusServiceUnavailable || attempts != 1 {
		t.Fatalf("conditional put: %v attempts=%d", err, attempts)
	}
}
