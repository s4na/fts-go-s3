package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func TestStorage(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	meter := &s3lite.Meter{}
	c := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: meter, Timeout: 10 * time.Second}}
	bucket := "fts-bench"
	call := func(method, key string, headers http.Header, body string) s3lite.Result {
		t.Helper()
		r, err := c.Request(ctx, method, bucket, key, nil, headers, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	check := func(r s3lite.Result, status int) {
		t.Helper()
		if r.Status != status {
			t.Fatalf("got HTTP %d, want %d: %s", r.Status, status, r.Body)
		}
	}
	check(call("PUT", "", nil, ""), 200)
	check(call("PUT", "test key/日本語", nil, "abcdef"), 200)
	r := call("GET", "test key/日本語", http.Header{"Range": {"bytes=1-3"}}, "")
	check(r, 206)
	if string(r.Body) != "bcd" {
		t.Fatalf("range: %q", r.Body)
	}
	r = call("HEAD", "test key/日本語", nil, "")
	check(r, 200)
	etag := r.Header.Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}
	page, err := c.List(ctx, bucket, "test", "")
	if err != nil || len(page.Contents) != 1 || page.Contents[0].Key != "test key/日本語" {
		t.Fatalf("list: %+v, %v", page, err)
	}
	results := map[string]interface{}{}
	for _, tc := range []struct {
		name   string
		header http.Header
	}{
		{"if_none_match", http.Header{"If-None-Match": {"*"}}},
		{"if_match_stale", http.Header{"If-Match": {"\"definitely-stale\""}}},
	} {
		r = call("PUT", "test key/日本語", tc.header, "overwrite")
		after := call("GET", "test key/日本語", nil, "")
		check(after, 200)
		results[tc.name] = map[string]interface{}{"status": r.Status, "preserved": string(after.Body) == "abcdef", "supported": r.Status == 412 && string(after.Body) == "abcdef"}
		check(call("PUT", "test key/日本語", nil, "abcdef"), 200)
	}
	r = call("HEAD", "test key/日本語", nil, "")
	check(r, 200)
	r = call("PUT", "test key/日本語", http.Header{"If-Match": {r.Header.Get("ETag")}}, "matched")
	check(r, 200)
	r = call("GET", "test key/日本語", nil, "")
	check(r, 200)
	if string(r.Body) != "matched" {
		t.Fatalf("matched update: %q", r.Body)
	}
	check(call("DELETE", "test key/日本語", nil, ""), 204)
	check(call("GET", "test key/日本語", nil, ""), 404)
	results["metrics"] = meter.Snapshot()
	results["storage"] = os.Getenv("STORAGE")
	results["body_bytes_note"] = "HTTP request and response bodies consumed, excluding headers and protocol overhead"
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("results.json", data, 0644); err != nil {
		t.Fatal(err)
	}
	if summary := os.Getenv("GITHUB_STEP_SUMMARY"); summary != "" {
		f, err := os.OpenFile(summary, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fmt.Fprintf(f, "## %s S3 compatibility\n\n```json\n%s\n```\n", strings.ToUpper(os.Getenv("STORAGE")), data)
		closeErr := f.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}
