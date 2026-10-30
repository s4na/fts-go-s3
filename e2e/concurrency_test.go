package e2e

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

type manifestBarrier struct {
	base http.RoundTripper
	ready sync.WaitGroup
}

func (b *manifestBarrier) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := b.base.RoundTrip(req)
	if err == nil && req.Method == "GET" && strings.HasSuffix(req.URL.Path, "/manifest.json") {
		b.ready.Done()
		b.ready.Wait()
	}
	return resp, err
}

func TestConcurrentWriters(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Timeout: 10 * time.Second}}
	r, err := client.Request(ctx, "PUT", "fts-race", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("create bucket: %d %v", r.Status, err)
	}
	idx := index.Index{S3: client, Bucket: "fts-race", Prefix: "index"}
	if _, err := idx.Build(ctx, []oracle.Document{{ID: "base", Text: "baseline"}}, 8); err != nil {
		t.Fatal(err)
	}
	barrier := &manifestBarrier{base: http.DefaultTransport}
	barrier.ready.Add(2)
	racingClient := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: barrier, Timeout: 10 * time.Second}}
	racing := index.Index{S3: racingClient, Bucket: "fts-race", Prefix: "index"}
	out := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		go func(id string) {
			doc := oracle.Document{ID: id, Text: "concurrent"}
			out <- tracing.Update(ctx, id, &doc)
		}(id)
	}
	first, second := <-out, <-out
	if !((first == nil && errors.Is(second, index.ErrConflict)) || (second == nil && errors.Is(first, index.ErrConflict))) {
		t.Fatalf("expected one success and one conflict, got %v and %v", first, second)
	}
	got, err := idx.Search(ctx, "concurrent", nil)
	if err != nil || len(got) != 1 {
		t.Fatalf("expected exactly one committed update, got %v, %v", got, err)
	}
	missing := "first"
	if got[0] == "first" {
		missing = "second"
	}
	doc := oracle.Document{ID: missing, Text: "concurrent"}
	if err := idx.Update(ctx, missing, &doc); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err = idx.Search(ctx, "concurrent", nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("retry should preserve both writers: %v, %v", got, err)
	}
}
