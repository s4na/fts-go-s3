package e2e

import (
	"context"
	"fmt"
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

type searchGate struct {
	base http.RoundTripper
	segment string
	started chan struct{}
	release chan struct{}
	once sync.Once
}

func (g *searchGate) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "GET" && strings.Contains(r.URL.Path, "/segments/"+g.segment+"/") {
		g.once.Do(func() {
			close(g.started)
			select {
			case <-g.release:
			case <-r.Context().Done():
			}
		})
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
	}
	return g.base.RoundTrip(r)
}

func TestSearchDuringCompaction(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Timeout: 10 * time.Second}}
	r, err := client.Request(ctx, "PUT", "fts-compact-race", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("create bucket: %d %v", r.Status, err)
	}
	idx := index.Index{S3: client, Bucket: "fts-compact-race", Prefix: "index"}
	old, err := idx.Build(ctx, []oracle.Document{{ID: "one", Text: "並行検索の文章"}}, 8)
	if err != nil {
		t.Fatal(err)
	}
	gate := &searchGate{base: http.DefaultTransport, segment: old.Segments[0], started: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
	}()
	readerClient := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: gate, Timeout: 10 * time.Second}}
	reader := index.Index{S3: readerClient, Bucket: "fts-compact-race", Prefix: "index"}
	type searchResult struct {
		ids []string
		err error
	}
	result := make(chan searchResult, 1)
	go func() {
		ids, err := reader.Search(ctx, "並行検索", &old)
		result <- searchResult{ids: ids, err: err}
	}()
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := idx.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if deleted, err := idx.Reclaim(ctx, index.ReadLease+index.ClockSkewAllowance); err != nil || deleted != 0 {
		t.Fatalf("reclaimed during active search: %d %v", deleted, err)
	}
	close(gate.release)
	released = true
	select {
	case got := <-result:
		if got.err != nil || len(got.ids) != 1 || got.ids[0] != "one" {
			t.Fatalf("search across compaction: %v %v", got.ids, got.err)
		}
	case <-ctx.Done():
		t.Fatal(fmt.Errorf("search did not finish: %w", ctx.Err()))
	}
}
