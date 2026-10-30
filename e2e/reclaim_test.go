package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func TestReclaimAfterGrace(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" || os.Getenv("RECLAIM_FULL") != "1" {
		t.Skip("requires S3_ENDPOINT and RECLAIM_FULL=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	client := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Timeout: 10 * time.Second}}
	r, err := client.Request(ctx, "PUT", "fts-reclaim", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("create bucket: %d %v", r.Status, err)
	}
	idx := index.Index{S3: client, Bucket: "fts-reclaim", Prefix: "index", Range: true}
	old, err := idx.Build(ctx, []oracle.Document{{ID: "one", Text: "文章"}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	grace := index.ReadLease + index.ClockSkewAllowance
	if _, err := idx.Reclaim(ctx, grace-time.Second); err == nil {
		t.Fatal("unsafe grace was accepted")
	}
	if deleted, err := idx.Reclaim(ctx, grace); err != nil || deleted != 0 {
		t.Fatalf("early reclamation: %d %v", deleted, err)
	}
	if got, err := idx.Search(ctx, "文章", &old); err != nil || len(got) != 1 {
		t.Fatalf("old snapshot during grace: %v %v", got, err)
	}
	select {
	case <-time.After(2*index.ReadLease + index.ClockSkewAllowance + 2*time.Second):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if deleted, err := idx.Reclaim(ctx, grace); err != nil || deleted != 1 {
		t.Fatalf("reclamation after grace: %d %v", deleted, err)
	}
	r, err = client.Request(ctx, "GET", "fts-reclaim", fmt.Sprintf("index/segments/%s/packed", old.Segments[0]), nil, nil, nil)
	if err != nil || r.Status != http.StatusNotFound {
		t.Fatalf("retired object remains: HTTP %d %v", r.Status, err)
	}
	if _, err := idx.Search(ctx, "文章", &old); err == nil {
		t.Fatal("expired snapshot was accepted")
	}
	if got, err := idx.Search(ctx, "文章", nil); err != nil || len(got) != 1 {
		t.Fatalf("current snapshot after reclamation: %v %v", got, err)
	}
}
