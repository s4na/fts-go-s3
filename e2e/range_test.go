package e2e

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func TestRangeLayout(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Timeout: 10 * time.Second}}
	r, err := client.Request(ctx, "PUT", "fts-range", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("create bucket: %d %v", r.Status, err)
	}
	idx := index.Index{S3: client, Bucket: "fts-range", Prefix: "index", Range: true}
	old, err := idx.Build(ctx, []oracle.Document{{ID: "one", Text: "最初の文章"}}, 16)
	if err != nil {
		t.Fatal(err)
	}
	updated := oracle.Document{ID: "one", Text: "新しい文章"}
	if err := idx.Update(ctx, "one", &updated); err != nil {
		t.Fatal(err)
	}
	if err := idx.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		want int
		manifest *index.Manifest
	}{{"最初", 1, &old}, {"最初", 0, nil}, {"新しい", 1, nil}} {
		got, err := idx.Search(ctx, tc.query, tc.manifest)
		if err != nil || len(got) != tc.want {
			t.Fatalf("%q: got %v want %d: %v", tc.query, got, tc.want, err)
		}
	}
}
