package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func TestUpdates(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	meter := &s3lite.Meter{}
	client := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: meter, Timeout: 10 * time.Second}}
	r, err := client.Request(ctx, "PUT", "fts-updates", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("create bucket: %d, %v, %s", r.Status, err, r.Body)
	}
	idx := index.Index{S3: client, Bucket: "fts-updates", Prefix: "index"}
	original := []oracle.Document{{ID: "old", Text: "古い文章"}, {ID: "keep", Text: "残る文章"}}
	old, err := idx.Build(ctx, original, 16)
	if err != nil {
		t.Fatal(err)
	}
	before := meter.Snapshot()
	newDoc := oracle.Document{ID: "old", Text: "新しい文章"}
	if err := idx.Update(ctx, "old", &newDoc); err != nil {
		t.Fatal(err)
	}
	single := meter.Snapshot()
	if err := idx.Update(ctx, "keep", nil); err != nil {
		t.Fatal(err)
	}
	var batch []index.Change
	for n := 0; n < 100; n++ {
		doc := &oracle.Document{ID: fmt.Sprintf("new-%03d", n), Text: "追加した文章"}
		batch = append(batch, index.Change{ID: doc.ID, Document: doc})
	}
	beforeBatch := meter.Snapshot()
	if err := idx.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	afterBatch := meter.Snapshot()
	for _, tc := range []struct {
		query  string
		want   int
		cached *index.Manifest
	}{{"古い", 0, nil}, {"新しい", 1, nil}, {"残る", 0, nil}, {"追加", 100, nil}, {"古い", 1, &old}} {
		got, err := idx.Search(ctx, tc.query, tc.cached)
		if err != nil || len(got) != tc.want {
			t.Errorf("search %q: got %d, want %d: %v", tc.query, len(got), tc.want, err)
		}
	}
	if t.Failed() {
		return
	}
	manifest, err := idx.Manifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]interface{}{
		"single_puts":            single.Requests["PUT"] - before.Requests["PUT"],
		"single_sent_body_bytes": single.SentBytes - before.SentBytes,
		"batch_documents":        len(batch),
		"batch_puts":             afterBatch.Requests["PUT"] - beforeBatch.Requests["PUT"],
		"batch_sent_body_bytes":  afterBatch.SentBytes - beforeBatch.SentBytes,
		"segments_after_batch":   len(manifest.Segments),
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("update-results.json", data, 0644); err != nil {
		t.Fatal(err)
	}
	if summary := os.Getenv("GITHUB_STEP_SUMMARY"); summary != "" {
		f, err := os.OpenFile(summary, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fmt.Fprintf(f, "## %s updates\n\n```json\n%s\n```\n", os.Getenv("STORAGE"), data)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write summary: %v, close: %v", writeErr, closeErr)
		}
	}
}
