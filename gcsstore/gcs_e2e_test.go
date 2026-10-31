package gcsstore

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/storetest"
)

func TestGCSEndToEnd(t *testing.T) {
	endpoint := os.Getenv("GCS_ENDPOINT")
	if endpoint == "" {
		t.Skip("GCS_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}
	store, err := NewGCSStore(GCSConfig{Endpoint: endpoint, Bucket: "zenbun-test", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	target := strings.TrimRight(endpoint, "/") + "/storage/v1/b?project=test"
	status, _, _, err := store.request(ctx, http.MethodPost, target, http.Header{"Content-Type": {"application/json"}}, []byte(`{"name":"zenbun-test"}`), false)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("create bucket: %d %v", status, err)
	}
	storetest.Run(t, store)
}
