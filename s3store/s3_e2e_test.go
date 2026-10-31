package s3store

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/storetest"
)

func TestS3EndToEnd(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := NewS3Store(S3Config{Endpoint: endpoint, Bucket: "zenbun-test", AccessKey: "testaccess", SecretKey: "testsecret"})
	if err != nil {
		t.Fatal(err)
	}
	status, _, _, err := store.request(ctx, http.MethodPut, "", nil, nil, nil, false)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("create bucket: %d %v", status, err)
	}
	storetest.Run(t, store)
}
