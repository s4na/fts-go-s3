package main

import (
	"testing"

	"github.com/s4na/fts-go-s3/gcsstore"
	"github.com/s4na/fts-go-s3/s3store"
)

func TestNewStoreSelectsAdapter(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("S3_ENDPOINT", "http://localhost:8333")
	t.Setenv("S3_BUCKET", "s3-bucket")
	t.Setenv("GOOGLE_OAUTH_ACCESS_TOKEN", "token")
	t.Setenv("GCS_BUCKET", "gcs-bucket")
	t.Setenv("GCS_ENDPOINT", "")
	store, err := newStore(storageConfig{kind: "s3"})
	if _, ok := store.(*s3store.S3Store); err != nil || !ok {
		t.Fatalf("s3: %T %v", store, err)
	}
	store, err = newStore(storageConfig{kind: "gcs"})
	if _, ok := store.(*gcsstore.GCSStore); err != nil || !ok {
		t.Fatalf("gcs: %T %v", store, err)
	}
	if _, err := newStore(storageConfig{kind: "gcs", region: "us-east-1"}); err == nil {
		t.Fatal("gcs accepted an S3-only flag")
	}
	if _, err := newStore(storageConfig{kind: "azure"}); err == nil {
		t.Fatal("unknown storage accepted")
	}
}
