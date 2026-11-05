package main

import (
	"testing"

	"github.com/s4na/zenbun/gcsstore"
	"github.com/s4na/zenbun/s3store"
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

func TestParseQuery(t *testing.T) {
	query, err := parseQuery(`{"all":[["猫","ネコ"],["請求書"]],"none":["下書き"],"key_prefix":"articles/"}`)
	if err != nil || len(query.All) != 2 || query.All[0][1] != "ネコ" || query.None[0] != "下書き" || query.KeyPrefix != "articles/" {
		t.Fatalf("parse: %+v %v", query, err)
	}
	for _, text := range []string{`{"any":[["猫"]]}`, `{"all":[["猫"]]} {}`, `{"all":[["猫"]]}]`, `not json`} {
		if _, err := parseQuery(text); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
}
