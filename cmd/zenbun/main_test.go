package main

import (
	"bytes"
	"os"
	"strings"
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

func TestRunDoesNotBuildStoreForHelpOrBadUsage(t *testing.T) {
	for _, name := range []string{"S3_ENDPOINT", "S3_BUCKET", "AWS_REGION", "GCS_ENDPOINT", "GCS_BUCKET", "ZENBUN_STORAGE"} {
		t.Setenv(name, "")
	}
	var buffer bytes.Buffer
	output = &buffer
	t.Cleanup(func() { output = os.Stdout })

	for _, args := range [][]string{nil, {"help"}, {"-h"}, {"--help"}, {"sync", "-h"}, {"search", "-h"}} {
		buffer.Reset()
		if err := run(args); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if !strings.Contains(buffer.String(), "usage: zenbun") {
			t.Fatalf("%v: no usage: %q", args, buffer.String())
		}
	}
	if err := run(nil); err != nil || !strings.Contains(usageText, "must come before") {
		t.Fatalf("usage lacks flag ordering note: %v", err)
	}

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"bogus"}, "unknown command"},
		{[]string{"search"}, "search requires one query"},
		{[]string{"search", "a", "b"}, "search requires one query"},
		{[]string{"search", "日本", "-index", "x"}, "options must come before"},
		{[]string{"sync", "extra"}, "sync takes no arguments"},
		{[]string{"sync", "--source", "x", "extra", "-index", "y"}, "options must come before"},
		{[]string{"find"}, "find requires one JSON query"},
		{[]string{"read", "key", "-start", "2"}, "options must come before"},
	}
	for _, c := range cases {
		err := run(c.args)
		if err == nil || !strings.Contains(err.Error(), c.want) || strings.Contains(err.Error(), "s3:") || strings.Contains(err.Error(), "endpoint") {
			t.Fatalf("%v: got %v, want %q", c.args, err, c.want)
		}
	}
}
