package index

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestBuildLeaseBeginsBeforeManifestPublication(t *testing.T) {
	var publication time.Time
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/manifest.json") {
			publication = time.Now()
			time.Sleep(10 * time.Millisecond)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	client := &s3lite.Client{Endpoint: "http://example.invalid", HTTP: &http.Client{Transport: transport}}
	idx := Index{S3: client, Bucket: "test", Prefix: "index"}
	manifest, err := idx.Build(context.Background(), []oracle.Document{{ID: "one", Text: "text"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if publication.IsZero() || manifest.acquiredAt.After(publication) {
		t.Fatalf("lease %v started after publication %v", manifest.acquiredAt, publication)
	}
}
