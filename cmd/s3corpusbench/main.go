// s3corpusbench exercises source documents stored as individual S3 objects.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func difference(after, before s3lite.Counts) s3lite.Counts {
	out := s3lite.Counts{Requests: make(map[string]int64), SentBytes: after.SentBytes - before.SentBytes, ReceivedBytes: after.ReceivedBytes - before.ReceivedBytes}
	for method, n := range after.Requests {
		out.Requests[method] = n - before.Requests[method]
	}
	return out
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: s3corpusbench documents.jsonl results.json")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 64*1024*1024)
	original := make(map[string]oracle.Document)
	for scan.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scan.Bytes(), &doc); err != nil {
			return err
		}
		if doc.ID == "" {
			return fmt.Errorf("empty document ID")
		}
		if _, exists := original[doc.ID]; exists {
			return fmt.Errorf("duplicate document ID %q", doc.ID)
		}
		original[doc.ID] = doc
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if len(original) == 0 {
		return fmt.Errorf("empty input")
	}
	keys := make([]string, 0, len(original))
	for id := range original {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	meter := &s3lite.Meter{}
	client := &s3lite.Client{Endpoint: os.Getenv("S3_ENDPOINT"), AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: meter, Timeout: 30 * time.Second}}
	bucket := "fts-source"
	request := func(method, key string, body []byte) (s3lite.Result, error) {
		r, err := client.Request(ctx, method, bucket, key, nil, nil, body)
		if err != nil {
			return r, err
		}
		if r.Status < 200 || r.Status >= 300 {
			return r, fmt.Errorf("%s %s: HTTP %d: %s", method, key, r.Status, r.Body)
		}
		return r, nil
	}
	if _, err := request("PUT", "", nil); err != nil {
		return err
	}
	beforeUpload := meter.Snapshot()
	for _, id := range keys {
		data, err := json.Marshal(original[id])
		if err != nil {
			return err
		}
		if _, err := request("PUT", "docs/"+id, data); err != nil {
			return err
		}
	}
	afterUpload := meter.Snapshot()
	var objectKeys []string
	token := ""
	pages := 0
	for {
		page, err := client.List(ctx, bucket, "docs/", token)
		if err != nil {
			return err
		}
		pages++
		for _, item := range page.Contents {
			objectKeys = append(objectKeys, item.Key)
		}
		if !page.Truncated {
			break
		}
		if page.Next == "" || page.Next == token {
			return fmt.Errorf("invalid continuation token")
		}
		token = page.Next
	}
	if len(objectKeys) != len(original) {
		return fmt.Errorf("listed %d objects, expected %d", len(objectKeys), len(original))
	}
	var docs []oracle.Document
	seen := make(map[string]bool)
	for _, key := range objectKeys {
		r, err := request("GET", key, nil)
		if err != nil {
			return err
		}
		var doc oracle.Document
		if err := json.Unmarshal(r.Body, &doc); err != nil {
			return err
		}
		if seen[doc.ID] || !reflect.DeepEqual(original[doc.ID], doc) {
			return fmt.Errorf("source content mismatch: %q", key)
		}
		seen[doc.ID] = true
		docs = append(docs, doc)
	}
	afterRead := meter.Snapshot()
	idx := index.Index{S3: client, Bucket: bucket, Prefix: "index"}
	if _, err := idx.Build(ctx, docs, 256); err != nil {
		return err
	}
	afterBuild := meter.Snapshot()
	first := docs[0]
	first.Text += "\n検証用の追記文章"
	data, err := json.Marshal(first)
	if err != nil {
		return err
	}
	if _, err := request("PUT", "docs/"+first.ID, data); err != nil {
		return err
	}
	r, err := request("GET", "docs/"+first.ID, nil)
	if err != nil {
		return err
	}
	var updated oracle.Document
	if err := json.Unmarshal(r.Body, &updated); err != nil {
		return err
	}
	if !reflect.DeepEqual(updated, first) {
		return fmt.Errorf("updated source differs from intended document %q", first.ID)
	}
	if err := idx.Update(ctx, updated.ID, &updated); err != nil {
		return err
	}
	docs[0] = updated
	afterUpdate := meter.Snapshot()
	truth := oracle.New(docs)
	var missed int
	for _, query := range []string{"検証用の追記文章", first.Title, "猫", "𪛖𪛖𪛖𪛖𪛖𪛖"} {
		got, err := idx.Search(ctx, query, nil)
		if err != nil {
			return err
		}
		set := make(map[string]bool, len(got))
		for _, id := range got {
			set[id] = true
		}
		for _, id := range truth.Search(query) {
			if !set[id] {
				missed++
			}
		}
	}
	payload := map[string]interface{}{
		"documents":               len(docs),
		"list_pages":              pages,
		"source_upload":           difference(afterUpload, beforeUpload),
		"source_list_and_read":    difference(afterRead, afterUpload),
		"index_build":             difference(afterBuild, afterRead),
		"source_and_index_update": difference(afterUpdate, afterBuild),
		"queries":                 4,
		"false_negatives":         missed,
	}
	data, err = json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(os.Args[2], data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	if missed != 0 {
		return fmt.Errorf("source-backed index missed %d oracle hits", missed)
	}
	return nil
}
