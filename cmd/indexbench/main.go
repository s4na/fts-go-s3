// indexbench compares S3 index candidates with the linear scan oracle.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

type query struct {
	Kind string `json:"kind"`
	Query string `json:"query"`
}

type stats struct {
	Queries int `json:"queries"`
	FalseNegatives int `json:"false_negatives"`
	FalsePositives int `json:"false_positives"`
	Candidates int `json:"candidates"`
	Requests int64 `json:"requests"`
	ReceivedBytes int64 `json:"received_body_bytes"`
	P50Millis int64 `json:"p50_ms"`
	P95Millis int64 `json:"p95_ms"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	docsPath := flag.String("docs", "", "JSONL documents")
	queriesPath := flag.String("queries", "", "JSON query set")
	output := flag.String("out", "index-results.json", "output JSON")
	count := flag.Int("shards", 256, "shard count")
	flag.Parse()
	f, err := os.Open(*docsPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var docs []oracle.Document
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64*1024*1024)
	for scanner.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		docs = append(docs, doc)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	data, err := os.ReadFile(*queriesPath)
	if err != nil {
		return err
	}
	var queries []query
	if err := json.Unmarshal(data, &queries); err != nil {
		return err
	}
	meter := &s3lite.Meter{}
	client := &s3lite.Client{Endpoint: os.Getenv("S3_ENDPOINT"), AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: meter, Timeout: 60 * time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	r, err := client.Request(ctx, "PUT", "fts-index", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		return fmt.Errorf("create bucket: status %d: %v %s", r.Status, err, r.Body)
	}
	idx := index.Index{S3: client, Bucket: "fts-index", Prefix: "index"}
	start := time.Now()
	manifest, err := idx.Build(ctx, docs, *count)
	if err != nil {
		return err
	}
	buildMillis := time.Since(start).Milliseconds()
	build := meter.Snapshot()
	truth := oracle.New(docs)
	measure := func(cached *index.Manifest) (stats, error) {
		before := meter.Snapshot()
		var s stats
		var times []int64
		for _, q := range queries {
			start := time.Now()
			candidates, err := idx.Search(ctx, q.Query, cached)
			if err != nil {
				return s, err
			}
			times = append(times, time.Since(start).Milliseconds())
			got := make(map[string]bool, len(candidates))
			for _, id := range candidates {
				got[id] = true
			}
			actual := truth.Search(q.Query)
			for _, id := range actual {
				if !got[id] {
					s.FalseNegatives++
				}
			}
			s.Candidates += len(candidates)
			s.FalsePositives += len(candidates) - len(actual) + sFalseMissing(actual, got)
			s.Queries++
		}
		after := meter.Snapshot()
		s.Requests = after.Requests["GET"] - before.Requests["GET"]
		s.ReceivedBytes = after.ReceivedBytes - before.ReceivedBytes
		if len(times) > 0 {
			sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
			s.P50Millis = times[(len(times)-1)*50/100]
			s.P95Millis = times[(len(times)-1)*95/100]
		}
		return s, nil
	}
	uncached, err := measure(nil)
	if err != nil {
		return err
	}
	cached, err := measure(&manifest)
	if err != nil {
		return err
	}
	payload := struct {
		Documents int `json:"documents"`
		Shards int `json:"shards"`
		BuildMillis int64 `json:"build_ms"`
		Build s3lite.Counts `json:"build_including_bucket"`
		Uncached stats `json:"uncached"`
		Cached stats `json:"cached_manifest"`
	}{len(docs), *count, buildMillis, build, uncached, cached}
	data, err = json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	if uncached.FalseNegatives != 0 || cached.FalseNegatives != 0 {
		return fmt.Errorf("index missed oracle results")
	}
	return nil
}

func sFalseMissing(actual []string, candidates map[string]bool) int {
	n := 0
	for _, id := range actual {
		if !candidates[id] {
			n++
		}
	}
	return n
}
