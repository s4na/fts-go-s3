// oraclebench checks and records deterministic search results for later PRs.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/s4na/fts-go-s3/internal/oracle"
)

type query struct {
	Kind  string `json:"kind"`
	Query string `json:"query"`
}

type result struct {
	Kind  string   `json:"kind"`
	Query string   `json:"query"`
	IDs   []string `json:"ids"`
}

func run() error {
	docsPath := flag.String("docs", "", "JSONL documents")
	queriesPath := flag.String("queries", "", "JSON query set")
	output := flag.String("out", "oracle-results.json", "output JSON")
	flag.Parse()
	f, err := os.Open(*docsPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var docs []oracle.Document
	seen := map[string]bool{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64*1024*1024)
	for scanner.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		if doc.ID == "" || seen[doc.ID] {
			return fmt.Errorf("empty or duplicate document ID %q", doc.ID)
		}
		seen[doc.ID] = true
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
	corpus := oracle.New(docs)
	start := time.Now()
	results := make([]result, 0, len(queries))
	for _, q := range queries {
		ids := corpus.Search(q.Query)
		if (q.Kind == "title" || q.Kind == "substring" || q.Kind == "frequent" || q.Kind == "rare") && len(ids) == 0 {
			return fmt.Errorf("expected at least one hit for %s %q", q.Kind, q.Query)
		}
		if q.Kind == "no_hit" && len(ids) != 0 {
			return fmt.Errorf("expected no hits for %q", q.Query)
		}
		results = append(results, result{Kind: q.Kind, Query: q.Query, IDs: ids})
	}
	payload := struct {
		Documents     int      `json:"documents"`
		Queries       int      `json:"queries"`
		ElapsedMillis int64    `json:"elapsed_ms"`
		Results       []result `json:"results"`
	}{len(docs), len(queries), time.Since(start).Milliseconds(), results}
	data, err = json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, data, 0644); err != nil {
		return err
	}
	fmt.Printf("documents=%d queries=%d scan_ms=%d\n", payload.Documents, payload.Queries, payload.ElapsedMillis)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
