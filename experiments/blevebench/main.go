// blevebench records a reference baseline, including semantic differences.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/tokenize"
)

type query struct {
	Kind  string `json:"kind"`
	Query string `json:"query"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	docsPath := flag.String("docs", "", "JSONL documents")
	queriesPath := flag.String("queries", "", "JSON queries")
	indexPath := flag.String("index", "bleve-index", "new index directory")
	output := flag.String("out", "bleve.json", "output JSON")
	flag.Parse()
	data, err := os.ReadFile(*queriesPath)
	if err != nil {
		return err
	}
	var queries []query
	if err := json.Unmarshal(data, &queries); err != nil {
		return err
	}
	f, err := os.Open(*docsPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var docs []oracle.Document
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 64*1024*1024)
	for scan.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scan.Bytes(), &doc); err != nil {
			return err
		}
		docs = append(docs, doc)
	}
	if err := scan.Err(); err != nil {
		return err
	}
	mapping := bleve.NewIndexMapping()
	start := time.Now()
	idx, err := bleve.New(*indexPath, mapping)
	if err != nil {
		return err
	}
	defer func() { _ = idx.Close() }()
	batch := idx.NewBatch()
	for n, doc := range docs {
		if err := batch.Index(doc.ID, map[string]string{"content": tokenize.Normalize(doc.Title + "\n" + doc.Text)}); err != nil {
			return err
		}
		if (n+1)%500 == 0 {
			if err := idx.Batch(batch); err != nil {
				return err
			}
			batch = idx.NewBatch()
		}
	}
	if batch.Size() > 0 {
		if err := idx.Batch(batch); err != nil {
			return err
		}
	}
	buildMillis := time.Since(start).Milliseconds()
	truth := oracle.New(docs)
	var falseNegatives, falsePositives, candidates int
	var times []int64
	byKind := make(map[string]map[string]int)
	for _, q := range queries {
		match := bleve.NewMatchQuery(tokenize.Normalize(q.Query))
		match.SetField("content")
		request := bleve.NewSearchRequestOptions(match, len(docs), 0, false)
		start := time.Now()
		result, err := idx.Search(request)
		if err != nil {
			return err
		}
		times = append(times, time.Since(start).Milliseconds())
		got := make(map[string]bool, len(result.Hits))
		for _, hit := range result.Hits {
			got[hit.ID] = true
		}
		actual := truth.Search(q.Query)
		missing := 0
		for _, id := range actual {
			if !got[id] {
				missing++
			}
		}
		fp := len(got) - len(actual) + missing
		falseNegatives += missing
		falsePositives += fp
		candidates += len(got)
		if byKind[q.Kind] == nil {
			byKind[q.Kind] = make(map[string]int)
		}
		byKind[q.Kind]["queries"]++
		byKind[q.Kind]["false_negatives"] += missing
		byKind[q.Kind]["false_positives"] += fp
	}
	if err := idx.Close(); err != nil {
		return err
	}
	var indexBytes int64
	if err := filepath.WalkDir(*indexPath, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			indexBytes += info.Size()
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	var p50, p95 int64
	if len(times) > 0 {
		p50, p95 = times[(len(times)-1)*50/100], times[(len(times)-1)*95/100]
	}
	outputData := map[string]interface{}{
		"documents":       len(docs),
		"queries":         len(queries),
		"build_ms":        buildMillis,
		"index_bytes":     indexBytes,
		"p50_ms":          p50,
		"p95_ms":          p95,
		"candidates":      candidates,
		"false_negatives": falseNegatives,
		"false_positives": falsePositives,
		"by_query_kind":   byKind,
		"query_semantics": "bleve default match analyzer; compared to normalized substring oracle",
	}
	data, err = json.MarshalIndent(outputData, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
