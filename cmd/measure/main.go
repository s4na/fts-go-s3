// Command measure verifies recall against a linear scan and records I/O costs.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
	"golang.org/x/text/unicode/norm"
)

type meter struct {
	zenbun.Store
	Gets, Received, Puts int64
}

func (m *meter) Get(ctx context.Context, key string, r *zenbun.ByteRange) (zenbun.Object, error) {
	obj, err := m.Store.Get(ctx, key, r)
	m.Gets++
	m.Received += int64(len(obj.Data))
	return obj, err
}
func (m *meter) Put(ctx context.Context, key string, data []byte, opts zenbun.PutOptions) (string, error) {
	m.Puts++
	return m.Store.Put(ctx, key, data, opts)
}

type document struct{ ID, Text string }
type query struct{ Query string }

func run() error {
	corpus := flag.String("corpus", "", "input JSONL corpus")
	queries := flag.String("queries", "testdata/queries.json", "query JSON")
	output := flag.String("output", "metrics.json", "output metrics")
	flag.Parse()
	f, err := os.Open(*corpus)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var docs []document
	var changes []zenbun.Change
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64<<20)
	for scanner.Scan() {
		var doc document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		changes = append(changes, zenbun.Change{Key: doc.ID, Data: []byte(doc.Text)})
		doc.Text = strings.ToLower(norm.NFKC.String(doc.Text))
		docs = append(docs, doc)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	data, err := os.ReadFile(*queries)
	if err != nil {
		return err
	}
	var items []query
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	if len(items) != 178 {
		return fmt.Errorf("expected 178 queries, got %d", len(items))
	}
	ctx := context.Background()
	store := &meter{Store: &zenbun.MemoryStore{}}
	index, err := zenbun.Open(store, "index/")
	if err != nil {
		return err
	}
	started := time.Now()
	if err := index.Apply(ctx, changes); err != nil {
		return err
	}
	buildTime := time.Since(started)
	store.Gets, store.Received, store.Puts = 0, 0, 0
	for _, item := range items {
		keys, err := index.Search(ctx, item.Query)
		if err != nil {
			return err
		}
		found := make(map[string]bool, len(keys))
		for _, key := range keys {
			found[key] = true
		}
		q := strings.ToLower(norm.NFKC.String(item.Query))
		for _, doc := range docs {
			if strings.Contains(doc.Text, q) && !found[doc.ID] {
				return fmt.Errorf("false negative: %q in %q", item.Query, doc.ID)
			}
		}
	}
	gets, received := store.Gets, store.Received
	store.Puts = 0
	if err := index.Apply(ctx, []zenbun.Change{{Key: "measurement-update", Data: []byte("updated document")}}); err != nil {
		return err
	}
	manifest, err := store.Get(ctx, "index/manifest", nil)
	if err != nil {
		return err
	}
	metrics := map[string]any{"documents": len(docs), "queries": len(items), "false_negatives": 0, "search_gets": gets, "search_received_bytes": received, "gets_per_query": float64(gets) / float64(len(items)), "bytes_per_query": float64(received) / float64(len(items)), "single_update_puts": store.Puts, "manifest_bytes": len(manifest.Data), "build_seconds": buildTime.Seconds()}
	encoded, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return os.WriteFile(*output, encoded, 0644)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
