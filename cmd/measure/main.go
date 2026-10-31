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
	"sync"
	"sync/atomic"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
	"github.com/s4na/fts-go-s3/s3store"
	"golang.org/x/text/unicode/norm"
)

type meter struct {
	zenbun.Store
	Gets, Received, Puts atomic.Int64
	delay                time.Duration
	serialize            bool
	mu                   sync.Mutex
}

func (m *meter) Get(ctx context.Context, key string, r *zenbun.ByteRange) (zenbun.Object, error) {
	if m.serialize {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	if m.delay > 0 {
		timer := time.NewTimer(m.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return zenbun.Object{}, ctx.Err()
		case <-timer.C:
		}
	}
	obj, err := m.Store.Get(ctx, key, r)
	m.Gets.Add(1)
	m.Received.Add(int64(len(obj.Data)))
	return obj, err
}
func (m *meter) Put(ctx context.Context, key string, data []byte, opts zenbun.PutOptions) (string, error) {
	m.Puts.Add(1)
	return m.Store.Put(ctx, key, data, opts)
}

type document struct{ ID, Text string }
type query struct{ Query string }

// readRatio compares the index bytes read for all queries with a full scan of
// the original UTF-8 document bytes for each query. A zero denominator is
// represented as unavailable, rather than emitting invalid JSON numbers.
func readRatio(sourceBytes, received int64, queries int) (fraction, inverse *float64) {
	if sourceBytes <= 0 || received < 0 || queries <= 0 {
		return nil, nil
	}
	value := (float64(received) / float64(queries)) / float64(sourceBytes)
	fraction = &value
	if value > 0 {
		ratio := 1 / value
		inverse = &ratio
	}
	return fraction, inverse
}

func run() error {
	corpus := flag.String("corpus", "", "input JSONL corpus")
	queries := flag.String("queries", "testdata/queries.json", "query JSON")
	output := flag.String("output", "metrics.json", "output metrics")
	batch := flag.Int("batch", 1000000, "documents per segment")
	endpoint := flag.String("endpoint", "", "optional S3 endpoint")
	flag.Parse()
	if *batch < 1 {
		return fmt.Errorf("batch must be positive")
	}
	var backend zenbun.Store = &zenbun.MemoryStore{}
	if *endpoint != "" {
		var err error
		backend, err = s3store.NewS3Store(s3store.S3Config{Endpoint: *endpoint, Bucket: "zenbun-measure", AccessKey: "testaccess", SecretKey: "testsecret"})
		if err != nil {
			return err
		}
	}
	ctx := context.Background()
	store := &meter{Store: backend}
	index, err := zenbun.Open(store, "index/")
	if err != nil {
		return err
	}
	started := time.Now()
	f, err := os.Open(*corpus)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var docs []document
	var sourceBytes int64
	var changes []zenbun.Change
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64<<20)
	for scanner.Scan() {
		var doc document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		sourceBytes += int64(len(doc.Text))
		changes = append(changes, zenbun.Change{Key: doc.ID, Data: []byte(doc.Text)})
		if len(changes) == *batch {
			if err := index.Apply(ctx, changes); err != nil {
				return err
			}
			changes = nil
			_, _ = fmt.Fprintf(os.Stderr, "indexed %d documents\n", len(docs)+1)
		}
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
	if len(changes) > 0 {
		if err := index.Apply(ctx, changes); err != nil {
			return err
		}
	}
	buildTime := time.Since(started)
	store.Gets.Store(0)
	store.Received.Store(0)
	store.Puts.Store(0)
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
	gets, received := store.Gets.Load(), store.Received.Load()
	// Measure real elapsed time with a representative 20ms delay per GET.
	// Serializing GETs provides a same-corpus, warm-cache comparison.
	store.delay = 20 * time.Millisecond
	sampleCount := min(3, len(items))
	measureLatency := func(serial bool) (float64, error) {
		store.serialize = serial
		started := time.Now()
		for _, item := range items[:sampleCount] {
			if _, err := index.Search(ctx, item.Query); err != nil {
				return 0, err
			}
		}
		return time.Since(started).Seconds(), nil
	}
	parallelSeconds, err := measureLatency(false)
	if err != nil {
		return err
	}
	serialSeconds, err := measureLatency(true)
	if err != nil {
		return err
	}
	store.delay = 0
	store.serialize = false
	store.Puts.Store(0)
	if err := index.Apply(ctx, []zenbun.Change{{Key: "measurement-update", Data: []byte("updated document")}}); err != nil {
		return err
	}
	manifest, err := store.Get(ctx, "index/manifest", nil)
	if err != nil {
		return err
	}
	fraction, inverse := readRatio(sourceBytes, received, len(items))
	metrics := map[string]any{"source_document_bytes": sourceBytes, "index_to_full_scan_fraction": fraction, "full_scan_to_index_ratio": inverse, "batch_size": *batch, "documents": len(docs), "queries": len(items), "false_negatives": 0, "search_gets": gets, "search_received_bytes": received, "gets_per_query": float64(gets) / float64(len(items)), "bytes_per_query": float64(received) / float64(len(items)), "single_update_puts": store.Puts.Load(), "manifest_bytes": len(manifest.Data), "build_seconds": buildTime.Seconds(), "latency_sample_queries": sampleCount, "simulated_get_latency_ms": 20, "parallel_latency_seconds": parallelSeconds, "serialized_latency_seconds": serialSeconds}
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
