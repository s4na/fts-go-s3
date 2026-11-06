// Command measure verifies recall against a linear scan and records I/O costs.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	zenbun "github.com/s4na/zenbun"
	"github.com/s4na/zenbun/s3store"
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

// corpusDocuments calls fn for each document in a JSONL corpus, holding only
// one document at a time.
func corpusDocuments(path string, fn func(document) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64<<20)
	for scanner.Scan() {
		var doc document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		if err := fn(doc); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func normalize(s string) string { return strings.ToLower(norm.NFKC.String(s)) }

const sourcePrefix = "docs/"

func run() error {
	corpus := flag.String("corpus", "", "input JSONL corpus")
	queries := flag.String("queries", "testdata/queries.json", "query JSON")
	output := flag.String("output", "metrics.json", "output metrics")
	batch := flag.Int("batch", 1000000, "documents per segment")
	endpoint := flag.String("endpoint", "", "optional S3 endpoint")
	dataDir := flag.String("data-dir", "", "directory for the on-disk index and source (default: a temporary directory)")
	budgetMiB := flag.Int("memory-budget-mib", 0, "fail when an operation's peak heap delta exceeds this many MiB (0 disables)")
	measureSync := flag.Bool("sync", true, "also measure Sync from an on-disk source")
	flag.Parse()
	if *batch < 1 {
		return errors.New("batch must be positive")
	}
	if *budgetMiB < 0 {
		return errors.New("memory budget must not be negative")
	}
	if *dataDir == "" {
		dir, err := os.MkdirTemp("", "zenbun-measure-")
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(dir) }()
		*dataDir = dir
	}
	var backend zenbun.Store
	if *endpoint != "" {
		var err error
		backend, err = s3store.New(s3store.Config{Endpoint: *endpoint, Bucket: "zenbun-measure", AccessKey: "testaccess", SecretKey: "testsecret"})
		if err != nil {
			return err
		}
	} else {
		disk, err := newDiskStore(filepath.Join(*dataDir, "index"))
		if err != nil {
			return err
		}
		backend = disk
	}
	source, err := newDiskStore(filepath.Join(*dataDir, "source"))
	if err != nil {
		return err
	}
	ctx := context.Background()
	store := &meter{Store: backend}
	open := func(prefix string) (*zenbun.Index, error) { return zenbun.Open(store, prefix) }

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

	// The source copy is written outside measured operations.
	var sourceBytes int64
	var documents int
	var firstKey string
	if err := corpusDocuments(*corpus, func(doc document) error {
		sourceBytes += int64(len(doc.Text))
		if documents == 0 {
			firstKey = doc.ID
		}
		documents++
		if !*measureSync {
			return nil
		}
		_, err := source.Put(ctx, sourcePrefix+doc.ID, []byte(doc.Text), zenbun.PutOptions{})
		return err
	}); err != nil {
		return err
	}
	if documents == 0 {
		return errors.New("corpus has no documents")
	}

	operations := make(map[string]operation)
	measure := func(name string, fn func() error) error {
		op, err := measureOperation(store, fn)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		operations[name] = op
		_, _ = fmt.Fprintf(os.Stderr, "%s: %.1fs, peak heap delta %d bytes\n", name, op.Seconds, op.PeakHeapDeltaBytes)
		return nil
	}

	// Each operation opens a fresh Index so caches from earlier operations do
	// not count toward its baseline.
	if err := measure("build", func() error {
		index, err := open("index/")
		if err != nil {
			return err
		}
		var changes []zenbun.Change
		indexed := 0
		if err := corpusDocuments(*corpus, func(doc document) error {
			changes = append(changes, zenbun.Change{Key: doc.ID, Data: []byte(doc.Text)})
			indexed++
			if len(changes) < *batch {
				return nil
			}
			if err := index.Apply(ctx, changes); err != nil {
				return err
			}
			changes = nil
			_, _ = fmt.Fprintf(os.Stderr, "indexed %d documents\n", indexed)
			return nil
		}); err != nil {
			return err
		}
		if len(changes) > 0 {
			return index.Apply(ctx, changes)
		}
		return nil
	}); err != nil {
		return err
	}

	searchIndex, err := open("index/")
	if err != nil {
		return err
	}
	results := make([][]string, len(items))
	if err := measure("search", func() error {
		for n, item := range items {
			keys, err := searchIndex.Search(ctx, item.Query)
			if err != nil {
				return err
			}
			results[n] = keys
		}
		return nil
	}); err != nil {
		return err
	}
	gets, received := operations["search"].Gets, operations["search"].ReceivedBytes

	// Verify recall against a linear scan, streaming the corpus once.
	normalizedQueries := make([]string, len(items))
	for n, item := range items {
		normalizedQueries[n] = normalize(item.Query)
		sort.Strings(results[n])
	}
	if err := corpusDocuments(*corpus, func(doc document) error {
		text := normalize(doc.Text)
		for n, q := range normalizedQueries {
			if !strings.Contains(text, q) {
				continue
			}
			keys := results[n]
			if at := sort.SearchStrings(keys, doc.ID); at == len(keys) || keys[at] != doc.ID {
				return fmt.Errorf("false negative: %q in %q", items[n].Query, doc.ID)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Measure real elapsed time with a representative 20ms delay per GET.
	// Serializing GETs provides a same-corpus, warm-cache comparison.
	store.delay = 20 * time.Millisecond
	sampleCount := min(3, len(items))
	measureLatency := func(serial bool) (float64, error) {
		store.serialize = serial
		started := time.Now()
		for _, item := range items[:sampleCount] {
			if _, err := searchIndex.Search(ctx, item.Query); err != nil {
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

	if err := measure("apply_one", func() error {
		index, err := open("index/")
		if err != nil {
			return err
		}
		return index.Apply(ctx, []zenbun.Change{{Key: "measurement-update", Data: []byte("updated document")}})
	}); err != nil {
		return err
	}
	manifest, err := store.Get(ctx, "index/manifest", nil)
	if err != nil {
		return err
	}

	compactions := 0
	if err := measure("compact_all", func() error {
		index, err := open("index/")
		if err != nil {
			return err
		}
		for {
			merged, err := index.Compact(ctx)
			if err != nil || !merged {
				return err
			}
			compactions++
		}
	}); err != nil {
		return err
	}

	if *measureSync {
		runSync := func() error {
			index, err := open("sync-index/")
			if err != nil {
				return err
			}
			return index.Sync(ctx, source, sourcePrefix)
		}
		if err := measure("sync_initial", runSync); err != nil {
			return err
		}
		if err := measure("sync_unchanged", runSync); err != nil {
			return err
		}
		if _, err := source.Put(ctx, sourcePrefix+firstKey, []byte("updated document"), zenbun.PutOptions{}); err != nil {
			return err
		}
		if err := measure("sync_one_change", runSync); err != nil {
			return err
		}
	}

	var budget uint64
	if *budgetMiB > 0 {
		budget = uint64(*budgetMiB) << 20
	}
	fraction, inverse := readRatio(sourceBytes, received, len(items))
	metrics := map[string]any{"source_document_bytes": sourceBytes, "index_to_full_scan_fraction": fraction, "full_scan_to_index_ratio": inverse, "batch_size": *batch, "documents": documents, "queries": len(items), "false_negatives": 0, "search_gets": gets, "search_received_bytes": received, "gets_per_query": float64(gets) / float64(len(items)), "bytes_per_query": float64(received) / float64(len(items)), "single_update_puts": operations["apply_one"].Puts, "manifest_bytes": len(manifest.Data), "build_seconds": operations["build"].Seconds, "latency_sample_queries": sampleCount, "simulated_get_latency_ms": 20, "parallel_latency_seconds": parallelSeconds, "serialized_latency_seconds": serialSeconds, "compactions": compactions, "memory_budget_bytes": budget, "operations": operations}
	encoded, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	if err := os.WriteFile(*output, encoded, 0644); err != nil {
		return err
	}
	if over := overBudget(operations, budget); len(over) > 0 {
		return fmt.Errorf("peak heap delta exceeds the %d MiB budget: %s", *budgetMiB, strings.Join(over, ", "))
	}
	return nil
}

// overBudget lists operations whose peak heap delta exceeds budget, in name
// order. A zero budget disables the check.
func overBudget(operations map[string]operation, budget uint64) []string {
	if budget == 0 {
		return nil
	}
	var over []string
	for name, op := range operations {
		if op.PeakHeapDeltaBytes > budget {
			over = append(over, fmt.Sprintf("%s=%d", name, op.PeakHeapDeltaBytes))
		}
	}
	sort.Strings(over)
	return over
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
