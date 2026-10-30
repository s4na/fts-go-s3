// stressbench measures update amplification and query degradation against an oracle.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/s4na/fts-go-s3/internal/index"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

type snapshot struct {
	Stage          string `json:"stage"`
	Segments       int    `json:"segments"`
	Queries        int    `json:"queries"`
	Gets           int64  `json:"gets"`
	ReceivedBytes  int64  `json:"received_bytes"`
	FalseNegatives int    `json:"false_negatives"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	docsPath := flag.String("docs", "", "input JSONL")
	singles := flag.Int("singles", 1000, "individual updates")
	batches := flag.Int("batches", 10, "100-document batches")
	output := flag.String("out", "stress.json", "output JSON")
	flag.Parse()
	f, err := os.Open(*docsPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 64*1024*1024)
	var base []oracle.Document
	for len(base) < 100 && scan.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scan.Bytes(), &doc); err != nil {
			return err
		}
		base = append(base, doc)
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if len(base) != 100 {
		return fmt.Errorf("need 100 documents, got %d", len(base))
	}
	active := make(map[string]oracle.Document)
	for _, doc := range base {
		active[doc.ID] = doc
	}
	meter := &s3lite.Meter{}
	client := &s3lite.Client{Endpoint: os.Getenv("S3_ENDPOINT"), AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: meter, Timeout: 30 * time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	r, err := client.Request(ctx, "PUT", "fts-stress", "", nil, nil, nil)
	if err != nil || r.Status != 200 {
		return fmt.Errorf("create bucket: %d %v %s", r.Status, err, r.Body)
	}
	idx := index.Index{S3: client, Bucket: "fts-stress", Prefix: "index"}
	if _, err := idx.Build(ctx, base, 16); err != nil {
		return err
	}
	var snapshots []snapshot
	defer func() {
		if runErr == nil {
			return
		}
		data, err := json.MarshalIndent(map[string]interface{}{"error": runErr.Error(), "snapshots": snapshots}, "", "  ")
		if err == nil {
			err = os.WriteFile(*output, data, 0644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "write failure report:", err)
		}
	}()
	check := func(stage string) error {
		manifest, err := idx.Manifest(ctx)
		if err != nil {
			return err
		}
		var docs []oracle.Document
		for _, doc := range active {
			docs = append(docs, doc)
		}
		truth := oracle.New(docs)
		before := meter.Snapshot()
		s := snapshot{Stage: stage, Segments: len(manifest.Segments)}
		for _, q := range []string{"検証更新", "日本", "猫", "𪛖𪛖𪛖𪛖𪛖𪛖"} {
			candidates, err := idx.Search(ctx, q, nil)
			if err != nil {
				return err
			}
			set := make(map[string]bool)
			for _, id := range candidates {
				set[id] = true
			}
			for _, id := range truth.Search(q) {
				if !set[id] {
					s.FalseNegatives++
				}
			}
			s.Queries++
		}
		after := meter.Snapshot()
		s.Gets = after.Requests["GET"] - before.Requests["GET"]
		s.ReceivedBytes = after.ReceivedBytes - before.ReceivedBytes
		snapshots = append(snapshots, s)
		if s.FalseNegatives != 0 {
			return fmt.Errorf("%s: %d false negatives", stage, s.FalseNegatives)
		}
		return nil
	}
	if err := check("initial"); err != nil {
		return err
	}
	before := meter.Snapshot()
	start := time.Now()
	for n := 0; n < *singles; n++ {
		original := base[n%len(base)]
		_, exists := active[original.ID]
		if n%10 == 0 && exists {
			if err := idx.Update(ctx, original.ID, nil); err != nil {
				return err
			}
			delete(active, original.ID)
		} else {
			doc := original
			if n%3 == 0 {
				runes := []rune(doc.Text)
				doc.Text = string(runes[:len(runes)/2])
			}
			doc.Text += "\n検証更新"
			if err := idx.Update(ctx, doc.ID, &doc); err != nil {
				return err
			}
			active[doc.ID] = doc
		}
		if n+1 == 10 || n+1 == 100 || n+1 == *singles {
			if err := check(fmt.Sprintf("single-%d", n+1)); err != nil {
				return err
			}
		}
	}
	afterSingles := meter.Snapshot()
	for n := 0; n < *batches; n++ {
		var changes []index.Change
		for _, original := range base {
			doc := original
			doc.Text += "\n検証更新 一括"
			changes = append(changes, index.Change{ID: doc.ID, Document: &doc})
			active[doc.ID] = doc
		}
		if err := idx.Apply(ctx, changes); err != nil {
			return err
		}
	}
	afterBatches := meter.Snapshot()
	if err := check("after-batches"); err != nil {
		return err
	}
	beforeCompact := meter.Snapshot()
	if err := idx.Compact(ctx); err != nil {
		return err
	}
	afterCompact := meter.Snapshot()
	if err := check("after-compact"); err != nil {
		return err
	}
	payload := map[string]interface{}{
		"singles":           *singles,
		"single_puts":       afterSingles.Requests["PUT"] - before.Requests["PUT"],
		"single_sent_bytes": afterSingles.SentBytes - before.SentBytes,
		"batches":           *batches,
		"batch_documents":   100,
		"batch_puts":        afterBatches.Requests["PUT"] - afterSingles.Requests["PUT"],
		"batch_sent_bytes":  afterBatches.SentBytes - afterSingles.SentBytes,
		"compact_gets":      afterCompact.Requests["GET"] - beforeCompact.Requests["GET"],
		"compact_puts":      afterCompact.Requests["PUT"] - beforeCompact.Requests["PUT"],
		"elapsed_ms":        time.Since(start).Milliseconds(),
		"snapshots":         snapshots,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
