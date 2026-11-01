package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
	"github.com/s4na/fts-go-s3/gcsstore"
	"github.com/s4na/fts-go-s3/s3store"
)

// storageConfig holds connection flags. Empty endpoint and bucket fall back to
// environment variables named for the selected storage.
type storageConfig struct {
	kind, endpoint, bucket, region string
	virtualHost                    bool
}

func newStore(config storageConfig) (zenbun.Store, error) {
	switch config.kind {
	case "s3":
		return s3store.NewS3Store(s3store.S3Config{
			Endpoint:    firstNonEmpty(config.endpoint, os.Getenv("S3_ENDPOINT")),
			Bucket:      firstNonEmpty(config.bucket, os.Getenv("S3_BUCKET")),
			Region:      firstNonEmpty(config.region, os.Getenv("AWS_REGION")),
			VirtualHost: config.virtualHost,
		})
	case "gcs":
		if config.region != "" || config.virtualHost {
			return nil, fmt.Errorf("-region and -virtual-host apply only to s3")
		}
		return gcsstore.NewGCSStore(gcsstore.GCSConfig{
			Endpoint: firstNonEmpty(config.endpoint, os.Getenv("GCS_ENDPOINT")),
			Bucket:   firstNonEmpty(config.bucket, os.Getenv("GCS_BUCKET")),
		})
	default:
		return nil, fmt.Errorf("unknown storage %q (want s3 or gcs)", config.kind)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: zenbun {sync|search|find|compact|reclaim} [options]")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	var config storageConfig
	flags.StringVar(&config.kind, "storage", firstNonEmpty(os.Getenv("ZENBUN_STORAGE"), "s3"), "storage adapter: s3 or gcs")
	flags.StringVar(&config.endpoint, "endpoint", "", "endpoint URL (default $S3_ENDPOINT or $GCS_ENDPOINT)")
	flags.StringVar(&config.bucket, "bucket", "", "bucket (default $S3_BUCKET or $GCS_BUCKET)")
	flags.StringVar(&config.region, "region", "", "S3 region (default $AWS_REGION)")
	prefix := flags.String("index", "zenbun-index/", "index prefix")
	source := flags.String("source", "documents/", "source prefix for sync")
	flags.BoolVar(&config.virtualHost, "virtual-host", false, "use S3 virtual-host addressing")
	grace := flags.Duration("grace", 24*time.Hour, "reclaim grace period")
	var findOptions zenbun.FindOptions
	flags.Func("mode", "find output: snippets, keys or count (default snippets)", func(value string) error {
		findOptions.Mode = zenbun.FindMode(value)
		return nil
	})
	flags.IntVar(&findOptions.Limit, "limit", 0, "find: maximum matching documents (default 20)")
	flags.IntVar(&findOptions.Context, "context", 0, "find: lines around each matching line")
	flags.IntVar(&findOptions.MaxSnippetsPerDoc, "max-snippets", 0, "find: snippets per document (default 3)")
	flags.IntVar(&findOptions.MaxBytes, "max-bytes", 0, "find: total snippet bytes (default 16384)")
	flags.StringVar(&findOptions.After, "after", "", "find: continue after this key (a previous result's next)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	store, err := newStore(config)
	if err != nil {
		return err
	}
	index, err := zenbun.Open(store, *prefix)
	if err != nil {
		return err
	}
	ctx := context.Background()
	switch command {
	case "sync":
		if flags.NArg() != 0 {
			return fmt.Errorf("sync takes no arguments")
		}
		return zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error { return index.Sync(ctx, store, *source) })
	case "search":
		if flags.NArg() != 1 {
			return fmt.Errorf("search requires one query")
		}
		keys, err := index.Search(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		for _, key := range keys {
			fmt.Println(key)
		}
		return nil
	case "find":
		if flags.NArg() != 1 {
			return fmt.Errorf("find requires one JSON query")
		}
		query, err := parseQuery(flags.Arg(0))
		if err != nil {
			return err
		}
		result, err := index.Find(ctx, store, query, findOptions)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(false)
		return encoder.Encode(result)
	case "compact":
		if flags.NArg() != 0 {
			return fmt.Errorf("compact takes no arguments")
		}
		for {
			var changed bool
			err := zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
				var err error
				changed, err = index.Compact(ctx)
				return err
			})
			if err != nil || !changed {
				return err
			}
		}
	case "reclaim":
		if flags.NArg() != 0 {
			return fmt.Errorf("reclaim takes no arguments")
		}
		var count int
		err := zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
			var err error
			count, err = index.Reclaim(ctx, *grace)
			return err
		})
		if err == nil {
			fmt.Println(count)
		}
		return err
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

// parseQuery rejects unknown fields so a misspelled condition is not ignored.
func parseQuery(text string) (zenbun.Query, error) {
	var query zenbun.Query
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&query); err != nil {
		return zenbun.Query{}, fmt.Errorf("invalid query JSON: %w", err)
	}
	// More misses a stray closing bracket, so require the end of input instead.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return zenbun.Query{}, fmt.Errorf("invalid query JSON: trailing data")
	}
	return query, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
