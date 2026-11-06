package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	zenbun "github.com/s4na/zenbun"
	"github.com/s4na/zenbun/gcsstore"
	"github.com/s4na/zenbun/s3store"
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

// output receives help text; tests replace it.
var output io.Writer = os.Stdout

const usageText = `usage: zenbun <command> [options] [argument]

commands:
  sync              index documents under the source prefix
  search <query>    print keys of documents containing the query
  find <json>       search with a JSON query and print snippets
  read <key>        print lines of one document
  compact           merge index segments
  reclaim           delete unreferenced index objects after the grace period
  help              show this help

Options must come before the argument: zenbun search -index x query
Run "zenbun <command> -h" for the options of a command.
`

// argCounts lists the known commands with the positional arguments each takes,
// and a description of that argument used in error messages.
var argCounts = map[string]struct {
	count int
	what  string
}{
	"sync":    {0, ""},
	"search":  {1, "one query"},
	"find":    {1, "one JSON query"},
	"read":    {1, "one key"},
	"compact": {0, ""},
	"reclaim": {0, ""},
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(output, usageText)
		return nil
	}
	command := args[0]
	switch command {
	case "help", "-h", "--help", "-help":
		fmt.Fprint(output, usageText)
		return nil
	}
	spec, known := argCounts[command]
	if !known {
		return fmt.Errorf("unknown command %q (run \"zenbun help\" for usage)", command)
	}
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
	flags.IntVar(&findOptions.MaxBytes, "max-bytes", 0, "find, read: total text bytes (default 16384)")
	flags.StringVar(&findOptions.After, "after", "", "find: continue after this key (a previous result's next)")
	flags.BoolVar(&findOptions.Matches, "matches", false, "find: include matching byte ranges in each snippet")
	var readOptions zenbun.ReadOptions
	flags.IntVar(&readOptions.Start, "start", 1, "read: first line, from 1")
	flags.IntVar(&readOptions.End, "end", 0, "read: last line (default the end of the document)")
	flags.SetOutput(io.Discard)
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(output, "usage: zenbun %s [options]%s\n\noptions:\n", command, argSuffix(spec.what))
			flags.SetOutput(output)
			flags.PrintDefaults()
			return nil
		}
		return err
	}
	if flags.NArg() != spec.count {
		for _, arg := range flags.Args() {
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("%s: options must come before the argument (got %q after it)", command, arg)
			}
		}
		if spec.count == 0 {
			return fmt.Errorf("%s takes no arguments", command)
		}
		return fmt.Errorf("%s requires %s", command, spec.what)
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
		return zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error { return index.Sync(ctx, store, *source) })
	case "search":
		keys, err := index.Search(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		for _, key := range keys {
			fmt.Println(key)
		}
		return nil
	case "find":
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
	case "read":
		readOptions.MaxBytes = findOptions.MaxBytes
		result, err := index.Read(ctx, store, flags.Arg(0), readOptions)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(false)
		return encoder.Encode(result)
	case "compact":
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

func argSuffix(what string) string {
	if what == "" {
		return ""
	}
	return " <" + strings.TrimPrefix(strings.TrimPrefix(what, "one JSON "), "one ") + ">"
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
