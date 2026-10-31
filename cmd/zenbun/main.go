package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
	"github.com/s4na/fts-go-s3/s3store"
)

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: zenbun {sync|search|compact|reclaim} [options]")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	endpoint := flags.String("endpoint", os.Getenv("S3_ENDPOINT"), "S3 endpoint URL")
	bucket := flags.String("bucket", os.Getenv("S3_BUCKET"), "S3 bucket")
	region := flags.String("region", os.Getenv("AWS_REGION"), "S3 region")
	prefix := flags.String("index", "zenbun-index/", "index prefix")
	source := flags.String("source", "documents/", "source prefix for sync")
	virtual := flags.Bool("virtual-host", false, "use virtual-host addressing")
	grace := flags.Duration("grace", 24*time.Hour, "reclaim grace period")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	store, err := s3store.NewS3Store(s3store.S3Config{Endpoint: *endpoint, Bucket: *bucket, Region: *region, VirtualHost: *virtual})
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

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
