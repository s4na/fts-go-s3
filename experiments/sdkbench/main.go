// sdkbench compares SDK and s3lite operation sequences on the same storage.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	endpoint := os.Getenv("S3_ENDPOINT")
	liteMeter, sdkMeter := &s3lite.Meter{}, &s3lite.Meter{}
	lite := &s3lite.Client{Endpoint: endpoint, AccessKey: "testaccess", SecretKey: "testsecret", HTTP: &http.Client{Transport: liteMeter, Timeout: 10 * time.Second}}
	sdk := s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "testaccess", SecretAccessKey: "testsecret"}, nil
		}),
		HTTPClient: &http.Client{Transport: sdkMeter, Timeout: 10 * time.Second},
		RetryMaxAttempts: 1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	bucket := "sdk-compare"
	result, err := lite.Request(ctx, "PUT", bucket, "", nil, nil, nil)
	if err != nil || result.Status != 200 {
		return fmt.Errorf("create bucket: %d %v %s", result.Status, err, result.Body)
	}
	start := time.Now()
	key := "lite/日本語 +:@"
	put, err := lite.Request(ctx, "PUT", bucket, key, nil, nil, []byte("abcdef"))
	if err != nil || put.Status != 200 {
		return fmt.Errorf("lite put: %d %v %s", put.Status, err, put.Body)
	}
	read, err := lite.Request(ctx, "GET", bucket, key, nil, http.Header{"Range": {"bytes=1-3"}}, nil)
	if err != nil || read.Status != 206 || string(read.Body) != "bcd" {
		return fmt.Errorf("lite range: %d %v %q", read.Status, err, read.Body)
	}
	head, err := lite.Request(ctx, "HEAD", bucket, key, nil, nil, nil)
	if err != nil || head.Status != 200 || head.Header.Get("ETag") == "" {
		return fmt.Errorf("lite head: %d %v", head.Status, err)
	}
	page, err := lite.List(ctx, bucket, "lite/", "")
	if err != nil || len(page.Contents) != 1 || page.Contents[0].Key != key {
		return fmt.Errorf("lite list: %+v %v", page, err)
	}
	conditional, err := lite.Request(ctx, "PUT", bucket, key, nil, http.Header{"If-None-Match": {"*"}}, []byte("overwrite"))
	if err != nil || conditional.Status != 412 {
		return fmt.Errorf("lite condition: %d %v", conditional.Status, err)
	}
	deleted, err := lite.Request(ctx, "DELETE", bucket, key, nil, nil, nil)
	if err != nil || deleted.Status != 204 {
		return fmt.Errorf("lite delete: %d %v", deleted.Status, err)
	}
	liteMillis := time.Since(start).Milliseconds()
	liteCounts := liteMeter.Snapshot()
	start = time.Now()
	key = "sdk/日本語 +:@"
	_, err = sdk.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("abcdef")})
	if err != nil {
		return fmt.Errorf("SDK put: %w", err)
	}
	got, err := sdk.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: aws.String("bytes=1-3")})
	if err != nil {
		return fmt.Errorf("SDK get: %w", err)
	}
	body, err := io.ReadAll(got.Body)
	closeErr := got.Body.Close()
	if err != nil || closeErr != nil || !bytes.Equal(body, []byte("bcd")) {
		return fmt.Errorf("SDK range %q: %v %v", body, err, closeErr)
	}
	sdkHead, err := sdk.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil || sdkHead.ETag == nil || *sdkHead.ETag == "" {
		return fmt.Errorf("SDK head: %v", err)
	}
	listed, err := sdk.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: aws.String("sdk/")})
	if err != nil || len(listed.Contents) != 1 || aws.ToString(listed.Contents[0].Key) != key {
		return fmt.Errorf("SDK list: %v", err)
	}
	_, err = sdk.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, IfNoneMatch: aws.String("*"), Body: strings.NewReader("overwrite")})
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) || responseError.HTTPStatusCode() != 412 {
		return fmt.Errorf("SDK condition: %v", err)
	}
	if _, err := sdk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		return fmt.Errorf("SDK delete: %w", err)
	}
	sdkMillis := time.Since(start).Milliseconds()
	resultJSON := map[string]interface{}{
		"lite_including_bucket": liteCounts,
		"sdk": sdkMeter.Snapshot(),
		"lite_ms": liteMillis,
		"sdk_ms": sdkMillis,
		"conditional_status": 412,
		"body_bytes_exclude_headers": true,
	}
	data, err := json.MarshalIndent(resultJSON, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile("sdk-results.json", data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
