package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.Background()
	client := s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "testaccess", SecretAccessKey: "testsecret"}, nil
		}),
	}, func(o *s3.Options) { o.BaseEndpoint = aws.String(os.Getenv("S3_ENDPOINT")); o.UsePathStyle = true })
	bucket, key := "compare", "key"
	put, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("abcdef")})
	fmt.Println(put, err)
	get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err == nil {
		_, _ = io.Copy(io.Discard, get.Body)
		_ = get.Body.Close()
	}
	fmt.Println(err)
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	fmt.Println(head, err)
	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	fmt.Println(listed, err)
	deleted, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	fmt.Println(deleted, err)
}
