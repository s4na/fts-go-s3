package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/s4na/fts-go-s3/internal/s3lite"
)

func main() {
	ctx := context.Background()
	client := &s3lite.Client{Endpoint: os.Getenv("S3_ENDPOINT"), AccessKey: "testaccess", SecretKey: "testsecret"}
	for _, method := range []string{"PUT", "GET", "HEAD", "DELETE"} {
		result, err := client.Request(ctx, method, "compare", "key", nil, http.Header{}, []byte("abcdef"))
		fmt.Println(result.Status, err)
	}
	result, err := client.List(ctx, "compare", "", "")
	fmt.Println(len(result.Contents), err)
}
