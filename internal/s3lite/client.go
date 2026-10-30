// Package s3lite implements the small subset of S3 needed by the experiment.
package s3lite

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Client struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	HTTP      *http.Client
}

type Result struct {
	Status int
	Header http.Header
	Body   []byte
}

// Request sends a signed, path-style S3 request. Non-2xx status codes are
// returned to the caller so conditional-write failures remain observable.
func (c *Client) Request(ctx context.Context, method, bucket, key string, query url.Values, headers http.Header, body []byte) (Result, error) {
	base, err := url.Parse(c.Endpoint)
	if err != nil {
		return Result{}, err
	}
	if base.Scheme == "" || base.Host == "" || base.RawQuery != "" || base.Fragment != "" {
		return Result{}, fmt.Errorf("invalid endpoint %q", c.Endpoint)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + bucket
	if key != "" {
		base.Path += "/" + key
	}
	base.RawPath = s3Path(base.Path)
	base.RawQuery = strings.ReplaceAll(query.Encode(), "+", "%20")
	req, err := http.NewRequestWithContext(ctx, method, base.String(), bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	c.sign(req, body, time.Now().UTC())
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, err
	}
	return Result{Status: resp.StatusCode, Header: resp.Header.Clone(), Body: data}, nil
}

func s3Path(path string) string {
	const hexDigits = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(path); i++ {
		b := path[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-_.~/", rune(b)) {
			out.WriteByte(b)
		} else {
			out.WriteByte('%')
			out.WriteByte(hexDigits[b>>4])
			out.WriteByte(hexDigits[b&15])
		}
	}
	return out.String()
}

func hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mac(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func (c *Client) sign(req *http.Request, body []byte, now time.Time) {
	date := now.Format("20060102")
	stamp := now.Format("20060102T150405Z")
	payload := hash(body)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	canonical := map[string]string{"host": req.URL.Host}
	for name, values := range req.Header {
		name = strings.ToLower(name)
		if name == "authorization" {
			continue
		}
		parts := make([]string, len(values))
		for i, value := range values {
			parts[i] = strings.Join(strings.Fields(value), " ")
		}
		canonical[name] = strings.Join(parts, ",")
	}
	names := make([]string, 0, len(canonical))
	for name := range canonical {
		names = append(names, name)
	}
	sort.Strings(names)
	var headers strings.Builder
	for _, name := range names {
		fmt.Fprintf(&headers, "%s:%s\n", name, canonical[name])
	}
	signed := strings.Join(names, ";")
	request := strings.Join([]string{req.Method, req.URL.EscapedPath(), req.URL.RawQuery, headers.String(), signed, payload}, "\n")
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	scope := date + "/" + region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hash([]byte(request))
	key := mac([]byte("AWS4"+c.SecretKey), date)
	key = mac(key, region)
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	signature := hex.EncodeToString(mac(key, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.AccessKey+"/"+scope+", SignedHeaders="+signed+", Signature="+signature)
}

// ListPage exposes continuation tokens; callers must follow them until empty.
type ListPage struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	Next      string `xml:"NextContinuationToken"`
	Truncated bool   `xml:"IsTruncated"`
}

func (c *Client) List(ctx context.Context, bucket, prefix, token string) (ListPage, error) {
	q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
	if token != "" {
		q.Set("continuation-token", token)
	}
	result, err := c.Request(ctx, http.MethodGet, bucket, "", q, nil, nil)
	if err != nil {
		return ListPage{}, err
	}
	if result.Status != http.StatusOK {
		return ListPage{}, fmt.Errorf("list: HTTP %d: %s", result.Status, result.Body)
	}
	var page ListPage
	err = xml.Unmarshal(result.Body, &page)
	return page, err
}
