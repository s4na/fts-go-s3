// Package s3store adapts an S3-compatible bucket to zenbun.Store.
package s3store

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	zenbun "github.com/s4na/zenbun"
)

// Config configures an S3-compatible bucket. Credentials are static; when
// omitted, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN are
// read from the environment. Endpoint must contain a scheme and host.
type Config struct {
	Endpoint, Bucket, Region           string
	AccessKey, SecretKey, SessionToken string
	VirtualHost                        bool
	HTTPClient                         *http.Client
}

var _ zenbun.Store = (*Store)(nil)

// Store implements zenbun.Store using the S3 API and Signature Version 4.
type Store struct {
	config   Config
	endpoint *url.URL
	client   *http.Client
}

// New creates a store for an S3-compatible bucket.
func New(config Config) (*Store, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("s3: invalid endpoint")
	}
	if config.Bucket == "" || strings.ContainsAny(config.Bucket, "/?#") {
		return nil, errors.New("s3: invalid bucket")
	}
	if config.AccessKey == "" && config.SecretKey == "" {
		config.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		config.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if config.SessionToken == "" {
			config.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
		}
	}
	if config.AccessKey == "" || config.SecretKey == "" {
		return nil, errors.New("s3: access key and secret key are required")
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Store{config: config, endpoint: endpoint, client: client}, nil
}

func escape(s string, slash bool) string {
	const digits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.~", rune(c)) || slash && c == '/' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(digits[c>>4])
			b.WriteByte(digits[c&15])
		}
	}
	return b.String()
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func signature(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

// maxErrorBody bounds how much of an HTTP error response is read.
const maxErrorBody = 64 << 10

func (s *Store) request(ctx context.Context, method, key string, query url.Values, headers http.Header, data []byte, retry bool) (int, http.Header, []byte, error) {
	u := *s.endpoint
	path := strings.TrimRight(u.Path, "/")
	if s.config.VirtualHost {
		u.Host = s.config.Bucket + "." + u.Host
	} else {
		path += "/" + s.config.Bucket
	}
	path += "/" + key
	u.Path = path
	u.RawPath = escape(path, true)
	u.RawQuery = strings.ReplaceAll(query.Encode(), "+", "%20")
	attempts := 1
	if retry {
		attempts = 4
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(1<<(attempt-1)) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, nil, nil, ctx.Err()
			case <-timer.C:
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(data))
		if err != nil {
			return 0, nil, nil, err
		}
		req.Header = headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		s.sign(req, data, time.Now().UTC())
		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, nil, ctx.Err()
			}
			if attempt+1 < attempts {
				continue
			}
			return 0, nil, nil, fmt.Errorf("s3: request: %w", err)
		}
		reader := io.Reader(resp.Body)
		if resp.StatusCode >= 400 {
			// Error bodies are only inspected for an error code; keep them bounded.
			reader = io.LimitReader(resp.Body, maxErrorBody)
		}
		body, readErr := io.ReadAll(reader)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return 0, nil, nil, fmt.Errorf("s3: read response: %w", readErr)
		}
		if closeErr != nil {
			return 0, nil, nil, fmt.Errorf("s3: close response: %w", closeErr)
		}
		if (resp.StatusCode == 429 || resp.StatusCode >= 500) && attempt+1 < attempts {
			continue
		}
		return resp.StatusCode, resp.Header, body, nil
	}
	panic("unreachable")
}

func (s *Store) sign(req *http.Request, data []byte, now time.Time) {
	date, stamp := now.Format("20060102"), now.Format("20060102T150405Z")
	payload := digest(data)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	if s.config.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.config.SessionToken)
	}
	canonical := map[string]string{"host": req.URL.Host}
	for name, values := range req.Header {
		parts := make([]string, len(values))
		for i, value := range values {
			parts[i] = strings.Join(strings.Fields(value), " ")
		}
		canonical[strings.ToLower(name)] = strings.Join(parts, ",")
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
	scope := date + "/" + s.config.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + digest([]byte(request))
	key := signature([]byte("AWS4"+s.config.SecretKey), date)
	key = signature(key, s.config.Region)
	key = signature(key, "s3")
	key = signature(key, "aws4_request")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.config.AccessKey+"/"+scope+", SignedHeaders="+signed+", Signature="+hex.EncodeToString(signature(key, toSign)))
}

// Error reports an S3 HTTP error not mapped to a Store sentinel. Code is the
// <Code> element of the XML error body (for example "NoSuchBucket"), or empty
// when the response carried none. Store methods return it as *Error; use
// errors.As to inspect it.
type Error struct {
	Operation  string
	StatusCode int
	Code       string
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("s3 %s: HTTP %d (%s)", e.Operation, e.StatusCode, e.Code)
	}
	return fmt.Sprintf("s3 %s: HTTP %d", e.Operation, e.StatusCode)
}

// errorCode extracts <Code> from an S3 XML error body, or "" if unparsable.
func errorCode(body []byte) string {
	var e struct {
		Code string `xml:"Code"`
	}
	if xml.Unmarshal(body, &e) != nil {
		return ""
	}
	return strings.TrimSpace(e.Code)
}

// objectMissing reports whether a 404 means the object (not the bucket) is
// absent. Only a missing-bucket style code is rejected: a 404 without a
// recognizable code (for example from a compatible server that sends no body)
// is treated as a missing object.
func objectMissing(body []byte) bool {
	code := errorCode(body)
	return code == "" || code == "NoSuchKey"
}

func responseError(operation string, status int, body []byte) error {
	switch status {
	case http.StatusNotFound:
		if objectMissing(body) {
			return fmt.Errorf("s3 %s: %w", operation, zenbun.ErrNotFound)
		}
		return &Error{Operation: operation, StatusCode: status, Code: errorCode(body)}
	case http.StatusPreconditionFailed, http.StatusConflict:
		return fmt.Errorf("s3 %s: %w", operation, zenbun.ErrConflict)
	case http.StatusRequestedRangeNotSatisfiable:
		return fmt.Errorf("s3 %s: %w", operation, zenbun.ErrInvalidRange)
	default:
		return &Error{Operation: operation, StatusCode: status, Code: errorCode(body)}
	}
}

// Get implements zenbun.Store.
func (s *Store) Get(ctx context.Context, key string, r *zenbun.ByteRange) (zenbun.Object, error) {
	headers := make(http.Header)
	if r != nil {
		if r.Start < 0 || r.End <= r.Start {
			return zenbun.Object{}, zenbun.ErrInvalidRange
		}
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Start, r.End-1))
	}
	status, header, body, err := s.request(ctx, http.MethodGet, key, nil, headers, nil, true)
	if err != nil {
		return zenbun.Object{}, err
	}
	if r == nil && status != http.StatusOK || r != nil && status != http.StatusPartialContent {
		return zenbun.Object{}, responseError("get", status, body)
	}
	if r != nil && int64(len(body)) != r.End-r.Start {
		return zenbun.Object{}, zenbun.ErrInvalidRange
	}
	return zenbun.Object{Data: body, ETag: header.Get("ETag")}, nil
}

// Put implements zenbun.Store.
func (s *Store) Put(ctx context.Context, key string, data []byte, options zenbun.PutOptions) (string, error) {
	if options.IfNoneMatch && options.IfMatch != "" {
		return "", errors.New("s3 put: mutually exclusive conditions")
	}
	headers := make(http.Header)
	if options.IfNoneMatch {
		headers.Set("If-None-Match", "*")
	}
	if options.IfMatch != "" {
		headers.Set("If-Match", options.IfMatch)
	}
	status, header, body, err := s.request(ctx, http.MethodPut, key, nil, headers, data, false)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound && options.IfMatch != "" && objectMissing(body) {
		return "", fmt.Errorf("s3 put: %w", zenbun.ErrConflict)
	}
	if status != http.StatusOK {
		return "", responseError("put", status, body)
	}
	etag := header.Get("ETag")
	if etag == "" {
		return "", errors.New("s3 put: missing ETag")
	}
	return etag, nil
}

// Delete implements zenbun.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	status, _, body, err := s.request(ctx, http.MethodDelete, key, nil, nil, nil, false)
	if err != nil {
		return err
	}
	switch {
	case status == http.StatusNoContent, status == http.StatusOK:
		return nil
	case status == http.StatusNotFound && objectMissing(body):
		return nil
	default:
		return responseError("delete", status, body)
	}
}

// List implements zenbun.Store.
func (s *Store) List(ctx context.Context, prefix string) ([]zenbun.ObjectInfo, error) {
	var result []zenbun.ObjectInfo
	token := ""
	for {
		query := url.Values{"list-type": {"2"}, "prefix": {prefix}, "encoding-type": {"url"}}
		if token != "" {
			query.Set("continuation-token", token)
		}
		status, _, body, err := s.request(ctx, http.MethodGet, "", query, nil, nil, true)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, responseError("list", status, body)
		}
		var page struct {
			Contents []struct {
				Key, ETag, LastModified string
				Size                    int64
			} `xml:"Contents"`
			Truncated bool   `xml:"IsTruncated"`
			Next      string `xml:"NextContinuationToken"`
		}
		if err := xml.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("s3 list: %w", err)
		}
		for _, item := range page.Contents {
			key, err := url.QueryUnescape(item.Key)
			if err != nil {
				return nil, fmt.Errorf("s3 list key: %w", err)
			}
			modified, err := time.Parse(time.RFC3339, item.LastModified)
			if err != nil {
				return nil, fmt.Errorf("s3 list timestamp: %w", err)
			}
			result = append(result, zenbun.ObjectInfo{Key: key, ETag: item.ETag, Size: item.Size, LastModified: modified})
		}
		if !page.Truncated {
			break
		}
		if page.Next == "" || page.Next == token {
			return nil, errors.New("s3 list: invalid continuation token")
		}
		token = page.Next
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
