// Package gcsstore adapts a Google Cloud Storage bucket to zenbun.Store using
// the JSON API. Object generations serve as ETags, so conditional writes use
// ifGenerationMatch.
package gcsstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	zenbun "github.com/s4na/fts-go-s3"
)

// DefaultEndpoint is the Google Cloud Storage JSON API endpoint.
const DefaultEndpoint = "https://storage.googleapis.com"

// GCSConfig configures a bucket. AccessToken is a static OAuth 2.0 access token
// sent as a bearer token; when it is empty and HTTPClient is nil,
// GOOGLE_OAUTH_ACCESS_TOKEN is read from the environment. To refresh tokens,
// pass an authenticated HTTPClient such as one from golang.org/x/oauth2/google
// and leave AccessToken empty. Endpoint defaults to DefaultEndpoint.
type GCSConfig struct {
	Endpoint, Bucket, AccessToken string
	HTTPClient                    *http.Client
}

var _ zenbun.Store = (*GCSStore)(nil)

// GCSStore implements zenbun.Store using the Cloud Storage JSON API.
type GCSStore struct {
	config   GCSConfig
	endpoint *url.URL
	client   *http.Client
}

// NewGCSStore creates a store for a Cloud Storage bucket.
func NewGCSStore(config GCSConfig) (*GCSStore, error) {
	if config.Endpoint == "" {
		config.Endpoint = DefaultEndpoint
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("gcs: invalid endpoint")
	}
	if config.Bucket == "" || strings.ContainsAny(config.Bucket, "/?#") {
		return nil, errors.New("gcs: invalid bucket")
	}
	client := config.HTTPClient
	if config.AccessToken == "" && client == nil {
		config.AccessToken = os.Getenv("GOOGLE_OAUTH_ACCESS_TOKEN")
		if config.AccessToken == "" {
			return nil, errors.New("gcs: access token or authenticated HTTP client is required")
		}
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &GCSStore{config: config, endpoint: endpoint, client: client}, nil
}

// escapePath percent-encodes everything except unreserved characters, so
// object names containing "/", "+" or spaces stay a single path segment.
func escapePath(s string) string {
	const digits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.~", rune(c)) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(digits[c>>4])
			b.WriteByte(digits[c&15])
		}
	}
	return b.String()
}

func (s *GCSStore) objectURL(upload bool, key string, query url.Values) string {
	base := strings.TrimRight(s.endpoint.String(), "/")
	if upload {
		base += "/upload"
	}
	base += "/storage/v1/b/" + escapePath(s.config.Bucket) + "/o"
	if key != "" {
		base += "/" + escapePath(key)
	}
	if len(query) > 0 {
		base += "?" + strings.ReplaceAll(query.Encode(), "+", "%20")
	}
	return base
}

func (s *GCSStore) request(ctx context.Context, method, target string, headers http.Header, data []byte, retry bool) (int, http.Header, []byte, error) {
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
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(data))
		if err != nil {
			return 0, nil, nil, err
		}
		req.Header = headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		if s.config.AccessToken != "" {
			req.Header.Set("Authorization", "Bearer "+s.config.AccessToken)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, nil, ctx.Err()
			}
			if attempt+1 < attempts {
				continue
			}
			return 0, nil, nil, fmt.Errorf("gcs: request: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return 0, nil, nil, fmt.Errorf("gcs: read response: %w", readErr)
		}
		if closeErr != nil {
			return 0, nil, nil, fmt.Errorf("gcs: close response: %w", closeErr)
		}
		if (resp.StatusCode == 429 || resp.StatusCode >= 500) && attempt+1 < attempts {
			continue
		}
		return resp.StatusCode, resp.Header, body, nil
	}
	panic("unreachable")
}

// GCSError reports a Cloud Storage HTTP error not mapped to a Store sentinel.
type GCSError struct {
	Operation  string
	StatusCode int
}

func (e *GCSError) Error() string {
	return fmt.Sprintf("gcs %s: HTTP %d", e.Operation, e.StatusCode)
}

func gcsError(operation string, status int) error {
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("gcs %s: %w", operation, zenbun.ErrNotFound)
	case http.StatusPreconditionFailed, http.StatusConflict:
		return fmt.Errorf("gcs %s: %w", operation, zenbun.ErrConflict)
	case http.StatusRequestedRangeNotSatisfiable:
		return fmt.Errorf("gcs %s: %w", operation, zenbun.ErrInvalidRange)
	default:
		return &GCSError{Operation: operation, StatusCode: status}
	}
}

func (s *GCSStore) Get(ctx context.Context, key string, r *zenbun.ByteRange) (zenbun.Object, error) {
	headers := make(http.Header)
	if r != nil {
		if r.Start < 0 || r.End <= r.Start {
			return zenbun.Object{}, zenbun.ErrInvalidRange
		}
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Start, r.End-1))
	}
	target := s.objectURL(false, key, url.Values{"alt": {"media"}})
	status, header, body, err := s.request(ctx, http.MethodGet, target, headers, nil, true)
	if err != nil {
		return zenbun.Object{}, err
	}
	if r == nil && status != http.StatusOK || r != nil && status != http.StatusPartialContent {
		return zenbun.Object{}, gcsError("get", status)
	}
	if r != nil && int64(len(body)) != r.End-r.Start {
		return zenbun.Object{}, zenbun.ErrInvalidRange
	}
	generation := header.Get("X-Goog-Generation")
	if generation == "" {
		return zenbun.Object{}, errors.New("gcs get: missing generation")
	}
	return zenbun.Object{Data: body, ETag: generation}, nil
}

type objectResource struct {
	Name       string `json:"name"`
	Generation string `json:"generation"`
	Size       string `json:"size"`
	Updated    string `json:"updated"`
}

func (s *GCSStore) Put(ctx context.Context, key string, data []byte, options zenbun.PutOptions) (string, error) {
	if options.IfNoneMatch && options.IfMatch != "" {
		return "", errors.New("put: mutually exclusive conditions")
	}
	query := url.Values{"uploadType": {"media"}, "name": {key}}
	if options.IfNoneMatch {
		query.Set("ifGenerationMatch", "0")
	}
	if options.IfMatch != "" {
		// ETags are generations; anything else cannot match an existing object.
		if generation, err := strconv.ParseInt(options.IfMatch, 10, 64); err != nil || generation <= 0 {
			return "", fmt.Errorf("gcs put: %w", zenbun.ErrConflict)
		}
		query.Set("ifGenerationMatch", options.IfMatch)
	}
	headers := http.Header{"Content-Type": {"application/octet-stream"}}
	status, _, body, err := s.request(ctx, http.MethodPost, s.objectURL(true, "", query), headers, data, false)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound && options.IfMatch != "" {
		return "", fmt.Errorf("gcs put: %w", zenbun.ErrConflict)
	}
	if status != http.StatusOK {
		return "", gcsError("put", status)
	}
	var object objectResource
	if err := json.Unmarshal(body, &object); err != nil {
		return "", fmt.Errorf("gcs put: %w", err)
	}
	if object.Generation == "" {
		return "", errors.New("gcs put: missing generation")
	}
	return object.Generation, nil
}

func (s *GCSStore) Delete(ctx context.Context, key string) error {
	status, _, _, err := s.request(ctx, http.MethodDelete, s.objectURL(false, key, nil), nil, nil, false)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent && status != http.StatusOK && status != http.StatusNotFound {
		return gcsError("delete", status)
	}
	return nil
}

func (s *GCSStore) List(ctx context.Context, prefix string) ([]zenbun.ObjectInfo, error) {
	var result []zenbun.ObjectInfo
	token := ""
	for {
		query := url.Values{"prefix": {prefix}, "fields": {"items(name,generation,size,updated),nextPageToken"}}
		if token != "" {
			query.Set("pageToken", token)
		}
		status, _, body, err := s.request(ctx, http.MethodGet, s.objectURL(false, "", query), nil, nil, true)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, gcsError("list", status)
		}
		var page struct {
			Items []objectResource `json:"items"`
			Next  string           `json:"nextPageToken"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("gcs list: %w", err)
		}
		for _, item := range page.Items {
			size, err := strconv.ParseInt(item.Size, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("gcs list size: %w", err)
			}
			modified, err := time.Parse(time.RFC3339, item.Updated)
			if err != nil {
				return nil, fmt.Errorf("gcs list timestamp: %w", err)
			}
			result = append(result, zenbun.ObjectInfo{Key: item.Name, ETag: item.Generation, Size: size, LastModified: modified})
		}
		if page.Next == "" {
			break
		}
		if page.Next == token {
			return nil, errors.New("gcs list: invalid page token")
		}
		token = page.Next
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
