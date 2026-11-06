package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	zenbun "github.com/s4na/zenbun"
)

// diskStore keeps objects in files so that measured indexes and source
// documents do not occupy the process heap. Each object is one data file
// named by the hex-encoded key and one sidecar file holding its ETag.
type diskStore struct {
	dir string
	mu  sync.Mutex // serializes conditional writes
}

func newDiskStore(dir string) (*diskStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &diskStore{dir: dir}, nil
}

const etagSuffix = ".etag"

func (s *diskStore) path(key string) (string, error) {
	name := hex.EncodeToString([]byte(key))
	if len(name)+len(etagSuffix) > 255 {
		return "", fmt.Errorf("disk store: key too long: %q", key)
	}
	return filepath.Join(s.dir, name), nil
}

func (s *diskStore) etag(path string) (string, error) {
	data, err := os.ReadFile(path + etagSuffix)
	if errors.Is(err, os.ErrNotExist) {
		return "", zenbun.ErrNotFound
	}
	return string(data), err
}

func (s *diskStore) Get(ctx context.Context, key string, r *zenbun.ByteRange) (zenbun.Object, error) {
	if err := ctx.Err(); err != nil {
		return zenbun.Object{}, err
	}
	path, err := s.path(key)
	if err != nil {
		return zenbun.Object{}, err
	}
	// Writers replace the ETag after the data, so read it under the lock to
	// return a consistent pair.
	s.mu.Lock()
	defer s.mu.Unlock()
	etag, err := s.etag(path)
	if err != nil {
		return zenbun.Object{}, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return zenbun.Object{}, zenbun.ErrNotFound
	}
	if err != nil {
		return zenbun.Object{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return zenbun.Object{}, err
	}
	start, end := int64(0), info.Size()
	if r != nil {
		if r.Start < 0 || r.End <= r.Start || r.End > info.Size() {
			return zenbun.Object{}, zenbun.ErrInvalidRange
		}
		start, end = r.Start, r.End
	}
	data := make([]byte, end-start)
	if _, err := f.ReadAt(data, start); err != nil && !errors.Is(err, io.EOF) {
		return zenbun.Object{}, err
	}
	return zenbun.Object{Data: data, ETag: etag}, nil
}

func (s *diskStore) Put(ctx context.Context, key string, data []byte, options zenbun.PutOptions) (string, error) {
	if options.IfNoneMatch && options.IfMatch != "" {
		return "", errors.New("put: mutually exclusive conditions")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := s.path(key)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.etag(path)
	exists := err == nil
	if err != nil && !errors.Is(err, zenbun.ErrNotFound) {
		return "", err
	}
	if options.IfNoneMatch && exists || options.IfMatch != "" && (!exists || old != options.IfMatch) {
		return "", zenbun.ErrConflict
	}
	sum := sha256.Sum256(data)
	etag := "\"" + hex.EncodeToString(sum[:]) + "\""
	if err := writeFile(path, data); err != nil {
		return "", err
	}
	if err := writeFile(path+etagSuffix, []byte(etag)); err != nil {
		return "", err
	}
	return etag, nil
}

func writeFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Delete succeeds if the object is already absent.
func (s *diskStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(path + etagSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *diskStore) List(ctx context.Context, prefix string) ([]zenbun.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var result []zenbun.ObjectInfo
	for _, entry := range entries {
		name := entry.Name()
		if strings.Contains(name, ".") {
			continue
		}
		raw, err := hex.DecodeString(name)
		if err != nil {
			continue
		}
		key := string(raw)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		etag, err := s.etag(filepath.Join(s.dir, name))
		if err != nil {
			return nil, err
		}
		result = append(result, zenbun.ObjectInfo{Key: key, ETag: etag, Size: info.Size(), LastModified: info.ModTime().UTC()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
