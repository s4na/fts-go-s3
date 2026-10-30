package zenbun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore is a concurrency-safe, in-memory Store. Its zero value is ready
// for use. Returned data and stored input are independent copies.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]memoryObject
}

type memoryObject struct {
	data []byte
	etag string
	time time.Time
}

func (s *MemoryStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	obj, ok := s.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	data := obj.data
	if r != nil {
		if r.Start < 0 || r.End <= r.Start || r.End > int64(len(data)) {
			return Object{}, ErrInvalidRange
		}
		data = data[r.Start:r.End]
	}
	return Object{Data: append([]byte(nil), data...), ETag: obj.etag}, nil
}

func (s *MemoryStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if options.IfNoneMatch && options.IfMatch != "" {
		return "", errors.New("put: mutually exclusive conditions")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	old, exists := s.objects[key]
	if options.IfNoneMatch && exists || options.IfMatch != "" && (!exists || old.etag != options.IfMatch) {
		return "", ErrConflict
	}
	if s.objects == nil {
		s.objects = make(map[string]memoryObject)
	}
	copied := append([]byte(nil), data...)
	sum := sha256.Sum256(copied)
	etag := "\"" + hex.EncodeToString(sum[:]) + "\""
	s.objects[key] = memoryObject{data: copied, etag: etag, time: time.Now().UTC()}
	return etag, nil
}

// Delete succeeds if the object is already absent.
func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(s.objects, key)
	return nil
}

func (s *MemoryStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []ObjectInfo
	for key, obj := range s.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, ObjectInfo{Key: key, ETag: obj.etag, Size: int64(len(obj.data)), LastModified: obj.time})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
