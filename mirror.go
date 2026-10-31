package zenbun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Mirror optionally stores document copies next to an index. Document IDs are
// relative to prefix, contain no empty, dot or dot-dot path components, and
// must be unique within the mirror. It owns its document prefix and index; do
// not mix its updates with Index.Apply or Sync.
//
// Apply is retryable but not atomic across document copies and the index. A
// caller must durably retain changes, serialize all writers for this mirror,
// and retry a failed batch before processing newer changes. Searches concurrent
// with updates may observe a partially updated mirror.
type Mirror struct {
	index  *Index
	store  Store
	prefix string
}

// NewMirror associates an index with document copies under a separate prefix.
// Prefixes must not overlap, even when the stores are different.
func NewMirror(index *Index, store Store, prefix string) (*Mirror, error) {
	if index == nil || store == nil {
		return nil, errors.New("zenbun: index and document store are required")
	}
	prefix = strings.TrimRight(prefix, "/")
	if !validMirrorKey(prefix) || !validMirrorKey(strings.TrimSuffix(index.prefix, "/")) {
		return nil, errors.New("zenbun: invalid document or index prefix")
	}
	prefix += "/"
	if strings.HasPrefix(prefix, index.prefix) || strings.HasPrefix(index.prefix, prefix) {
		return nil, errors.New("zenbun: document and index prefixes overlap")
	}
	return &Mirror{index: index, store: store, prefix: prefix}, nil
}

func validMirrorKey(key string) bool {
	if key == "" || !utf8.ValidString(key) {
		return false
	}
	for _, component := range strings.Split(key, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

// Apply saves or deletes document copies and publishes the corresponding index
// changes. It returns an error if either stage fails. Changes may already be
// partially visible on error; retry the same batch before newer changes.
// Change.ETag is ignored: the copy's ETag is recorded in the index instead.
func (m *Mirror) Apply(ctx context.Context, changes []Change) error {
	seen := make(map[string]bool, len(changes))
	for _, change := range changes {
		if !validMirrorKey(change.Key) || !change.Delete && !utf8.Valid(change.Data) {
			return errors.New("zenbun: invalid document key or UTF-8 content")
		}
		if seen[change.Key] {
			return errors.New("zenbun: duplicate key in batch")
		}
		seen[change.Key] = true
	}

	indexed := make([]Change, 0, len(changes))
	for _, change := range changes {
		key := m.prefix + change.Key
		if change.Delete {
			if err := m.store.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("zenbun: delete document copy %q: %w", change.Key, err)
			}
			indexed = append(indexed, Change{Key: change.Key, Delete: true})
			continue
		}
		etag, err := m.store.Put(ctx, key, change.Data, PutOptions{})
		if err != nil {
			return fmt.Errorf("zenbun: save document copy %q: %w", change.Key, err)
		}
		indexed = append(indexed, Change{Key: change.Key, Data: change.Data, ETag: etag})
	}
	if err := m.index.Apply(ctx, indexed); err != nil {
		return fmt.Errorf("zenbun: update mirror index: %w", err)
	}
	return nil
}

// MirroredDocument is a matching document copy and its original ID.
type MirroredDocument struct {
	Key  string
	Data []byte
}

// Search reads candidate copies and returns only documents containing query
// after NFKC normalization and lowercasing. A missing or unreadable copy is an
// error, not an empty result. Ordering follows Index.Search's key ordering.
func (m *Mirror) Search(ctx context.Context, query string) ([]MirroredDocument, error) {
	keys, err := m.index.Search(ctx, query)
	if err != nil {
		return nil, err
	}
	needle := strings.ToLower(norm.NFKC.String(query))
	var result []MirroredDocument
	for _, key := range keys {
		obj, err := m.store.Get(ctx, m.prefix+key, nil)
		if err != nil {
			return nil, fmt.Errorf("zenbun: read document copy %q: %w", key, err)
		}
		if !utf8.Valid(obj.Data) {
			return nil, fmt.Errorf("zenbun: document copy %q is not valid UTF-8", key)
		}
		if strings.Contains(strings.ToLower(norm.NFKC.String(string(obj.Data))), needle) {
			result = append(result, MirroredDocument{Key: key, Data: obj.Data})
		}
	}
	return result, nil
}
