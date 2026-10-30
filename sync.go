package zenbun

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// syncBatchSize bounds the number of changed document bodies held at once.
const syncBatchSize = 1000

// Sync mirrors every object under sourcePrefix into the index, using ETags to
// detect changes. Existing indexed keys absent from the source are deleted.
// It is intended for an index dedicated to one source prefix; source listing
// and reads must be stable or externally coordinated during synchronization.
// Large changes are published in batches: if an error occurs, prior batches
// remain visible and a subsequent Sync can resume. Each batch uses manifest CAS.
func (i *Index) Sync(ctx context.Context, source Store, sourcePrefix string) error {
	if source == nil || sourcePrefix == "" {
		return errors.New("zenbun: source and source prefix are required")
	}
	if strings.HasPrefix(i.prefix, sourcePrefix) || strings.HasPrefix(sourcePrefix, i.prefix) {
		return errors.New("zenbun: source and index prefixes overlap")
	}
	m, etag, err := i.loadManifest(ctx)
	if err != nil {
		return err
	}
	indexed := make(map[string]indexedDocument)
	for _, ref := range m.Segments {
		reader, err := i.segment(ctx, ref)
		if err != nil {
			return err
		}
		docs, err := segmentDocuments(ctx, reader)
		if err != nil {
			return err
		}
		for key, doc := range docs {
			indexed[key] = doc
		}
	}
	objects, err := source.List(ctx, sourcePrefix)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(objects))
	batch := make(map[string]indexedDocument, syncBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ref, err := i.writeSegment(ctx, batch)
		if err != nil {
			return err
		}
		m.Segments = append(m.Segments, ref)
		etag, err = i.publishWithETag(ctx, m, etag)
		if err != nil {
			return err
		}
		if etag == "" {
			return errors.New("zenbun: manifest has no ETag")
		}
		batch = make(map[string]indexedDocument, syncBatchSize)
		return nil
	}
	for _, info := range objects {
		if info.ETag == "" {
			return fmt.Errorf("zenbun: source %q has no ETag", info.Key)
		}
		if seen[info.Key] {
			return fmt.Errorf("zenbun: duplicate source key %q", info.Key)
		}
		seen[info.Key] = true
		old, exists := indexed[info.Key]
		if exists && !old.Deleted && old.ETag == info.ETag {
			continue
		}
		obj, err := source.Get(ctx, info.Key, nil)
		if err != nil {
			return err
		}
		if obj.ETag != info.ETag {
			return fmt.Errorf("zenbun: source %q changed during sync: %w", info.Key, ErrConflict)
		}
		if !validDocument(info.Key, obj.Data) {
			return fmt.Errorf("zenbun: invalid UTF-8 in source %q", info.Key)
		}
		batch[info.Key] = indexedDocument{ETag: info.ETag, Terms: terms(string(obj.Data))}
		if len(batch) >= syncBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	for key, doc := range indexed {
		if !doc.Deleted && !seen[key] {
			batch[key] = indexedDocument{Deleted: true}
			if len(batch) >= syncBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}
