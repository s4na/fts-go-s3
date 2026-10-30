package zenbun

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Sync mirrors every object under sourcePrefix into the index, using ETags to
// detect changes. Existing indexed keys absent from the source are deleted.
// It is intended for an index dedicated to one source prefix; source listing
// and reads must be stable or externally coordinated during synchronization.
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
		reader, err := openSegment(ctx, i.store, ref)
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
	var changes []Change
	for _, info := range objects {
		if info.ETag == "" {
			return fmt.Errorf("zenbun: source %q has no ETag", info.Key)
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
		changes = append(changes, Change{Key: info.Key, Data: obj.Data, ETag: obj.ETag})
	}
	for key, doc := range indexed {
		if !doc.Deleted && !seen[key] {
			changes = append(changes, Change{Key: key, Delete: true})
		}
	}
	// Apply must publish against the manifest used to compute the diff. Calling
	// Apply directly could instead overwrite a concurrent index update.
	if len(changes) == 0 {
		return nil
	}
	docs := make(map[string]indexedDocument, len(changes))
	for _, change := range changes {
		if change.Delete {
			docs[change.Key] = indexedDocument{Deleted: true}
		} else {
			if !validDocument(change.Key, change.Data) {
				return fmt.Errorf("zenbun: invalid UTF-8 in source %q", change.Key)
			}
			docs[change.Key] = indexedDocument{ETag: change.ETag, Terms: terms(string(change.Data))}
		}
	}
	// Publish against the version from which the diff was computed.
	ref, err := i.writeSegment(ctx, docs)
	if err != nil {
		return err
	}
	m.Segments = append(m.Segments, ref)
	return i.publish(ctx, m, etag)
}
