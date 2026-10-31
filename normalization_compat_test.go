package zenbun

import (
	"context"
	"testing"
)

// TestSearchIndexFromPreviousNormalization guards persisted indexes across
// normalization changes. x/text v0.39.0 normalized U+05D2 U+0307 to U+105C9
// and therefore saved its unigram below. x/text v0.43.0 fixes the normalization
// and produces two different runes, so a matching query must still find the
// already indexed document or the upgrade must provide an index migration.
func TestSearchIndexFromPreviousNormalization(t *testing.T) {
	ctx := context.Background()
	index, err := Open(&MemoryStore{}, "index/")
	if err != nil {
		t.Fatal(err)
	}

	// Use the term persisted by the previous version instead of recalculating
	// it with the version under test. Recalculating would hide incompatibility.
	ref, err := index.writeSegment(ctx, map[string]indexedDocument{
		"old-document": {Terms: []string{"u\U000105c9"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.publish(ctx, manifest{Segments: []segmentRef{ref}}, ""); err != nil {
		t.Fatal(err)
	}

	got, err := index.Search(ctx, "\u05d2\u0307")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "old-document" {
		t.Fatalf("searching an index written with the previous normalization: got %q, want [old-document]; a normalization change requires migrating or rebuilding existing indexes", got)
	}
}
