package zenbun

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMirrorApplyAndSearch(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewMirror(index, store, "documents/")
	if err != nil {
		t.Fatal(err)
	}
	changes := []Change{
		{Key: "正解", Data: []byte("ｶﾀｶﾅと猫犬猫")},
		{Key: "候補", Data: []byte("猫犬と犬猫")},
	}
	if err := mirror.Apply(ctx, changes); err != nil {
		t.Fatal(err)
	}
	matches, err := mirror.Search(ctx, "猫犬猫")
	if err != nil || len(matches) != 1 || matches[0].Key != "正解" || string(matches[0].Data) != "ｶﾀｶﾅと猫犬猫" {
		t.Fatalf("exact search: %+v, %v", matches, err)
	}
	matches, err = mirror.Search(ctx, "カタカナ")
	if err != nil || len(matches) != 1 || matches[0].Key != "正解" {
		t.Fatalf("normalized search: %+v, %v", matches, err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "正解", Data: []byte("犬")}, {Key: "候補", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	matches, err = mirror.Search(ctx, "猫")
	if err != nil || len(matches) != 0 {
		t.Fatalf("updated search: %+v, %v", matches, err)
	}
	matches, err = mirror.Search(ctx, "犬")
	if err != nil || len(matches) != 1 || matches[0].Key != "正解" {
		t.Fatalf("new content: %+v, %v", matches, err)
	}
	if _, err := store.Get(ctx, "documents/候補", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted copy: %v", err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "候補", Delete: true}}); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
}

type mirrorFailStore struct {
	Store
	failKey    string
	failDelete string
}

func (s *mirrorFailStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if key == s.failKey {
		return "", errors.New("injected copy failure")
	}
	return s.Store.Put(ctx, key, data, options)
}

func (s *mirrorFailStore) Delete(ctx context.Context, key string) error {
	if key == s.failDelete {
		return errors.New("injected delete failure")
	}
	return s.Store.Delete(ctx, key)
}

func TestMirrorRetriesPartialWritesAndIndexFailure(t *testing.T) {
	ctx := context.Background()
	memory := &MemoryStore{}
	indexStore := &failStore{Store: memory}
	index, _ := Open(indexStore, "index/")
	docs := &mirrorFailStore{Store: memory, failKey: "documents/b"}
	mirror, _ := NewMirror(index, docs, "documents/")
	batch := []Change{{Key: "a", Data: []byte("日本語")}, {Key: "b", Data: []byte("日本語")}}
	if err := mirror.Apply(ctx, batch); err == nil || !strings.Contains(err.Error(), "copy failure") {
		t.Fatalf("expected copy error: %v", err)
	}
	if got, err := mirror.Search(ctx, "日本語"); err != nil || len(got) != 0 {
		t.Fatalf("partial copy must not publish index: %+v, %v", got, err)
	}
	docs.failKey = ""
	indexStore.failManifest = true
	if err := mirror.Apply(ctx, batch); err == nil || !strings.Contains(err.Error(), "update mirror index") {
		t.Fatalf("expected publication error: %v", err)
	}
	indexStore.failManifest = false
	if err := mirror.Apply(ctx, batch); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err := mirror.Search(ctx, "日本語")
	if err != nil || len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("search after retry: %+v, %v", got, err)
	}
	if err := memory.Delete(ctx, "documents/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Search(ctx, "日本語"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing copy must be an error: %v", err)
	}
}

func TestMirrorRejectsInvalidBatchAndOverlappingPrefixes(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, _ := Open(store, "index/")
	for _, prefix := range []string{"", "index", "index/nested", "documents/../index", "/absolute"} {
		if _, err := NewMirror(index, store, prefix); err == nil {
			t.Errorf("accepted invalid or overlapping prefix %q", prefix)
		}
	}
	mirror, err := NewMirror(index, store, "documents/")
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range [][]Change{
		{{Key: "a", Data: []byte("good")}, {Key: "a", Delete: true}},
		{{Key: "a", Data: []byte("good")}, {Key: "b", Data: []byte{0xff}}},
		{{Key: "a", Data: []byte("good")}, {Key: "../index/manifest", Data: []byte("bad")}},
	} {
		if err := mirror.Apply(ctx, batch); err == nil {
			t.Fatalf("accepted invalid batch: %+v", batch)
		}
	}
	if _, err := store.Get(ctx, "documents/a", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid batch must not write copies: %v", err)
	}
}

func TestMirrorDeletePublicationAndRetry(t *testing.T) {
	ctx := context.Background()
	memory := &MemoryStore{}
	indexStore := &failStore{Store: memory}
	index, _ := Open(indexStore, "index/")
	docs := &mirrorFailStore{Store: memory}
	mirror, _ := NewMirror(index, docs, "documents/")
	if err := mirror.Apply(ctx, []Change{{Key: "a", Data: []byte("日本語の猫")}}); err != nil {
		t.Fatal(err)
	}
	deletion := []Change{{Key: "a", Delete: true}}
	indexStore.failManifest = true
	if err := mirror.Apply(ctx, deletion); err == nil {
		t.Fatal("expected index publication failure")
	}
	got, err := mirror.Search(ctx, "猫")
	if err != nil || len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("failed index update must leave searchable copy: %+v, %v", got, err)
	}
	indexStore.failManifest = false
	docs.failDelete = "documents/a"
	if err := mirror.Apply(ctx, deletion); err == nil || !strings.Contains(err.Error(), "delete document copy") {
		t.Fatalf("expected delete error: %v", err)
	}
	got, err = mirror.Search(ctx, "猫")
	if err != nil || len(got) != 0 {
		t.Fatalf("published deletion must not be searchable: %+v, %v", got, err)
	}
	if _, err := memory.Get(ctx, "documents/a", nil); err != nil {
		t.Fatalf("failed deletion must leave copy for retry: %v", err)
	}
	docs.failDelete = ""
	if err := mirror.Apply(ctx, deletion); err != nil {
		t.Fatalf("retry deletion: %v", err)
	}
	if _, err := memory.Get(ctx, "documents/a", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("copy after retry: %v", err)
	}
}

type mirrorReadCounter struct {
	Store
	reads int
}

func (s *mirrorReadCounter) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if strings.HasPrefix(key, "documents/") {
		s.reads++
	}
	return s.Store.Get(ctx, key, r)
}

func TestMirrorSearchLimit(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, _ := Open(store, "index/")
	docs := &mirrorReadCounter{Store: store}
	mirror, _ := NewMirror(index, docs, "documents/")
	changes := []Change{
		{Key: "a-false", Data: []byte("猫犬と犬猫")},
		{Key: "b-match", Data: []byte("猫犬猫")},
		{Key: "c-match", Data: []byte("猫犬猫")},
		{Key: "d-match", Data: []byte("猫犬猫")},
	}
	if err := mirror.Apply(ctx, changes); err != nil {
		t.Fatal(err)
	}
	got, err := mirror.SearchLimit(ctx, "猫犬猫", 2)
	if err != nil || len(got) != 2 || got[0].Key != "b-match" || got[1].Key != "c-match" {
		t.Fatalf("limited search: %+v, %v", got, err)
	}
	if docs.reads != 3 {
		t.Fatalf("limit must stop after two true matches: read %d copies", docs.reads)
	}
	if _, err := mirror.SearchLimit(ctx, "猫", 0); err == nil {
		t.Fatal("zero limit must be rejected")
	}
	if _, err := mirror.SearchLimit(ctx, "猫", -1); err == nil {
		t.Fatal("negative limit must be rejected")
	}
}
