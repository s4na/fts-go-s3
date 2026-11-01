package zenbun

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func newQueryIndex(t *testing.T, docs map[string]string) (*Index, *MemoryStore) {
	t.Helper()
	ctx := context.Background()
	store := &MemoryStore{}
	for key, body := range docs {
		if _, err := store.Put(ctx, key, []byte(body), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	return index, store
}

func TestSearchQueryCombinesGroups(t *testing.T) {
	ctx := context.Background()
	index, _ := newQueryIndex(t, map[string]string{
		"docs/a":  "猫の請求書",
		"docs/b":  "ネコの請求書（下書き）",
		"docs/c":  "猫の写真",
		"docs/d":  "犬の請求書",
		"other/e": "猫の請求書",
	})
	keys, err := index.SearchQuery(ctx, Query{All: [][]string{{"猫", "ネコ"}, {"請求書"}}})
	if err != nil || !reflect.DeepEqual(keys, []string{"docs/a", "docs/b"}) {
		t.Fatalf("and of ors: %v %v", keys, err)
	}
	keys, err = index.SearchQuery(ctx, Query{All: [][]string{{"猫", "犬"}}, KeyPrefix: "docs/c"})
	if err != nil || !reflect.DeepEqual(keys, []string{"docs/c"}) {
		t.Fatalf("key prefix: %v %v", keys, err)
	}
	keys, err = index.SearchQuery(ctx, Query{All: [][]string{{"象"}}})
	if err != nil || len(keys) != 0 {
		t.Fatalf("absent word: %v %v", keys, err)
	}
	single, err := index.Search(ctx, "請求書")
	if err != nil || !reflect.DeepEqual(single, []string{"docs/a", "docs/b", "docs/d"}) {
		t.Fatalf("search compatibility: %v %v", single, err)
	}
}

func TestQueryValidation(t *testing.T) {
	index, store := newQueryIndex(t, map[string]string{"docs/a": "猫"})
	invalid := []Query{
		{},
		{All: [][]string{{}}},
		{All: [][]string{{""}}},
		{All: [][]string{{"猫"}}, None: []string{""}},
		{All: [][]string{{strings.Repeat("猫", 100)}}},
		{All: [][]string{{"猫\nネコ"}}},
		{All: make([][]string, maxQueryGroups+1)},
	}
	for _, q := range invalid {
		if _, err := index.SearchQuery(context.Background(), q); err == nil {
			t.Fatalf("accepted %+v", q)
		}
	}
	// The word limit applies to structured queries only, not to Search.
	if keys, err := index.Search(context.Background(), strings.Repeat("猫", 100)); err != nil || len(keys) != 0 {
		t.Fatalf("long search: %v %v", keys, err)
	}
	for _, options := range []FindOptions{{Mode: "all"}, {Limit: -1}, {Context: maxFindContext + 1}} {
		if _, err := index.Find(context.Background(), store, Query{All: [][]string{{"猫"}}}, options); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
}

func TestFindVerifiesAndReturnsSnippets(t *testing.T) {
	ctx := context.Background()
	index, store := newQueryIndex(t, map[string]string{
		"docs/a": "一行目\n猫の請求書です\n三行目\n四行目\n五行目\nネコの写真\n七行目",
		"docs/b": "猫の請求書（下書き）",
		"docs/c": "猫犬と犬猫の請求書",
		"docs/d": "猫と請求\n書",
	})
	q := Query{All: [][]string{{"猫", "ネコ"}, {"請求書"}}, None: []string{"下書き"}}
	result, err := index.Find(ctx, store, q, FindOptions{Context: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := FindResult{Count: 2, Candidates: 3, Documents: []FoundDocument{
		{Key: "docs/a", Lines: 2, Snippets: []Snippet{
			{Line: 1, Text: "一行目\n猫の請求書です\n三行目"},
			{Line: 5, Text: "五行目\nネコの写真\n七行目"},
		}},
		{Key: "docs/c", Lines: 1, Snippets: []Snippet{{Line: 1, Text: "猫犬と犬猫の請求書"}}},
	}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("find:\n got %+v\nwant %+v", result, want)
	}

	exact, err := index.Find(ctx, store, Query{All: [][]string{{"猫犬猫"}}}, FindOptions{Mode: FindKeys})
	if err != nil || exact.Count != 0 || exact.Candidates != 1 || len(exact.Documents) != 0 {
		t.Fatalf("false positive: %+v %v", exact, err)
	}
	count, err := index.Find(ctx, store, q, FindOptions{Mode: FindCount})
	if err != nil || count.Count != 2 || len(count.Documents) != 0 {
		t.Fatalf("count: %+v %v", count, err)
	}
	keys, err := index.Find(ctx, store, q, FindOptions{Mode: FindKeys})
	if err != nil || len(keys.Documents) != 2 || keys.Documents[1].Key != "docs/c" || keys.Documents[1].Snippets != nil {
		t.Fatalf("keys: %+v %v", keys, err)
	}

	// A document deleted after Sync is skipped rather than failing the search.
	if err := store.Delete(ctx, "docs/a"); err != nil {
		t.Fatal(err)
	}
	result, err = index.Find(ctx, store, q, FindOptions{Mode: FindKeys})
	if err != nil || result.Count != 1 || result.Documents[0].Key != "docs/c" {
		t.Fatalf("deleted source: %+v %v", result, err)
	}
	if _, err := index.Find(ctx, nil, q, FindOptions{}); err == nil {
		t.Fatal("accepted a nil source")
	}
}

func TestFindPagesWithLimitAndCursor(t *testing.T) {
	ctx := context.Background()
	docs := map[string]string{}
	for _, key := range []string{"docs/1", "docs/2", "docs/3", "docs/4", "docs/5"} {
		docs[key] = "猫"
	}
	docs["docs/0"] = "犬"
	index, store := newQueryIndex(t, docs)
	q := Query{All: [][]string{{"猫"}}}
	var got []string
	after := ""
	for page := 0; page < 5; page++ {
		result, err := index.Find(ctx, store, q, FindOptions{Mode: FindKeys, Limit: 2, After: after})
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range result.Documents {
			got = append(got, doc.Key)
		}
		if !result.Truncated {
			if result.Next != "" {
				t.Fatalf("next without truncation: %+v", result)
			}
			break
		}
		after = result.Next
	}
	if !reflect.DeepEqual(got, []string{"docs/1", "docs/2", "docs/3", "docs/4", "docs/5"}) {
		t.Fatalf("pages: %v", got)
	}
	result, err := index.Find(ctx, store, q, FindOptions{Mode: FindCount, Limit: 2})
	if err != nil || result.Count != 2 || result.Candidates != 5 || !result.Truncated || result.Next != "docs/2" {
		t.Fatalf("count limit: %+v %v", result, err)
	}
}

// readLogStore records the keys of full-document reads.
type readLogStore struct {
	Store
	mu   sync.Mutex
	read []string
}

func (s *readLogStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if r == nil {
		s.mu.Lock()
		s.read = append(s.read, key)
		s.mu.Unlock()
	}
	return s.Store.Get(ctx, key, r)
}

// slowReadStore delays full-document reads and records how many overlap.
type slowReadStore struct {
	Store
	fail     string
	mu       sync.Mutex
	inFlight int
	maxIn    int
	reads    int
}

func (s *slowReadStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if r != nil {
		return s.Store.Get(ctx, key, r)
	}
	s.mu.Lock()
	s.reads++
	s.inFlight++
	s.maxIn = max(s.maxIn, s.inFlight)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return Object{}, ctx.Err()
	}
	if key == s.fail {
		return Object{}, errors.New("injected read failure")
	}
	return s.Store.Get(ctx, key, r)
}

func TestFindFilterSkipsKeysBeforeReading(t *testing.T) {
	ctx := context.Background()
	docs := map[string]string{}
	for _, key := range []string{"docs/1", "docs/2", "docs/3", "docs/4", "docs/5"} {
		docs[key] = "猫"
	}
	index, store := newQueryIndex(t, docs)
	source := &readLogStore{Store: store}
	q := Query{All: [][]string{{"猫"}}}
	hidden := map[string]bool{"docs/2": true, "docs/4": true}
	allow := func(key string) bool { return !hidden[key] }

	result, err := index.Find(ctx, source, q, FindOptions{Mode: FindKeys, Limit: 2, Filter: allow})
	if err != nil {
		t.Fatal(err)
	}
	want := FindResult{Count: 2, Candidates: 3, Documents: []FoundDocument{{Key: "docs/1"}, {Key: "docs/3"}}, Truncated: true, Next: "docs/3"}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("filtered page:\n got %+v\nwant %+v", result, want)
	}
	result, err = index.Find(ctx, source, q, FindOptions{Mode: FindKeys, Limit: 2, After: result.Next, Filter: allow})
	want = FindResult{Count: 1, Candidates: 1, Documents: []FoundDocument{{Key: "docs/5"}}}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("next page: %+v %v", result, err)
	}
	for _, key := range source.read {
		if hidden[key] {
			t.Fatalf("read excluded document %q (reads %v)", key, source.read)
		}
	}

	// Mirror.Find applies the filter to document IDs.
	mirrorStore := &MemoryStore{}
	mirrorIndex, err := Open(mirrorStore, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewMirror(mirrorIndex, mirrorStore, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "public/1", Data: []byte("猫")}, {Key: "private/1", Data: []byte("猫")}}); err != nil {
		t.Fatal(err)
	}
	result, err = mirror.Find(ctx, q, FindOptions{Filter: func(key string) bool { return strings.HasPrefix(key, "public/") }})
	if err != nil || result.Count != 1 || result.Candidates != 1 || result.Documents[0].Key != "public/1" {
		t.Fatalf("mirror filter: %+v %v", result, err)
	}
}

func TestFindReadsBodiesConcurrentlyInKeyOrder(t *testing.T) {
	ctx := context.Background()
	docs := map[string]string{}
	for n := range 10 {
		body := "猫犬猫"
		if n%3 == 0 {
			body = "猫犬と犬猫" // an index candidate that fails the body check
		}
		docs[fmt.Sprintf("docs/%02d", n)] = body
	}
	index, store := newQueryIndex(t, docs)
	q := Query{All: [][]string{{"猫犬猫"}}}
	serial, err := index.Find(ctx, store, q, FindOptions{Concurrency: 1, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	slow := &slowReadStore{Store: store}
	parallel, err := index.Find(ctx, slow, q, FindOptions{Concurrency: 3, Limit: 100})
	if err != nil || !reflect.DeepEqual(parallel, serial) {
		t.Fatalf("parallel result differs:\n got %+v %v\nwant %+v", parallel, err, serial)
	}
	if slow.maxIn < 2 || slow.maxIn > 3 {
		t.Fatalf("overlapping reads: %d, want 2 to 3", slow.maxIn)
	}

	// Find stops at docs/02 after reading 3 bodies; reads ahead of it are
	// canceled, so at most Concurrency-1 extra reads start.
	slow = &slowReadStore{Store: store}
	limited, err := index.Find(ctx, slow, q, FindOptions{Mode: FindKeys, Concurrency: 3, Limit: 2})
	if err != nil || limited.Count != 2 || !limited.Truncated {
		t.Fatalf("limited: %+v %v", limited, err)
	}
	if slow.reads > 3+2 {
		t.Fatalf("read %d bodies to check 3 candidates with concurrency 3", slow.reads)
	}
	if slow.inFlight != 0 {
		t.Fatalf("reads still running after Find returned: %d", slow.inFlight)
	}

	slow = &slowReadStore{Store: store, fail: "docs/04"}
	if _, err := index.Find(ctx, slow, q, FindOptions{Concurrency: 3, Limit: 100}); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("read failure: %v", err)
	}
	for _, c := range []int{-1, maxFindReads + 1} {
		if _, err := index.Find(ctx, store, q, FindOptions{Concurrency: c}); err == nil {
			t.Fatalf("accepted concurrency %d", c)
		}
	}
}

func TestFindBoundsSnippets(t *testing.T) {
	ctx := context.Background()
	index, store := newQueryIndex(t, map[string]string{
		"docs/a": "猫1\nx\n猫2\nx\n猫3\nx\n猫4",
		"docs/b": "猫" + strings.Repeat("あ", 20),
		"docs/c": "猫c",
	})
	q := Query{All: [][]string{{"猫"}}}
	result, err := index.Find(ctx, store, q, FindOptions{MaxSnippetsPerDoc: 2})
	if err != nil || result.Documents[0].Lines != 4 || len(result.Documents[0].Snippets) != 2 || result.Documents[0].Snippets[1] != (Snippet{Line: 3, Text: "猫2"}) {
		t.Fatalf("snippets per document: %+v %v", result, err)
	}
	// Adjacent context windows merge into one snippet.
	result, err = index.Find(ctx, store, q, FindOptions{Context: 1, MaxSnippetsPerDoc: 1})
	if err != nil || result.Documents[0].Snippets[0].Text != "猫1\nx\n猫2\nx\n猫3\nx\n猫4" {
		t.Fatalf("merged context: %+v %v", result, err)
	}
	// docs/a uses 4 bytes per snippet line; docs/b is cut at a rune boundary.
	result, err = index.Find(ctx, store, q, FindOptions{MaxSnippetsPerDoc: 1, MaxBytes: 10})
	if err != nil || !result.Truncated || result.Next != "docs/b" || len(result.Documents) != 2 || result.Documents[1].Snippets[0].Text != "猫あ" {
		t.Fatalf("byte budget: %+v %v", result, err)
	}
	result, err = index.Find(ctx, store, q, FindOptions{MaxSnippetsPerDoc: 1, MaxBytes: 4})
	if err != nil || !result.Truncated || result.Next != "docs/a" || result.Count != 1 {
		t.Fatalf("exhausted budget: %+v %v", result, err)
	}
}

func TestMirrorFind(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewMirror(index, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []Change{
		{Key: "user/1", Data: []byte("ｶﾀｶﾅの猫")},
		{Key: "user/2", Data: []byte("猫")},
		{Key: "team/1", Data: []byte("カタカナの猫")},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := mirror.Find(ctx, Query{All: [][]string{{"カタカナ"}}, KeyPrefix: "user/"}, FindOptions{})
	if err != nil || result.Count != 1 || result.Documents[0].Key != "user/1" || result.Documents[0].Snippets[0].Text != "ｶﾀｶﾅの猫" {
		t.Fatalf("mirror find: %+v %v", result, err)
	}
	if err := store.Delete(ctx, "copies/user/2"); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Find(ctx, Query{All: [][]string{{"猫"}}}, FindOptions{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing copy: %v", err)
	}
}
