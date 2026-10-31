package zenbun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/text/unicode/norm"
)

func normalized(s string) string { return strings.ToLower(norm.NFKC.String(s)) }

func assertOracle(t *testing.T, index *Index, docs map[string]string, queries []string) {
	t.Helper()
	for _, query := range queries {
		keys, err := index.Search(context.Background(), query)
		if err != nil {
			t.Fatalf("search %q: %v", query, err)
		}
		got := make(map[string]bool)
		for _, key := range keys {
			got[key] = true
			if _, exists := docs[key]; !exists {
				t.Fatalf("deleted key %q returned", key)
			}
		}
		for key, text := range docs {
			if strings.Contains(normalized(text), normalized(query)) && !got[key] {
				t.Fatalf("false negative query=%q key=%q text=%q", query, key, text)
			}
		}
	}
}

func TestRandomOperations(t *testing.T) {
	const seed int64 = 773
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	corpus := []string{"日本語の全文検索", "Hello ＷＯＲＬＤ", "青空文庫と猫", "東京の空", "かなカナ"}
	queries := []string{"日", "日本", "検索", "world", "猫", "空", "かな", "存在しない"}
	docs := make(map[string]string)
	for step := 0; step < 100; step++ {
		key := fmt.Sprintf("doc/%d", rng.Intn(8))
		change := Change{Key: key}
		if rng.Intn(4) == 0 {
			change.Delete = true
			delete(docs, key)
		} else {
			text := corpus[rng.Intn(len(corpus))]
			change.Data = []byte(text)
			docs[key] = text
		}
		if err := index.Apply(ctx, []Change{change}); err != nil {
			t.Fatalf("seed=%d step=%d: %v", seed, step, err)
		}
		if step%3 == 0 {
			if _, err := index.Compact(ctx); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(fmt.Sprintf("seed=%d/step=%d", seed, step), func(t *testing.T) { assertOracle(t, index, docs, queries) })
	}
}

type failStore struct {
	Store
	failManifest bool
}

func (s *failStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if s.failManifest && strings.HasSuffix(key, "/manifest") {
		return "", errors.New("injected failure")
	}
	return s.Store.Put(ctx, key, data, options)
}

func TestFailedPublishPreservesSnapshot(t *testing.T) {
	ctx := context.Background()
	store := &failStore{Store: &MemoryStore{}}
	index, _ := Open(store, "index")
	if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("original")}}); err != nil {
		t.Fatal(err)
	}
	store.failManifest = true
	if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("replacement")}}); err == nil {
		t.Fatal("expected injected failure")
	}
	store.failManifest = false
	assertOracle(t, index, map[string]string{"a": "original"}, []string{"original", "replacement"})
	keys, err := index.Search(ctx, "replacement")
	if err != nil || len(keys) != 0 {
		t.Fatalf("unpublished result: %v %v", keys, err)
	}
}

type barrierStore struct {
	Store
	mu    sync.Mutex
	wait  int
	ready chan struct{}
}

func (s *barrierStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	obj, err := s.Store.Get(ctx, key, r)
	if strings.HasSuffix(key, "/manifest") {
		s.mu.Lock()
		if s.wait < 2 {
			s.wait++
			if s.wait == 2 {
				close(s.ready)
			}
			ready := s.ready
			s.mu.Unlock()
			select {
			case <-ready:
			case <-ctx.Done():
				return Object{}, ctx.Err()
			}
			return obj, err
		}
		s.mu.Unlock()
	}
	return obj, err
}

func TestConcurrentWritersConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &barrierStore{Store: &MemoryStore{}, ready: make(chan struct{})}
	index, _ := Open(store, "index")
	results := make(chan error, 2)
	for _, key := range []string{"a", "b"} {
		go func(key string) { results <- index.Apply(ctx, []Change{{Key: key, Data: []byte(key)}}) }(key)
	}
	a, b := <-results, <-results
	if (a != nil || !errors.Is(b, ErrConflict)) && (b != nil || !errors.Is(a, ErrConflict)) {
		t.Fatalf("expected one conflict: %v, %v", a, b)
	}
}

func TestSync(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, _ := Open(store, "index/")
	if _, err := store.Put(ctx, "docs/a", []byte("猫"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	assertOracle(t, index, map[string]string{"docs/a": "猫"}, []string{"猫"})
	if _, err := store.Put(ctx, "docs/a", []byte("犬"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	assertOracle(t, index, map[string]string{"docs/a": "犬"}, []string{"犬"})
	if err := store.Delete(ctx, "docs/a"); err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Search(ctx, "犬")
	if err != nil || len(keys) != 0 {
		t.Fatalf("after delete: %v, %v", keys, err)
	}
}

func TestReclaim(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, _ := Open(store, "index/")
	if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("cat")}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Apply(ctx, []Change{{Key: "b", Data: []byte("dog")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	for key, obj := range store.objects {
		obj.time = time.Now().Add(-time.Hour)
		store.objects[key] = obj
	}
	store.mu.Unlock()
	if n, err := index.Reclaim(ctx, time.Minute); err != nil || n != 0 {
		t.Fatalf("first pass must only mark: %d, %v", n, err)
	}
	store.mu.Lock()
	for key, obj := range store.objects {
		obj.time = time.Now().Add(-time.Hour)
		store.objects[key] = obj
	}
	store.mu.Unlock()
	n, err := index.Reclaim(ctx, time.Minute)
	if err != nil || n != 2 {
		t.Fatalf("reclaim: %d, %v", n, err)
	}
	assertOracle(t, index, map[string]string{"a": "cat", "b": "dog"}, []string{"cat", "dog"})
}

func FuzzSegmentFooter(f *testing.F) {
	f.Add([]byte("invalid"), int64(32))
	f.Add(make([]byte, footerSize), int64(100))
	f.Fuzz(func(t *testing.T, data []byte, size int64) {
		_, _, _ = parseFooter(data, size)
	})
}

func FuzzManifest(f *testing.F) {
	f.Add([]byte(`{"version":1,"segments":[]}`))
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, data []byte) {
		store := &MemoryStore{}
		_, _ = store.Put(context.Background(), "index/manifest", data, PutOptions{})
		index, _ := Open(store, "index")
		_, _, _ = index.loadManifest(context.Background())
	})
}

type pausedReadStore struct {
	Store
	mu      sync.Mutex
	pause   bool
	started chan struct{}
	resume  chan struct{}
}

func (s *pausedReadStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if r != nil {
		s.mu.Lock()
		wait := s.pause
		if wait {
			s.pause = false
			close(s.started)
		}
		s.mu.Unlock()
		if wait {
			select {
			case <-s.resume:
			case <-ctx.Done():
				return Object{}, ctx.Err()
			}
		}
	}
	return s.Store.Get(ctx, key, r)
}

func TestReclaimProtectsReaderAfterCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memory := &MemoryStore{}
	store := &pausedReadStore{Store: memory, started: make(chan struct{}), resume: make(chan struct{})}
	index, _ := Open(store, "index")
	if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("cat")}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Apply(ctx, []Change{{Key: "b", Data: []byte("dog")}}); err != nil {
		t.Fatal(err)
	}
	memory.mu.Lock()
	for key, obj := range memory.objects {
		obj.time = time.Now().Add(-time.Hour)
		memory.objects[key] = obj
	}
	memory.mu.Unlock()
	store.mu.Lock()
	store.pause = true
	store.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		keys, err := index.Search(ctx, "cat")
		if err == nil && (len(keys) != 1 || keys[0] != "a") {
			err = fmt.Errorf("unexpected keys %v", keys)
		}
		result <- err
	}()
	<-store.started
	if changed, err := index.Compact(ctx); err != nil || !changed {
		t.Fatalf("compact: %v %v", changed, err)
	}
	if removed, err := index.Reclaim(ctx, time.Minute); err != nil || removed != 0 {
		t.Fatalf("first reclaim: %d %v", removed, err)
	}
	close(store.resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func FuzzSegment(f *testing.F) {
	valid, err := encodeSegment(map[string]indexedDocument{"one": {Terms: terms("日本語")}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		store := &MemoryStore{}
		ctx := context.Background()
		if _, err := store.Put(ctx, "segment", data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
		reader, err := openSegment(ctx, store, segmentRef{Key: "segment", Size: int64(len(data)), Documents: 1})
		if err == nil {
			_, _, _ = reader.lookup(ctx, "a")
		}
	})
}

func TestConcurrentSearchCompactAndReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := &MemoryStore{}
	index, _ := Open(store, "index")
	for n := 0; n < 12; n++ {
		if err := index.Apply(ctx, []Change{{Key: fmt.Sprintf("%d", n), Data: []byte("検索対象")}}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for n := 0; n < 20; n++ {
			keys, err := index.Search(ctx, "検索")
			if err != nil {
				errs <- err
				return
			}
			if len(keys) != 12 {
				errs <- fmt.Errorf("got %d results", len(keys))
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for n := 0; n < 20; n++ {
			_, err := index.Compact(ctx)
			if err != nil && !errors.Is(err, ErrConflict) {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for n := 0; n < 20; n++ {
			_, err := index.Reclaim(ctx, time.Hour)
			if err != nil && !errors.Is(err, ErrConflict) {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

type faultStore struct {
	Store
	failure string
}

func (s *faultStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if s.failure == "get" {
		return Object{}, errors.New("injected get")
	}
	return s.Store.Get(ctx, key, r)
}

func (s *faultStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if s.failure == "segment" && strings.Contains(key, "/segments/") || s.failure == "manifest" && strings.HasSuffix(key, "/manifest") {
		return "", errors.New("injected put")
	}
	return s.Store.Put(ctx, key, data, options)
}

func TestStoreFailuresPreservePublishedState(t *testing.T) {
	ctx := context.Background()
	for _, stage := range []string{"get", "segment", "manifest"} {
		t.Run(stage, func(t *testing.T) {
			store := &faultStore{Store: &MemoryStore{}}
			index, _ := Open(store, "index")
			if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("original")}}); err != nil {
				t.Fatal(err)
			}
			store.failure = stage
			if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("changed")}}); err == nil {
				t.Fatal("expected failure")
			}
			store.failure = ""
			keys, err := index.Search(ctx, "original")
			if err != nil || len(keys) != 1 || keys[0] != "a" {
				t.Fatalf("published state: %v, %v", keys, err)
			}
			keys, err = index.Search(ctx, "changed")
			if err != nil || len(keys) != 0 {
				t.Fatalf("unpublished state: %v, %v", keys, err)
			}
		})
	}
}

func TestCompactDropsObsoleteDeletes(t *testing.T) {
	ctx := context.Background()
	index, _ := Open(&MemoryStore{}, "index")
	if err := index.Apply(ctx, []Change{{Key: "a", Data: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Apply(ctx, []Change{{Key: "a", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	if changed, err := index.Compact(ctx); err != nil || !changed {
		t.Fatalf("compact: %v %v", changed, err)
	}
	m, _, err := index.loadManifest(ctx)
	if err != nil || len(m.Segments) != 0 {
		t.Fatalf("manifest: %+v %v", m, err)
	}
	keys, err := index.Search(ctx, "old")
	if err != nil || len(keys) != 0 {
		t.Fatalf("deleted key: %v %v", keys, err)
	}
}

type pausedListStore struct {
	Store
	started chan struct{}
	resume  chan struct{}
}

func (s *pausedListStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if strings.HasSuffix(prefix, "/segments/") {
		close(s.started)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.List(ctx, prefix)
}

func TestReclaimDoesNotMarkConcurrentPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memory := &MemoryStore{}
	paused := &pausedListStore{Store: memory, started: make(chan struct{}), resume: make(chan struct{})}
	collector, _ := Open(paused, "index")
	writer, _ := Open(memory, "index")
	result := make(chan error, 1)
	go func() { _, err := collector.Reclaim(ctx, time.Hour); result <- err }()
	<-paused.started
	if err := writer.Apply(ctx, []Change{{Key: "a", Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	close(paused.resume)
	if err := <-result; !errors.Is(err, ErrConflict) {
		t.Fatalf("reclaim must detect concurrent publication: %v", err)
	}
	markers, err := memory.List(ctx, "index/retired/")
	if err != nil || len(markers) != 0 {
		t.Fatalf("live object was marked: %v %v", markers, err)
	}
}

func TestSegmentRejectsCorruptBloom(t *testing.T) {
	data, err := encodeSegment(map[string]indexedDocument{"key": {Terms: terms("hello")}})
	if err != nil {
		t.Fatal(err)
	}
	offset, length, err := parseFooter(data[len(data)-footerSize:], int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var index segmentIndex
	if err := json.Unmarshal(data[offset:offset+length], &index); err != nil {
		t.Fatal(err)
	}
	index.Bloom[0] ^= 1
	corrupt, err := json.Marshal(index)
	if err != nil || int64(len(corrupt)) != length {
		t.Fatalf("corrupt index: %v", err)
	}
	copy(data[offset:offset+length], corrupt)
	store := &MemoryStore{}
	ctx := context.Background()
	if _, err := store.Put(ctx, "segment", data, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := openSegment(ctx, store, segmentRef{Key: "segment", Size: int64(len(data)), Documents: 1}); err == nil {
		t.Fatal("expected checksum error")
	}
}

type pausedBarrierStore struct {
	Store
	started chan struct{}
	resume  chan struct{}
}

func (s *pausedBarrierStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	etag, err := s.Store.Put(ctx, key, data, options)
	if err == nil && strings.HasSuffix(key, "/manifest") {
		close(s.started)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return etag, err
}

func TestReclaimExcludesWritesAfterBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memory := &MemoryStore{}
	paused := &pausedBarrierStore{Store: memory, started: make(chan struct{}), resume: make(chan struct{})}
	collector, _ := Open(paused, "index")
	writer, _ := Open(memory, "index")
	result := make(chan error, 1)
	go func() { _, err := collector.Reclaim(ctx, time.Hour); result <- err }()
	<-paused.started
	if err := writer.Apply(ctx, []Change{{Key: "a", Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	close(paused.resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	markers, err := memory.List(ctx, "index/retired/")
	if err != nil || len(markers) != 0 {
		t.Fatalf("new publication marked: %v %v", markers, err)
	}
}

func TestMixedSegmentVersions(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	old, err := encodeSegmentVersion(map[string]indexedDocument{
		"old": {Terms: terms("猫と犬")},
	}, legacySegmentVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "index/segments/old", old, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	initial, err := json.Marshal(manifest{
		Version: manifestVersion, Generation: "old",
		Segments: []segmentRef{{Key: "index/segments/old", Size: int64(len(old)), Documents: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "index/manifest", initial, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	index, err := Open(store, "index", WithSegmentVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Apply(ctx, []Change{{Key: "new", Data: []byte("猫と魚")}}); err != nil {
		t.Fatal(err)
	}
	assertOracle(t, index, map[string]string{"old": "猫と犬", "new": "猫と魚"}, []string{"猫", "猫と", "犬", "魚"})
	if ok, err := index.Compact(ctx); err != nil || !ok {
		t.Fatalf("compact mixed versions: %t, %v", ok, err)
	}
	assertOracle(t, index, map[string]string{"old": "猫と犬", "new": "猫と魚"}, []string{"猫", "猫と", "犬", "魚"})
}

func TestInvalidVersionTwoPostings(t *testing.T) {
	reader := &segmentReader{version: segmentVersion, docBlockSize: documentBlockSize, ref: segmentRef{Documents: 2}}
	for _, data := range [][]byte{{0}, {3}, {1, 0}, {0x80}, {1, 2}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}} {
		if _, err := reader.decodePosting(context.Background(), data); err == nil {
			t.Errorf("accepted invalid posting %x", data)
		}
	}
}

type countingSegmentStore struct {
	Store
	segmentGets  int
	segmentBytes int64
}

func (s *countingSegmentStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	obj, err := s.Store.Get(ctx, key, r)
	if strings.Contains(key, "/segments/") {
		s.segmentGets++
		s.segmentBytes += int64(len(obj.Data))
	}
	return obj, err
}

func TestSyncReadsSegmentsNotDocuments(t *testing.T) {
	ctx := context.Background()
	memory := &MemoryStore{}
	store := &countingSegmentStore{Store: memory}
	index, _ := Open(store, "index/")
	for n := 0; n < 120; n++ {
		key := fmt.Sprintf("docs/%03d", n)
		if _, err := memory.Put(ctx, key, []byte("猫"), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Sync(ctx, memory, "docs/"); err != nil {
		t.Fatal(err)
	}
	m, _, err := index.loadManifest(ctx)
	if err != nil || len(m.Segments) != 1 {
		t.Fatalf("load segment: %+v %v", m.Segments, err)
	}
	if _, err := index.segment(ctx, m.Segments[0]); err != nil {
		t.Fatal(err)
	}
	store.segmentGets = 0
	store.segmentBytes = 0
	if err := index.Sync(ctx, memory, "docs/"); err != nil {
		t.Fatal(err)
	}
	if store.segmentGets > 3 {
		t.Fatalf("unchanged sync issued %d segment GETs; want at most 3", store.segmentGets)
	}
	if store.segmentBytes >= m.Segments[0].Size {
		t.Fatalf("unchanged sync read %d bytes from a %d-byte segment; expected postings to be omitted", store.segmentBytes, m.Segments[0].Size)
	}
}

type failingSource struct {
	Store
	failKey string
}

func (s *failingSource) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if key == s.failKey {
		return Object{}, errors.New("injected source error")
	}
	return s.Store.Get(ctx, key, r)
}

func TestSyncBatchCanResumeAfterFailure(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	for n := 0; n <= syncBatchSize; n++ {
		key := fmt.Sprintf("docs/%04d", n)
		if _, err := store.Put(ctx, key, []byte("猫"), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	index, _ := Open(store, "index/")
	source := &failingSource{Store: store, failKey: fmt.Sprintf("docs/%04d", syncBatchSize)}
	if err := index.Sync(ctx, source, "docs/"); err == nil {
		t.Fatal("expected source error")
	}
	m, _, err := index.loadManifest(ctx)
	if err != nil || len(m.Segments) != 1 || m.Segments[0].Documents != syncBatchSize {
		t.Fatalf("first batch not published: %+v, %v", m, err)
	}
	if err := index.Sync(ctx, store, "docs/"); err != nil {
		t.Fatal(err)
	}
	results, err := index.Search(ctx, "猫")
	if err != nil || len(results) != syncBatchSize+1 {
		t.Fatalf("resume lost documents: %d, %v", len(results), err)
	}
}

type interleavingManifestStore struct {
	Store
	changed bool
}

func (s *interleavingManifestStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	etag, err := s.Store.Put(ctx, key, data, options)
	if err != nil || s.changed || !strings.HasSuffix(key, "/manifest") {
		return etag, err
	}
	s.changed = true
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return "", err
	}
	m.Generation = "another-writer"
	other, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if _, err := s.Store.Put(ctx, key, other, PutOptions{IfMatch: etag}); err != nil {
		return "", err
	}
	return etag, nil
}

func TestSyncBatchDetectsConcurrentManifestChange(t *testing.T) {
	ctx := context.Background()
	memory := &MemoryStore{}
	for n := 0; n <= syncBatchSize; n++ {
		if _, err := memory.Put(ctx, fmt.Sprintf("docs/%04d", n), []byte("猫"), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	store := &interleavingManifestStore{Store: memory}
	index, _ := Open(store, "index/")
	if err := index.Sync(ctx, memory, "docs/"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected concurrent update conflict, got %v", err)
	}
	m, _, err := index.loadManifest(ctx)
	if err != nil || m.Generation != "another-writer" {
		t.Fatalf("concurrent update was overwritten: %+v, %v", m, err)
	}
}

func TestWriteVersionRollout(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	writer, err := Open(store, "index")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Apply(ctx, []Change{{Key: "one", Data: []byte("猫")}}); err != nil {
		t.Fatal(err)
	}
	m, _, err := writer.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := writer.segment(ctx, m.Segments[0])
	if err != nil || r.version != legacySegmentVersion {
		t.Fatalf("default writer must use v1: reader=%+v error=%v", r, err)
	}
	writer, err = Open(store, "index", WithSegmentVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Apply(ctx, []Change{{Key: "two", Data: []byte("犬")}}); err != nil {
		t.Fatal(err)
	}
	m, _, err = writer.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err = writer.segment(ctx, m.Segments[1])
	if err != nil || r.version != segmentVersion {
		t.Fatalf("opt-in writer must use v2: reader=%+v error=%v", r, err)
	}
	assertOracle(t, writer, map[string]string{"one": "猫", "two": "犬"}, []string{"猫", "犬"})
	if _, err := Open(store, "index", WithSegmentVersion(3)); err == nil {
		t.Fatal("invalid writer version accepted")
	}
}

type documentBlockMeter struct {
	Store
	bytes int64
	gets  int
}

func (s *documentBlockMeter) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	obj, err := s.Store.Get(ctx, key, r)
	s.bytes += int64(len(obj.Data))
	s.gets++
	return obj, err
}

func TestSparseSearchOnLargeSegment(t *testing.T) {
	const count = 10000
	ctx := context.Background()
	store := &documentBlockMeter{Store: &MemoryStore{}}
	index, err := Open(store, "index", WithSegmentVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]Change, 0, count)
	for n := 0; n < count; n++ {
		text := "一般的な文書"
		if n == count-1 {
			text = "希少語"
		}
		changes = append(changes, Change{Key: fmt.Sprintf("docs/%08d", n), Data: []byte(text)})
	}
	if err := index.Apply(ctx, changes); err != nil {
		t.Fatal(err)
	}
	m, _, err := index.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	store.bytes = 0
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := index.Search(ctx, "希少語")
	if err != nil || len(got) != 1 || got[0] != "docs/00009999" {
		t.Fatalf("search: %q, %v", got, err)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	r, err := index.segment(ctx, m.Segments[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.docBlocks) != 1 || r.docBytes > maxDocumentCacheBytes {
		t.Fatalf("document cache must contain one bounded block: %d blocks, %d bytes", len(r.docBlocks), r.docBytes)
	}
	if store.bytes >= m.Segments[0].Size/4 {
		t.Fatalf("sparse read too large: %d of %d bytes", store.bytes, m.Segments[0].Size)
	}
	t.Logf("docs=%d segment_bytes=%d first_query_bytes=%d cache_encoded_bytes=%d go_heap_before=%d go_heap_after=%d go_sys_after=%d", count, m.Segments[0].Size, store.bytes, r.docBytes, before.HeapAlloc, after.HeapAlloc, after.Sys)
}

func TestSearchReadsOversizedBlockOnlyOnce(t *testing.T) {
	ctx := context.Background()
	store := &documentBlockMeter{Store: &MemoryStore{}}
	index, err := Open(store, "index", WithSegmentVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]Change, 0, documentBlockSize)
	prefix := strings.Repeat("x", 300)
	for n := 0; n < documentBlockSize; n++ {
		changes = append(changes, Change{Key: fmt.Sprintf("%s/%04d", prefix, n), Data: []byte("猫")})
	}
	if err := index.Apply(ctx, changes); err != nil {
		t.Fatal(err)
	}
	store.gets = 0
	keys, err := index.Search(ctx, "猫")
	if err != nil || len(keys) != documentBlockSize {
		t.Fatalf("search: %d, %v", len(keys), err)
	}
	if store.gets > 10 {
		t.Fatalf("oversized block was reread: %d GETs", store.gets)
	}
}

func TestUnblockedVersionTwoSegmentsRemainReadable(t *testing.T) {
	ctx := context.Background()
	data, err := encodeSegmentVersion(map[string]indexedDocument{"old": {Terms: terms("猫")}}, segmentVersion)
	if err != nil {
		t.Fatal(err)
	}
	offset, length, err := parseFooter(data[len(data)-footerSize:], int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	index := data[offset : offset+length]
	old := []byte(`"doc_block_size":256`)
	replacement := []byte(`"doc_block_size":  0`)
	if !bytes.Contains(index, old) || len(old) != len(replacement) {
		t.Fatal("unexpected index metadata")
	}
	copy(index, bytes.Replace(index, old, replacement, 1))
	store := &MemoryStore{}
	if _, err := store.Put(ctx, "index/segments/old", data, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	m, err := json.Marshal(manifest{Version: manifestVersion, Generation: "test", Segments: []segmentRef{{Key: "index/segments/old", Size: int64(len(data)), Documents: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "index/manifest", m, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(store, "index")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := reader.Search(ctx, "猫")
	if err != nil || len(keys) != 1 || keys[0] != "old" {
		t.Fatalf("legacy v2 search: %q, %v", keys, err)
	}
}

type concurrentReadStore struct {
	Store
	mu       sync.Mutex
	inFlight int
	peak     int
}

func (s *concurrentReadStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if !strings.Contains(key, "/segments/") {
		return s.Store.Get(ctx, key, r)
	}
	s.mu.Lock()
	s.inFlight++
	if s.inFlight > s.peak {
		s.peak = s.inFlight
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Object{}, ctx.Err()
	case <-timer.C:
	}
	return s.Store.Get(ctx, key, r)
}

func TestSearchReadsSegmentsInParallel(t *testing.T) {
	ctx := context.Background()
	store := &concurrentReadStore{Store: &MemoryStore{}}
	index, _ := Open(store, "index/")
	for n := 0; n < 8; n++ {
		if err := index.Apply(ctx, []Change{{Key: fmt.Sprintf("doc/%d", n), Data: []byte("猫")}}); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := index.Search(ctx, "猫")
	if err != nil || len(keys) != 8 {
		t.Fatalf("search: %q, %v", keys, err)
	}
	store.mu.Lock()
	peak := store.peak
	store.mu.Unlock()
	if peak < 2 || peak > 8 {
		t.Fatalf("expected bounded parallel segment reads, peak = %d", peak)
	}
}

func TestSyncPrefixUsesStoreStringMatching(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "docs-index/")
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(ctx, store, "doc"); err == nil {
		t.Fatal("doc also matches docs-index/ and must be rejected")
	}
	if err := index.Sync(ctx, store, "doc/"); err != nil {
		t.Fatalf("doc/ and docs-index/ are disjoint: %v", err)
	}
}

type reclaimConflictStore struct {
	Store
	conflicts int
	attempts  int
}

func (s *reclaimConflictStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if strings.HasSuffix(key, "/manifest") && s.conflicts > 0 {
		s.attempts++
		s.conflicts--
		return "", ErrConflict
	}
	if strings.HasSuffix(key, "/manifest") {
		s.attempts++
	}
	return s.Store.Put(ctx, key, data, options)
}

func TestReclaimConflictRetriesAreBounded(t *testing.T) {
	ctx := context.Background()
	store := &reclaimConflictStore{Store: &MemoryStore{}}
	index, _ := Open(store, "index/")
	if err := index.Apply(ctx, []Change{{Key: "doc", Data: []byte("猫")}}); err != nil {
		t.Fatal(err)
	}
	store.attempts, store.conflicts = 0, 2
	err := RetryConflict(ctx, 3, func(ctx context.Context) error {
		_, err := index.Reclaim(ctx, time.Hour)
		return err
	})
	if err != nil || store.attempts != 3 {
		t.Fatalf("two transient conflicts: attempts=%d error=%v", store.attempts, err)
	}
	store.attempts, store.conflicts = 0, 3
	err = RetryConflict(ctx, 3, func(ctx context.Context) error {
		_, err := index.Reclaim(ctx, time.Hour)
		return err
	})
	if !errors.Is(err, ErrConflict) || store.attempts != 3 {
		t.Fatalf("persistent conflicts: attempts=%d error=%v", store.attempts, err)
	}
}
