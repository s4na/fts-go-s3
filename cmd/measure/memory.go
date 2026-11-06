package main

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

const (
	heapObjectsMetric = "/memory/classes/heap/objects:bytes"
	heapAllocsMetric  = "/gc/heap/allocs:bytes"
	sampleInterval    = time.Millisecond
)

// operation records the cost of one measured operation. PeakHeapDeltaBytes is
// the highest sampled heap object size minus the size after a forced GC at
// the start, so memory the measurement tool already holds is excluded. The
// sample includes garbage not yet collected, as a process would.
type operation struct {
	Seconds            float64 `json:"seconds"`
	PeakHeapBytes      uint64  `json:"peak_heap_bytes"`
	PeakHeapDeltaBytes uint64  `json:"peak_heap_delta_bytes"`
	AllocBytes         uint64  `json:"alloc_bytes"`
	Gets               int64   `json:"gets"`
	ReceivedBytes      int64   `json:"received_bytes"`
	Puts               int64   `json:"puts"`
	// SourceGets and SourceReceivedBytes count reads from the Sync source,
	// which are separate from the index store's I/O above.
	SourceGets          int64 `json:"source_gets"`
	SourceReceivedBytes int64 `json:"source_received_bytes"`
}

func readHeap() (objects, allocs uint64) {
	samples := []metrics.Sample{{Name: heapObjectsMetric}, {Name: heapAllocsMetric}}
	metrics.Read(samples)
	return samples[0].Value.Uint64(), samples[1].Value.Uint64()
}

// measureOperation runs fn while sampling the heap and counting the I/O of the
// index store and the Sync source.
func measureOperation(store, source *meter, fn func() error) (operation, error) {
	runtime.GC()
	base, allocStart := readHeap()
	gets, received, puts := store.Gets.Load(), store.Received.Load(), store.Puts.Load()
	sourceGets, sourceReceived := source.Gets.Load(), source.Received.Load()
	peak := base
	var mu sync.Mutex
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(sampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				heap, _ := readHeap()
				mu.Lock()
				peak = max(peak, heap)
				mu.Unlock()
			}
		}
	}()
	started := time.Now()
	err := fn()
	elapsed := time.Since(started)
	close(stop)
	<-done
	heap, allocEnd := readHeap()
	peak = max(peak, heap)
	return operation{
		Seconds:             elapsed.Seconds(),
		PeakHeapBytes:       peak,
		PeakHeapDeltaBytes:  peak - base,
		AllocBytes:          allocEnd - allocStart,
		Gets:                store.Gets.Load() - gets,
		ReceivedBytes:       store.Received.Load() - received,
		Puts:                store.Puts.Load() - puts,
		SourceGets:          source.Gets.Load() - sourceGets,
		SourceReceivedBytes: source.Received.Load() - sourceReceived,
	}, err
}
