package main

import "testing"

func TestReadRatio(t *testing.T) {
	fraction, inverse := readRatio(1000, 50, 2)
	if fraction == nil || inverse == nil || *fraction != 0.025 || *inverse != 40 {
		t.Fatalf("fraction=%v inverse=%v; want 0.025 and 40", fraction, inverse)
	}
	fraction, inverse = readRatio(1000, 0, 2)
	if fraction == nil || *fraction != 0 || inverse != nil {
		t.Fatalf("zero reads: fraction=%v inverse=%v", fraction, inverse)
	}
	for _, tc := range []struct {
		source, received int64
		queries          int
	}{
		{0, 50, 2},
		{1000, 50, 0},
		{1000, -1, 2},
	} {
		fraction, inverse := readRatio(tc.source, tc.received, tc.queries)
		if fraction != nil || inverse != nil {
			t.Fatalf("invalid input %+v: fraction=%v inverse=%v", tc, fraction, inverse)
		}
	}
}

func TestOverBudget(t *testing.T) {
	ops := map[string]operation{
		"sync_initial": {PeakHeapDeltaBytes: 300},
		"build":        {PeakHeapDeltaBytes: 200},
		"search":       {PeakHeapDeltaBytes: 100},
	}
	if got := overBudget(ops, 0); got != nil {
		t.Fatalf("disabled budget reported %v", got)
	}
	got := overBudget(ops, 100)
	if len(got) != 2 || got[0] != "build=200" || got[1] != "sync_initial=300" {
		t.Fatalf("overBudget = %v", got)
	}
}
