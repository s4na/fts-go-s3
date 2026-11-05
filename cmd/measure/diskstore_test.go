package main

import (
	"testing"

	"github.com/s4na/zenbun/storetest"
)

func TestDiskStoreConformance(t *testing.T) {
	store, err := newDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storetest.Run(t, store)
}
