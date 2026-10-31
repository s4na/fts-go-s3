package zenbun_test

import (
	"testing"

	zenbun "github.com/s4na/fts-go-s3"
	"github.com/s4na/fts-go-s3/storetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storetest.Run(t, &zenbun.MemoryStore{})
}
