package zenbun_test

import (
	"testing"

	zenbun "github.com/s4na/zenbun"
	"github.com/s4na/zenbun/storetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storetest.Run(t, &zenbun.MemoryStore{})
}
