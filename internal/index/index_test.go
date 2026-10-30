package index

import (
	"reflect"
	"testing"
)

func TestPostingsEncoding(t *testing.T) {
	input := map[string][]uint64{"猫": {0, 1, 129, 10000}, "日本": {2, 3}}
	encoded := encode(input)
	got, err := decode(encoded)
	if err != nil || !reflect.DeepEqual(got, input) {
		t.Fatalf("decoded = %v, error = %v", got, err)
	}
	for _, broken := range [][]byte{nil, {1, 9}, append(encoded, 0)} {
		if _, err := decode(broken); err == nil {
			t.Errorf("accepted malformed shard %x", broken)
		}
	}
}
