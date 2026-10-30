package index

import (
	"reflect"
	"testing"

	"github.com/s4na/fts-go-s3/internal/tokenize"
)

func TestTermsUseSameNormalizationAsQuery(t *testing.T) {
	text := "J\u030cx"
	indexed := map[string]bool{}
	for _, term := range terms(text) {
		indexed[term] = true
	}
	for _, term := range (tokenize.Bigram{}).Tokens(text) {
		if !indexed[term] {
			t.Fatalf("query term %q missing from indexed terms %v", term, indexed)
		}
	}
}

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
