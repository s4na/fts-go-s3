package oracle

import (
	"reflect"
	"testing"

	"github.com/s4na/fts-go-s3/internal/tokenize"
)

func TestSearchNormalizationAndTitles(t *testing.T) {
	c := New([]Document{{ID: "b", Title: "検索", Text: "ＡＢＣとカタカナ"}, {ID: "a", Title: "ABC", Text: "別の文章"}})
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"abc", []string{"a", "b"}},
		{"ｶﾀｶﾅ", []string{"b"}},
		{"検索", []string{"b"}},
		{"不存在", nil},
		{"", nil},
	} {
		if got := c.Search(tc.query); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Search(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestBigram(t *testing.T) {
	if got, want := (tokenize.Bigram{}).Tokens("ＡＢＡＢ"), []string{"ab", "ba"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Tokens = %v, want %v", got, want)
	}
	if got := (tokenize.Bigram{}).Tokens("猫"); !reflect.DeepEqual(got, []string{"猫"}) {
		t.Fatalf("single rune = %v", got)
	}
}

func TestNormalizationAfterLowercase(t *testing.T) {
	normalized := tokenize.Normalize("J\u030c")
	if normalized != "ǰ" || tokenize.Normalize(normalized) != normalized {
		t.Fatalf("normalization is not stable: %q", normalized)
	}
	corpus := New([]Document{{ID: "j", Text: "J\u030c"}})
	if got := corpus.Search("ǰ"); !reflect.DeepEqual(got, []string{"j"}) {
		t.Fatalf("equivalent query = %v", got)
	}
}
