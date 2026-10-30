// Package tokenize defines the shared normalization and tokenization contract.
package tokenize

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Normalize applies the same NFKC normalization and case folding to both
// documents and queries. Unicode lowercasing is used, rather than full folding.
func Normalize(s string) string {
	return norm.NFKC.String(strings.ToLower(norm.NFKC.String(s)))
}

type Tokenizer interface {
	Tokens(string) []string
}

type Bigram struct{}

// Tokens returns distinct character bigrams, in first-occurrence order.
// One-character queries use a unigram so callers can provide a fallback.
func (Bigram) Tokens(s string) []string {
	r := []rune(Normalize(s))
	if len(r) == 0 {
		return nil
	}
	if len(r) == 1 {
		return []string{string(r)}
	}
	seen := map[string]bool{}
	var out []string
	for i := 0; i+1 < len(r); i++ {
		term := string(r[i : i+2])
		if !seen[term] {
			seen[term] = true
			out = append(out, term)
		}
	}
	return out
}
