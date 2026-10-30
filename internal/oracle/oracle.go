// Package oracle implements a linear scan to check candidate recall.
package oracle

import (
	"sort"
	"strings"

	"github.com/s4na/fts-go-s3/internal/tokenize"
)

type Document struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

type Corpus struct {
	documents []Document
}

// New normalizes documents once so repeated queries use a consistent contract.
func New(docs []Document) Corpus {
	out := make([]Document, len(docs))
	for i, d := range docs {
		d.Text = tokenize.Normalize(d.Title + "\n" + d.Text)
		out[i] = d
	}
	return Corpus{documents: out}
}

// Search returns IDs containing the normalized query, sorted lexicographically.
// An empty query matches no documents.
func (c Corpus) Search(query string) []string {
	query = tokenize.Normalize(query)
	if query == "" {
		return nil
	}
	var result []string
	for _, d := range c.documents {
		if strings.Contains(d.Text, query) {
			result = append(result, d.ID)
		}
	}
	sort.Strings(result)
	return result
}
