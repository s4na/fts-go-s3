package zenbun

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Query limits keep one call's index lookups and body checks bounded.
const (
	maxQueryGroups     = 16
	maxQueryWords      = 64
	maxQueryWordBytes  = 256
	maxFindContext     = 20
	defaultFindLimit   = 20
	defaultFindBytes   = 16 << 10
	defaultFindSnippet = 3
)

// Query selects documents that contain, for every group in All, at least one
// of its words, and none of the words in None. Words are matched as substrings
// after NFKC normalization and lowercasing. KeyPrefix restricts keys; for a
// Mirror it applies to document IDs.
//
// For example, All: [["猫", "ネコ"], ["請求書"]] matches documents mentioning
// a cat in either spelling and also an invoice.
type Query struct {
	All       [][]string `json:"all"`
	None      []string   `json:"none,omitempty"`
	KeyPrefix string     `json:"key_prefix,omitempty"`
}

func normalize(s string) string {
	return strings.ToLower(norm.NFKC.String(s))
}

func (q Query) validate() error {
	if len(q.All) == 0 || len(q.All) > maxQueryGroups {
		return fmt.Errorf("zenbun: query needs 1 to %d groups", maxQueryGroups)
	}
	words := len(q.None)
	for _, group := range q.All {
		if len(group) == 0 {
			return errors.New("zenbun: query group must not be empty")
		}
		words += len(group)
	}
	if words > maxQueryWords {
		return fmt.Errorf("zenbun: query has more than %d words", maxQueryWords)
	}
	check := func(word string) error {
		if word == "" || len(word) > maxQueryWordBytes || !utf8.ValidString(word) || normalize(word) == "" {
			return fmt.Errorf("zenbun: query words must be nonempty UTF-8 up to %d bytes", maxQueryWordBytes)
		}
		return nil
	}
	for _, group := range q.All {
		for _, word := range group {
			if err := check(word); err != nil {
				return err
			}
			// Snippets are lines, so a word in All must fit within one line.
			if strings.ContainsAny(word, "\r\n") {
				return errors.New("zenbun: words in All must not contain line breaks")
			}
		}
	}
	for _, word := range q.None {
		if err := check(word); err != nil {
			return err
		}
	}
	if !utf8.ValidString(q.KeyPrefix) {
		return errors.New("zenbun: key prefix must be UTF-8")
	}
	return nil
}

// indexGroups maps each word to the index terms it requires: its bi-grams, or
// its uni-gram when it has a single character.
func (q Query) indexGroups() [][][]string {
	groups := make([][][]string, len(q.All))
	for g, group := range q.All {
		for _, word := range group {
			all := terms(word)
			var wordTerms []string
			for _, term := range all {
				if strings.HasPrefix(term, "b") {
					wordTerms = append(wordTerms, term)
				}
			}
			if len(wordTerms) == 0 {
				wordTerms = all
			}
			groups[g] = append(groups[g], wordTerms)
		}
	}
	return groups
}

// FindMode selects what Find returns for each matching document.
type FindMode string

const (
	// FindSnippets returns keys with matching lines and their context.
	FindSnippets FindMode = "snippets"
	// FindKeys returns keys only.
	FindKeys FindMode = "keys"
	// FindCount returns only Count.
	FindCount FindMode = "count"
)

// FindOptions bounds what Find reads and returns. Zero values select the
// defaults: snippets, 20 documents, 3 snippets per document and 16 KiB of
// snippet text. Context is the number of lines around each matching line.
// After continues from a previous result's Next.
type FindOptions struct {
	Mode              FindMode `json:"mode,omitempty"`
	Limit             int      `json:"limit,omitempty"`
	Context           int      `json:"context,omitempty"`
	MaxSnippetsPerDoc int      `json:"max_snippets_per_doc,omitempty"`
	MaxBytes          int      `json:"max_bytes,omitempty"`
	After             string   `json:"after,omitempty"`
}

// Snippet is consecutive lines of a document starting at the 1-based Line.
type Snippet struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}

// FoundDocument is a verified match. Lines counts lines containing a word from
// All; Snippets covers at most MaxSnippetsPerDoc of them.
type FoundDocument struct {
	Key      string    `json:"key"`
	Lines    int       `json:"lines,omitempty"`
	Snippets []Snippet `json:"snippets,omitempty"`
}

// FindResult reports verified matches in key order. Candidates is the number
// of index candidates after After, an upper bound on remaining matches. When
// Truncated is true, more candidates remain or snippet text was cut; pass Next
// as After to continue.
type FindResult struct {
	Count      int             `json:"count"`
	Candidates int             `json:"candidates"`
	Documents  []FoundDocument `json:"documents,omitempty"`
	Truncated  bool            `json:"truncated,omitempty"`
	Next       string          `json:"next,omitempty"`
}

func (o FindOptions) withDefaults() (FindOptions, error) {
	switch o.Mode {
	case "":
		o.Mode = FindSnippets
	case FindSnippets, FindKeys, FindCount:
	default:
		return o, fmt.Errorf("zenbun: unknown find mode %q", o.Mode)
	}
	if o.Limit < 0 || o.Context < 0 || o.Context > maxFindContext || o.MaxSnippetsPerDoc < 0 || o.MaxBytes < 0 {
		return o, fmt.Errorf("zenbun: find limits must be nonnegative and context at most %d", maxFindContext)
	}
	if o.Limit == 0 {
		o.Limit = defaultFindLimit
	}
	if o.MaxSnippetsPerDoc == 0 {
		o.MaxSnippetsPerDoc = defaultFindSnippet
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = defaultFindBytes
	}
	return o, nil
}

// Find verifies SearchQuery candidates against the source documents last
// synchronized from source. Keys are source keys. A document deleted since the
// last Sync is skipped; a body that no longer matches is not returned. It makes
// one full-document GET per checked candidate and stops after Limit matches.
func (i *Index) Find(ctx context.Context, source Store, q Query, options FindOptions) (FindResult, error) {
	if source == nil {
		return FindResult{}, errors.New("zenbun: source is required")
	}
	return find(ctx, i, q, options, func(ctx context.Context, key string) ([]byte, bool, error) {
		obj, err := source.Get(ctx, key, nil)
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("zenbun: read source %q: %w", key, err)
		}
		return obj.Data, true, nil
	})
}

// Find verifies candidates against the document copies. As with Search, a
// missing copy is an error. It makes one full-document GET per checked
// candidate and stops after Limit matches.
func (m *Mirror) Find(ctx context.Context, q Query, options FindOptions) (FindResult, error) {
	return find(ctx, m.index, q, options, func(ctx context.Context, key string) ([]byte, bool, error) {
		obj, err := m.store.Get(ctx, m.prefix+key, nil)
		if err != nil {
			return nil, false, fmt.Errorf("zenbun: read document copy %q: %w", key, err)
		}
		return obj.Data, true, nil
	})
}

func find(ctx context.Context, index *Index, q Query, options FindOptions, read func(context.Context, string) ([]byte, bool, error)) (FindResult, error) {
	options, err := options.withDefaults()
	if err != nil {
		return FindResult{}, err
	}
	keys, err := index.SearchQuery(ctx, q)
	if err != nil {
		return FindResult{}, err
	}
	keys = keys[sort.SearchStrings(keys, options.After):]
	if len(keys) > 0 && keys[0] == options.After {
		keys = keys[1:]
	}
	result := FindResult{Candidates: len(keys)}
	budget := options.MaxBytes
	for n, key := range keys {
		data, found, err := read(ctx, key)
		if err != nil {
			return FindResult{}, err
		}
		if !found {
			continue
		}
		if !utf8.Valid(data) {
			return FindResult{}, fmt.Errorf("zenbun: document %q is not valid UTF-8", key)
		}
		doc, ok := matchDocument(key, string(data), q, options, &budget)
		if !ok {
			continue
		}
		if budget < 0 && doc.Lines > 0 && len(doc.Snippets) == 0 && len(result.Documents) > 0 {
			// No room is left for this document's text; continue from it next time.
			result.Truncated, result.Next = true, keys[n-1]
			break
		}
		result.Count++
		if options.Mode != FindCount {
			result.Documents = append(result.Documents, doc)
		}
		more := n+1 < len(keys)
		if budget < 0 {
			result.Truncated, result.Next = true, key
			break
		}
		if result.Count == options.Limit && more {
			result.Truncated, result.Next = true, key
			break
		}
	}
	return result, nil
}

// matchDocument checks the whole normalized body, then collects matching
// lines. budget drops below zero when snippet text had to be cut.
func matchDocument(key, body string, q Query, options FindOptions, budget *int) (FoundDocument, bool) {
	normalized := normalize(body)
	var words []string
	for _, group := range q.All {
		found := false
		for _, word := range group {
			word = normalize(word)
			words = append(words, word)
			found = found || strings.Contains(normalized, word)
		}
		if !found {
			return FoundDocument{}, false
		}
	}
	for _, word := range q.None {
		if strings.Contains(normalized, normalize(word)) {
			return FoundDocument{}, false
		}
	}
	doc := FoundDocument{Key: key}
	if options.Mode != FindSnippets {
		return doc, true
	}
	lines := strings.Split(body, "\n")
	var hits []int
	for n, line := range lines {
		line = normalize(line)
		for _, word := range words {
			if strings.Contains(line, word) {
				hits = append(hits, n)
				break
			}
		}
	}
	doc.Lines = len(hits)
	for h := 0; h < len(hits) && len(doc.Snippets) < options.MaxSnippetsPerDoc; {
		first := max(0, hits[h]-options.Context)
		last := min(len(lines)-1, hits[h]+options.Context)
		// Merge following hits whose context overlaps or touches this one.
		for h++; h < len(hits) && hits[h]-options.Context <= last+1; h++ {
			last = min(len(lines)-1, hits[h]+options.Context)
		}
		text := strings.Join(lines[first:last+1], "\n")
		text = strings.ReplaceAll(text, "\r", "")
		if len(text) > *budget {
			cut := *budget
			for cut > 0 && !utf8.RuneStart(text[cut]) {
				cut--
			}
			if cut > 0 {
				doc.Snippets = append(doc.Snippets, Snippet{Line: first + 1, Text: text[:cut]})
			}
			*budget = -1
			break
		}
		*budget -= len(text)
		doc.Snippets = append(doc.Snippets, Snippet{Line: first + 1, Text: text})
	}
	return doc, true
}
