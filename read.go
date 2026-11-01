package zenbun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const defaultReadBytes = 16 << 10

// ReadOptions selects lines Start through End, numbered from 1 like
// Snippet.Line. End 0 reads to the end of the document. MaxBytes bounds Text,
// 16 KiB by default.
type ReadOptions struct {
	Start    int `json:"start"`
	End      int `json:"end,omitempty"`
	MaxBytes int `json:"max_bytes,omitempty"`
}

// ReadResult is consecutive lines of a document starting at Start. Lines is
// the number of lines in the whole document. Truncated is true when MaxBytes
// cut the text; Next, if set, is the first line not returned, to pass as Start
// to continue. A single line longer than MaxBytes is cut at a character
// boundary, and Next skips the rest of it.
type ReadResult struct {
	Key       string `json:"key"`
	Start     int    `json:"start"`
	Text      string `json:"text"`
	Lines     int    `json:"lines"`
	Truncated bool   `json:"truncated,omitempty"`
	Next      int    `json:"next,omitempty"`
}

// Read returns lines of a source document, split the same way as Find's
// snippets. It reads any key in source and does not check the index, so check
// access rights before calling it with a key from an untrusted caller. A
// missing document returns an error wrapping ErrNotFound.
func (i *Index) Read(ctx context.Context, source Store, key string, options ReadOptions) (ReadResult, error) {
	if source == nil {
		return ReadResult{}, errors.New("zenbun: source is required")
	}
	if key == "" || !utf8.ValidString(key) {
		return ReadResult{}, errors.New("zenbun: key must be nonempty UTF-8")
	}
	obj, err := source.Get(ctx, key, nil)
	if err != nil {
		return ReadResult{}, fmt.Errorf("zenbun: read source %q: %w", key, err)
	}
	return readLines(key, obj.Data, options)
}

// Read returns lines of a document copy, split the same way as Find's
// snippets. A missing copy returns an error wrapping ErrNotFound.
func (m *Mirror) Read(ctx context.Context, key string, options ReadOptions) (ReadResult, error) {
	if !validMirrorKey(key) {
		return ReadResult{}, errors.New("zenbun: invalid document ID")
	}
	obj, err := m.store.Get(ctx, m.prefix+key, nil)
	if err != nil {
		return ReadResult{}, fmt.Errorf("zenbun: read document copy %q: %w", key, err)
	}
	return readLines(key, obj.Data, options)
}

func readLines(key string, data []byte, options ReadOptions) (ReadResult, error) {
	if !utf8.Valid(data) {
		return ReadResult{}, fmt.Errorf("zenbun: document %q is not valid UTF-8", key)
	}
	if options.MaxBytes < 0 {
		return ReadResult{}, errors.New("zenbun: max bytes must be nonnegative")
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultReadBytes
	}
	lines := strings.Split(string(data), "\n")
	if options.Start < 1 || options.Start > len(lines) {
		return ReadResult{}, fmt.Errorf("zenbun: start must be 1 to %d, the document's lines", len(lines))
	}
	end := len(lines)
	if options.End != 0 {
		if options.End < options.Start {
			return ReadResult{}, errors.New("zenbun: end must not precede start")
		}
		end = min(end, options.End)
	}
	result := ReadResult{Key: key, Start: options.Start, Lines: len(lines)}
	var text strings.Builder
	for n := options.Start; n <= end; n++ {
		line := strings.ReplaceAll(lines[n-1], "\r", "")
		size := len(line)
		if n > options.Start {
			size++ // the line break before this line
		}
		if text.Len()+size > options.MaxBytes {
			if n == options.Start {
				cut := options.MaxBytes
				for cut > 0 && !utf8.RuneStart(line[cut]) {
					cut--
				}
				text.WriteString(line[:cut])
				n++
			}
			result.Truncated = true
			if n <= end {
				result.Next = n
			}
			break
		}
		if n > options.Start {
			text.WriteByte('\n')
		}
		text.WriteString(line)
	}
	result.Text = text.String()
	return result, nil
}
