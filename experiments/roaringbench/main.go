// roaringbench compares actual query-term posting lists, not synthetic bitmaps.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/s4na/fts-go-s3/internal/oracle"
	"github.com/s4na/fts-go-s3/internal/tokenize"
)

type query struct {
	Query string `json:"query"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	docsPath := flag.String("docs", "", "JSONL documents")
	queriesPath := flag.String("queries", "", "JSON queries")
	output := flag.String("out", "roaring.json", "output JSON")
	flag.Parse()
	queryData, err := os.ReadFile(*queriesPath)
	if err != nil {
		return err
	}
	var queries []query
	if err := json.Unmarshal(queryData, &queries); err != nil {
		return err
	}
	termsSet := map[string]bool{}
	for _, q := range queries {
		for _, term := range (tokenize.Bigram{}).Tokens(q.Query) {
			termsSet[term] = true
		}
	}
	terms := make([]string, 0, len(termsSet))
	for term := range termsSet {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	f, err := os.Open(*docsPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64*1024*1024)
	postings := make([][]uint32, len(terms))
	var documents uint32
	for scanner.Scan() {
		var doc oracle.Document
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return err
		}
		text := tokenize.Normalize(doc.Title + "\n" + doc.Text)
		for n, term := range terms {
			if strings.Contains(text, term) {
				postings[n] = append(postings[n], documents)
			}
		}
		documents++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	var deltaBytes, roaringBytes, cardinality int
	var deltaNanos, roaringNanos int64
	for _, ids := range postings {
		start := time.Now()
		var delta bytes.Buffer
		var previous uint32
		for _, id := range ids {
			var data [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(data[:], uint64(id-previous))
			_, _ = delta.Write(data[:n])
			previous = id
		}
		deltaNanos += time.Since(start).Nanoseconds()
		var decoded []uint32
		reader := bytes.NewReader(delta.Bytes())
		previous = 0
		for reader.Len() > 0 {
			value, err := binary.ReadUvarint(reader)
			if err != nil {
				return err
			}
			previous += uint32(value)
			decoded = append(decoded, previous)
		}
		if !reflect.DeepEqual(ids, decoded) {
			return fmt.Errorf("varint roundtrip differs")
		}
		start = time.Now()
		bitmap := roaring.New()
		bitmap.AddMany(ids)
		var encoded bytes.Buffer
		if _, err := bitmap.WriteTo(&encoded); err != nil {
			return err
		}
		roaringNanos += time.Since(start).Nanoseconds()
		restored := roaring.New()
		if _, err := restored.ReadFrom(bytes.NewReader(encoded.Bytes())); err != nil {
			return err
		}
		if restored.GetCardinality() != uint64(len(ids)) {
			return fmt.Errorf("roaring cardinality differs")
		}
		for _, id := range ids {
			if !restored.Contains(id) {
				return fmt.Errorf("roaring roundtrip missing %d", id)
			}
		}
		cardinality += len(ids)
		deltaBytes += delta.Len()
		roaringBytes += encoded.Len()
	}
	result := map[string]interface{}{
		"documents":          documents,
		"query_count":        len(queries),
		"term_count":         len(terms),
		"posting_entries":    cardinality,
		"delta_varint_bytes": deltaBytes,
		"roaring_bytes":      roaringBytes,
		"delta_encode_ns":    deltaNanos,
		"roaring_encode_ns":  roaringNanos,
	}
	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, out, 0644); err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
