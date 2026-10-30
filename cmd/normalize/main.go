// normalize applies the same Go normalization used by the oracle to JSON strings.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/s4na/fts-go-s3/internal/tokenize"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 64*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	encoder := json.NewEncoder(out)
	for scanner.Scan() {
		var input string
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := encoder.Encode(tokenize.Normalize(input)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
