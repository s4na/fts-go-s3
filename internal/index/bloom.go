package index

import "hash/fnv"

// bloom uses five probes and twelve bits per distinct term. Empty filters are
// never treated as evidence of absence.
type bloom []byte

func newBloom(terms []string) bloom {
	if len(terms) == 0 {
		return nil
	}
	filter := make(bloom, (len(terms)*12+7)/8)
	if len(filter) < 8 {
		filter = make(bloom, 8)
	}
	for _, term := range terms {
		filter.add(term)
	}
	return filter
}

func (b bloom) positions(term string, visit func(uint64)) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(term))
	first := h.Sum64()
	h.Reset()
	_, _ = h.Write([]byte{0xff})
	_, _ = h.Write([]byte(term))
	step := h.Sum64() | 1
	bits := uint64(len(b) * 8)
	for n := uint64(0); n < 5; n++ {
		visit((first + n*step) % bits)
	}
}

func (b bloom) add(term string) {
	b.positions(term, func(bit uint64) { b[bit/8] |= 1 << (bit % 8) })
}

func (b bloom) mayContain(term string) bool {
	if len(b) == 0 {
		return true
	}
	present := true
	b.positions(term, func(bit uint64) {
		if b[bit/8]&(1<<(bit%8)) == 0 {
			present = false
		}
	})
	return present
}

func filterFor(shards []map[string][]uint64) bloom {
	var terms []string
	for _, shard := range shards {
		for term := range shard {
			terms = append(terms, term)
		}
	}
	return newBloom(terms)
}
