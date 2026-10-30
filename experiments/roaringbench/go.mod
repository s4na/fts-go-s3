module github.com/s4na/fts-go-s3/experiments/roaringbench

go 1.25.0

require (
	github.com/RoaringBitmap/roaring/v2 v2.30.0
	github.com/s4na/fts-go-s3 v0.0.0
)

require (
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.39.0 // indirect
)

replace github.com/s4na/fts-go-s3 => ../..
