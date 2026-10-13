package compression

import (
	"bytes"
	"testing"
)

func BenchmarkDecompress(b *testing.B) {
	input := bytes.Repeat([]byte(`{"name":"benchmark","n":12345,"tags":["a","b"]}`), 600) // ~28 KiB
	compressed, err := Compress(input, 3)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Decompress(compressed, len(input)); err != nil {
			b.Fatal(err)
		}
	}
}
