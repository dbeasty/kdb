package compression

import (
	"bytes"
	"testing"
)

// BenchmarkCompress guards the fix for the engine's single largest allocation
// source. Compress used to build a throwaway zstd.Encoder per call, which
// eagerly allocates one ~1.25MiB encoder state per GOMAXPROCS slot - ~21MB of
// garbage for one call, paid once per commit appended to the delta log and once
// per entry in an SSTable flush.
//
// Watch B/op, not ns/op: anything above a few KB here means the encoder pool in
// zstd.go stopped being used.
func BenchmarkCompress(b *testing.B) {
	// A commit payload's rough shape: small, structured, compressible.
	input := bytes.Repeat([]byte(`{"id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff","v":42}`), 8)
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Compress(input, 3); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompressParallel covers the concurrent case the pool exists to
// serve: several goroutines compressing at once should each get their own
// encoder state without either serializing or reallocating one per call.
func BenchmarkCompressParallel(b *testing.B) {
	input := bytes.Repeat([]byte(`{"id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff","v":42}`), 8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Compress(input, 3); err != nil {
				b.Fatal(err)
			}
		}
	})
}
