package document

// Phase 3 regression guard (docs/benchmarks/phase0-baseline.md): the real
// cost of BuildDocumentTree at realistic entry counts was its sort step
// recomputing UUID.String() on every comparison (O(n log n) calls) rather
// than the SHA256/wire-encode work - fixed in entriesToArrayValue by
// precomputing each sort key once. This benchmark exists so a future
// change to that function shows up as a regression here rather than only
// as an unexplained slowdown somewhere upstream.

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func makeEntries(n int) map[codec.UUID]codec.Hash {
	m := make(map[codec.UUID]codec.Hash, n)
	for i := 0; i < n; i++ {
		id, _ := codec.RandomUUID()
		sum := SHA256Digest([]byte(id.String()))
		h, _ := codec.HashFromBytes(sum)
		m[id] = h
	}
	return m
}

func BenchmarkBuildDocumentTree(b *testing.B) {
	base := makeEntries(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildDocumentTree(base); err != nil {
			b.Fatal(err)
		}
	}
}
