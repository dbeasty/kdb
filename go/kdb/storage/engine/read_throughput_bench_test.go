package engine_test

// Read-path counterpart to write_throughput_bench_test.go, requested
// alongside the write numbers already captured in
// docs/benchmarks/phases-1-6-summary.md. Measures ReadBlob (memTable
// lookup, no disk I/O - blobs are served from memory once written) and
// GetDocument (sharded doc store lookup, Phase 2) under concurrency.

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func BenchmarkReadBlobConcurrent(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			e := newDiskBackedServerEngine(b)
			const n = 5000
			hashes := make([]codec.Hash, n)
			for i := 0; i < n; i++ {
				h, err := e.WriteBlob([]byte(fmt.Sprintf("payload-%d", i)))
				if err != nil {
					b.Fatal(err)
				}
				hashes[i] = h
			}

			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					h := hashes[i%n]
					if _, err := e.ReadBlob(h); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		})
	}
}

func BenchmarkGetDocumentConcurrent(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			e := newDiskBackedServerEngine(b)
			const n = 5000
			ids := make([]codec.UUID, n)
			for i := 0; i < n; i++ {
				id, err := codec.RandomUUID()
				if err != nil {
					b.Fatal(err)
				}
				doc, err := document.FromJSONWithID(id, fmt.Sprintf(`{"v":%d}`, i))
				if err != nil {
					b.Fatal(err)
				}
				if err := e.PutDocument("bench-ns", doc); err != nil {
					b.Fatal(err)
				}
				ids[i] = id
			}

			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					id := ids[i%n]
					if _, err := e.GetDocument("bench-ns", id, codec.Hash{}); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		})
	}
}
