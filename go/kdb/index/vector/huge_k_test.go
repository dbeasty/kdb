package vector_test

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/vector"
)

// A vector query with no LIMIT asks for every document (the Kotlin engine does this; k is then
// effectively unbounded). k must not size an allocation: the visited-set hint, the candidate
// heap and the result heap are all bounded by the number of nodes in the index, never by k.

func bigStore(tb testing.TB, n, dims int) (*vector.Store, [][]float32) {
	tb.Helper()
	d, err := dag.NewInMemoryCommitDag("bench/vectors")
	if err != nil {
		tb.Fatal(err)
	}
	head, _ := d.Head()
	desc := index.Descriptor{
		IndexID: mustUUID2(tb, "00000000-0000-4000-8000-000000009003"), FieldName: "embedding",
		Fields: []string{"embedding"}, Type: index.IndexTypeVector,
		Options: map[string]string{index.OptionDimensions: itoa(dims), index.OptionMetric: "cosine"},
	}
	s, err := vector.NewVectorStore(desc, d, vector.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
		id := codec.UUID{MSB: int64(i) + 1, LSB: 1}
		if err := s.Put(index.Entry{DocID: id, Key: index.NewVectorKey(v), CommitHash: head}); err != nil {
			tb.Fatal(err)
		}
	}
	return s, vecs
}

func mustUUID2(tb testing.TB, s string) codec.UUID {
	id, err := codec.UUIDFromString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return id
}

func TestNearestNeighboursWithHugeKIsBoundedByTheIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("time and allocation bounds over a 20,000-vector index; runs nightly, or without -short")
	}
	const n, dims = 20000, 32
	s, vecs := bigStore(t, n, dims)
	q := vecs[123]

	// Warm: build the graph (first approximate query), so the timings below are the query alone.
	if _, err := s.NearestNeighbours(q, 10, nil); err != nil {
		t.Fatal(err)
	}
	base, _ := s.NearestNeighbours(q, 10, nil)

	for _, k := range []int{n, 1_000_000_000, int(^uint(0) >> 1)} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		got, err := s.NearestNeighbours(q, k, nil)
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		alloc := after.TotalAlloc - before.TotalAlloc
		t.Logf("k=%d: %d hits in %v, %d MiB allocated", k, len(got), elapsed, alloc>>20)
		if len(got) == 0 || len(got) > n {
			t.Fatalf("k=%d: %d hits from an index of %d", k, len(got), n)
		}
		for i := range base {
			if got[i].DocID != base[i].DocID {
				t.Fatalf("k=%d: the top results differ from k=10 at position %d", k, i)
			}
		}
		// The unbounded query may legitimately walk the whole graph; it may not allocate
		// orders of magnitude more than the graph itself holds.
		if alloc > 256<<20 {
			t.Fatalf("k=%d allocated %d MiB for an index of %d vectors of %d dims", k, alloc>>20, n, dims)
		}
		if elapsed > 10*time.Second {
			t.Fatalf("k=%d took %v", k, elapsed)
		}
	}
}

func BenchmarkNearestNeighboursUnboundedK(b *testing.B) {
	const n, dims = 50000, 64
	s, vecs := bigStore(b, n, dims)
	_, _ = s.NearestNeighbours(vecs[0], 10, nil)
	for _, k := range []int{10, n, 1_000_000_000} {
		b.Run(itoa(k), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := s.NearestNeighbours(vecs[i%n], k, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
