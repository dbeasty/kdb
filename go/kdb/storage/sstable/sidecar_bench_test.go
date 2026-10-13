package sstable

import (
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// BenchmarkSidecarLookup compares a store serving its tables from footer indexes on the heap with
// one serving them from mapped sidecars: lookups of present keys (hit) and absent ones (miss),
// with the heap the indexes cost and how many tables a miss has to search.
//
//	go test ./kdb/storage/sstable -run '^$' -bench SidecarLookup -benchmem
//
// KDB_SIDECAR_BENCH_TABLES and KDB_SIDECAR_BENCH_KEYS size the store (default 4 tables of 50,000
// keys: what a namespace holds between compactions at the default trigger).
func BenchmarkSidecarLookup(b *testing.B) {
	tables := envInt("KDB_SIDECAR_BENCH_TABLES", 4)
	perTable := envInt("KDB_SIDECAR_BENCH_KEYS", 50_000)
	for _, mode := range []string{"footer", "sidecar"} {
		b.Run(mode, func(b *testing.B) {
			if mode == "footer" {
				b.Setenv("KDB_SSTABLE_SIDECAR", "off")
			}
			shim, _ := osShim(b)
			var keys []codec.Hash
			for i := 0; i < tables; i++ {
				k, _ := writeSizedTable(b, shim, sidecarNS, perTable, 8, int64(i+1))
				keys = append(keys, k...)
			}
			heapBefore := liveHeap()
			mappedBefore := mmapfile.MappedBytes()
			store := NewLsmBlobStore(shim, sidecarNS, NewBlockCache(64<<20))
			store.SetIndexCacheBudget(1 << 40) // the footer path at its best: every index resident
			if err := store.DiscoverTables(); err != nil {
				b.Fatal(err)
			}
			for _, k := range keys[:tables*16] {
				store.Lookup(k) // load every table's index or sidecar
			}
			for i := 0; i < 1000; i++ {
				store.Lookup(keys[i*len(keys)/1000]) // and warm the block cache a little
			}
			heap := liveHeap() - heapBefore
			mapped := mmapfile.MappedBytes() - mappedBefore
			b.Logf("%s: %d tables x %d keys: index heap %.1f MB (%.0f B/key), mapped %.1f MB (%.0f B/key)",
				mode, tables, perTable, float64(heap)/1e6, float64(heap)/float64(len(keys)),
				float64(mapped)/1e6, float64(mapped)/float64(len(keys)))
			rng := rand.New(rand.NewSource(99))
			absent := make([]codec.Hash, 4096)
			for i := range absent {
				absent[i] = randomHash(rng)
			}
			if mode == "sidecar" {
				searched := 0
				order := store.pin()
				for _, k := range absent {
					for _, h := range order {
						if sc := store.sidecarFor(h); sc != nil && bloomMayContain(sc.bloom, sc.bloomBlocks, &k) {
							searched++
						}
					}
				}
				store.release(order)
				b.Logf("sidecar: a miss searches %.3f of %d tables (the footer path probes all %d)",
					float64(searched)/float64(len(absent)), tables, tables)
			}
			b.Run("hit", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, _, found := store.Lookup(keys[i%1000*len(keys)/1000]); !found {
						b.Fatal("miss")
					}
				}
			})
			b.Run("miss", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if store.Contains(absent[i%len(absent)]) {
						b.Fatal("found an absent key")
					}
				}
			})
			b.Run("contains", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if !store.Contains(keys[(i*7919)%len(keys)]) {
						b.Fatal("miss")
					}
				}
			})
			store.Close()
		})
	}
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}
