package embed_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// BenchmarkHistoricalReadCold reads random historical versions from an on-disk namespace whose
// version cache is far too small to hold them (10,000 versions of ~1.6 KB against the store's
// 4 MiB floor - 64 shards x 64 KiB), so most reads are cold: the tree at the
// commit, the version's location record from the SSTables, and the commit frame from the delta
// log (see engine.ServerEngine.GetDocument). This is the path the SSTable block cache, the pooled
// frame reads and the size-exact zstd decode change; the budget sets how much of the SSTable
// working set the block cache (a quarter of it) can hold.
//
// Measured for docs/benchmarks/2026-10-13-read-path-memory.md, A/B against origin/main.
func BenchmarkHistoricalReadCold(b *testing.B) {
	for _, budget := range []int64{4 << 20, 32 << 20} {
		b.Run(fmt.Sprintf("budget-%dMiB", budget>>20), func(b *testing.B) {
			root := b.TempDir()
			opts := embed.FileRuntimeOptions{}
			opts.Storage.MemoryBudgetBytes = budget
			opts.Storage.DocumentCacheBytes = 64 << 10
			// Trees stay resident, so what is measured is the version read, not tree rebuilding.
			opts.Storage.HistoryTreeCacheBytes = 512 << 20
			// Setup speed only: the reads measured do not depend on how the writes were synced.
			opts.Storage.Durability = storage.DurabilityAsync
			rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/hist", schema.None(), opts)
			if err != nil {
				b.Fatal(err)
			}
			defer rt.Close()
			const docs, versions = 400, 25
			type read struct {
				id   codec.UUID
				tree codec.Hash
				rev  int
			}
			var reads []read
			rng := rand.New(rand.NewSource(1))
			pad := strings.Repeat("x", 1500)
			ids := make([]string, docs)
			for d := range ids {
				ids[d] = fmt.Sprintf("%08x-0000-4000-8000-%012x", d, d)
			}
			for v := 0; v < versions; v++ {
				for d := 0; d < docs; d++ {
					text := fmt.Sprintf(`{"id":%q,"rev":%d,"r":%d,"pad":%q}`, ids[d], v, rng.Int63(), pad)
					res, err := embed.PutJSONDocument(rt, "bench/hist", text)
					if err != nil {
						b.Fatal(err)
					}
					c, err := rt.DAG.GetCommitOrThrow(res.Commit)
					if err != nil {
						b.Fatal(err)
					}
					reads = append(reads, read{res.DocID, c.DocumentTreeHash, v})
				}
			}
			rng.Shuffle(len(reads), func(i, j int) { reads[i], reads[j] = reads[j], reads[i] })
			// One untimed pass over every version first: the cold loader's one-off log index (built
			// on its first miss) and the caches' fill are not what a steady-state read costs.
			for _, r := range reads {
				if _, err := rt.Storage.GetDocument("bench/hist", r.id, r.tree); err != nil {
					b.Fatal(err)
				}
			}
			eng, _ := rt.Storage.(*engine.ServerEngine)
			var coldBefore int64
			if eng != nil {
				coldBefore = eng.ColdDocumentLoads()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := reads[i%len(reads)]
				doc, err := rt.Storage.GetDocument("bench/hist", r.id, r.tree)
				if err != nil {
					b.Fatal(err)
				}
				if doc == nil || !strings.Contains(doc.JSON, fmt.Sprintf(`"rev":%d,`, r.rev)) {
					b.Fatalf("read %d: wrong or missing version", i)
				}
			}
			b.StopTimer()
			if eng != nil && b.N > 0 {
				b.ReportMetric(float64(eng.ColdDocumentLoads()-coldBefore)/float64(b.N), "cold/op")
			}
		})
	}
}
