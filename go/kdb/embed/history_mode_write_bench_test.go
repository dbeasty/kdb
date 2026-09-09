package embed_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// BenchmarkWriteThroughput_HistoryMode compares commit throughput between
// HistoryModeFull and HistoryModeNone on the ordinary write path (see
// document_bodies.go): document bodies are no longer copied into the blob
// store per commit, only later, at PrepareForTruncation time, so this
// benchmark should now show the two modes converging. Run under both
// durability modes: sync isolates real-world cost (fsync-dominated
// either way), memory isolates whatever CPU/alloc difference remains
// between the two modes on a bare commit.
func BenchmarkWriteThroughput_HistoryMode(b *testing.B) {
	blob := strings.Repeat("x", 200)
	durabilities := []struct {
		name string
		d    storage.Durability
	}{
		{"sync", storage.DurabilitySync},
		{"memory", storage.DurabilityMemoryOnly},
	}
	for _, durability := range durabilities {
		for _, mode := range []storage.HistoryMode{storage.HistoryModeFull, storage.HistoryModeNone} {
			name := fmt.Sprintf("durability=%s/mode=%s", durability.name, mode)
			b.Run(name, func(b *testing.B) {
				root := b.TempDir()
				opts := embed.FileRuntimeOptions{}
				opts.Storage.MemoryBudgetBytes = 64 << 20
				opts.Storage.HistoryMode = mode
				opts.Storage.Durability = durability.d
				rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
				if err != nil {
					b.Fatal(err)
				}
				defer rt.Close()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					doc, err := json.Marshal(map[string]any{
						"id": fmt.Sprintf("%08d-0000-4000-8000-000000000000", i%100_000_000),
						"n":  i, "blob": blob,
					})
					if err != nil {
						b.Fatal(err)
					}
					if _, err := embed.PutJSONDocument(rt, "bench/matches", string(doc)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkWriteThroughput_HotKeyRewrites isolates the case
// PrepareForTruncation's deferred design is meant for: the same document
// rewritten repeatedly within one retention window. Only the *current*
// version needs a blob copy, so materializing bodies once per truncation
// pass rather than once per write should collapse rewrites-per-key of the
// blob-write cost to one - amortized cost per write should approach
// HistoryModeFull's as rewrites grows, where an eager per-commit write
// would instead have stayed constant per write.
//
// b.N here counts commits; the reported ns/op is (commits + one
// Maintain pass) / b.N, so it is the amortized cost a long-running
// writer actually pays, not the bare commit cost alone.
func BenchmarkWriteThroughput_HotKeyRewrites(b *testing.B) {
	blob := strings.Repeat("x", 200)
	for _, mode := range []storage.HistoryMode{storage.HistoryModeFull, storage.HistoryModeNone} {
		b.Run(fmt.Sprintf("mode=%s", mode), func(b *testing.B) {
			root := b.TempDir()
			opts := embed.FileRuntimeOptions{}
			opts.Storage.MemoryBudgetBytes = 64 << 20
			opts.Storage.HistoryMode = mode
			opts.Storage.Durability = storage.DurabilityMemoryOnly
			rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
			if err != nil {
				b.Fatal(err)
			}
			defer rt.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc, err := json.Marshal(map[string]any{
					"id": "00000000-0000-4000-8000-000000000000",
					"n":  i, "blob": blob,
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := embed.PutJSONDocument(rt, "bench/matches", string(doc)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := rt.Maintain(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
