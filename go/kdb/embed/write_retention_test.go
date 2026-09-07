package embed_test

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// writeSessionHeapMB is the heap still held part-way through a writing
// session, with the runtime open - what a long-running writer costs, as
// opposed to what reopening one costs. The document is a constant size, so
// what is measured is per-commit accumulation rather than document bytes.
func writeSessionHeapMB(t *testing.T, s storage.HistoryStrategy, rewrites int) float64 {
	t.Helper()
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.HistoryStrategy = s
	// A small budget so every cache is already saturated at the smaller of
	// the two write counts. Without it the comparison mostly measures
	// caches filling towards their budgets, which is bounded growth but
	// growth all the same, and it muddies what is being asserted:
	// accumulation *beyond* what the budgets allow.
	opts.Storage.MemoryBudgetBytes = 1 << 20
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	blob := strings.Repeat("x", 10_000)
	for i := 0; i < rewrites; i++ {
		b, err := json.Marshal(map[string]any{
			"id": retentionDocID, "rev": i, "blob": blob,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := embed.PutJSONDocument(rt, "bench/matches", string(b)); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(rt)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

// TestObjectsStrategyDoesNotHoardWritesInMemory guards a regression the
// objects strategy introduced and shipped as the default.
//
// Every document version written under that strategy is stored as an
// object, objects go into the memtable, and the memtable had no bound of
// its own: it grew on every put and shrank only when flushed, which only
// Close did. A writing session therefore held every version it had ever
// written - 350MB for this workload, against 41MB with the strategy off.
// The same unbounded retention the rest of this work removes, arriving
// through the write path instead of the read path.
//
// Asserted by writing four times as much, under a budget small enough
// that every cache is already full at the smaller count, and requiring the
// heap not to follow: accumulation is linear in commits, so 4x the commits
// would be close to 4x the memory, while a bounded writer barely moves.
// What is left growing is the commit graph, which is separately unbounded
// and not what this test is about.
// Deliberately not a ratio against the replay strategy - the two have
// legitimately different budgets, and pinning one against the other made
// this test fail the moment an unrelated improvement lowered the baseline.
func TestObjectsStrategyDoesNotHoardWritesInMemory(t *testing.T) {
	// Discarded: the first runtime in the process pays one-time costs that
	// would otherwise land entirely on whichever measurement ran first.
	writeSessionHeapMB(t, storage.HistoryStrategyObjects, 60)

	const base = 250
	small := writeSessionHeapMB(t, storage.HistoryStrategyObjects, base)
	large := writeSessionHeapMB(t, storage.HistoryStrategyObjects, base*4)
	t.Logf("heap while writing: %d versions %.2f MB, %d versions %.2f MB",
		base, small, base*4, large)

	if small <= 0 {
		t.Skip("could not measure heap")
	}
	// 2.5, not something tighter. Linear accumulation - the bug - is 4x for
	// 4x the writes, and the original was 8.5x. What is left growing is the
	// commit graph, plus per-allocation overhead that the race detector
	// inflates enough to matter at these sizes: the same comparison
	// measures anywhere from 1.1x to 1.7x run to run under -race. A
	// threshold that splits 1.7 from 4 separates a bounded writer from an
	// accumulating one; one that splits 1.4 from 1.7 just fails on
	// Tuesdays.
	if ratio := large / small; ratio > 2.5 {
		t.Fatalf("4x the writes cost %.1fx the memory (%.2f MB -> %.2f MB); "+
			"versions are accumulating in the memtable instead of being flushed", ratio, small, large)
	}
}
