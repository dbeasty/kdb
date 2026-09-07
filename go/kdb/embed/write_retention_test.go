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
// session, with the runtime open - what a long-running writer actually
// costs, as opposed to what reopening one costs.
func writeSessionHeapMB(t *testing.T, s storage.HistoryStrategy, rewrites int) float64 {
	t.Helper()
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.HistoryStrategy = s
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	type doc struct {
		ID     string           `json:"id"`
		Events []map[string]any `json:"events"`
	}
	d := doc{ID: "11111111-1111-4111-8111-111111111111"}
	blob := strings.Repeat("x", 3000)
	for i := 0; i < rewrites; i++ {
		d.Events = append(d.Events, map[string]any{"seq": i, "blob": blob})
		b, err := json.Marshal(d)
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

// TestObjectsStrategyDoesNotHoardWritesInMemory guards a regression that
// the objects strategy introduced and that the defaults would otherwise
// carry.
//
// Every document version written under that strategy is stored as an
// object, and objects go into the memtable, which has no bound of its own:
// it grows on every put and shrinks only when flushed. Nothing flushed it
// on size, so a writing session held every version it had written until
// close - 350MB for the workload below, against 41MB with the strategy
// off. That is the same unbounded retention the rest of this work removes,
// arriving through the write path instead of the read path.
//
// Compared against the replay strategy rather than an absolute figure, so
// what is asserted is "storing objects does not change the shape of what a
// writer holds", which is the actual requirement.
func TestObjectsStrategyDoesNotHoardWritesInMemory(t *testing.T) {
	// Discarded: the first runtime in the process pays one-time costs that
	// would otherwise land entirely on whichever strategy ran first.
	writeSessionHeapMB(t, storage.HistoryStrategyReplay, 40)

	replayMB := writeSessionHeapMB(t, storage.HistoryStrategyReplay, 463)
	objectsMB := writeSessionHeapMB(t, storage.HistoryStrategyObjects, 463)
	t.Logf("heap while writing 463 versions: replay %.2f MB, objects %.2f MB", replayMB, objectsMB)

	if objectsMB > replayMB*2 {
		t.Fatalf("the objects strategy held %.2f MB while writing against replay's %.2f MB - "+
			"versions are accumulating in the memtable instead of being flushed", objectsMB, replayMB)
	}
}
