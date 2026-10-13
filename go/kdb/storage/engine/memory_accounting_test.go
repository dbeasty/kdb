package engine

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

func commitDocs(t *testing.T, e *ServerEngine, ids []codec.UUID, rev int) document.DocumentTree {
	t.Helper()
	for _, id := range ids {
		doc, err := document.FromJSONWithID(id, fmt.Sprintf(`{"rev":%d,"pad":"0123456789012345678901234567890123456789"}`, rev))
		if err != nil {
			t.Fatal(err)
		}
		if err := e.PutDocument("ns", doc); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := e.CommitTree("ns", codec.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// Trees published commit by commit are charged what they add - their changed paths - not a full
// copy each. Charged as copies, every tree of a 20,000-document namespace "cost" 3.5 MB, so a
// 1 MiB history budget held exactly one and every read at an older commit rebuilt its tree; in
// fact the run of recent trees costs a few KB each.
func TestHistoryTreesChargedByTheirDelta(t *testing.T) {
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 4 << 20, HistoryTreeCacheBytes: 1 << 20}
	e := NewServerEngine("ns", cfg, nil)
	const n = 20000
	ids := make([]codec.UUID, n)
	for i := range ids {
		ids[i], _ = codec.RandomUUID()
	}
	commitDocs(t, e, ids, 0)
	var history []document.DocumentTree
	for i := 0; i < 100; i++ {
		history = append(history, commitDocs(t, e, ids[i:i+1], i+1))
	}
	held := 0
	for _, tr := range history {
		if _, ok := e.treesByHash.Peek(tr.TreeHash); ok {
			held++
		}
	}
	if held < 90 {
		t.Fatalf("only %d of the last 100 single-document commits' trees are held under a 1 MiB budget", held)
	}
	if ev := e.treesByHash.EvictableBytes(); ev > 1<<20 {
		t.Fatalf("evictable charge %d over the 1 MiB budget", ev)
	}
	// The charge is honest: what the held trees retain beyond the live tree is about what they
	// were charged. Measured by dropping them all and seeing what the heap gives back.
	charged := e.treesByHash.ResidentBytes()
	before := heapNow()
	e.treesByHash.SetBudget(1) // keeps one: the live tree's own entry
	history = nil
	freed := before - heapNow()
	t.Logf("%d trees held, charged %.0f KB, freed %.0f KB when evicted", held, float64(charged)/1e3, float64(freed)/1e3)
	if freed > 3*charged {
		t.Fatalf("trees charged %d bytes held %d: the delta estimate undercounts by more than 3x", charged, freed)
	}
	b := e.MemoryBreakdown()
	if b.LiveTree < treeBytesPerDoc*n || b.DocumentVersions < n || b.DocumentBodiesLive > b.DocumentBodies {
		t.Fatalf("breakdown: %+v", b)
	}
	runtime.KeepAlive(e)
}

// The live-tree estimate is close to what a live tree actually occupies.
func TestLiveTreeEstimateMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	const n = 100_000
	before := heapNow()
	tree := treeOfSize(t, 7, n)
	heap := heapNow() - before
	est := treeBytesPerDoc * int64(tree.Size())
	t.Logf("%d documents: heap %.1f MB, estimate %.1f MB", n, float64(heap)/1e6, float64(est)/1e6)
	if r := float64(est) / float64(heap); r < 0.75 || r > 1.33 {
		t.Fatalf("live tree estimate is %.2fx the heap", r)
	}
	runtime.KeepAlive(tree)
}
