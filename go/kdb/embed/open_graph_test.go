package embed_test

import (
	"encoding/json"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// openHeapPerCommit is what holding a namespace open costs per commit in
// its history, measured after two collections.
//
// Twice, because one collection can leave freed-but-unswept spans counted
// as live - which is exactly the phantom that sends you hunting a
// retention bug that is not there.
func openHeapPerCommit(t *testing.T, commits int) float64 {
	t.Helper()
	root := t.TempDir()
	open := func() *embed.EmbeddedKdbRuntime {
		o := embed.FileRuntimeOptions{}
		o.Storage.MemoryBudgetBytes = 1 << 20
		rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), o)
		if err != nil {
			t.Fatal(err)
		}
		return rt
	}
	rt := open()
	for i := 0; i < commits; i++ {
		b, err := json.Marshal(map[string]any{"id": retentionDocID, "rev": i})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := embed.PutJSONDocument(rt, "bench/matches", string(b)); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	re := open()
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(re)
	re.Close()
	return float64(m.HeapAlloc) / float64(commits)
}

// TestOpenCostPerCommitDoesNotGrow guards the shape of what holding a
// namespace open costs.
//
// The commit graph is resident by design - ancestry, merge and peer-sync
// all resolve against it in memory - so this cost is linear in commits and
// this test does not pretend otherwise. What it pins is that it stays
// *linear*: a per-commit figure that climbs with history means something
// super-linear has been added, and a large one means something is being
// held that has no business being held.
//
// It has caught one of each. Opening used to read every delta segment in
// full, three times, to extract two hashes per segment, and retain the
// result - 9.17MB of a 21.11MB heap at 20,000 commits, proportional to the
// log rather than to anything live. And a sorted []string of every
// commit's hex was maintained alongside the commits, costing ~96 bytes
// each and making the graph O(n squared) to build, to serve a prefix
// lookup that scanned it linearly anyway.
func TestOpenCostPerCommitDoesNotGrow(t *testing.T) {
	small := openHeapPerCommit(t, 4000)
	large := openHeapPerCommit(t, 16000)
	t.Logf("open cost: %.0f bytes/commit at 4k commits, %.0f at 16k", small, large)

	// Linear means the per-commit figure holds; a super-linear term shows
	// up as the larger run costing more per commit than the smaller.
	if large > small*1.5 {
		t.Fatalf("open costs %.0f bytes/commit at 16k commits against %.0f at 4k - "+
			"something scales worse than linearly with history", large, small)
	}
	// A ceiling as well, because "linear in commits" is only acceptable
	// while the constant stays small. Generous against the ~520 bytes this
	// currently measures, so ordinary variation does not trip it.
	const ceiling = 900
	if large > ceiling {
		t.Fatalf("open costs %.0f bytes per commit, over the %d byte ceiling - "+
			"something is being retained per commit that need not be", large, ceiling)
	}
}
