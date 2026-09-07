package embed_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// tinyTreeBudgetRuntime opens a runtime whose historical-tree store is far
// too small to hold a run's worth of trees, so these tests exercise
// eviction and the paths that recover from it rather than finding
// everything still resident.
func tinyTreeBudgetRuntime(tb testing.TB, root string, s storage.HistoryStrategy) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.HistoryStrategy = s
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.HistoryTreeCacheBytes = 32 << 10
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

// writeTinyVersions rewrites one small document, returning each version's
// commit and text. Small on purpose: what these tests measure is the cost
// per commit, which the document trie charges regardless of size.
func writeTinyVersions(tb testing.TB, rt *embed.EmbeddedKdbRuntime, n int) ([]codec.Hash, []string) {
	tb.Helper()
	commits := make([]codec.Hash, 0, n)
	texts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		b, err := json.Marshal(map[string]any{"id": retentionDocID, "rev": i})
		if err != nil {
			tb.Fatal(err)
		}
		res, err := embed.PutJSONDocument(rt, "bench/matches", string(b))
		if err != nil {
			tb.Fatal(err)
		}
		commits = append(commits, res.Commit)
		texts = append(texts, string(b))
	}
	return commits, texts
}

func readAt(t *testing.T, rt *embed.EmbeddedKdbRuntime, commit codec.Hash, docID codec.UUID) *document.Document {
	t.Helper()
	c, err := rt.DAG.GetCommitOrThrow(commit)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rt.Storage.GetDocument("bench/matches", docID, c.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestHistoricalReadsSurviveTreeEviction is the guarantee that makes
// bounding the tree store acceptable: a tree evicted under the budget is
// still resolvable, by object lookup under `objects` and by folding the
// log under `replay`.
func TestHistoricalReadsSurviveTreeEviction(t *testing.T) {
	for _, s := range []storage.HistoryStrategy{
		storage.HistoryStrategyReplay,
		storage.HistoryStrategyObjects,
	} {
		t.Run(s.String(), func(t *testing.T) {
			root := t.TempDir()
			rt := tinyTreeBudgetRuntime(t, root, s)
			commits, texts := writeTinyVersions(t, rt, 400)
			rt.Close()

			re := tinyTreeBudgetRuntime(t, root, s)
			defer re.Close()
			docID, _, err := document.ResolveID(texts[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, i := range []int{0, 1, 199, 398, 399} {
				got := readAt(t, re, commits[i], docID)
				if got == nil {
					t.Fatalf("version %d: not readable at its own commit after tree eviction", i)
				}
				if got.JSON != texts[i] {
					t.Fatalf("version %d: wrong content after tree eviction", i)
				}
			}

			e, ok := re.Storage.(*engine.ServerEngine)
			if !ok {
				t.Skip("not the server engine")
			}
			// The budget is the point: reading history must not have
			// quietly retained all of it.
			if resident := e.HistoryTreesResidentBytes(); resident > 4*(32<<10) {
				t.Fatalf("historical trees hold %d bytes against a %d byte budget - eviction is not happening",
					resident, 32<<10)
			}
		})
	}
}

// TestHistoryWalkStaysLinear is the test that pins the nearest-cached-
// ancestor behaviour of the `replay` rebuild.
//
// Folding a tree from genesis on every miss makes a tree at position k
// cost O(k), so walking a history of H commits costs 1 + 2 + ... + H -
// quadratic, and doubling the history quadruples the work. Folding from
// the nearest cached ancestor instead makes each step of an
// oldest-to-newest walk O(1), because the tree for the previous commit is
// still resident one step behind.
//
// Measured as allocation rather than time, which is what a quadratic walk
// actually spends and what does not depend on how busy the machine is.
func TestHistoryWalkStaysLinear(t *testing.T) {
	walk := func(n int) float64 {
		root := t.TempDir()
		rt := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyReplay)
		commits, texts := writeTinyVersions(t, rt, n)
		rt.Close()

		re := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyReplay)
		defer re.Close()
		docID, _, err := document.ResolveID(texts[0])
		if err != nil {
			t.Fatal(err)
		}
		// Touch the oldest commit first, so the one-off pass that indexes
		// commit operations is paid before the measurement starts and does
		// not land on whichever size ran first.
		readAt(t, re, commits[0], docID)

		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range commits {
			if got := readAt(t, re, commits[i], docID); got == nil {
				t.Fatalf("version %d unreadable during the walk", i)
			}
		}
		runtime.ReadMemStats(&after)
		return float64(after.TotalAlloc-before.TotalAlloc) / (1024 * 1024)
	}

	const base = 300
	small := walk(base)
	large := walk(base * 2)
	t.Log(fmt.Sprintf("walking history: %d commits %.2f MB, %d commits %.2f MB", base, small, base*2, large))

	if small <= 0 {
		t.Skip("could not measure allocation")
	}
	// Linear is 2x for twice the commits; quadratic is 4x. Allow room for
	// the constant factors either side of that without admitting 4x.
	if ratio := large / small; ratio > 3 {
		t.Fatalf("doubling the history multiplied the walk's work by %.1fx (%.2f MB -> %.2f MB); "+
			"a rebuild is restarting from genesis instead of folding from the nearest cached ancestor",
			ratio, small, large)
	}
}

// TestTreeMemoryDoesNotFollowCommitCount is the property the whole change
// exists for. Every tree a namespace produced used to stay resident, at
// roughly 8.6KB per commit whatever the document's size, so holding a
// namespace open cost memory proportional to how many times it had been
// written.
func TestTreeMemoryDoesNotFollowCommitCount(t *testing.T) {
	measure := func(n int) float64 {
		root := t.TempDir()
		rt := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyObjects)
		writeTinyVersions(t, rt, n)
		rt.Close()

		re := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyObjects)
		defer re.Close()
		return liveHeapMB(re)
	}
	measure(200) // discard: one-off per-process costs

	small := measure(1000)
	large := measure(4000)
	t.Log(fmt.Sprintf("holding a namespace open: %d commits %.2f MB, %d commits %.2f MB",
		1000, small, 4000, large))

	if small <= 0 {
		t.Skip("could not measure heap")
	}
	// 4x the commits. Trees no longer scale with that; what still does is
	// the commit graph itself, which is separately unbounded and is called
	// out in docs/kdb-bounded-history-trees.md.
	if ratio := large / small; ratio > 3 {
		t.Fatalf("4x the commits cost %.1fx the memory (%.2f MB -> %.2f MB); "+
			"trees are accumulating with commit count again", ratio, small, large)
	}
}
