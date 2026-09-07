package embed_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func strategyRuntime(tb testing.TB, root string, s storage.HistoryStrategy) (*embed.EmbeddedKdbRuntime, error) {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.HistoryStrategy = s
	return embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
}

func mustStrategyRuntime(tb testing.TB, root string, s storage.HistoryStrategy) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	rt, err := strategyRuntime(tb, root, s)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

func readMetaStrategy(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "ns", "bench", "matches", "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		HistoryStrategy string `json:"historyStrategy"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m.HistoryStrategy
}

// checkHistoricalReads asserts every recorded version is still readable at
// the commit that wrote it - the guarantee both strategies owe, by
// different means.
func checkHistoricalReads(t *testing.T, rt *embed.EmbeddedKdbRuntime, commits []codec.Hash, texts []string, at []int) {
	t.Helper()
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range at {
		commit, err := rt.DAG.GetCommitOrThrow(commits[i])
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		got, err := rt.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("version %d: not readable at its own commit", i)
		}
		if got.JSON != texts[i] {
			t.Fatalf("version %d: wrong content", i)
		}
	}
}

// TestBothHistoryStrategiesServeHistoricalReads runs the same workload
// under each strategy and requires the same answers. They reach them
// differently - one by lookup, one by replaying the log - and that
// difference must not be visible in the results.
func TestBothHistoryStrategiesServeHistoricalReads(t *testing.T) {
	for _, s := range []storage.HistoryStrategy{
		storage.HistoryStrategyReplay,
		storage.HistoryStrategyObjects,
	} {
		t.Run(s.String(), func(t *testing.T) {
			root := t.TempDir()
			rt := mustStrategyRuntime(t, root, s)
			commits, texts := writeVersions(t, rt, 60)
			rt.Close()

			if got := readMetaStrategy(t, root); got != s.String() {
				t.Fatalf("namespace recorded strategy %q, want %q", got, s.String())
			}

			reopened := mustStrategyRuntime(t, root, s)
			defer reopened.Close()
			checkHistoricalReads(t, reopened, commits, texts, []int{0, 17, 45, 59})
		})
	}
}

// TestOpeningUnderTheWrongStrategyIsRefused is the compatibility rule. The
// two strategies leave different things on disk, so a namespace opened as
// the one it is not must fail loudly and name the conversion rather than
// quietly behaving as something in between.
func TestOpeningUnderTheWrongStrategyIsRefused(t *testing.T) {
	root := t.TempDir()
	rt := mustStrategyRuntime(t, root, storage.HistoryStrategyObjects)
	writeVersions(t, rt, 5)
	rt.Close()

	_, err := strategyRuntime(t, root, storage.HistoryStrategyReplay)
	if err == nil {
		t.Fatal("opened an objects namespace as replay - the strategies are not interchangeable")
	}
	var mismatch *embed.HistoryStrategyMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("got %v, want a HistoryStrategyMismatchError", err)
	}
	if mismatch.Recorded != storage.HistoryStrategyObjects ||
		mismatch.Requested != storage.HistoryStrategyReplay {
		t.Fatalf("mismatch reports %v -> %v", mismatch.Recorded, mismatch.Requested)
	}
}

// TestUnsetStrategyOpensTheNamespaceAsWhateverItIs covers the ordinary
// caller, who passes nothing and must get the namespace as it stands
// rather than the default.
func TestUnsetStrategyOpensTheNamespaceAsWhateverItIs(t *testing.T) {
	root := t.TempDir()
	rt := mustStrategyRuntime(t, root, storage.HistoryStrategyReplay)
	writeVersions(t, rt, 5)
	rt.Close()

	reopened, err := embed.OpenFileRuntime(root, "bench", "bench/matches", schema.None())
	if err != nil {
		t.Fatalf("opening with no strategy requested should adopt the namespace's own: %v", err)
	}
	reopened.Close()
	if got := readMetaStrategy(t, root); got != "replay" {
		t.Fatalf("namespace strategy became %q after a default open", got)
	}
}

// TestMigrateHistoryStrategyConvertsAndKeepsHistoryReadable is the escape
// hatch the mismatch error points at.
func TestMigrateHistoryStrategyConvertsAndKeepsHistoryReadable(t *testing.T) {
	root := t.TempDir()
	rt := mustStrategyRuntime(t, root, storage.HistoryStrategyReplay)
	commits, texts := writeVersions(t, rt, 40)
	rt.Close()

	if err := embed.MigrateHistoryStrategy(root, "bench/matches", storage.HistoryStrategyObjects); err != nil {
		t.Fatal(err)
	}
	if got := readMetaStrategy(t, root); got != "objects" {
		t.Fatalf("after migrating, namespace strategy is %q", got)
	}

	// Now openable as objects, refused as replay, and every version still
	// readable at the commit that wrote it - via tree objects this time,
	// since the objects strategy installs no rebuild fallback.
	if _, err := strategyRuntime(t, root, storage.HistoryStrategyReplay); err == nil {
		t.Fatal("a migrated namespace still opened under the old strategy")
	}
	reopened := mustStrategyRuntime(t, root, storage.HistoryStrategyObjects)
	defer reopened.Close()
	checkHistoricalReads(t, reopened, commits, texts, []int{0, 9, 25, 39})
}

// TestMigrateHistoryStrategyIsIdempotent - running it twice, or running it
// against a namespace already in the target state, must be a no-op rather
// than an error or a second conversion.
func TestMigrateHistoryStrategyIsIdempotent(t *testing.T) {
	root := t.TempDir()
	rt := mustStrategyRuntime(t, root, storage.HistoryStrategyObjects)
	commits, texts := writeVersions(t, rt, 20)
	rt.Close()

	for i := 0; i < 2; i++ {
		if err := embed.MigrateHistoryStrategy(root, "bench/matches", storage.HistoryStrategyObjects); err != nil {
			t.Fatalf("migration %d: %v", i+1, err)
		}
	}
	reopened := mustStrategyRuntime(t, root, storage.HistoryStrategyObjects)
	defer reopened.Close()
	checkHistoricalReads(t, reopened, commits, texts, []int{0, 7, 19})
}

// TestObjectsStrategyMakesHistoricalReadsCheap is the reason the objects
// strategy exists. Under replay, the first read at a historical commit
// replays the whole delta log and re-hashes every document; under objects
// it walks a short chain of tree objects. Measured as allocation, which is
// what that difference actually consists of.
func TestObjectsStrategyMakesHistoricalReadsCheap(t *testing.T) {
	measure := func(s storage.HistoryStrategy) (float64, int) {
		root := t.TempDir()
		rt := mustStrategyRuntime(t, root, s)
		commits, texts := writeVersions(t, rt, 120)
		rt.Close()

		reopened := mustStrategyRuntime(t, root, s)
		defer reopened.Close()
		docID, _, err := document.ResolveID(texts[0])
		if err != nil {
			t.Fatal(err)
		}
		commit, err := reopened.DAG.GetCommitOrThrow(commits[0])
		if err != nil {
			t.Fatal(err)
		}

		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		if got == nil || got.JSON != texts[0] {
			t.Fatalf("%s: the historical read did not return the right document", s)
		}
		return float64(after.TotalAlloc-before.TotalAlloc) / (1024 * 1024), len(texts)
	}

	replayMB, versions := measure(storage.HistoryStrategyReplay)
	objectsMB, _ := measure(storage.HistoryStrategyObjects)
	t.Logf("first historical read over %d versions: replay %.2f MB, objects %.2f MB",
		versions, replayMB, objectsMB)
	if objectsMB*4 > replayMB {
		t.Fatalf("the objects strategy allocated %.2f MB for a historical read against replay's %.2f MB - "+
			"it is not resolving by lookup", objectsMB, replayMB)
	}
}
