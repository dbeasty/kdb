package embed_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// openChurnMB is how much a single open allocates, which is the number the
// checkpoint exists to hold down: it tracks how much of the delta log the
// open had to decode.
func openChurnMB(t *testing.T, root string) float64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rt, err := embed.OpenFileRuntime(root, "bench", "bench/matches", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	rt.Close()
	return float64(after.TotalAlloc-before.TotalAlloc) / (1024 * 1024)
}

// TestCheckpointMakesOpenSkipTheLog is the property the checkpoint exists
// for. Compared as a ratio against the same store opened with its
// checkpoint removed, so it measures "did this read the history" rather
// than any particular machine's megabytes.
func TestCheckpointMakesOpenSkipTheLog(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	writeVersions(t, rt, 200)
	rt.Close()

	withCheckpoint := openChurnMB(t, root)

	// Same bytes, same code, only the checkpoint taken away.
	removeCheckpoints(t, root)
	withoutCheckpoint := openChurnMB(t, root)

	t.Logf("open churn: %.2f MB with a checkpoint, %.2f MB without", withCheckpoint, withoutCheckpoint)
	if withCheckpoint*4 > withoutCheckpoint {
		t.Fatalf("opening with a checkpoint allocated %.2f MB against %.2f MB without one - "+
			"the checkpoint is not keeping open off the delta log", withCheckpoint, withoutCheckpoint)
	}
}

// TestCheckpointReplaysTheTail covers the ordinary crash: a process wrote
// commits and died before it could checkpoint, leaving a checkpoint that
// is older than the newest segments. Everything written after the point
// the checkpoint covers has to come back from the log.
//
// Simulated by putting the earlier checkpoint back over a log that has
// since grown, which is byte-for-byte the state a kill leaves behind -
// there is deliberately no API for abandoning a runtime uncleanly.
func TestCheckpointReplaysTheTail(t *testing.T) {
	root := t.TempDir()
	first := smallBudgetRuntime(t, root)
	writeVersions(t, first, 5)
	first.Close()

	stale := readCheckpointFiles(t, root)

	second := smallBudgetRuntime(t, root)
	body, err := json.Marshal(map[string]any{
		"id": retentionDocID, "rev": "written-after-the-checkpoint",
		"blob": strings.Repeat("q", 10_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(second, "bench/matches", string(body)); err != nil {
		t.Fatal(err)
	}
	second.Close()

	// Roll the checkpoint back to what it was before that write.
	writeCheckpointFiles(t, stale)

	third := smallBudgetRuntime(t, root)
	defer third.Close()
	docID, _, err := document.ResolveID(string(body))
	if err != nil {
		t.Fatal(err)
	}
	_, head, ok, err := third.DAG.HeadCommit()
	if err != nil || !ok {
		t.Fatalf("no head after reopen: %v", err)
	}
	got, err := third.Storage.GetDocument("bench/matches", docID, head.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != string(body) {
		t.Fatal("the write made after the checkpoint was lost - the delta tail was not replayed")
	}
}

// TestCheckpointIsIgnoredWhenTheLogMovedUnderneathIt keeps the checkpoint
// from masking damage. A checkpoint that no longer matches the segments it
// was written against must be discarded, so the log - the authority - is
// read and whatever is wrong with it surfaces.
func TestCheckpointIsIgnoredWhenTheLogMovedUnderneathIt(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	commits, texts := writeVersions(t, rt, 20)
	rt.Close()

	// Truncate the oldest sealed segment, exactly the damage a checkpoint
	// would otherwise let open sail past.
	deltaDir := filepath.Join(root, "ns", "bench/matches", "delta")
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one delta segment")
	}
	victim := filepath.Join(deltaDir, entries[0].Name())
	if err := os.WriteFile(victim, []byte{0xDE, 0xAD, 0xBE, 0xEF}, 0o644); err != nil {
		t.Fatal(err)
	}

	// Either the open fails outright (the log cannot be replayed) or it
	// succeeds having actually read the log. What must not happen is a
	// clean open that silently serves the checkpoint's view of history.
	reopened, err := embed.OpenFileRuntime(root, "bench", "bench/matches", schema.None())
	if err != nil {
		return // replayed the damaged log and refused, which is the point
	}
	defer reopened.Close()
	commit, err := reopened.DAG.GetCommitOrThrow(commits[0])
	if err != nil {
		return // the damaged history is reported missing rather than faked
	}
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
	if err == nil && got != nil && got.JSON != texts[0] {
		t.Fatal("opened over a truncated log and served the wrong content for a historical commit")
	}
}

// TestHistoricalReadAfterCheckpointRestore covers what a checkpoint
// deliberately leaves out. It carries the live tree only, so a read at an
// older commit has to resolve through the historical-tree path rather than
// finding the tree already resident.
func TestHistoricalReadAfterCheckpointRestore(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	commits, texts := writeVersions(t, rt, 60)
	rt.Close()

	reopened := smallBudgetRuntime(t, root)
	defer reopened.Close()

	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 17, 45} {
		commit, err := reopened.DAG.GetCommitOrThrow(commits[i])
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("version %d: not readable at its own commit after a checkpoint restore", i)
		}
		if got.JSON != texts[i] {
			t.Fatalf("version %d: wrong content after a checkpoint restore", i)
		}
	}
}

type checkpointFile struct {
	path string
	data []byte
}

func checkpointPaths(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		// Checkpoints land under snap/ with the ":" of their key replaced
		// (see OSByteStore.snapPathFor); enlistment snapshots share the
		// directory, hence matching the checkpoint key rather than the dir.
		if strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func readCheckpointFiles(t *testing.T, root string) []checkpointFile {
	t.Helper()
	paths := checkpointPaths(t, root)
	if len(paths) == 0 {
		t.Fatal("no checkpoint was written, so this test is not exercising what it claims to")
	}
	out := make([]checkpointFile, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, checkpointFile{path: p, data: b})
	}
	return out
}

func writeCheckpointFiles(t *testing.T, files []checkpointFile) {
	t.Helper()
	for _, f := range files {
		if err := os.WriteFile(f.path, f.data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func removeCheckpoints(t *testing.T, root string) {
	t.Helper()
	found := false
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") {
			if err := os.Remove(path); err != nil {
				return err
			}
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("no checkpoint was written, so this test is not comparing what it claims to")
	}
}

// TestCheckpointsCanBeTurnedOff covers the setting, and the part of it
// that is easy to get wrong: a namespace with checkpoints disabled must
// also not *read* one left behind from when they were enabled, or turning
// the setting off would not actually put the open back on the log.
func TestCheckpointsCanBeTurnedOff(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	commits, texts := writeVersions(t, rt, 60)
	rt.Close()

	if len(checkpointPaths(t, root)) == 0 {
		t.Fatal("expected a checkpoint from the first session")
	}

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.DisableCheckpoints = true
	reopened, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// Replayed from the log rather than restored, and therefore complete:
	// every version still readable at the commit that wrote it.
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 30, 59} {
		commit, err := reopened.DAG.GetCommitOrThrow(commits[i])
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		if got == nil || got.JSON != texts[i] {
			t.Fatalf("version %d: wrong content with checkpoints disabled", i)
		}
	}
}

// TestExplicitRetentionBudgetsAreHonoured checks the budget settings reach
// the engine rather than being quietly replaced by the derived defaults.
func TestExplicitRetentionBudgetsAreHonoured(t *testing.T) {
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	// A hot-tier budget large enough that the derived defaults would keep
	// everything, with explicit budgets small enough that they cannot.
	opts.Storage.MemoryBudgetBytes = 512 << 20
	opts.Storage.DocumentCacheBytes = 1 << 20
	opts.Storage.CommitOpsBytes = 1 << 20
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	commits, texts := writeVersions(t, rt, 120)
	defer rt.Close()

	e, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skip("not the server engine")
	}
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	commit, err := rt.DAG.GetCommitOrThrow(commits[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := rt.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != texts[0] {
		t.Fatal("the oldest version was not readable")
	}
	if e.ColdDocumentLoads() == 0 {
		t.Fatal("nothing was evicted under a 1MB document budget, so the explicit setting was ignored " +
			"in favour of the budget derived from MemoryBudgetBytes")
	}
}
