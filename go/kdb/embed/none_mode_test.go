package embed_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func noneRuntime(t *testing.T, root string, window storage.RetentionWindow) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func fullRuntime(t *testing.T, root string) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeFull
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func deltaSegmentCount(t *testing.T, root string) int {
	t.Helper()
	dir := filepath.Join(root, "ns", "app", "docs", "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatal(err)
	}
	return len(entries)
}

// writeSessions runs n open/write/close cycles. A delta segment is sealed
// at close, so this is how a namespace comes to have several of them.
func writeSessions(t *testing.T, root string, n int, window storage.RetentionWindow) {
	t.Helper()
	for i := 0; i < n; i++ {
		rt := noneRuntime(t, root, window)
		for j := 0; j < 3; j++ {
			if _, err := embed.PutJSONDocument(rt, "app/docs",
				fmt.Sprintf(`{"id":"doc-%d","session":%d}`, j, i)); err != nil {
				t.Fatal(err)
			}
		}
		rt.Close()
	}
}

// The headline property: with a window that keeps nothing, the log stops
// growing with history, and the data is still all there.
func TestTruncationReclaimsSegmentsAndKeepsTheData(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	// Written under the default window, so the sessions accumulate
	// segments rather than each one reclaiming the last.
	writeSessions(t, root, 5, storage.RetentionWindow{})

	before := deltaSegmentCount(t, root)
	if before < 5 {
		t.Fatalf("expected at least 5 delta segments after 5 sessions, got %d", before)
	}

	rt := noneRuntime(t, root, window)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Fatal("maintenance reclaimed nothing under a zero retention window")
	}
	rt.Close()

	after := deltaSegmentCount(t, root)
	if after >= before {
		t.Fatalf("segment count did not fall: %d -> %d", before, after)
	}

	// Everything the last session wrote must still be readable, from a
	// fresh process, with most of the log gone.
	rt = noneRuntime(t, root, window)
	defer rt.Close()
	for j := 0; j < 3; j++ {
		body, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", j))
		if !ok {
			t.Fatalf("doc-%d is unreadable after truncation", j)
		}
		if !strings.Contains(body, `"session":4`) {
			t.Fatalf("doc-%d has stale content after truncation: %s", j, body)
		}
	}
}

// The same run under history=full must reclaim nothing at all: there the
// log is the record.
func TestFullModeNeverTruncates(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 4; i++ {
		rt := fullRuntime(t, root)
		if _, err := embed.PutJSONDocument(rt, "app/docs", fmt.Sprintf(`{"id":"a","n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
		rt.Close()
	}
	before := deltaSegmentCount(t, root)

	rt := fullRuntime(t, root)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	if res.Removed != 0 {
		t.Fatalf("history=full reclaimed %d segments; it must reclaim none", res.Removed)
	}
	if got := deltaSegmentCount(t, root); got != before {
		t.Fatalf("segment count changed under history=full: %d -> %d", before, got)
	}
}

// The default window keeps a day, so a namespace written seconds ago
// reclaims nothing even though everything in it is checkpointed.
func TestTheDefaultWindowReclaimsNothingImmediately(t *testing.T) {
	root := t.TempDir()
	writeSessions(t, root, 4, storage.RetentionWindow{})
	before := deltaSegmentCount(t, root)

	rt := noneRuntime(t, root, storage.RetentionWindow{})
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	if res.Removed != 0 {
		t.Fatalf("the 24h default reclaimed %d segments written moments ago", res.Removed)
	}
	if got := deltaSegmentCount(t, root); got != before {
		t.Fatalf("segments went missing under the default window: %d -> %d", before, got)
	}
}

// Deleting the checkpoint of a truncated namespace leaves nothing that can
// reconstruct it. That must fail loudly rather than opening with a silently
// incomplete database.
func TestATruncatedNamespaceWithNoCheckpointRefusesToOpen(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	writeSessions(t, root, 4, storage.RetentionWindow{})

	rt := noneRuntime(t, root, window)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Skip("nothing was truncated, so there is no truncated namespace to test")
	}
	rt.Close()

	// Remove the snapshot directory, which is where the checkpoint lives.
	if err := os.RemoveAll(filepath.Join(root, "snap")); err != nil {
		t.Fatal(err)
	}

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	_, err = embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	var truncated *embed.TruncatedLogError
	if !errors.As(err, &truncated) {
		t.Fatalf("want TruncatedLogError when the checkpoint of a truncated namespace is gone, got %v", err)
	}
}

// Truncation must never run before the live dataset is durable somewhere
// other than the log. This asserts the invariant directly.
func TestLiveBodiesAreDurableBeforeAnySegmentGoes(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	writeSessions(t, root, 3, storage.RetentionWindow{})

	rt := noneRuntime(t, root, window)
	defer rt.Close()
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	type durability interface{ LiveBodiesDurable() bool }
	eng, ok := rt.Storage.(durability)
	if !ok {
		t.Skip("this runtime has no blob store to check")
	}
	if !eng.LiveBodiesDurable() {
		t.Fatal("truncation ran with live document bodies still only in the delta log")
	}
}

// Maintenance is idempotent: running it repeatedly must converge rather
// than eating one more segment each time.
func TestMaintenanceIsIdempotent(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	writeSessions(t, root, 4, storage.RetentionWindow{})

	rt := noneRuntime(t, root, window)
	defer rt.Close()
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	settled := deltaSegmentCount(t, root)
	for i := 0; i < 3; i++ {
		res, err := rt.Maintain()
		if err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		if res.Removed != 0 {
			t.Fatalf("pass %d removed %d more segments; maintenance should have converged", i, res.Removed)
		}
	}
	if got := deltaSegmentCount(t, root); got != settled {
		t.Fatalf("repeated maintenance kept deleting: %d -> %d", settled, got)
	}
}

// The accident that wrote these tests: with a zero window, ordinary
// open/write/close cycles keep the log at a constant size on their own,
// because close checkpoints and then reclaims. That is history=none
// working as designed - the footprint is a function of the dataset, not of
// how many sessions have ever run - so it is worth asserting rather than
// rediscovering.
func TestRepeatedSessionsDoNotGrowTheLogUnderAZeroWindow(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	writeSessions(t, root, 3, window)
	settled := deltaSegmentCount(t, root)

	writeSessions(t, root, 6, window)
	if got := deltaSegmentCount(t, root); got > settled {
		t.Fatalf("the log grew with sessions: %d after 3, %d after 9", settled, got)
	}

	// And the data is still right.
	rt := noneRuntime(t, root, window)
	defer rt.Close()
	body, ok := readDoc(t, rt, "doc-0")
	if !ok || !strings.Contains(body, `"session":5`) {
		t.Fatalf("after nine sessions doc-0 reads %q", body)
	}
}

// Crash cases. There is deliberately no API for abandoning a runtime
// uncleanly, so these reconstruct the on-disk state a kill leaves behind -
// the same technique TestCheckpointReplaysTheTail uses.

// The ordinary crash on a namespace that has not truncated: commits
// written after the last checkpoint come back from the log tail, exactly
// as under history=full. history=none must not have broken that.
func TestNoneModeReplaysTheTailAfterACrash(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{}

	rt := noneRuntime(t, root, window)
	if _, err := embed.PutJSONDocument(rt, "app/docs", `{"id":"a","n":0}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	stale := readCheckpointFiles(t, root)

	rt = noneRuntime(t, root, window)
	if _, err := embed.PutJSONDocument(rt, "app/docs", `{"id":"a","n":1}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	// Roll the checkpoint back to before that write: the state a kill
	// between the commit and the next checkpoint leaves.
	writeCheckpointFiles(t, stale)

	rt = noneRuntime(t, root, window)
	defer rt.Close()
	body, ok := readDoc(t, rt, "a")
	if !ok || !strings.Contains(body, `"n":1`) {
		t.Fatalf("the commit written after the checkpoint did not come back: %q", body)
	}
}

// The unrecoverable one, and the trade history=none makes: a checkpoint
// rolled back to before a truncation describes segments that no longer
// exist, and nothing left on disk can reconstruct them. That must fail
// loudly rather than open with a hole in it.
func TestAStaleCheckpointOverATruncatedLogRefusesToOpen(t *testing.T) {
	root := t.TempDir()
	writeSessions(t, root, 4, storage.RetentionWindow{})
	stale := readCheckpointFiles(t, root)

	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	rt := noneRuntime(t, root, window)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	if res.Removed == 0 {
		t.Skip("nothing was truncated, so there is no state to test")
	}

	writeCheckpointFiles(t, stale)

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	_, err = embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	var truncated *embed.TruncatedLogError
	if !errors.As(err, &truncated) {
		t.Fatalf("want TruncatedLogError for a stale checkpoint over a truncated log, got %v", err)
	}
	if !strings.Contains(err.Error(), "backup") {
		t.Fatalf("the error should say what the operator can actually do: %v", err)
	}
}

// Disabling checkpoints on a namespace that has already truncated is the
// same unrecoverable state by another route, and must fail the same way
// rather than silently replaying a partial log.
func TestDisablingCheckpointsOnATruncatedNamespaceRefusesToOpen(t *testing.T) {
	root := t.TempDir()
	writeSessions(t, root, 4, storage.RetentionWindow{})

	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	rt := noneRuntime(t, root, window)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	if res.Removed == 0 {
		t.Skip("nothing was truncated, so there is no state to test")
	}

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	opts.Storage.DisableCheckpoints = true
	_, err = embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	var truncated *embed.TruncatedLogError
	if !errors.As(err, &truncated) {
		t.Fatalf("want TruncatedLogError with checkpoints disabled on a truncated namespace, got %v", err)
	}
}
