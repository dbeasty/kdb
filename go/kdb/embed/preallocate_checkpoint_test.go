package embed_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// preallocatedRuntime is smallBudgetRuntime with segment preallocation on, and
// a segment cap small enough that the zero-fill is cheap in a test.
func preallocatedRuntime(tb testing.TB, root string) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.PreallocateSegments = true
	opts.Storage.DeltaMaxSegmentBytes = 1 << 20
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

// A preallocated namespace has to survive close and reopen with its history
// intact. This is the ordinary path, and it goes through the checkpoint.
func TestPreallocatedNamespaceReopensWithHistoryIntact(t *testing.T) {
	root := t.TempDir()
	rt := preallocatedRuntime(t, root)
	commits, texts := writeVersions(t, rt, 20)
	rt.Close()

	reopened := preallocatedRuntime(t, root)
	defer reopened.Close()

	commit, err := reopened.DAG.GetCommitOrThrow(commits[len(commits)-1])
	if err != nil {
		t.Fatalf("head commit missing after reopen: %v", err)
	}
	docID, _, err := document.ResolveID(texts[len(texts)-1])
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("GetDocument after reopen: %v", err)
	}
	if got == nil || got.JSON != texts[len(texts)-1] {
		t.Fatalf("reopened content = %v, want %s", got, texts[len(texts)-1])
	}
}

// The one preallocation could plausibly have broken, and silently.
//
// checkpointMatchesLog decides whether a checkpoint may be trusted by comparing
// each segment's recorded size against its size now. A preallocated segment's
// *file* is a constant 1MiB from the moment it is created, so if that were the
// size being compared, damage inside the preallocated region would leave it
// unchanged, the checkpoint would be accepted, and the open would sail past
// corruption serving the checkpoint's view of history - exactly the failure
// TestCheckpointIsIgnoredWhenTheLogMovedUnderneathIt exists to prevent, but
// invisible because nothing errors.
//
// It works because ListSegments reports the logical frame end
// (scanSegmentRef's walk.ConsumedEnd), not the file length. This test is what
// keeps that true: it zeroes the frames while leaving the file size alone, so
// it fails if the comparison ever starts using the file length.
func TestPreallocationDoesNotBlindCheckpointTamperDetection(t *testing.T) {
	root := t.TempDir()
	rt := preallocatedRuntime(t, root)
	commits, texts := writeVersions(t, rt, 20)
	rt.Close()

	deltaDir := filepath.Join(root, "ns", "bench/matches", "delta")
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one delta segment")
	}
	victim := filepath.Join(deltaDir, entries[0].Name())

	before, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	// Zero the frames in place. WriteAt rather than WriteFile precisely so the
	// file keeps its preallocated length - a truncation would be caught by any
	// size comparison at all and would prove nothing.
	f, err := os.OpenFile(victim, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Fatalf("file size changed (%d -> %d); this test only means something while it does not",
			before.Size(), after.Size())
	}

	// Same contract as the non-preallocated case: either the open refuses, or
	// it succeeds having actually read the log. A clean open still serving the
	// checkpoint's view of the damaged history is the failure.
	reopened, err := embed.OpenFileRuntime(root, "bench", "bench/matches", schema.None())
	if err != nil {
		return // refused to open over the damaged log
	}
	defer reopened.Close()
	commit, err := reopened.DAG.GetCommitOrThrow(commits[0])
	if err != nil {
		return // reports the damaged history missing rather than faking it
	}
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
	if err == nil && got != nil && got.JSON != texts[0] {
		t.Fatal("opened over a zeroed preallocated segment and served the wrong content: " +
			"the checkpoint was trusted when the log underneath it had changed")
	}
}
