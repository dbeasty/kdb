package recovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Promotion is tested on what is left on disk after each step, including steps interrupted
// halfway, because that is the only thing the next boot has to work from. A promotion that looks
// right in the happy path and loses a namespace when the machine reboots mid-copy is worse than no
// promotion at all.

func fixedClock(t string) func() time.Time {
	when, err := time.Parse(time.RFC3339, t)
	if err != nil {
		panic(err)
	}
	return func() time.Time { return when }
}

// stagedRoot builds a data root holding one namespace with n verified commits, by restoring from
// a source built the same way. Using HybridRestore rather than hand-written bytes means the
// fixture's segments are the real thing: CRC-framed, sequenced, and readable by the same scanner
// promotion verifies with.
func rootWithCommits(t *testing.T, ns string, docs int) string {
	t.Helper()
	root := t.TempDir()
	shim := newDirShimAt(t, root)
	seedCommits(t, shim, root, ns, docs)
	return root
}

func TestStageAndApplyPromotesTheNamespace(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)

	liveBefore, err := countVerifiedCommits(live, ns)
	if err != nil {
		t.Fatal(err)
	}
	stagedCount, err := countVerifiedCommits(staged, ns)
	if err != nil {
		t.Fatal(err)
	}
	if liveBefore == stagedCount {
		t.Fatalf("the fixture needs to differ: both roots hold %d commits", liveBefore)
	}

	in, err := StagePromotion(live, staged, ns, PromotionIntent{RequestedBy: "alice", JobID: "job-1"}, nil)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if in.Commits != stagedCount {
		t.Errorf("the intent should record the staged commit count: got %d, want %d", in.Commits, stagedCount)
	}
	if in.Bytes <= 0 {
		t.Error("the intent should record how much was copied")
	}

	// Staging must not have touched the live namespace: until the intent is acted on, an operator
	// can still change their mind.
	if got, _ := countVerifiedCommits(live, ns); got != liveBefore {
		t.Errorf("staging changed the live namespace: %d commits, was %d", got, liveBefore)
	}
	if pending, _ := PendingPromotion(live); pending == nil {
		t.Fatal("the promotion should be pending")
	}

	out, err := ApplyPromotion(live, fixedClock("2026-09-09T10:00:00Z"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.State != "promoted" {
		t.Fatalf("state %q, error %q", out.State, out.Error)
	}
	if got, _ := countVerifiedCommits(live, ns); got != stagedCount {
		t.Errorf("the live namespace holds %d commits, want the staged %d", got, stagedCount)
	}

	// The replaced namespace is kept, not deleted. This is the whole difference between a
	// promotion and a bet.
	if out.Superseded == "" {
		t.Fatal("the outcome should say where the old namespace went")
	}
	kept := filepath.Join(live, filepath.FromSlash(out.Superseded))
	if _, err := os.Stat(filepath.Join(kept, "meta.json")); err != nil {
		t.Errorf("the superseded namespace is not at %s: %v", kept, err)
	}

	// And nothing is left pending or staged.
	if pending, _ := PendingPromotion(live); pending != nil {
		t.Error("the intent should be cleared")
	}
	if _, err := os.Stat(filepath.Join(live, promotePayloadDir)); !os.IsNotExist(err) {
		t.Error("the staged payload should be removed")
	}
	last, err := LastPromotion(live)
	if err != nil || last == nil {
		t.Fatalf("the outcome should be readable after the fact: %v", err)
	}
	if last.RequestedBy != "alice" || last.JobID != "job-1" {
		t.Errorf("the outcome should carry who asked and why: %+v", last)
	}
}

func TestApplyDoesNothingWithoutAnIntent(t *testing.T) {
	out, err := ApplyPromotion(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Errorf("no intent means nothing to do, got %+v", out)
	}
}

// TestApplyRefusesAnIncompletePayload is the case that justifies recording a commit count: a copy
// cut short by a crash is a truncated namespace, and installing one would destroy the live data in
// favour of something incomplete.
func TestApplyRefusesAnIncompletePayload(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)
	liveBefore, _ := countVerifiedCommits(live, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	// Simulate the copy having been interrupted: drop a segment out of the payload.
	segs, err := filepath.Glob(filepath.Join(live, promotePayloadDir, "ns",
		filepath.FromSlash(ns), "delta", "*.seg"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments in the payload to remove: %v", err)
	}
	if err := os.Remove(segs[len(segs)-1]); err != nil {
		t.Fatal(err)
	}

	out, err := ApplyPromotion(live, nil)
	if err == nil {
		t.Fatal("an incomplete payload should be refused")
	}
	if out.State != "failed" {
		t.Errorf("state should be failed, got %q", out.State)
	}
	if !strings.Contains(out.Error, "incomplete") {
		t.Errorf("the reason should say what is wrong: %q", out.Error)
	}
	// The live namespace is untouched, which is the point.
	if got, _ := countVerifiedCommits(live, ns); got != liveBefore {
		t.Errorf("the live namespace was changed anyway: %d commits, was %d", got, liveBefore)
	}
	// And the failure is not retried on the next boot.
	if pending, _ := PendingPromotion(live); pending != nil {
		t.Error("a failed promotion must clear its intent rather than loop on every boot")
	}
	if again, err := ApplyPromotion(live, nil); again != nil || err != nil {
		t.Errorf("the next boot should have nothing to do, got %+v / %v", again, err)
	}
}

// TestApplyResumesAfterACrashBetweenTheRenames: the live namespace has been moved aside but the
// payload has not been installed. The next boot must finish the job, not conclude the namespace is
// gone.
func TestApplyResumesAfterACrashBetweenTheRenames(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)
	stagedCount, _ := countVerifiedCommits(staged, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	// Interrupt exactly between the two renames: the live namespace is aside, the payload waits.
	aside := filepath.Join(live, SupersededDir, "demo", "users-crash")
	if err := os.MkdirAll(filepath.Dir(aside), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(NamespaceDir(live, ns), aside); err != nil {
		t.Fatal(err)
	}

	out, err := ApplyPromotion(live, nil)
	if err != nil {
		t.Fatalf("apply should resume: %v", err)
	}
	if out.State != "promoted" {
		t.Fatalf("state %q, error %q", out.State, out.Error)
	}
	if got, _ := countVerifiedCommits(live, ns); got != stagedCount {
		t.Errorf("the payload should have been installed: %d commits, want %d", got, stagedCount)
	}
}

// TestApplyFinishesAfterACrashBetweenInstallAndCleanup: the payload has already been renamed into
// place and the intent is still there. Re-running must recognise that and not fail.
func TestApplyFinishesAfterACrashBetweenInstallAndCleanup(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)
	stagedCount, _ := countVerifiedCommits(staged, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	// Do both renames by hand, leaving the intent and an emptied payload directory behind.
	if err := os.RemoveAll(NamespaceDir(live, ns)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(NamespaceDir(filepath.Join(live, promotePayloadDir), ns),
		NamespaceDir(live, ns)); err != nil {
		t.Fatal(err)
	}

	out, err := ApplyPromotion(live, nil)
	if err != nil {
		t.Fatalf("apply should finish an interrupted install: %v", err)
	}
	if out.State != "promoted" {
		t.Fatalf("state %q, error %q", out.State, out.Error)
	}
	if got, _ := countVerifiedCommits(live, ns); got != stagedCount {
		t.Errorf("%d commits, want %d", got, stagedCount)
	}
	if pending, _ := PendingPromotion(live); pending != nil {
		t.Error("the intent should be cleared")
	}
}

// A recorded promotion whose payload is gone must not be treated as "the namespace disappeared".
func TestApplyFailsSafelyWhenThePayloadIsGone(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)
	liveBefore, _ := countVerifiedCommits(live, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(live, promotePayloadDir)); err != nil {
		t.Fatal(err)
	}

	out, err := ApplyPromotion(live, nil)
	if err == nil {
		t.Fatal("a promotion with no payload should fail")
	}
	if !strings.Contains(out.Error, "nothing was changed") {
		t.Errorf("the reason should say the live data is intact: %q", out.Error)
	}
	if got, _ := countVerifiedCommits(live, ns); got != liveBefore {
		t.Errorf("the live namespace was changed: %d commits, was %d", got, liveBefore)
	}
}

// Promoting into a namespace this root does not have is a restore of something lost, not an error.
func TestApplyPromotesIntoAnAbsentNamespace(t *testing.T) {
	ns := "demo/users"
	live := t.TempDir()
	staged := rootWithCommits(t, ns, 3)
	stagedCount, _ := countVerifiedCommits(staged, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	out, err := ApplyPromotion(live, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Superseded != "" {
		t.Errorf("there was nothing to supersede, got %q", out.Superseded)
	}
	if got, _ := countVerifiedCommits(live, ns); got != stagedCount {
		t.Errorf("%d commits, want %d", got, stagedCount)
	}
}

// TestApplyRemovesAStaleCheckpoint: a checkpoint describes the delta log it was written from. Left
// behind over a replaced log it would let the next open skip replaying data it has never seen.
func TestApplyRemovesAStaleCheckpoint(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)

	stale := checkpointPath(live, ns)
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("checkpoint of the namespace being replaced"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyPromotion(live, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("the checkpoint of the replaced log is still in place: %v", err)
	}
}

// The staged copy's own checkpoint, when it has one, comes with it.
func TestApplyInstallsTheStagedCheckpoint(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)

	from := checkpointPath(staged, ns)
	if err := os.MkdirAll(filepath.Dir(from), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(from, []byte("staged checkpoint"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyPromotion(live, nil); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(checkpointPath(live, ns))
	if err != nil {
		t.Fatalf("the staged checkpoint should have been installed: %v", err)
	}
	if string(body) != "staged checkpoint" {
		t.Errorf("wrong checkpoint in place: %q", body)
	}
}

func TestStageRefusesAStagedRootWithoutTheNamespace(t *testing.T) {
	live := rootWithCommits(t, "demo/users", 2)
	_, err := StagePromotion(live, t.TempDir(), "demo/users", PromotionIntent{}, nil)
	if err == nil {
		t.Fatal("a staged root with no such namespace should be refused")
	}
	if !strings.Contains(err.Error(), "does not hold namespace") {
		t.Errorf("the reason should name the problem: %v", err)
	}
	if pending, _ := PendingPromotion(live); pending != nil {
		t.Error("a refused stage must not leave an intent behind")
	}
}

// An empty staged copy would replace a namespace with nothing, which is never what was meant.
func TestStageRefusesAnEmptyStagedNamespace(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := t.TempDir()
	if err := os.MkdirAll(NamespaceDir(staged, ns), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(NamespaceDir(staged, ns), "meta.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil)
	if err == nil {
		t.Fatal("an empty staged namespace should be refused")
	}
	if !strings.Contains(err.Error(), "no verified commits") {
		t.Errorf("the reason should say the copy is empty: %v", err)
	}
}

func TestAbandonRemovesTheIntentAndThePayload(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 5)
	liveBefore, _ := countVerifiedCommits(live, ns)

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, nil); err != nil {
		t.Fatal(err)
	}
	had, err := AbandonPromotion(live)
	if err != nil || !had {
		t.Fatalf("abandon: %v (had=%v)", err, had)
	}
	if pending, _ := PendingPromotion(live); pending != nil {
		t.Error("the intent should be gone")
	}
	if _, err := os.Stat(filepath.Join(live, promotePayloadDir)); !os.IsNotExist(err) {
		t.Error("the payload should be gone")
	}
	// The next boot does nothing, and the live namespace is as it was.
	if out, err := ApplyPromotion(live, nil); out != nil || err != nil {
		t.Errorf("nothing should be applied after abandoning: %+v / %v", out, err)
	}
	if got, _ := countVerifiedCommits(live, ns); got != liveBefore {
		t.Errorf("%d commits, was %d", got, liveBefore)
	}

	if had, err := AbandonPromotion(live); had || err != nil {
		t.Errorf("abandoning nothing should say so quietly: had=%v err=%v", had, err)
	}
}

// Staging twice replaces the first request rather than accumulating payloads: there is one intent,
// so there must be one payload.
func TestStagingTwiceReplacesThePreviousRequest(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	first := rootWithCommits(t, ns, 4)
	second := rootWithCommits(t, ns, 7)
	secondCount, _ := countVerifiedCommits(second, ns)

	if _, err := StagePromotion(live, first, ns, PromotionIntent{JobID: "first"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := StagePromotion(live, second, ns, PromotionIntent{JobID: "second"}, nil); err != nil {
		t.Fatal(err)
	}
	pending, _ := PendingPromotion(live)
	if pending == nil || pending.JobID != "second" {
		t.Fatalf("the second request should be the pending one: %+v", pending)
	}
	out, err := ApplyPromotion(live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.State != "promoted" {
		t.Fatalf("state %q, error %q", out.State, out.Error)
	}
	if got, _ := countVerifiedCommits(live, ns); got != secondCount {
		t.Errorf("the second staged copy should be live: %d commits, want %d", got, secondCount)
	}
}

func TestSameFilesystemAnswersForPathsThatDoNotExistYet(t *testing.T) {
	dir := t.TempDir()
	same, err := SameFilesystem(dir, filepath.Join(dir, "not", "created", "yet"))
	if err != nil {
		t.Fatalf("a path with an existing ancestor should be answerable: %v", err)
	}
	if !same {
		t.Error("a subdirectory of the same directory is on the same filesystem")
	}
}

// --- fixtures -------------------------------------------------------------------------------

// newDirShimAt is newDirShim over a caller-chosen root, so a test can hold the path as well as
// the shim - promotion is about directories, not about the shim over them.
func newDirShimAt(t *testing.T, root string) storage.PlatformIOShim {
	t.Helper()
	r := root
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &r})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(
		storio.PlatformIOConfig{RootDirectory: &r, FsyncOnFlush: true}, store)
}

// seedCommits writes a chain of n commits into one sequenced segment, plus the meta.json that
// makes the directory recognisable as a namespace (embed.ListNamespaces and StagePromotion both
// key off it).
func seedCommits(t *testing.T, shim storage.PlatformIOShim, root, ns string, n int) {
	t.Helper()
	frames := make([][]byte, 0, n)
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, ns, parent)
		h := c.Hash
		parent = &h
		frames = append(frames, rawFrame(t, c))
	}
	appendSegment(t, shim, ns, 0, frames...)
	if err := shim.FlushSegment(storio.SegmentNameBuilder.DeltaSequenced(ns, 0)); err != nil {
		t.Fatal(err)
	}
	dir := NamespaceDir(root, ns)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"),
		[]byte(`{"namespaceId":"`+ns+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestStageWritesTheMarkerWhenTheCopyHasNone: a restore rebuilds the delta log and never writes a
// namespace marker, so the caller supplies one. It goes into the payload, not into the staged copy,
// which may still be attached and read from.
func TestStageWritesTheMarkerWhenTheCopyHasNone(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 4)
	if err := os.Remove(filepath.Join(NamespaceDir(staged, ns), "meta.json")); err != nil {
		t.Fatal(err)
	}

	marker := []byte(`{"namespaceId":"demo/users","historyStrategy":"replay"}`)
	if _, err := StagePromotion(live, staged, ns, PromotionIntent{}, marker); err != nil {
		t.Fatalf("a staged copy without a marker is the normal case: %v", err)
	}
	if _, err := os.Stat(filepath.Join(NamespaceDir(staged, ns), "meta.json")); !os.IsNotExist(err) {
		t.Error("the staged copy should not have been written to")
	}
	if _, err := ApplyPromotion(live, nil); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(NamespaceDir(live, ns), "meta.json"))
	if err != nil {
		t.Fatalf("the promoted namespace has no marker: %v", err)
	}
	if string(body) != string(marker) {
		t.Errorf("marker on disk is %q, want %q", body, marker)
	}
}

// A staged copy that has its own marker keeps it: it knows what it is better than the caller does.
func TestStageKeepsTheCopysOwnMarker(t *testing.T) {
	ns := "demo/users"
	live := rootWithCommits(t, ns, 2)
	staged := rootWithCommits(t, ns, 4)
	own := filepath.Join(NamespaceDir(staged, ns), "meta.json")
	if err := os.WriteFile(own, []byte(`{"namespaceId":"demo/users","historyMode":"none"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := StagePromotion(live, staged, ns, PromotionIntent{},
		[]byte(`{"namespaceId":"demo/users","historyStrategy":"replay"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyPromotion(live, nil); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(NamespaceDir(live, ns), "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"historyMode":"none"`) {
		t.Errorf("the copy's own marker should have been kept, got %q", body)
	}
}
