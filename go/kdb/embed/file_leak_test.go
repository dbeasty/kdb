package embed

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
)

// TestOpenFileRuntime_ReleasesLockAndHandleOnPostOpenFailure is the regression test for the
// finding recorded in docs/kdb-finish-up-plan.md as 1-G4: every failure path in
// OpenFileRuntimeWithOptions between engine.DefaultFactory.Open succeeding and the function
// returning released the directory lock but never called handle.Close(), leaking open file
// descriptors and an unsealed WAL on any post-open failure (delta replay, schema sync, ...).
//
// Forcing a genuine fd leak to manifest in a fast unit test isn't practical (Unix doesn't
// enforce mandatory locking on leaked fds the way our own explicit dir-lock flock does, so a
// leaked handle doesn't by itself block a later open). What's directly observable instead: if
// the fix's added handle.Close()-on-early-return cleanup broke anything (e.g. released the lock
// in the wrong order, or panicked closing a partially-initialized handle), a failing open would
// either hang, panic, or leave the directory lock stuck - a second open attempt would then fail
// with "data directory locked" instead of propagating the real underlying error. This test
// forces a real post-open failure (a delta segment that references a commit no earlier segment
// in the log actually has, since the referenced parent commit lives in a segment corrupted out
// from under it) and asserts two consecutive open attempts both fail with the real replay error,
// never a stuck-lock error.
func TestOpenFileRuntime_ReleasesLockAndHandleOnPostOpenFailure(t *testing.T) {
	dir := t.TempDir()
	ns := "demo/users"

	rt, err := OpenFileRuntime(dir, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PutJSONDocument(rt, ns, `{"name":"a"}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	rt2, err := OpenFileRuntime(dir, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PutJSONDocument(rt2, ns, `{"name":"b"}`); err != nil {
		t.Fatal(err)
	}
	rt2.Close()

	deltaDir := filepath.Join(dir, "ns", ns, "delta")
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 sealed segments to set up this scenario, got %d", len(entries))
	}
	// Corrupt the oldest (non-most-recent) segment - delta_replay.go only tolerates a corrupt
	// frame in the *most recent* segment, so this forces a hard failure inside
	// OpenFileRuntimeWithOptions, after handle has already been opened.
	oldest := filepath.Join(deltaDir, entries[0].Name())
	if err := os.WriteFile(oldest, []byte{0xDE, 0xAD, 0xBE, 0xEF}, 0o644); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		rt3, err := OpenFileRuntime(dir, "demo", ns, schema.None())
		if rt3 != nil {
			rt3.Close()
			t.Fatalf("attempt %d: expected the corrupted log to fail to open, got a runtime instead", attempt)
		}
		if err == nil {
			t.Fatalf("attempt %d: expected an error opening a runtime over a corrupted delta log", attempt)
		}
		if strings.Contains(err.Error(), "locked") {
			t.Fatalf("attempt %d: got a stuck-lock error instead of the real replay failure - the lock (and/or handle) from a prior failed open was not released: %v", attempt, err)
		}
	}
}
