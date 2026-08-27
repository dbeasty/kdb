package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// TestUnlockDoesNotOpenARuntime is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G14: execute unconditionally called openRuntime(cfg,
// namespaceFor(cmd)) before dispatching, including for UnlockCmd - whose namespaceFor case falls
// through to the empty string, so this opened a runtime for namespace "", creating bogus
// ns//delta and ns//meta directories and a meta.json under the empty namespace purely as a side
// effect of a command whose whole job is to remove a stale lock file. It also meant that if the
// lock genuinely was held, openRuntime would fail before cmdUnlock's own body ever ran - the
// exact situation unlock exists to recover from.
func TestUnlockDoesNotOpenARuntime(t *testing.T) {
	dir := t.TempDir()

	code := Run([]string{"--data-dir", dir, "--quiet", "unlock"})
	if code != 0 {
		t.Fatalf("expected unlock on a fresh data dir to exit 0, got %d", code)
	}

	nsDir := filepath.Join(dir, "ns")
	if _, err := os.Stat(nsDir); err == nil {
		t.Fatalf("expected no ns/ directory to be created by unlock, but %s exists", nsDir)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

// TestUnlockRemovesLockFile is unlock's positive case: given an actual lock file, it's removed.
func TestUnlockRemovesLockFile(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, ".kdb.lock")
	if err := os.WriteFile(lockPath, []byte("pid=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code := Run([]string{"--data-dir", dir, "--quiet", "unlock"})
	if code != 0 {
		t.Fatalf("expected unlock to exit 0, got %d", code)
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected the lock file to be removed, stat err = %v", err)
	}
}

// TestPutSealsSegmentOnExit is a basic end-to-end smoke test for the put command (commands.go
// had no test exercising any command through execute() at all - see docs/kdb-finish-up-plan.md's
// Go test-coverage notes for cmd/kdb/cli). It also exercises execute()'s new deferred rt.Close()
// (1-G14's other fix, alongside the unlock tests above: execute previously never called
// rt.Close() on any command path), though a segment already turns out to be flushed to disk
// per-commit regardless of an orderly close - Close's specific contribution here isn't visible
// from this test's black-box file check alone.
func TestPutSealsSegmentOnExit(t *testing.T) {
	dir := t.TempDir()
	ns := "demo/users"

	code := Run([]string{"--data-dir", dir, "--quiet", "put", ns, `{"name":"Ada"}`})
	if code != 0 {
		t.Fatalf("expected put to exit 0, got %d", code)
	}

	segPath := filepath.Join(dir, "ns", ns, "delta", "00000000000000000000.seg")
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("expected a sealed segment file to exist after put returns: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected a non-empty sealed segment")
	}
}
