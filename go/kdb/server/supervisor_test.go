package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

type fakeListener struct{ closed bool }

func (f *fakeListener) Close() error { f.closed = true; return nil }

// TestAbortWatchdogTriggersOrderlyShutdown proves kdb-spec-layer13 Component 50's full sequence:
// sustained pressure -> draining set (new writes get *UnavailableError) -> listener closed ->
// storage flushed and sealed (so the next open replays fast, without needing the corrupt-tail
// tolerance path) -> exit called with AbortExitCode. Uses a file-backed runtime specifically so
// the "log and disk end up in a correct state" half of the requirement is actually checked on
// disk, not just asserted about in-memory state.
func TestAbortWatchdogTriggersOrderlyShutdown(t *testing.T) {
	dir := t.TempDir()
	ns := "demo/users"
	rt, err := embed.OpenFileRuntime(dir, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, ns, `{"name":"a"}`); err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.SetMemoryLimit(1, 0.85) // 1 byte - trips on the guard's very first sample
	defer srv.memGuard.Stop()

	ln := &fakeListener{}
	w := NewAbortWatchdog(srv, ln, 150*time.Millisecond)
	w.pollInterval = 20 * time.Millisecond

	var exitCode int
	exited := make(chan struct{})
	w.exit = func(code int) {
		exitCode = code
		close(exited)
	}

	w.Start()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the watchdog to trigger an abort within 5s of sustained pressure")
	}

	if exitCode != AbortExitCode {
		t.Fatalf("expected exit code %d, got %d", AbortExitCode, exitCode)
	}
	if !ln.closed {
		t.Fatal("expected the listener to be closed as part of the abort sequence")
	}
	if !srv.draining.Load() {
		t.Fatal("expected the runtime to be draining after an abort")
	}

	segPath := filepath.Join(dir, "ns", ns, "delta", "00000000000000000000.seg")
	if _, err := os.Stat(segPath); err != nil {
		t.Fatalf("expected the abort sequence to have flushed/sealed the active segment: %v", err)
	}
}

// TestAbortWatchdogDisabledWhenAbortAfterIsZero proves the nil-safe opt-in convention (matching
// MemoryGuard's own limitBytes==0 default): abortAfter<=0 must not start any background work.
func TestAbortWatchdogDisabledWhenAbortAfterIsZero(t *testing.T) {
	w := NewAbortWatchdog(nil, nil, 0)
	if w != nil {
		t.Fatal("expected a nil watchdog for abortAfter <= 0")
	}
	w.Start() // must not panic
	w.Stop()  // must not panic
}

// TestReleaseSealsActiveSegment is a regression test for the gap Release() had until this
// change: it decremented the ref count and stopped the memory guard, but never called
// EmbeddedKdbRuntime.Close() - so an ordinary process shutdown (a service's SIGTERM handler
// calling Release, the normal path in go/cmd/kdb-service/main.go) never flushed or sealed the
// active delta segment, exactly the gap kdb-spec-layer13 Component 47 §2.4/§4.5 fixed for
// EmbeddedKdbRuntime.Close itself - Release just never reached it.
func TestReleaseSealsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	ns := "demo/users"
	rt, err := embed.OpenFileRuntime(dir, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, ns, `{"name":"a"}`); err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)

	srv.Release()

	segPath := filepath.Join(dir, "ns", ns, "delta", "00000000000000000000.seg")
	if _, err := os.Stat(segPath); err != nil {
		t.Fatalf("expected Release to have flushed/sealed the active segment: %v", err)
	}
}
