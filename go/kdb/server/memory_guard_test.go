package server

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestMemoryGuardDisabledByDefaultNeverRejects(t *testing.T) {
	g := NewMemoryGuard(0, 0.85)
	defer g.Stop()
	if g.ShouldReject() {
		t.Fatal("a disabled guard (limitBytes=0) must never reject")
	}
}

func TestNilMemoryGuardNeverRejects(t *testing.T) {
	var g *MemoryGuard
	if g.ShouldReject() {
		t.Fatal("a nil guard must never reject")
	}
	g.Stop() // must not panic
}

// TestMemoryGuardRejectsOnceOverBudget sets an unrealistically tiny budget (a few KB) so the
// process's actual heap usage - already well above that just from the Go runtime starting up -
// is guaranteed to trip the threshold on the guard's very first sample, without needing to
// actually allocate hundreds of MB in the test itself.
func TestMemoryGuardRejectsOnceOverBudget(t *testing.T) {
	g := NewMemoryGuard(1, 0.85) // 1 byte * 0.85 - anything trips this immediately
	defer g.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g.ShouldReject() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected the guard to report pressure well within 2s for a 1-byte budget")
}

func TestMemoryGuardStopOnDisabledGuardIsSafe(t *testing.T) {
	g := NewMemoryGuard(0, 0.85)
	g.Stop() // disabled guard: no goroutine/channel was ever started - must not panic
	g.Stop() // repeat calls on a disabled guard must also not panic
}

// Regression test for kdb-spec-layer13 Component 48 §2.5: the original guard sampled
// runtime.MemStats.Sys, which never decreases (Go returns freed pages to the OS but keeps
// counting them in Sys), so a real trip - not just an unrealistic 1-byte budget - stayed
// permanently tripped for the rest of the process's life even once the memory that caused it was
// gone. Proves pressure clears on its own, under a budget real allocation can actually cross both
// ways, with no reconfiguration (no SetMemoryLimit(0, ...) escape hatch) involved.
func TestMemoryGuardPressureClearsOnItsOwn(t *testing.T) {
	runtime.GC()
	debug.FreeOSMemory()
	base := currentMemoryUsageBytes()
	// A large margin above baseline (256MB), not a tight one: this test runs alongside other
	// tests in the package, whose residual live memory at the time this test starts is part of
	// "base" but can still fluctuate a little as their own goroutines/buffers finish unwinding.
	// The margin needs to comfortably dominate that noise so this test's own allocate/release is
	// what actually drives the trip and clear, not incidental churn from elsewhere in the suite.
	const margin = 256 << 20
	limit := uint64(base) + margin
	g := NewMemoryGuard(limit, 0.85)
	defer g.Stop()

	if g.ShouldReject() {
		t.Fatal("must not report pressure before anything pushes usage toward the budget")
	}

	hold := make([][]byte, 0, 512)
	tripped := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !tripped {
		buf := make([]byte, 4<<20) // 4MB/iteration, kept alive in hold
		for i := range buf {
			buf[i] = 1 // touch every page so it's really resident, not just reserved
		}
		hold = append(hold, buf)
		if g.ShouldReject() {
			tripped = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !tripped {
		t.Fatal("expected sustained allocation to eventually trip the guard")
	}

	// Release the memory that caused the trip and force it back to the OS, then wait for the
	// guard to notice - the exact recovery path the Sys-based version could never take.
	hold = nil
	runtime.GC()
	debug.FreeOSMemory()

	cleared := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !g.ShouldReject() {
			cleared = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !cleared {
		t.Fatalf("expected pressure to clear on its own once the memory was freed and returned to "+
			"the OS - a guard that never reverses is exactly the zombie behavior Component 48 fixes "+
			"(base=%d limit=%d final usage=%v)", uint64(base), limit, currentMemoryUsageBytes())
	}
}
