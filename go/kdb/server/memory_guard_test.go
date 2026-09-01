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

// TestMemoryGuardStopWaitsForTheSamplerToActuallyExit is the regression test for two related
// gaps, together the cause of intermittent, unrelated-looking failures in
// memory_pressure_test.go (present under CI load, absent on a quiet machine - the kind of
// "flaky" that looks like a race in the *test* rather than in what it exercises):
//
//  1. Stop used to close g.stop and return immediately, with no guarantee sampleLoop had
//     actually noticed and stopped. A goroutine that had already selected a ticker tick just
//     before Stop was called could still be mid-sampleOnce/observe, executing concurrently with
//     whatever the caller does right after Stop returns.
//  2. Even with (1) fixed, Stop only guarantees no *more* samples land after it returns - not
//     that the ring is empty. A real sample can already be sitting there from a tick that fired
//     before Stop was ever called (entirely possible if the caller's own goroutine was slow to
//     get scheduled between constructing the guard and calling Stop - exactly what a loaded CI
//     runner, worse under -race, does). That stale, low ambient-usage sample then gets averaged
//     in alongside a caller's explicit high one and pulls the computed zone back under threshold.
//
// pinZone's fix is Stop, then reset (which empties the ring), then the explicit observe. This
// test proves that combination is deterministic even when a real sample has genuinely landed:
// sleeping past one real tick guarantees it (rather than hoping the race is exercised by luck),
// and NewMemoryGuard's real default poll interval is used throughout - overriding pollInterval
// after construction would itself be a fresh data race, since sampleLoop reads it the instant the
// goroutine starts.
func TestMemoryGuardStopWaitsForTheSamplerToActuallyExit(t *testing.T) {
	for i := 0; i < 3; i++ {
		g := NewMemoryGuard(1<<30, 0.85)   // 1 GiB - real ambient usage won't trip this on its own
		time.Sleep(220 * time.Millisecond) // past one real 200ms tick - guarantees a sample lands
		g.Stop()
		// Stop must not return until sampleLoop has actually exited: a receive on the already-
		// closed g.done must not block at all.
		select {
		case <-g.done:
		default:
			t.Fatalf("iteration %d: g.done not closed immediately after Stop returned", i)
		}
		g.reset()
		g.observe(float64(1 << 30 * 0.90))
		if got := g.CurrentZone(); got != ZoneHigh {
			t.Fatalf("iteration %d: expected ZoneHigh from the explicit observe alone, got %v - a stale sample survived Stop+reset", i, got)
		}
	}
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
