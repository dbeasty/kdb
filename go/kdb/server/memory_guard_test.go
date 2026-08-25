package server

import (
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
