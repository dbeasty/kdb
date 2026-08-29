package server

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
)

// testBudget is a realistic budget for these tests - large enough that an ordinary commit fits
// its grant comfortably, so what is being exercised is the *zone policy*, not the degenerate
// "this operation could never fit at all" path (which TestCommitRejectedWhenLargerThanCapacity
// covers separately). The previous version of these tests used a 1-byte budget, which conflated
// the two: under Component 48's cost model a 1-byte budget cannot admit any write at all, so it
// tested un-admittability rather than backpressure.
const testBudget = 1 << 30 // 1 GiB

// pinZone stops srv's background sampler and drives the guard directly to the zone implied by
// usedFraction of the budget, so a test can put the server under a specific, exact pressure
// without allocating gigabytes or waiting on a 200ms ticker.
func pinZone(t *testing.T, srv *KdbServerRuntime, usedFraction float64) {
	t.Helper()
	g := srv.memGuard
	g.Stop() // no background sampler racing the value we are about to set
	g.observe(float64(testBudget) * usedFraction)
}

// TestCommitRejectedUnderMemoryPressure proves the memory budget's backpressure actually reaches
// Commit/Upsert: once the server is in a zone that sheds writes, both must return a
// *MemoryPressureError immediately rather than proceeding - the "throttle back the input"
// mechanism this exists for (see MemoryGuard's own doc comment and
// docs/benchmarks/lightsail-sim/README.md for the OOM finding that motivated it), not just the
// guard's internal state in isolation (memory_guard_test.go).
func TestCommitRejectedUnderMemoryPressure(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	defer srv.memGuard.Stop()

	pinZone(t, srv, 0.90) // above the 85% ZoneHigh entry, below the 93% Critical entry
	if got := srv.MemoryZone(); got != ZoneHigh {
		t.Fatalf("expected ZoneHigh at 90%% of budget, got %v", got)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{})
	if err == nil {
		t.Fatal("expected Upsert to be rejected under memory pressure")
	}
	var pressureErr *MemoryPressureError
	if !asError(err, &pressureErr) {
		t.Fatalf("expected *MemoryPressureError, got %T: %v", err, err)
	}
	// The rejection has to tell the client when to come back, not just that it failed -
	// Component 51 §8.1's whole point is that a client should not have to parse prose.
	if pressureErr.RetryAfterMs <= 0 {
		t.Errorf("expected a positive retry-after hint, got %d", pressureErr.RetryAfterMs)
	}
	if pressureErr.Zone != ZoneHigh {
		t.Errorf("expected the error to carry the zone that shed it, got %v", pressureErr.Zone)
	}
}

// TestCommitSucceedsOnceMemoryLimitDisabled proves SetMemoryLimit(0, ...) genuinely turns
// backpressure back off, not just raises the threshold - a real "undo" path, not a one-way trip.
func TestCommitSucceedsOnceMemoryLimitDisabled(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	pinZone(t, srv, 0.90)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{}); err == nil {
		t.Fatal("expected the write to be shed before the limit is disabled")
	}

	srv.SetMemoryLimit(0, 0.85) // disable
	if _, err := srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{}); err != nil {
		t.Fatalf("expected Upsert to succeed once the memory limit is disabled, got: %v", err)
	}
}

// TestPointReadsSurviveEveryZone is the load-shedding priority order from §5.5, asserted end to
// end: writes are shed at High and Critical, but a point read is admitted in every zone. A server
// that cannot answer a point read is indistinguishable from one that is down - and point reads
// are the signal an operator uses to diagnose the pressure in the first place.
func TestPointReadsSurviveEveryZone(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	defer srv.memGuard.Stop()
	srv.memGuard.Stop()

	for _, tc := range []struct {
		fraction  float64
		wantZone  Zone
		wantWrite bool
	}{
		{0.10, ZoneNormal, true},
		{0.75, ZoneElevated, true},
		{0.90, ZoneHigh, false},
		{0.99, ZoneCritical, false},
	} {
		srv.memGuard.reset()
		srv.memGuard.observe(float64(testBudget) * tc.fraction)
		if got := srv.MemoryZone(); got != tc.wantZone {
			t.Fatalf("at %.0f%% of budget: expected %v, got %v", tc.fraction*100, tc.wantZone, got)
		}

		readGrant, err := srv.admission.Acquire(t.Context(), ClassPointRead, 128)
		if err != nil {
			t.Errorf("%v: point reads must never be shed, got %v", tc.wantZone, err)
		} else {
			readGrant.Release()
		}

		writeGrant, err := srv.admission.Acquire(t.Context(), ClassWrite, 128)
		if tc.wantWrite && err != nil {
			t.Errorf("%v: expected writes to be admitted, got %v", tc.wantZone, err)
		}
		if !tc.wantWrite && err == nil {
			t.Errorf("%v: expected writes to be shed", tc.wantZone)
		}
		if writeGrant != nil {
			writeGrant.Release()
		}
	}
}

// TestPressureRecoversAfterDwell proves the zone comes back down on its own once usage subsides -
// the "no one-way doors" principle (P3). It also pins the asymmetry: escalation is immediate,
// de-escalation waits out the dwell time, so a single dip cannot un-shed a server that is still
// genuinely under pressure.
func TestPressureRecoversAfterDwell(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	defer srv.memGuard.Stop()
	g := srv.memGuard
	g.Stop()

	// A controllable clock, so the dwell is exercised exactly rather than slept through.
	now := time.Now()
	g.now = func() time.Time { return now }

	g.observe(float64(testBudget) * 0.90)
	if g.CurrentZone() != ZoneHigh {
		t.Fatalf("escalation should be immediate, got %v", g.CurrentZone())
	}

	// Usage drops well under the ZoneHigh clear threshold, but no time has passed: the zone must
	// hold, or one lucky sample would be enough to re-admit a flood.
	g.reset()
	g.observe(float64(testBudget) * 0.10)
	if g.CurrentZone() != ZoneHigh {
		t.Fatalf("de-escalation must wait out the dwell time, got %v", g.CurrentZone())
	}

	now = now.Add(zoneDwell + time.Millisecond)
	g.observe(float64(testBudget) * 0.10)
	if g.CurrentZone() != ZoneNormal {
		t.Fatalf("expected recovery to ZoneNormal once the dwell elapsed, got %v", g.CurrentZone())
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{}); err != nil {
		t.Fatalf("writes must resume once pressure clears, got: %v", err)
	}
}

// TestCommitRejectedWhenLargerThanCapacity covers the other rejection shape: an operation too
// large to ever be admitted must say so with RESOURCE_EXHAUSTED ("resubmit smaller"), not BUSY
// ("try again later"), because retrying it unchanged can never succeed. This is the code
// Component 51 defined but nothing produced until the cost model existed to make the judgment.
func TestCommitRejectedWhenLargerThanCapacity(t *testing.T) {
	srv := newTestRuntime(t)
	// A small budget (the reserve clamps to a quarter of it, leaving 96KB of capacity) against
	// a document whose write estimate is ~1.5MB: too large to ever fit, however long it waits.
	srv.SetMemoryBudget(128<<10, 0.85, DefaultRescueReserveBytes, DefaultScanRowBudget)
	defer srv.memGuard.Stop()
	srv.memGuard.Stop()
	srv.memGuard.observe(1) // stay in ZoneNormal, so the zone policy is not what rejects

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	huge := make([]byte, 1<<20)
	for i := range huge {
		huge[i] = 'a'
	}
	_, err = srv.Upsert("app/data", docID, `{"v":"`+string(huge)+`"}`, auth.Principal{})
	var exhausted *ResourceExhaustedError
	if !asError(err, &exhausted) {
		t.Fatalf("expected *ResourceExhaustedError, got %T: %v", err, err)
	}
	if exhausted.EstimateBytes <= exhausted.CapacityBytes {
		t.Errorf("the error should show why it can never fit: estimate=%d capacity=%d",
			exhausted.EstimateBytes, exhausted.CapacityBytes)
	}
}

// reset clears the moving-average ring so a test can establish a new usage level in one observe
// call rather than having to feed sampleWindow of them. Test-only: in production the whole point
// of the ring is that readings are never discarded like this.
func (g *MemoryGuard) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ring, g.ringNext = nil, 0
}
