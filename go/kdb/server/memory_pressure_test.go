package server

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
)

// TestCommitRejectedUnderMemoryPressure proves KdbServerRuntime.SetMemoryLimit's backpressure
// actually reaches Commit/Upsert: with an unrealistically tiny budget, both must return a
// *MemoryPressureError immediately rather than proceeding - the "throttle back the input"
// mechanism this exists for (see MemoryGuard's own doc comment and
// docs/benchmarks/lightsail-sim/README.md for the OOM finding that motivated it), not just the
// guard's internal state in isolation (memory_guard_test.go).
func TestCommitRejectedUnderMemoryPressure(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(1, 0.85) // 1 byte - the process's actual heap already exceeds this
	defer srv.memGuard.Stop()

	// Give the background sampler a moment to take its first reading (it polls every 200ms).
	waitForRejection(t, srv)

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
}

// TestCommitSucceedsOnceMemoryLimitDisabled proves SetMemoryLimit(0, ...) genuinely turns
// backpressure back off, not just raises the threshold - a real "undo" path, not a one-way trip.
func TestCommitSucceedsOnceMemoryLimitDisabled(t *testing.T) {
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(1, 0.85)
	waitForRejection(t, srv)

	srv.SetMemoryLimit(0, 0.85) // disable
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{}); err != nil {
		t.Fatalf("expected Upsert to succeed once the memory limit is disabled, got: %v", err)
	}
}

func waitForRejection(t *testing.T, srv *KdbServerRuntime) {
	t.Helper()
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := srv.Upsert("app/data", docID, `{"probe":true}`, auth.Principal{}); err != nil {
			var pressureErr *MemoryPressureError
			if asError(err, &pressureErr) {
				return
			}
			t.Fatalf("unexpected error while waiting for memory pressure: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("guard never reported memory pressure within the retry budget")
}
