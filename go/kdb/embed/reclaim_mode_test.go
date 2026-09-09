package embed_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The reclaim axis, which is what makes a history-mode switch non-destructive:
// the mode says what may be reclaimed, this says whether anything is.

// reclaimRuntime is a rotating, history=none runtime that keeps nothing -
// so anything sealed is immediately eligible and the only thing deciding
// whether it goes is the reclaim mode.
func reclaimRuntime(t *testing.T, root string) *embed.EmbeddedKdbRuntime {
	t.Helper()
	return rotatingRuntime(t, root, 4096)
}

func writeUntilSealed(t *testing.T, rt *embed.EmbeddedKdbRuntime, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
}

// manual must reclaim nothing, and must say that is what it did rather than
// leaving an operator to read zeroes as "nothing to do".
func TestManualReclaimHoldsAndReportsWhatItHeld(t *testing.T) {
	root := t.TempDir()
	rt := reclaimRuntime(t, root)
	defer rt.Close()
	if err := rt.SetReclaimMode(storage.ReclaimManual); err != nil {
		t.Fatal(err)
	}
	writeUntilSealed(t, rt, 60)

	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 0 {
		t.Fatalf("manual reclaimed %d segments; it must reclaim nothing", res.Removed)
	}
	if !res.ReclaimHeld {
		t.Fatal("a held pass did not report itself as held, so zero removals is indistinguishable from nothing to do")
	}
	if res.EligibleSegments == 0 {
		t.Fatal("manual held without reporting how much it is holding, which is the number that makes it an informed choice")
	}
	if res.EligibleBytes == 0 {
		t.Fatal("eligible segments were reported with no byte count")
	}
}

// And the data is all still there, which is the whole point: switching a
// namespace to history=none under manual costs nothing and is reversible.
func TestManualReclaimLeavesEverythingReadable(t *testing.T) {
	root := t.TempDir()
	rt := reclaimRuntime(t, root)
	defer rt.Close()
	if err := rt.SetReclaimMode(storage.ReclaimManual); err != nil {
		t.Fatal(err)
	}
	writeUntilSealed(t, rt, 40)
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d is missing after a held pass, which should have deleted nothing", i)
		}
	}
}

// CompactHistory is the explicit ask manual is waiting for, and it has to
// override the mode rather than be refused by it.
func TestCompactHistoryOverridesManual(t *testing.T) {
	root := t.TempDir()
	rt := reclaimRuntime(t, root)
	defer rt.Close()
	if err := rt.SetReclaimMode(storage.ReclaimManual); err != nil {
		t.Fatal(err)
	}
	writeUntilSealed(t, rt, 60)
	if held, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	} else if held.Removed != 0 {
		t.Fatalf("manual reclaimed %d segments before being asked", held.Removed)
	}

	res, err := rt.CompactHistory()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Fatal("CompactHistory reclaimed nothing; it is the explicit ask that manual defers to")
	}
	// And the mode is put back, so one compaction does not silently turn a
	// manual namespace into a reclaiming one.
	if got := rt.ReclaimMode(); got != storage.ReclaimManual {
		t.Fatalf("after CompactHistory the namespace is on %v, want it back on manual", got)
	}
	// Live data survives the compaction.
	for i := 0; i < 60; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d was lost by an explicit compaction", i)
		}
	}
}

// A namespace left on a reclaiming mode still reclaims on its own, which is
// the behaviour manual is the exception to.
func TestBalancedStillReclaimsWithoutBeingAsked(t *testing.T) {
	root := t.TempDir()
	rt := reclaimRuntime(t, root)
	defer rt.Close()
	if err := rt.SetReclaimMode(storage.ReclaimBalanced); err != nil {
		t.Fatal(err)
	}
	writeUntilSealed(t, rt, 60)
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Fatal("balanced reclaimed nothing on an ordinary pass")
	}
	if res.ReclaimHeld {
		t.Fatal("balanced reported itself as held")
	}
}

// The presets have to actually reach the scheduler's knobs, and immediate
// has to stop deferring to load - "immediate, unless busy" is not immediate.
func TestReclaimPresetsDriveTheScheduler(t *testing.T) {
	root := t.TempDir()
	rt := reclaimRuntime(t, root)
	defer rt.Close()

	sched := embed.StartMaintenance(rt, embed.MaintenanceOptions{
		Interval: time.Hour,
		Busy:     func() bool { return true }, // never quiet
	})
	if sched == nil {
		t.Fatal("no scheduler started")
	}
	defer sched.Stop()

	if err := sched.ApplyReclaimMode(storage.ReclaimLazy); err != nil {
		t.Fatal(err)
	}
	lazy := storage.ReclaimLazy.Cadence()
	if got := sched.Interval(); got != lazy.Interval {
		t.Errorf("lazy left the interval at %v, want %v", got, lazy.Interval)
	}
	if got := rt.ReclaimMode(); got != storage.ReclaimLazy {
		t.Errorf("the namespace is on %v after applying lazy", got)
	}

	if err := sched.ApplyReclaimMode(storage.ReclaimImmediate); err != nil {
		t.Fatal(err)
	}
	immediate := storage.ReclaimImmediate.Cadence()
	if got := sched.Interval(); got != immediate.Interval {
		t.Errorf("immediate left the interval at %v, want %v", got, immediate.Interval)
	}
	// Busy always returns true here, so a mode that respected load would
	// defer forever; immediate must run anyway.
	writeUntilSealed(t, rt, 40)
	sched.TickForTest()
	if st := sched.Stats(); st.Passes == 0 {
		t.Fatalf("immediate deferred to load: %d deferred, %d passes", st.Deferred, st.Passes)
	}
}

// Every mode name round-trips, and an unknown one is refused rather than
// quietly becoming the default.
func TestReclaimModeParsing(t *testing.T) {
	for _, mode := range []storage.ReclaimMode{
		storage.ReclaimManual, storage.ReclaimImmediate,
		storage.ReclaimBalanced, storage.ReclaimLazy,
	} {
		got, err := storage.ParseReclaimMode(mode.String())
		if err != nil {
			t.Fatalf("parsing %q: %v", mode, err)
		}
		if got != mode {
			t.Fatalf("%q parsed back as %v", mode, got)
		}
	}
	if _, err := storage.ParseReclaimMode("agressive"); err == nil {
		t.Fatal("a misspelled mode was accepted")
	}
	// Unset resolves to the default and stays there on a second resolve -
	// the idempotency RetentionWindow.Resolve had to learn the hard way.
	once := storage.ReclaimUnset.Resolve()
	if once != storage.DefaultReclaimMode || once.Resolve() != once {
		t.Fatalf("Resolve is not idempotent: %v then %v", once, once.Resolve())
	}
}
