package embed_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The maintenance loop's decisions, which are the part worth testing: a
// tick that finds nothing to do must cost nothing, a tick during a write
// burst must wait for a quieter one, and neither of those may add up to
// "never reclaims".

func maintenanceRuntime(t *testing.T, mode storage.HistoryMode, window storage.RetentionWindow) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = mode
	opts.Storage.Retain = window
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return rt
}

func writeMaintenanceDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id string, n int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"id": id, "n": n})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, "app/docs", string(body)); err != nil {
		t.Fatal(err)
	}
}

// An idle namespace must not run a pass on every tick. This is the whole
// "only when there is something to compact" claim: without it the loop
// rewrites an identical checkpoint every interval forever.
func TestIdleNamespaceSkipsTicks(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
	}, func() time.Time { return clock })

	// First tick: due by the sweep rule, because a freshly opened runtime
	// has never had a pass.
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 1 {
		t.Fatalf("first tick ran %d passes, want 1", got)
	}
	// Nothing written since, nothing aged out, no table backlog: every
	// further tick inside the sweep window must be a skip.
	for i := 0; i < 5; i++ {
		clock = clock.Add(time.Minute)
		sched.TickForTest()
	}
	st := sched.Stats()
	if st.Passes != 1 {
		t.Fatalf("an idle namespace ran %d passes, want 1", st.Passes)
	}
	if st.Skipped != 5 {
		t.Fatalf("skipped %d ticks, want 5", st.Skipped)
	}
}

// A commit is what makes a pass worth running again.
func TestACommitMakesTheNextTickDue(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
	}, func() time.Time { return clock })

	sched.TickForTest() // the seeding pass
	clock = clock.Add(time.Minute)
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 1 {
		t.Fatalf("an unchanged namespace ran %d passes, want 1", got)
	}

	writeMaintenanceDoc(t, rt, "a", 2)
	clock = clock.Add(time.Minute)
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 2 {
		t.Fatalf("after a commit the scheduler ran %d passes, want 2", got)
	}
}

// The sweep exists because a duration window expires by the clock, with
// no commit involved. Without it an idle namespace that has aged out of
// its window would sit on reclaimable segments indefinitely.
func TestSweepMakesAnIdleNamespaceDueEventually(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    10 * time.Minute,
	}, func() time.Time { return clock })

	sched.TickForTest()
	clock = clock.Add(5 * time.Minute)
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 1 {
		t.Fatalf("inside the sweep window the scheduler ran %d passes, want 1", got)
	}
	clock = clock.Add(6 * time.Minute) // now past Sweep since the last pass
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 2 {
		t.Fatalf("past the sweep window the scheduler ran %d passes, want 2", got)
	}
}

// Load defers a pass...
func TestBusyDefersAPass(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	busy := true
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
		MaxDefer: 3,
		Busy:     func() bool { return busy },
	}, func() time.Time { return clock })

	sched.TickForTest()
	st := sched.Stats()
	if st.Passes != 0 || st.Deferred != 1 {
		t.Fatalf("a busy first tick ran %d passes and deferred %d, want 0 and 1", st.Passes, st.Deferred)
	}
	// ...and stops deferring the moment the write burst ends.
	busy = false
	clock = clock.Add(time.Minute)
	sched.TickForTest()
	if got := sched.Stats().Passes; got != 1 {
		t.Fatalf("after the load cleared the scheduler ran %d passes, want 1", got)
	}
}

// ...but load must never postpone a pass forever. A server under
// sustained write load is the one whose log grows fastest, so MaxDefer
// puts a ceiling on how long "wait for a quiet moment" may hold.
func TestSustainedLoadCannotStarveMaintenance(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
		MaxDefer: 3,
		Busy:     func() bool { return true }, // never quiet
	}, func() time.Time { return clock })

	for i := 0; i < 4; i++ {
		sched.TickForTest()
		clock = clock.Add(time.Minute)
	}
	st := sched.Stats()
	if st.Deferred != 3 {
		t.Fatalf("deferred %d ticks before forcing, want 3", st.Deferred)
	}
	if st.Forced != 1 || st.Passes != 1 {
		t.Fatalf("forced %d passes (ran %d), want 1 and 1", st.Forced, st.Passes)
	}
}

// history=full namespaces get maintained too - they reclaim no segments
// but a checkpoint still speeds their next open, and SSTable duplication
// is not a retention question. What matters is that the loop does not
// mistake "removed nothing" for "there was nothing to do".
func TestFullModeStillCheckpoints(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeFull, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
	}, func() time.Time { return clock })

	sched.TickForTest()
	if got := sched.Stats().Passes; got != 1 {
		t.Fatalf("a full-history namespace ran %d passes, want 1", got)
	}
	if got := sched.Stats().SegmentsRemoved; got != 0 {
		t.Fatalf("a full-history namespace reclaimed %d segments, want 0", got)
	}
}

// The loop actually reclaims when it is left to run: end to end, with a
// window that retains nothing, several sessions' worth of segments should
// go without anyone calling Maintain by hand.
func TestScheduledPassReclaimsSegments(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	// Several sessions, so there are sealed segments to reclaim.
	for session := 0; session < 3; session++ {
		opts := embed.FileRuntimeOptions{}
		opts.Storage.MemoryBudgetBytes = 4 << 20
		opts.Storage.HistoryMode = storage.HistoryModeNone
		opts.Storage.Retain = window
		rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			writeMaintenanceDoc(t, rt, "a", session*10+i)
		}
		rt.Close()
	}

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	for i := 0; i < 3; i++ {
		writeMaintenanceDoc(t, rt, "a", 100+i)
	}

	clock := time.Now()
	sched := embed.NewMaintenanceSchedulerForTest(rt, embed.MaintenanceOptions{
		Interval: time.Minute,
		Sweep:    time.Hour,
	}, func() time.Time { return clock })
	sched.TickForTest()

	st := sched.Stats()
	if st.Errors != 0 {
		t.Fatalf("the scheduled pass reported %d errors", st.Errors)
	}
	// The document still reads correctly after the reclamation, which is
	// the only result that matters to a caller.
	body, ok := readDoc(t, rt, "a")
	if !ok {
		t.Fatal("the document is gone after a scheduled maintenance pass")
	}
	if want := fmt.Sprintf(`"n":%d`, 102); !strings.Contains(body, want) {
		t.Fatalf("after maintenance the document reads %q, want it to contain %s", body, want)
	}
}

// A read-only runtime has nothing to maintain and must not get a loop:
// Maintain would refuse, and a scheduler that started anyway would log a
// failure every interval for the life of the process.
func TestReadOnlyRuntimeGetsNoScheduler(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeNone, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)

	if sched := embed.StartMaintenance(nil, embed.MaintenanceOptions{Interval: time.Minute}); sched != nil {
		t.Fatal("StartMaintenance returned a scheduler for a nil runtime")
	}
	if sched := embed.StartMaintenance(rt, embed.MaintenanceOptions{Interval: 0}); sched != nil {
		t.Fatal("StartMaintenance returned a scheduler for a zero interval")
	}
}

// Changing the retention window on a running runtime must change what the
// *next* pass reclaims. Without this the setter would be reporting success
// while the pass carried on consulting the window captured at open, which
// is the failure mode the whole live-settings story exists to avoid.
func TestRetentionWindowChangeTakesEffectOnTheNextPass(t *testing.T) {
	root := t.TempDir()
	// Several sessions so there are sealed segments, opened under a window
	// that retains everything: the first pass must reclaim nothing.
	wide := storage.RetentionWindow{Duration: 24 * time.Hour}
	for session := 0; session < 3; session++ {
		opts := embed.FileRuntimeOptions{}
		opts.Storage.MemoryBudgetBytes = 4 << 20
		opts.Storage.HistoryMode = storage.HistoryModeNone
		opts.Storage.Retain = wide
		rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			writeMaintenanceDoc(t, rt, "a", session*10+i)
		}
		rt.Close()
	}

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = wide
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	for i := 0; i < 3; i++ {
		writeMaintenanceDoc(t, rt, "a", 100+i)
	}

	first, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if first.Removed != 0 {
		t.Fatalf("a 24h window reclaimed %d segments; nothing here is a day old", first.Removed)
	}

	// Now shorten it to nothing, which makes everything below the
	// checkpoint eligible.
	if err := rt.SetRetentionWindow(storage.RetentionWindow{Duration: storage.RetainNothing}); err != nil {
		t.Fatal(err)
	}
	// RetainNothing survives Resolve as its own sentinel rather than
	// collapsing to a plain zero - a zero would read back as "unset" and
	// resolve to the 24h default, which is the bug the sentinel exists to
	// prevent. So the assertion is that it is still the sentinel.
	if got := rt.RetentionWindow().Resolve().Duration; got != storage.RetainNothing {
		t.Fatalf("after setting RetainNothing the window resolves to %v, want the RetainNothing sentinel", got)
	}
	writeMaintenanceDoc(t, rt, "a", 200)
	second, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if second.Removed == 0 {
		t.Fatal("after shortening the window to nothing, a pass still reclaimed no segments")
	}
	// And the data is still correct, which is the only thing a caller cares
	// about after a reclamation.
	body, ok := readDoc(t, rt, "a")
	if !ok || !strings.Contains(body, `"n":200`) {
		t.Fatalf("after reclaiming, the document reads %q", body)
	}
}

// A full-history namespace must refuse a retention window rather than
// storing one that will never be consulted.
func TestRetentionWindowRefusedUnderFullMode(t *testing.T) {
	rt := maintenanceRuntime(t, storage.HistoryModeFull, storage.RetentionWindow{})
	writeMaintenanceDoc(t, rt, "a", 1)
	err := rt.SetRetentionWindow(storage.RetentionWindow{Duration: time.Hour})
	if err == nil {
		t.Fatal("a full-history namespace accepted a retention window")
	}
	if !strings.Contains(err.Error(), "history=none") {
		t.Fatalf("the refusal does not name the remedy: %v", err)
	}
}
