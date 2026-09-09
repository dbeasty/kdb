package embed

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// Running Maintain on a timer, which is what turns HistoryModeNone from a
// mode that reclaims at shutdown into one that keeps a running process
// bounded.
//
// Two things make this more than a bare ticker, and both exist because a
// maintenance pass is not free: it walks the live tree
// (PrepareForTruncation), writes a checkpoint, and may rewrite every
// SSTable in the store (CompactBlobStore).
//
//   - It runs only when there is something to do. A pass over a namespace
//     that has not committed anything, has no table backlog, and has no
//     segment that has aged out of its window would write a checkpoint
//     identical to the last one and merge tables that are already merged.
//     See needsWork.
//   - It prefers to run when the server is not busy writing. Maintenance
//     and commits contend for the same engine, so a pass started in the
//     middle of a write burst makes the burst slower. See Busy.
//
// The second of those is a *preference*, deliberately bounded by
// MaxDefer: a server under sustained write load is exactly the server
// whose log is growing fastest, and deferring its maintenance forever
// would reintroduce the unbounded growth history=none exists to prevent.
// Load is a reason to wait for a better moment, never a reason to skip
// the work.

// MaintenanceOptions configures a MaintenanceScheduler. The zero value
// disables the scheduler entirely (Interval 0), which is what an embedded
// caller that wants to keep calling Maintain by hand should leave it as.
type MaintenanceOptions struct {
	// Interval is how often the scheduler wakes to consider a pass. Each
	// wake-up is cheap when there is nothing to do - see needsWork. Zero
	// disables the scheduler.
	Interval time.Duration

	// Sweep bounds how long the scheduler may go without running a pass
	// while the namespace is idle, and exists because retention is partly
	// a function of wall-clock time: a segment ages out of a duration
	// window with no new commit involved, so "nothing has been written"
	// is not the same as "nothing can be reclaimed". Zero uses
	// DefaultMaintenanceSweep.
	Sweep time.Duration

	// Busy reports whether the process is under enough write load that a
	// maintenance pass should wait for a quieter moment. Nil means never
	// busy. See MaxDefer, which bounds how long this may hold a pass off.
	Busy func() bool

	// MaxDefer is how many consecutive ticks Busy may postpone a pass
	// that is otherwise due before it runs anyway. Zero uses
	// DefaultMaintenanceMaxDefer. Negative means "never force", which is
	// available for a caller that has some other guarantee of quiet
	// periods but is not the default for the reason in this file's
	// header.
	MaxDefer int

	// CompactTables is the number of SSTables that justifies a pass on
	// its own, independent of whether anything has been committed. Zero
	// uses sstable.DefaultCompactionTrigger, which is what
	// CompactBlobStore itself applies.
	CompactTables int

	// Logger receives one line per pass that actually reclaimed
	// something, and one per error. Nil uses slog.Default.
	Logger *slog.Logger

	// now and after are test seams: they let a test drive an exact
	// sequence of ticks and clock readings without waiting on real time.
	now   func() time.Time
	after func(time.Duration) <-chan time.Time
}

// Defaults for MaintenanceOptions. The interval is short enough that a
// busy namespace reclaims continuously rather than in visible steps, and
// long enough that an idle one costs a handful of cheap probes an hour.
const (
	DefaultMaintenanceInterval = 5 * time.Minute
	DefaultMaintenanceSweep    = 30 * time.Minute
	DefaultMaintenanceMaxDefer = 6
)

// MaintenanceScheduler calls a runtime's Maintain on a timer, skipping
// passes with nothing to do and preferring quiet moments for the rest.
//
// Start it with StartMaintenance and stop it with Stop. One scheduler
// serves one namespace, matching Maintain itself.
type MaintenanceScheduler struct {
	rt *EmbeddedKdbRuntime
	// opts holds the parts of the configuration that cannot change after
	// construction: the Busy predicate, the logger and the test seams. The
	// three that *can* change live below, under mu - see SetInterval.
	opts MaintenanceOptions

	stop chan struct{}
	done chan struct{}
	// reconfigured wakes the loop when a setter changes the cadence, so a
	// shortened interval takes effect now rather than after the sleep that
	// was already pending under the old one. Buffered by one and sent
	// non-blockingly: several changes before the loop wakes coalesce into
	// the single recomputation they amount to.
	reconfigured chan struct{}

	mu sync.Mutex
	// interval, sweep and maxDefer are the live-changeable cadence. Read
	// through their accessors, never directly, so a change from the
	// control plane cannot race the loop reading them.
	interval time.Duration
	sweep    time.Duration
	maxDefer int
	// lastHead is the commit the last pass observed, and lastPassAt when
	// that pass ran. Together they answer "has anything happened since?"
	// without touching the disk.
	lastHead   codec.Hash
	lastPassAt time.Time
	// deferrals counts consecutive ticks Busy has postponed. Reset by a
	// pass and by a tick that finds nothing to do.
	deferrals int
	// stats are cumulative, for tests and for whoever asks the scheduler
	// what it has been doing.
	stats MaintenanceStats
}

// MaintenanceStats is what a scheduler has done since it started.
type MaintenanceStats struct {
	// Ticks is how many times the scheduler woke up; Passes how many of
	// those ran a maintenance pass. Skipped is ticks with nothing to do,
	// Deferred is ticks postponed because the process was busy, and
	// Forced is passes that ran despite Busy because MaxDefer was hit.
	Ticks    int
	Passes   int
	Skipped  int
	Deferred int
	Forced   int
	// Errors counts failed passes. A failure is logged and the pass is
	// retried on the next tick rather than escalated: maintenance is
	// reclamation, and a namespace that cannot reclaim right now is still
	// a correct one.
	Errors int
	// Reclaimed sums what the passes actually freed.
	SegmentsRemoved int
	BytesReclaimed  int64
	TablesRemoved   int
}

// StartMaintenance begins a maintenance loop over rt and returns the
// scheduler running it. A zero Interval returns nil, having started
// nothing, so a caller can wire this unconditionally and let
// configuration decide.
//
// A read-only runtime also returns nil: it has no maintain closure and
// Maintain would refuse anyway.
func StartMaintenance(rt *EmbeddedKdbRuntime, opts MaintenanceOptions) *MaintenanceScheduler {
	if rt == nil || opts.Interval <= 0 || rt.ReadOnly || rt.maintain == nil {
		return nil
	}
	s := newMaintenanceScheduler(rt, opts)
	go s.loop()
	return s
}

// newMaintenanceScheduler fills in defaults and seeds the "has anything
// changed" baseline, without starting a goroutine. Split from
// StartMaintenance so tests can drive tick and decide directly.
func newMaintenanceScheduler(rt *EmbeddedKdbRuntime, opts MaintenanceOptions) *MaintenanceScheduler {
	if opts.Sweep <= 0 {
		opts.Sweep = DefaultMaintenanceSweep
	}
	if opts.MaxDefer == 0 {
		opts.MaxDefer = DefaultMaintenanceMaxDefer
	}
	if opts.CompactTables <= 0 {
		opts.CompactTables = sstable.DefaultCompactionTrigger
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.after == nil {
		opts.after = time.After
	}
	s := &MaintenanceScheduler{
		rt:           rt,
		opts:         opts,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		reconfigured: make(chan struct{}, 1),
		interval:     opts.Interval,
		sweep:        opts.Sweep,
		maxDefer:     opts.MaxDefer,
	}
	// Seed from the current head rather than the zero hash, so a runtime
	// that opens with existing history does not read as "something was
	// just committed" on its very first tick. lastPassAt stays zero,
	// which makes the first tick due by the sweep rule - a namespace
	// opened with segments already past its window should not have to
	// wait a full sweep to reclaim them.
	if h, err := rt.DAG.Head(); err == nil {
		s.lastHead = h
	}
	return s
}

// Stop ends the loop and waits for an in-flight pass to finish, so a
// caller can close the runtime immediately afterwards without racing a
// pass that is mid-truncation. Idempotent.
func (s *MaintenanceScheduler) Stop() {
	if s == nil {
		return
	}
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// Stats reports what this scheduler has done so far.
func (s *MaintenanceScheduler) Stats() MaintenanceStats {
	if s == nil {
		return MaintenanceStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *MaintenanceScheduler) loop() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-s.reconfigured:
			// The cadence changed. Fall through to recompute the wait
			// rather than serving out the old one.
		case <-s.opts.after(s.Interval()):
			s.tick()
		}
	}
}

// Interval, Sweep and MaxDefer report the cadence currently in force.
func (s *MaintenanceScheduler) Interval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.interval
}

func (s *MaintenanceScheduler) Sweep() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweep
}

func (s *MaintenanceScheduler) MaxDefer() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxDefer
}

// SetInterval changes how often the scheduler wakes, taking effect
// immediately rather than after the pending sleep - a caller who shortens
// the interval because a namespace is growing should not wait out the old
// one first.
//
// Safe to call while the loop is running and while a pass is in flight: it
// changes when the *next* wake-up happens and never interrupts a pass.
// A non-positive interval is refused rather than treated as "disable",
// because a scheduler that has silently stopped looks exactly like one
// that has nothing to do. Stop it if that is what is wanted.
func (s *MaintenanceScheduler) SetInterval(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("kdb: maintenance interval must be positive; stop the scheduler to disable it")
	}
	s.mu.Lock()
	s.interval = d
	s.mu.Unlock()
	s.signalReconfigured()
	return nil
}

// SetSweep changes how long the scheduler may go without a pass while the
// namespace is idle. Safe under load, and takes effect at the next tick -
// the sweep is a comparison made inside needsWork, not a timer.
func (s *MaintenanceScheduler) SetSweep(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("kdb: maintenance sweep must be positive")
	}
	s.mu.Lock()
	s.sweep = d
	s.mu.Unlock()
	return nil
}

// SetMaxDefer changes how many consecutive busy ticks may postpone a due
// pass before it runs anyway. Safe under load.
//
// Zero is rejected rather than silently meaning the default: from the
// control plane, "0" reads as "never defer", and quietly turning that into
// "defer six times" would be the opposite of what was asked. Pass a
// negative value for "never force", which is the genuinely unbounded
// setting and is documented on MaintenanceOptions as the foot-gun it is.
func (s *MaintenanceScheduler) SetMaxDefer(n int) error {
	if n == 0 {
		return fmt.Errorf("kdb: maintenance maxDefer of 0 is ambiguous; " +
			"use a positive count to bound deferral, or -1 to never force a pass through load")
	}
	s.mu.Lock()
	s.maxDefer = n
	s.mu.Unlock()
	return nil
}

func (s *MaintenanceScheduler) signalReconfigured() {
	select {
	case s.reconfigured <- struct{}{}:
	default:
	}
}

// maintenanceDecision is what one tick concluded. Named rather than a
// bare bool because "did not run" has two quite different meanings and an
// operator reading the stats needs to tell them apart: nothing to do is
// the healthy steady state, deferred means work is piling up behind load.
type maintenanceDecision int

const (
	maintenanceSkip maintenanceDecision = iota
	maintenanceDefer
	maintenanceRun
	maintenanceForce
)

// tick considers one wake-up and runs a pass if it is due.
func (s *MaintenanceScheduler) tick() {
	now := s.opts.now()
	decision := s.decide(now)

	s.mu.Lock()
	s.stats.Ticks++
	switch decision {
	case maintenanceSkip:
		s.stats.Skipped++
		s.deferrals = 0
	case maintenanceDefer:
		s.stats.Deferred++
		s.deferrals++
	case maintenanceForce:
		s.stats.Forced++
	}
	s.mu.Unlock()

	if decision != maintenanceRun && decision != maintenanceForce {
		return
	}
	s.runPass(now)
}

// decide is the whole policy, kept pure so it can be tested without a
// runtime, a clock, or a disk.
func (s *MaintenanceScheduler) decide(now time.Time) maintenanceDecision {
	if !s.needsWork(now) {
		return maintenanceSkip
	}
	if s.opts.Busy == nil || !s.opts.Busy() {
		return maintenanceRun
	}
	s.mu.Lock()
	deferred := s.deferrals
	s.mu.Unlock()
	if maxDefer := s.MaxDefer(); maxDefer >= 0 && deferred >= maxDefer {
		// Load has held this pass off long enough. A server that is
		// always busy is the one that most needs its log reclaimed.
		return maintenanceForce
	}
	return maintenanceDefer
}

// needsWork answers "is there anything a pass could reclaim right now"
// from three cheap probes, none of which touches the delta log.
//
// The three are not redundant. Commits drive truncation, flushes drive
// compaction, and the clock drives a duration window - a namespace can
// have work waiting under any one of them with the other two quiet.
func (s *MaintenanceScheduler) needsWork(now time.Time) bool {
	s.mu.Lock()
	lastHead, lastPassAt := s.lastHead, s.lastPassAt
	s.mu.Unlock()

	// 1. Anything committed since the last pass. The head hash moves on
	//    every commit, so this is exact and costs a mutex.
	if h, err := s.rt.DAG.Head(); err == nil && h != lastHead {
		return true
	}
	// 2. A table backlog, which builds from memtable flushes rather than
	//    from commits and so can outlive a quiet head.
	if eng, ok := s.rt.Storage.(*engine.ServerEngine); ok {
		if eng.BlobTableCount() >= s.opts.CompactTables {
			return true
		}
	}
	// 3. The sweep. A duration window expires by the clock alone, so an
	//    idle namespace still has to look occasionally - and this is also
	//    what makes the first tick after open a real one.
	return now.Sub(lastPassAt) >= s.Sweep()
}

// runPass calls Maintain and records what it did.
func (s *MaintenanceScheduler) runPass(now time.Time) {
	res, err := s.rt.Maintain()

	s.mu.Lock()
	s.stats.Passes++
	s.deferrals = 0
	s.lastPassAt = now
	if h, herr := s.rt.DAG.Head(); herr == nil {
		s.lastHead = h
	}
	if err != nil {
		s.stats.Errors++
	} else {
		s.stats.SegmentsRemoved += res.Removed
		s.stats.BytesReclaimed += res.ReclaimedBytes
		s.stats.TablesRemoved += res.TablesRemoved
	}
	s.mu.Unlock()

	if err != nil {
		// Logged, not escalated: the namespace is still correct, it is
		// merely still holding bytes it could have released, and the next
		// tick tries again.
		s.opts.Logger.Warn("scheduled maintenance did not complete; it will be retried",
			"namespace", s.rt.DefaultNamespace, "error", err)
		return
	}
	if res.Removed > 0 || res.TablesRemoved > 0 {
		s.opts.Logger.Info("scheduled maintenance reclaimed storage",
			"namespace", s.rt.DefaultNamespace,
			"segments_removed", res.Removed, "bytes_reclaimed", res.ReclaimedBytes,
			"tables_merged", res.TablesMerged, "tables_removed", res.TablesRemoved,
			"floor_sequence", res.FloorSequence)
	}
}

// NewMaintenanceSchedulerForTest builds a scheduler with a caller-supplied
// clock and *no goroutine*, so a test can drive an exact sequence of ticks
// against real storage without waiting on wall time. TickForTest is the
// only way to advance it. Exported for other packages' tests, matching
// AcquireWriteSlotForTest's precedent in the server package.
func NewMaintenanceSchedulerForTest(rt *EmbeddedKdbRuntime, opts MaintenanceOptions, now func() time.Time) *MaintenanceScheduler {
	opts.now = now
	return newMaintenanceScheduler(rt, opts)
}

// TickForTest runs exactly one scheduler wake-up, synchronously.
func (s *MaintenanceScheduler) TickForTest() { s.tick() }

// HistoryModeOf reports the history mode a runtime is serving, for a
// caller deciding whether a maintenance loop is worth starting at all.
// HistoryModeFull namespaces still benefit - a checkpoint speeds their
// next open and compaction still reclaims dead SSTable space - but a
// deployment that wants to spend nothing on maintenance can use this to
// skip them.
func HistoryModeOf(rt *EmbeddedKdbRuntime) storage.HistoryMode {
	if rt == nil {
		return storage.HistoryModeUnset
	}
	return rt.HistoryMode()
}
