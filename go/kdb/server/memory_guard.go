package server

import (
	"fmt"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

// Zone is the graduated pressure level kdb-spec-layer13 Component 48 §5.5 defines. Policy is
// attached to zones rather than to a single boolean, because "reject everything" and "admit
// everything" are not the only two useful responses to memory pressure - and because a server
// that only ever does one or the other gives an operator no warning before it starts shedding.
type Zone int

const (
	// ZoneNormal: admit every class.
	ZoneNormal Zone = iota
	// ZoneElevated: still admitting every class, but scans get smaller row budgets and
	// replication concurrency is halved - shed the most deferrable work first, while the node
	// is still healthy enough that shedding it costs nothing a client will notice.
	ZoneElevated
	// ZoneHigh: reject ClassWrite and ClassScan with Busy+retryAfterMs; keep admitting point
	// reads and the replication drain. This is the zone the old boolean ShouldReject meant.
	ZoneHigh
	// ZoneCritical: reject everything but point reads, release the rescue reserve, and start
	// the abort timer. Entering this zone is a logged event and a metric, never silent.
	ZoneCritical
)

func (z Zone) String() string {
	switch z {
	case ZoneNormal:
		return "normal"
	case ZoneElevated:
		return "elevated"
	case ZoneHigh:
		return "high"
	case ZoneCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// MemoryGuard periodically samples process memory usage and exposes a cheap, lock-free view of
// which pressure Zone the server is in, so admission can reject new work with a clear, actionable
// error instead of the process silently accumulating garbage until the OS SIGKILLs it for
// exceeding a container's memory limit.
//
// This exists because fixing the known O(n)-per-commit allocation bugs (see
// go/kdb/dag.GetCommitByTransactionID and document.DocumentTree's lazy Entries) narrows the
// problem but cannot eliminate it: an in-memory, uncompacted commit DAG grows without bound by
// design (every write is a new permanent commit; nothing evicts history), so any fixed memory
// budget is eventually exhaustible under sustained write volume regardless of how efficient each
// individual commit is. A constrained deployment (e.g. the $7/mo Lightsail tier - see
// docs/benchmarks/lightsail-sim/README.md) needs to degrade by rejecting new writes with a clear
// error, not by getting killed outright with no signal to the client beyond the connection dying.
//
// kdb-spec-layer13 Component 48 fixed two problems with the original version of this guard:
//
//  1. It sampled runtime.MemStats.Sys, which never decreases - Go returns freed pages to the OS
//     but keeps counting them in Sys (they move into HeapReleased instead). Once tripped, the
//     guard therefore rejected writes *forever*, for the rest of the process's life, even after a
//     GC cycle freed everything that had pushed it over: a zombie that kept answering reads while
//     permanently refusing writes, which is exactly the failure mode a crash-only design (see
//     Component 50) exists to make unnecessary. This version samples
//     /memory/classes/total:bytes minus /memory/classes/heap/released:bytes via runtime/metrics -
//     the portion of the process's mapped memory that hasn't been handed back to the OS, which
//     both tracks real footprint (unlike HeapAlloc, which can drop to near zero within one GC
//     cycle while the OS-visible footprint stays elevated) and actually decreases once the GC
//     returns memory (unlike Sys).
//  2. It called runtime.ReadMemStats, which stops every goroutine in the process for the
//     duration of the read, every 200ms, for the life of the process - a recurring, self-inflicted
//     latency source in precisely the high-load conditions the guard exists to help with.
//     runtime/metrics.Read provides the same category of number without the stop-the-world pause.
//
// Two further §5.1/§5.5 requirements are met here rather than in Admission, because both are
// properties of *measurement* rather than of policy:
//
//   - Decisions are driven off a short moving average over a ring of recent samples, not off the
//     single latest reading, so one allocation spike between two GC cycles cannot trip the whole
//     server into shedding.
//   - Zone transitions require the condition to hold for a minimum dwell time, and downward
//     transitions use lower thresholds than upward ones (design principle P3, "no one-way
//     doors"). Together these give real hysteresis: pressure that was genuine and has genuinely
//     subsided lets work resume, without the flag flickering on every sample near a boundary.
type MemoryGuard struct {
	limitBytes uint64
	// upper[z] is the smoothed usage at or above which the guard enters zone z; lower[z] is the
	// level it must fall back below to leave it. lower < upper is the hysteresis band.
	upper [4]float64
	lower [4]float64

	zone     atomic.Int32 // current Zone, lock-free for ShouldReject/CurrentZone
	pressure atomic.Bool  // cached (zone >= ZoneHigh)

	mu          sync.Mutex
	ring        []float64 // recent raw samples, newest at ringNext-1
	ringNext    int
	candidate   Zone      // zone the samples currently argue for
	candidateAt time.Time // when candidate last changed - dwell is measured from here

	dwell        time.Duration
	pollInterval time.Duration
	now          func() time.Time // overridable in tests

	// observer, if set, is called after every sample with the smoothed usage and the current
	// zone. Admission uses this to resize grant capacity (the nonGrantedFloor of §5.3) and to
	// drop/restore the rescue reserve on Critical transitions.
	observerMu sync.RWMutex
	observer   func(smoothedUsed float64, zone Zone)

	stop     chan struct{}
	stopOnce sync.Once
	// done is closed by sampleLoop right before it returns. Stop waits on it, which is what
	// makes Stop actually synchronous - see Stop's doc comment for why that matters.
	done chan struct{}
}

// clearRatio is how far below a zone's entry threshold smoothed usage must fall before the guard
// leaves that zone - see the type doc's hysteresis note. 0.9 means: enter High at 85% of the
// budget, fall back out of it at 76.5%. A deliberately modest gap: wide enough that normal
// sampling noise near a boundary doesn't flicker the zone on every sample, without leaving so
// much slack that a server sits needlessly throttled long after real pressure passed.
const clearRatio = 0.9

// zoneDwell is how long the samples must agree on a new zone before the guard actually moves
// there (§5.5: "every zone transition requires the condition to hold for a minimum dwell time").
// At the default 200ms poll this is three consecutive agreeing samples.
const zoneDwell = 600 * time.Millisecond

// sampleWindow is how many raw samples the moving average covers - 1 second at the default 200ms
// poll interval. Long enough to absorb a single spike, short enough that the guard still reacts
// well inside the time it takes a burst to exhaust the headroom the zones leave.
const sampleWindow = 5

// zoneFractionOfReject positions the four zones relative to the caller's rejectFraction, which
// stays the knob that means "where writes start being refused" - i.e. it is the entry point of
// ZoneHigh. §5.5 specifies the zones at 70/85/93% of the budget; expressing the other three as
// ratios of the 85% case preserves those defaults exactly at rejectFraction=0.85 while keeping a
// single, meaningful operator knob rather than four independent ones that can be set into
// nonsensical orders relative to each other.
var zoneFractionOfReject = [4]float64{
	ZoneNormal:   0,
	ZoneElevated: 70.0 / 85.0,
	ZoneHigh:     1.0,
	ZoneCritical: 93.0 / 85.0,
}

// NewMemoryGuard starts a background sampler polling every 200ms. limitBytes is the deployment's
// known memory budget (e.g. a container's --memory limit); pass 0 to disable (ShouldReject always
// returns false, CurrentZone is always ZoneNormal, no goroutine started). rejectFraction is what
// fraction of limitBytes puts the server into ZoneHigh and so starts rejecting writes - e.g. 0.85
// leaves headroom for commits already in flight and for GC to actually reclaim memory before the
// hard limit is hit.
func NewMemoryGuard(limitBytes uint64, rejectFraction float64) *MemoryGuard {
	g := &MemoryGuard{
		limitBytes:   limitBytes,
		dwell:        zoneDwell,
		pollInterval: 200 * time.Millisecond,
		now:          time.Now,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	for z := ZoneNormal; z <= ZoneCritical; z++ {
		entry := float64(limitBytes) * rejectFraction * zoneFractionOfReject[z]
		g.upper[z] = entry
		g.lower[z] = entry * clearRatio
	}
	if limitBytes == 0 {
		return g
	}
	go g.sampleLoop()
	return g
}

// SetObserver registers a callback invoked after every sample with the smoothed usage and the
// current zone. Replaces any previous observer; pass nil to clear.
func (g *MemoryGuard) SetObserver(fn func(smoothedUsed float64, zone Zone)) {
	if g == nil {
		return
	}
	g.observerMu.Lock()
	g.observer = fn
	g.observerMu.Unlock()
}

// LimitBytes reports the configured budget, or 0 when the guard is disabled.
func (g *MemoryGuard) LimitBytes() uint64 {
	if g == nil {
		return 0
	}
	return g.limitBytes
}

func (g *MemoryGuard) sampleLoop() {
	ticker := time.NewTicker(g.pollInterval)
	defer ticker.Stop()
	defer close(g.done)
	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
			g.sampleOnce()
		}
	}
}

func (g *MemoryGuard) sampleOnce() {
	g.observe(currentMemoryUsageBytes())
}

// observe feeds one raw reading through the moving average and the dwell-gated zone state
// machine. Split out from sampleOnce so tests can drive an exact sequence of readings without
// waiting on a real ticker or trying to provoke real allocation.
func (g *MemoryGuard) observe(used float64) {
	g.mu.Lock()
	if len(g.ring) < sampleWindow {
		g.ring = append(g.ring, used)
	} else {
		g.ring[g.ringNext] = used
		g.ringNext = (g.ringNext + 1) % sampleWindow
	}
	var sum float64
	for _, v := range g.ring {
		sum += v
	}
	smoothed := sum / float64(len(g.ring))

	current := Zone(g.zone.Load())
	want := g.zoneFor(smoothed, current)
	now := g.now()
	if want != g.candidate {
		g.candidate, g.candidateAt = want, now
	}
	// Moving *up* a zone happens immediately: the dwell time exists to stop the server flapping
	// back down out of a zone (and to stop one spike dragging it up), but delaying an escalation
	// that the smoothed average already supports would spend the very headroom the zones exist
	// to protect. Coming back down is what waits for the dwell to elapse.
	if want > current || now.Sub(g.candidateAt) >= g.dwell {
		if want != current {
			g.zone.Store(int32(want))
			g.pressure.Store(want >= ZoneHigh)
			current = want
		}
	}
	g.mu.Unlock()

	g.observerMu.RLock()
	obs := g.observer
	g.observerMu.RUnlock()
	if obs != nil {
		obs(smoothed, current)
	}
}

// zoneFor maps smoothed usage to a zone, using the upper thresholds to move up and the lower
// ones to move down (the hysteresis band). from is the zone currently in effect.
func (g *MemoryGuard) zoneFor(used float64, from Zone) Zone {
	for z := ZoneCritical; z >= ZoneElevated; z-- {
		threshold := g.upper[z]
		if z <= from {
			// Already at or above this zone: it takes a fall below the *lower* threshold to
			// leave, not merely dropping back under the entry point.
			threshold = g.lower[z]
		}
		if used >= threshold {
			return z
		}
	}
	return ZoneNormal
}

// currentMemoryUsageBytes reports process memory usage, preferring the Linux cgroup's own
// memory.current (kdb-spec-layer13 Component 48 §5.1) - the exact figure a container's --memory
// limit is enforced against - over the runtime/metrics-based estimate below, which measures
// virtual address space the Go runtime has *mapped* rather than what the cgroup actually charges
// against the limit. Found to matter empirically: running this guard's Docker e2e harness
// (docs/benchmarks/resource-governance-sim) showed docker stats reporting a stable, small
// resident-memory figure throughout a sustained write burst while relying on runtime/metrics
// alone measures something meaningfully different - not every environment needs the two to
// agree, but a guard sized off a container's --memory limit should measure the same thing that
// limit is enforced against.
//
// Falls back to /memory/classes/total:bytes minus /memory/classes/heap/released:bytes (see the
// type doc's point 1 for why this, not MemStats.Sys or HeapAlloc) when no cgroup memory
// controller is available - non-Linux hosts, or a Linux host not running inside one.
func currentMemoryUsageBytes() float64 {
	if v, ok := cgroupMemoryCurrentBytes(); ok {
		return float64(v)
	}
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(samples)
	total := sampleUint64(samples[0])
	released := sampleUint64(samples[1])
	if released > total {
		return 0
	}
	return float64(total - released)
}

func sampleUint64(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}

// CurrentZone reports the pressure zone in effect right now. O(1), lock-free. Nil-safe: a nil
// *MemoryGuard (no limit configured) is always ZoneNormal.
func (g *MemoryGuard) CurrentZone() Zone {
	if g == nil {
		return ZoneNormal
	}
	return Zone(g.zone.Load())
}

// ShouldReject reports whether a new *write* should be rejected right now due to memory pressure -
// i.e. whether the guard is in ZoneHigh or above. O(1), lock-free - cheap enough to call on every
// commit. Nil-safe: a nil *MemoryGuard (no limit configured) never rejects.
func (g *MemoryGuard) ShouldReject() bool {
	if g == nil {
		return false
	}
	return g.pressure.Load()
}

// Stop halts the background sampler and, unlike a bare "signal and return", does not return
// until sampleLoop has actually exited. Safe to call on a disabled (limitBytes == 0) guard, a
// nil one, or more than once.
//
// Synchronous on purpose: a caller that stops the guard specifically to drive it with its own
// observe() calls (see the test helper pinZone) needs the guarantee that no straggler background
// sample can land after Stop returns. Before this waited on g.done, it only closed g.stop and
// returned immediately - select does not preempt an already-fired ticker case, so a sample could
// still be in flight, and under real scheduling delay (heavier on a loaded CI runner, worse still
// under -race) a tick could fire for the first time in the gap between NewMemoryGuard and the
// very next line calling Stop. That sample's real, low process-RSS reading would land in the
// ring buffer either just before or concurrently with the caller's own explicit observe(), and
// being averaged into the moving window would pull the zone back under threshold - intermittent,
// unrelated-looking failures with the exact shape "expected ZoneHigh, got ZoneNormal" or "the
// write was not shed", on a schedule (present under load, absent on a quiet machine) that looked
// like a race in the test rather than in what it was testing.
func (g *MemoryGuard) Stop() {
	if g == nil || g.limitBytes == 0 {
		return
	}
	g.stopOnce.Do(func() { close(g.stop) })
	<-g.done
}

// MemoryPressureError is returned instead of admitting work when the server's pressure Zone sheds
// that operation's class - a clean, actionable rejection (throttling the input, per the
// component's own hardening goal) instead of continuing to accumulate garbage until the OS kills
// the process outright with no signal to the client beyond the connection dying.
//
// Distinct from BusyError, which means the node is *contended* (a full write queue, or grant
// capacity momentarily held by other in-flight work) rather than near its memory budget. Both
// map to the wire's BUSY code, because the client's move is the same either way - back off and
// retry - but they are different conditions, and an operator reading a log or a metric needs to
// be able to tell "we are running out of memory" from "we are running out of concurrency".
type MemoryPressureError struct {
	// Zone is the pressure zone in effect when the operation was shed.
	Zone Zone
	// Class is the operation class that was shed. Point reads are never shed, so this is never
	// ClassPointRead.
	Class OpClass
	// RetryAfterMs is how long the caller should wait before retrying - longer in deeper zones,
	// where an immediate retry is both less likely to succeed and actively harmful, since it
	// spends the headroom the abort sequence may be about to need.
	RetryAfterMs int
}

func (e *MemoryPressureError) Error() string {
	if e.Zone == ZoneNormal && e.RetryAfterMs == 0 {
		return "kdb server: rejecting write - server is near its configured memory budget, retry later"
	}
	return fmt.Sprintf("kdb server: rejecting %s - memory pressure zone %s (retry after %dms)",
		e.Class, e.Zone, e.RetryAfterMs)
}

// RetryAfter returns how long a caller should wait before retrying.
func (e *MemoryPressureError) RetryAfter() time.Duration {
	return time.Duration(e.RetryAfterMs) * time.Millisecond
}
