package server

import (
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// MemoryGuard periodically samples process memory usage and exposes a cheap, lock-free "are we
// under memory pressure" check, so commitWith can reject new writes with a clear, actionable error
// instead of the process silently accumulating garbage until the OS SIGKILLs it for exceeding a
// container's memory limit.
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
// It also adds real hysteresis (kdb-spec-layer13 design principle P3, "no one-way doors"): a
// separate, lower clear threshold, so pressure that was real and has genuinely subsided lets
// writes resume - not just the escape hatch of reconfiguring the limit via SetMemoryLimit(0, ...),
// which was the only way to un-stick the original guard even after the underlying memory was
// long gone.
type MemoryGuard struct {
	limitBytes uint64
	tripBytes  float64
	clearBytes float64
	pressure   atomic.Bool
	stop       chan struct{}
}

// clearRatio is how far below tripBytes usage must fall before pressure clears - see the type
// doc's hysteresis note. 0.9 means: trip at rejectFraction of the budget, clear at 90% of that
// trip point (e.g. trip at 85%, clear at 76.5%) - a deliberately modest gap, wide enough that
// normal sampling noise near the boundary doesn't flicker the flag on every sample, without
// leaving so much slack that a server sits needlessly throttled long after real pressure passed.
const clearRatio = 0.9

// NewMemoryGuard starts a background sampler polling every 200ms. limitBytes is the deployment's
// known memory budget (e.g. a container's --memory limit); pass 0 to disable (ShouldReject always
// returns false, no goroutine started). rejectFraction is what fraction of limitBytes triggers
// rejection - e.g. 0.85 starts rejecting new writes once usage crosses 85% of the budget, leaving
// headroom for commits already in flight and for GC to actually reclaim memory before the hard
// limit is hit.
func NewMemoryGuard(limitBytes uint64, rejectFraction float64) *MemoryGuard {
	g := &MemoryGuard{
		limitBytes: limitBytes,
		tripBytes:  float64(limitBytes) * rejectFraction,
		clearBytes: float64(limitBytes) * rejectFraction * clearRatio,
		stop:       make(chan struct{}),
	}
	if limitBytes == 0 {
		return g
	}
	go g.sampleLoop()
	return g
}

func (g *MemoryGuard) sampleLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
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
	used := currentMemoryUsageBytes()
	switch {
	case used >= g.tripBytes:
		g.pressure.Store(true)
	case used <= g.clearBytes:
		g.pressure.Store(false)
	default:
		// Between the two thresholds: hold whatever state we were already in (the hysteresis
		// band itself) - neither a fresh trip nor a clear.
	}
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

// ShouldReject reports whether a new write should be rejected right now due to memory pressure.
// O(1), lock-free - cheap enough to call on every commit. Nil-safe: a nil *MemoryGuard (no limit
// configured) never rejects.
func (g *MemoryGuard) ShouldReject() bool {
	if g == nil {
		return false
	}
	return g.pressure.Load()
}

// Stop halts the background sampler. Safe to call on a disabled (limitBytes == 0) guard, or a
// nil one.
func (g *MemoryGuard) Stop() {
	if g == nil || g.limitBytes == 0 {
		return
	}
	close(g.stop)
}

// MemoryPressureError is returned instead of committing when the server is near its configured
// memory budget - a clean, actionable rejection (throttling the input, per the component's own
// hardening goal) instead of continuing to accumulate garbage until the OS kills the process
// outright with no signal to the client beyond the connection dying.
type MemoryPressureError struct{}

func (e *MemoryPressureError) Error() string {
	return "kdb server: rejecting write - server is near its configured memory budget, retry later"
}
