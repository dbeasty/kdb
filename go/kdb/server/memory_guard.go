package server

import (
	"runtime"
	"sync/atomic"
	"time"
)

// MemoryGuard periodically samples heap usage and exposes a cheap, lock-free "are we under
// memory pressure" check, so commitWith can reject new writes with a clear, actionable error
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
type MemoryGuard struct {
	limitBytes     uint64
	rejectFraction float64
	pressure       atomic.Bool
	stop           chan struct{}
}

// NewMemoryGuard starts a background sampler polling runtime.MemStats every 200ms. limitBytes is
// the deployment's known memory budget (e.g. a container's --memory limit); pass 0 to disable
// (ShouldReject always returns false, no goroutine started). rejectFraction is what fraction of
// limitBytes triggers rejection - e.g. 0.85 starts rejecting new writes once heap usage crosses
// 85% of the budget, leaving headroom for commits already in flight and for GC to actually
// reclaim memory before the hard limit is hit.
func NewMemoryGuard(limitBytes uint64, rejectFraction float64) *MemoryGuard {
	g := &MemoryGuard{limitBytes: limitBytes, rejectFraction: rejectFraction, stop: make(chan struct{})}
	if limitBytes == 0 {
		return g
	}
	go g.sampleLoop()
	return g
}

func (g *MemoryGuard) sampleLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	threshold := float64(g.limitBytes) * g.rejectFraction
	var m runtime.MemStats
	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
			runtime.ReadMemStats(&m)
			// Sys, not HeapAlloc: HeapAlloc is only the currently-*live* heap, which a GC cycle
			// can drop back down to near zero almost immediately - but Go does not eagerly
			// return freed pages to the OS (see runtime/debug's FreeOSMemory doc), so the
			// process's actual OS-visible footprint (what a container cgroup's hard memory
			// limit is enforced against) tracks much closer to Sys, which stays elevated long
			// after HeapAlloc has dropped. Gating on HeapAlloc alone was measured to let the
			// process keep accepting writes for a long stretch after Sys had already climbed
			// past the container's real limit, right up until the kernel OOM-killed it with no
			// warning - see docs/benchmarks/lightsail-sim/README.md.
			g.pressure.Store(float64(m.Sys) >= threshold)
		}
	}
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
