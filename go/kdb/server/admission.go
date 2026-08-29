package server

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/document"
	"golang.org/x/sync/semaphore"
)

// Admission is kdb-spec-layer13 Component 48's grant system: work reserves the memory it is
// expected to hold *before* it starts, and releases the reservation when it finishes.
//
// This is the difference between backpressure and a smoke alarm. MemoryGuard on its own samples
// every 200ms and flips a boolean; that is reactive, and a burst admitted between two samples can
// still carry the process past its ceiling before the next sample ever runs - which is exactly
// why the pre-Component-48 guidance was to set --memory-limit-mb no higher than 60-80% of the
// container's real limit (see docs/benchmarks/lightsail-sim/README.md, where 90% and 95% were
// still OOM-killed with the guard enabled). Reserving up front closes that window: capacity that
// has been granted to in-flight work is capacity the next caller cannot also be given, whether or
// not a sample has happened in between.
//
// Capacity is effectiveLimit - reserve - nonGrantedFloor (§5.3), where nonGrantedFloor is the
// memory the grant system does not govern: goroutine stacks, indexes, and above all the commit
// DAG itself. Because the DAG grows monotonically - every write is a new permanent commit and
// nothing evicts history - that floor rises over the life of the process, and grant capacity
// shrinks to match. **That is the mechanism that turns "the DAG grows without bound by design"
// from an eventual OOM into a smooth, predictable throttle**: writes get harder to admit as
// retained history grows, instead of all succeeding right up until the kernel kills the process.
//
// The floor is applied by holding that many bytes of the semaphore permanently (adjusted on every
// sample), rather than by trying to resize the semaphore - semaphore.Weighted has a fixed
// capacity, and expressing the floor as a long-lived reservation against it means one primitive
// enforces both halves of the accounting with no separate bookkeeping to drift out of sync.
type Admission struct {
	guard *MemoryGuard
	costs *CostModel

	// sem's capacity is the *static* budget (effectiveLimit - reserve). The dynamic part of
	// capacity - the non-granted floor - is expressed as bytes of sem held by floorHeld.
	sem      *semaphore.Weighted
	capacity int64

	floorMu   sync.Mutex
	floorHeld int64

	outstandingBytes atomic.Int64
	outstandingOps   atomic.Int64

	reserve *rescueReserve
	// reserveLost records that the rescue reserve could not be re-allocated on the way back down
	// to Normal. §5.6: "failure to re-allocate is itself a signal" - the node could not get 48MB
	// back from an allocator it had just handed it to, which says the headroom the abort sequence
	// depends on is not actually there.
	reserveLost atomic.Bool

	scanRowBudget    atomic.Int64
	baseScanRowLimit int64

	stats AdmissionStats

	lastZone atomic.Int32
}

// AdmissionStats counts governance decisions. §13: "a shedding server that cannot be observed
// shedding is indistinguishable from a broken one" - and given §2.5's latching bug went unnoticed
// precisely because the service still answered reads, that is not a hypothetical concern.
type AdmissionStats struct {
	Granted        [numOpClasses]atomic.Int64
	DeniedZone     [numOpClasses]atomic.Int64
	DeniedCapacity [numOpClasses]atomic.Int64
	DeniedTooLarge [numOpClasses]atomic.Int64
	ZoneChanges    atomic.Int64
	CriticalEnters atomic.Int64
	ReserveDrops   atomic.Int64
}

// DefaultRescueReserveBytes is §5.6's rescue reserve: memory held back from the grant system and
// dropped on entry to Critical, so the abort sequence has headroom to actually run - finish
// in-flight commits, flush the log, write the typed rejections, log the abort - instead of dying
// partway through for want of a few megabytes.
const DefaultRescueReserveBytes int64 = 48 << 20

// DefaultScanRowBudget bounds rows *examined* per scan, not merely rows returned (§5.2, closing
// §2.8's "no bound on scan work" gap). A scan that exceeds it is aborted with a typed
// ResourceExhausted rather than being allowed to consume the node.
const DefaultScanRowBudget int64 = 1_000_000

// NewAdmission builds a grant system over guard's budget. A guard with no configured limit
// (LimitBytes() == 0) yields a nil-behaving Admission: Acquire always succeeds with a no-op
// grant, so an unconfigured deployment behaves exactly as it did before this existed.
func NewAdmission(guard *MemoryGuard, reserveBytes int64, scanRowBudget int64) *Admission {
	limit := guard.LimitBytes()
	if limit == 0 {
		return nil
	}
	if reserveBytes < 0 {
		reserveBytes = 0
	}
	if scanRowBudget <= 0 {
		scanRowBudget = DefaultScanRowBudget
	}
	capacity := int64(limit) - reserveBytes
	if capacity < 1 {
		// A budget smaller than the reserve is a misconfiguration, but it must not produce a
		// zero-capacity semaphore that deadlocks every caller forever. Leave a token capacity so
		// the system still functions (and still rejects, loudly, via the zone policy) rather
		// than wedging.
		capacity = 1
	}
	a := &Admission{
		guard:            guard,
		costs:            NewCostModel(),
		sem:              semaphore.NewWeighted(capacity),
		capacity:         capacity,
		reserve:          newRescueReserve(reserveBytes),
		baseScanRowLimit: scanRowBudget,
	}
	a.scanRowBudget.Store(scanRowBudget)
	a.reserve.allocate()
	guard.SetObserver(a.onSample)
	return a
}

// Costs exposes the cost model, for metrics and tests.
func (a *Admission) Costs() *CostModel {
	if a == nil {
		return nil
	}
	return a.costs
}

// Stats exposes the governance counters. Nil-safe.
func (a *Admission) Stats() *AdmissionStats {
	if a == nil {
		return nil
	}
	return &a.stats
}

// ScanRowBudget is the current per-scan cap on rows examined - lower in Elevated and above, per
// §5.5's "shrink scan row budgets". Nil-safe: an unconfigured Admission imposes no budget.
func (a *Admission) ScanRowBudget() int64 {
	if a == nil {
		return 0
	}
	return a.scanRowBudget.Load()
}

// ReserveLost reports whether the rescue reserve could not be re-allocated (§5.6).
func (a *Admission) ReserveLost() bool {
	if a == nil {
		return false
	}
	return a.reserveLost.Load()
}

// onSample runs after every MemoryGuard sample. It does the two things that must track measured
// reality rather than a fixed configuration: resize the non-granted floor, and act on zone
// transitions.
func (a *Admission) onSample(smoothedUsed float64, zone Zone) {
	a.applyFloor(smoothedUsed)

	prev := Zone(a.lastZone.Swap(int32(zone)))
	if prev == zone {
		return
	}
	a.stats.ZoneChanges.Add(1)

	// Scan row budgets shrink as pressure rises (§5.5). Halving per zone rather than switching
	// off: a scan that still fits a smaller budget is still worth serving.
	switch zone {
	case ZoneNormal:
		a.scanRowBudget.Store(a.baseScanRowLimit)
	case ZoneElevated:
		a.scanRowBudget.Store(a.baseScanRowLimit / 2)
	case ZoneHigh:
		a.scanRowBudget.Store(a.baseScanRowLimit / 4)
	case ZoneCritical:
		a.scanRowBudget.Store(a.baseScanRowLimit / 8)
	}

	if zone == ZoneCritical && prev < ZoneCritical {
		a.stats.CriticalEnters.Add(1)
		a.stats.ReserveDrops.Add(1)
		// Drop the reserve and hand the pages straight back, so the headroom is available to the
		// abort sequence now rather than whenever the GC would next have got to it.
		a.reserve.release()
		debug.FreeOSMemory()
	}
	if zone == ZoneNormal && prev != ZoneNormal {
		if !a.reserve.allocate() {
			a.reserveLost.Store(true)
		} else {
			a.reserveLost.Store(false)
		}
	}
}

// applyFloor recomputes the non-granted floor and adjusts how much of the semaphore is held on
// its behalf. See the type doc: this is what makes monotonic DAG growth throttle rather than
// kill.
func (a *Admission) applyFloor(smoothedUsed float64) {
	granted := a.outstandingBytes.Load()
	floor := int64(smoothedUsed) - granted
	if floor < 0 {
		floor = 0
	}
	// Never let the floor claim the entire semaphore: at least one maximally-sized operation
	// must remain admissible, or the node stops making progress entirely and can never work off
	// the backlog that got it here. This is the throttle's floor-stop, and reaching it is what
	// the abort watchdog (Component 50) is for.
	if maxFloor := a.capacity - (1 << 20); floor > maxFloor {
		floor = maxFloor
	}
	if floor < 0 {
		floor = 0
	}

	a.floorMu.Lock()
	defer a.floorMu.Unlock()
	switch {
	case floor > a.floorHeld:
		want := floor - a.floorHeld
		// TryAcquire, never a blocking Acquire: this runs on the sampler goroutine, and blocking
		// it would stop the very measurements that decide when to give the capacity back. If the
		// bytes aren't available right now because in-flight work holds them, the next tick
		// retries - the floor rises as that work completes and returns its grants.
		if a.sem.TryAcquire(want) {
			a.floorHeld = floor
		} else if a.sem.TryAcquire(want / 2) {
			a.floorHeld += want / 2
		}
	case floor < a.floorHeld:
		a.sem.Release(a.floorHeld - floor)
		a.floorHeld = floor
	}
}

// FloorHeldBytes reports how much capacity is currently withheld as the non-granted floor.
func (a *Admission) FloorHeldBytes() int64 {
	if a == nil {
		return 0
	}
	a.floorMu.Lock()
	defer a.floorMu.Unlock()
	return a.floorHeld
}

// OutstandingBytes reports the total bytes currently reserved by live grants.
func (a *Admission) OutstandingBytes() int64 {
	if a == nil {
		return 0
	}
	return a.outstandingBytes.Load()
}

// admitInZone applies §5.5's per-zone class policy. Point reads are never shed: a server that
// cannot answer a point read is indistinguishable from one that is down, and reads are the
// signal an operator uses to diagnose the pressure in the first place.
func admitInZone(zone Zone, class OpClass) bool {
	switch zone {
	case ZoneNormal, ZoneElevated:
		return true
	case ZoneHigh:
		return class == ClassPointRead || class == ClassReplication
	default: // ZoneCritical
		return class == ClassPointRead
	}
}

// retryAfterMsForZone is how long a rejected caller should wait. Higher zones suggest longer
// backoff: the deeper the pressure, the less likely an immediate retry finds room, and a client
// hammering a node in Critical is spending the very headroom the abort sequence needs.
func retryAfterMsForZone(zone Zone) int {
	switch zone {
	case ZoneHigh:
		return 100
	case ZoneCritical:
		return 500
	default:
		return 50
	}
}

// Acquire reserves the memory an operation of this class and size is expected to hold, blocking
// until either the capacity is available or ctx is done. Returns a typed error a client can act
// on without parsing prose (Component 51):
//
//   - *BusyError - the zone policy sheds this class right now, or capacity was unavailable
//     before ctx expired. The same request should succeed later.
//   - *ResourceExhaustedError - this operation is larger than the node's entire grant capacity
//     and will therefore never be admissible. Resubmit smaller; retrying as-is cannot help.
//
// The returned Grant must be Released exactly once (Release is idempotent, so defer is safe).
// Nil-safe: an unconfigured Admission admits everything with a no-op grant.
func (a *Admission) Acquire(ctx context.Context, class OpClass, payloadBytes int) (*Grant, error) {
	if a == nil {
		return &Grant{}, nil
	}
	zone := a.guard.CurrentZone()
	if !admitInZone(zone, class) {
		a.stats.DeniedZone[classIndex(class)].Add(1)
		return nil, &MemoryPressureError{
			Zone:         zone,
			Class:        class,
			RetryAfterMs: retryAfterMsForZone(zone),
		}
	}

	cost := a.costs.Estimate(class, payloadBytes)
	if cost > a.capacity {
		a.stats.DeniedTooLarge[classIndex(class)].Add(1)
		return nil, &ResourceExhaustedError{
			Reason:        fmt.Sprintf("%s of %d bytes needs an estimated %d bytes, more than the node's entire %d byte grant capacity", class, payloadBytes, cost, a.capacity),
			EstimateBytes: cost,
			CapacityBytes: a.capacity,
		}
	}

	// Try without blocking first: the overwhelmingly common case is that capacity is free, and
	// this keeps that path off the semaphore's waiter queue entirely.
	if !a.sem.TryAcquire(cost) {
		if err := a.sem.Acquire(ctx, cost); err != nil {
			a.stats.DeniedCapacity[classIndex(class)].Add(1)
			return nil, &BusyError{
				RetryAfterMs: retryAfterMsForZone(zone),
				Reason:       fmt.Sprintf("no grant capacity for a %s of an estimated %d bytes", class, cost),
			}
		}
	}

	a.outstandingBytes.Add(cost)
	a.outstandingOps.Add(1)
	a.stats.Granted[classIndex(class)].Add(1)
	return &Grant{
		adm:          a,
		class:        class,
		cost:         cost,
		payloadBytes: payloadBytes,
		startAllocs:  heapAllocsBytes(),
		soleInFlight: a.outstandingOps.Load() == 1,
	}, nil
}

func classIndex(c OpClass) int {
	if !c.valid() {
		return int(ClassWrite)
	}
	return int(c)
}

// Grant is a live memory reservation. Release returns it; releasing twice is a no-op.
type Grant struct {
	adm          *Admission
	class        OpClass
	cost         int64
	payloadBytes int
	startAllocs  uint64
	soleInFlight bool
	released     atomic.Bool
}

// Release returns the reservation and, when the measurement is attributable, feeds the operation's
// real cost back into the model (§5.2's P2 loop). Idempotent.
func (g *Grant) Release() {
	if g == nil || g.adm == nil || !g.released.CompareAndSwap(false, true) {
		return
	}
	// Only record actuals when this grant was the *only* operation in flight for its whole
	// lifetime - otherwise the allocation delta includes concurrent work and would teach the
	// model a cost that belongs to somebody else. Under the write gate (which runs one commit at
	// a time) this is the common case for writes, which is the class whose estimate matters most.
	if g.soleInFlight && g.adm.outstandingOps.Load() == 1 {
		if allocated := heapAllocsBytes(); allocated > g.startAllocs {
			g.adm.costs.Observe(g.class, g.payloadBytes, int64(allocated-g.startAllocs))
		}
	}
	g.adm.outstandingBytes.Add(-g.cost)
	g.adm.outstandingOps.Add(-1)
	g.adm.sem.Release(g.cost)
}

// CostBytes is what this grant reserved.
func (g *Grant) CostBytes() int64 {
	if g == nil {
		return 0
	}
	return g.cost
}

// heapAllocsBytes reads the process's cumulative allocated-bytes counter. Unlike
// runtime.ReadMemStats this does not stop the world (§2.6), which matters because it is read on
// every admitted operation. Cumulative-allocated over an operation is an *upper* bound on what
// that operation retains - the safe direction to be wrong in, per §5.2's "bias high".
func heapAllocsBytes() uint64 {
	s := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}}
	metrics.Read(s)
	return sampleUint64(s[0])
}

// rescueReserve is §5.6's held-back headroom. It is a real allocation that is really touched -
// a []byte the runtime has actually committed pages for - because a reservation the allocator
// has not honored yet is not headroom, it is an intention.
type rescueReserve struct {
	mu   sync.Mutex
	held []byte
	size int64
}

func newRescueReserve(size int64) *rescueReserve { return &rescueReserve{size: size} }

// allocate takes the reserve. Returns false if it could not be obtained - which §5.6 treats as a
// signal in its own right, not a benign miss.
func (r *rescueReserve) allocate() bool {
	if r == nil || r.size <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held != nil {
		return true
	}
	defer func() { _ = recover() }() // a failed reserve must not take the process down
	buf := make([]byte, r.size)
	// Touch one byte per 4KiB page so the pages are genuinely committed rather than merely
	// mapped - an untouched allocation can be backed lazily, which would make the "reserve"
	// nothing but an address-space claim the kernel is free to not honor until it is too late.
	for i := 0; i < len(buf); i += 4096 {
		buf[i] = 1
	}
	r.held = buf
	runtime.KeepAlive(buf)
	return true
}

func (r *rescueReserve) release() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held = nil
}

func (r *rescueReserve) heldBytes() int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.held))
}

// ResourceExhaustedError means this specific operation can never be admitted at the node's
// current capacity, however long the caller waits - resubmit it smaller rather than retrying it
// unchanged (kdb-spec-layer13 Component 51 §8.1's RESOURCE_EXHAUSTED code). Distinct from
// BusyError, which is the same request being told to come back later.
type ResourceExhaustedError struct {
	Reason        string
	EstimateBytes int64
	CapacityBytes int64
}

func (e *ResourceExhaustedError) Error() string {
	return "kdb server: resource exhausted: " + e.Reason
}

// transactionPayloadBytes sizes a transaction for the cost model: the bytes of document content
// it carries. Patch strings dominate by orders of magnitude - a DeleteOp is a UUID and a WriteOp's
// non-patch part is a UUID too - so the per-op fixed cost is left to the model's base term rather
// than being re-derived here from struct sizes that would drift the moment document.Op changes.
func transactionPayloadBytes(tx document.Transaction) int {
	total := 0
	for _, op := range tx.Operations {
		if w, ok := op.(document.WriteOp); ok {
			total += len(w.Patch)
		}
	}
	return total
}
