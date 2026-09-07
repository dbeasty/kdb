package storage

import (
	"sort"
	"sync"
	"time"
)

// DefaultNamespaceFloorBytes is the smallest share the arbiter will hand a namespace that is
// pulling its weight for nothing. A namespace below its floor still answers reads - it just
// re-reads more of them from the delta log - so the floor is about keeping a cold namespace
// usable, not fast. Deliberately small: nine of these is 36 MiB, which has to fit inside a pool
// a single-namespace deployment would have been given on its own.
const DefaultNamespaceFloorBytes int64 = 4 << 20

// DefaultBudgetRebalanceInterval is how often shares are recomputed. Slow on purpose. Eviction
// is what a shrinking share costs, and paying that every few seconds to chase a workload that
// moves every few minutes is worse than being a little wrong in between.
const DefaultBudgetRebalanceInterval = 10 * time.Second

// budgetHysteresisFraction is how far a namespace's computed share must move from what it
// currently holds before the arbiter actually applies it. Without this, ordinary tick-to-tick
// noise in demand would re-cut every budget every interval, and each re-cut that shrinks a
// namespace evicts.
const budgetHysteresisFraction = 0.125

// BudgetParticipant is one namespace's side of the memory pool: how much it would hold if
// nothing bounded it, and where to put the ceiling it is given.
//
// Both halves are called from the arbiter's own goroutine, so implementations must be safe to
// call concurrently with the namespace serving traffic - which, for the stores behind these,
// they already are.
type BudgetParticipant interface {
	// DemandBytes is what this participant wants. Not what it holds: a participant pinned at
	// its ceiling and missing reads because of it should report more than its ceiling, or the
	// arbiter has no way to tell "exactly right" from "starving".
	DemandBytes() int64
	// SetBudgetBytes installs a new ceiling. May evict.
	SetBudgetBytes(int64)
}

// BudgetArbiter divides one memory pool across the namespaces sharing a host.
//
// This is the half of Component 64 that pays for itself. Before it, every namespace opened with
// its own hardcoded 64 MiB hot tier and derived four sub-budgets from it, so nine namespaces in
// one process meant 720 MiB of ceilings that could not see each other - while the workload
// behind them was never evenly hot. A static Nth share is not an answer either: it makes the hot
// namespace evict against a sliver while eight cold ones sit on headroom they will never touch.
// See docs/kdb-spec-layer17-multi-namespace-runtime.md §1.1.
//
// The policy is deliberately dumb, and should stay that way until something measured says
// otherwise: every namespace gets a floor, what is left over is split in proportion to demand,
// and a share is only applied when it has moved far enough to be worth the eviction. There is no
// prediction, no decay curve, and no attempt to be optimal - just an arrangement where a busy
// namespace can borrow the headroom an idle one is not using.
type BudgetArbiter struct {
	mu       sync.Mutex
	total    int64
	floor    int64
	parts    map[string]BudgetParticipant
	reserved map[string]int64
	applied  map[string]int64

	stop   chan struct{}
	closed bool
	wg     sync.WaitGroup
}

// NewBudgetArbiter returns an arbiter over totalBytes, rebalancing every interval. A
// non-positive interval means no ticker: shares are then recomputed only when membership
// changes, or when Rebalance is called - which is what tests want, and what a host with one
// namespace never needs.
func NewBudgetArbiter(totalBytes int64, interval time.Duration) *BudgetArbiter {
	a := &BudgetArbiter{
		total:    totalBytes,
		floor:    DefaultNamespaceFloorBytes,
		parts:    make(map[string]BudgetParticipant),
		reserved: make(map[string]int64),
		applied:  make(map[string]int64),
		stop:     make(chan struct{}),
	}
	if interval > 0 {
		a.wg.Add(1)
		go a.loop(interval)
	}
	return a
}

// TotalBytes is the pool being divided.
func (a *BudgetArbiter) TotalBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total
}

// SetFloorBytes overrides the per-namespace floor. For tests and for a host whose pool is small
// enough that the default floor would not fit the namespaces it expects.
func (a *BudgetArbiter) SetFloorBytes(b int64) {
	a.mu.Lock()
	a.floor = b
	a.mu.Unlock()
	a.Rebalance()
}

// Register adds a namespace to the pool and recomputes every share immediately, so the new
// namespace does not run unbounded until the first tick and the existing ones give up their
// share of the room at the moment it is actually taken.
func (a *BudgetArbiter) Register(name string, p BudgetParticipant) {
	if p == nil {
		return
	}
	a.mu.Lock()
	a.parts[name] = p
	a.mu.Unlock()
	a.Rebalance()
}

// Reserve carves a fixed amount out of the pool for name and takes it out of the rebalancing -
// the escape hatch for a namespace whose budget a caller wants to pin rather than arbitrate.
// The remaining namespaces divide whatever is left.
func (a *BudgetArbiter) Reserve(name string, fixedBytes int64) {
	a.mu.Lock()
	if fixedBytes > 0 {
		a.reserved[name] = fixedBytes
	} else {
		delete(a.reserved, name)
	}
	a.mu.Unlock()
	a.Rebalance()
}

// Unregister drops a namespace and hands its share back to the others.
func (a *BudgetArbiter) Unregister(name string) {
	a.mu.Lock()
	delete(a.parts, name)
	delete(a.reserved, name)
	delete(a.applied, name)
	a.mu.Unlock()
	a.Rebalance()
}

// Shares reports the budget most recently applied to each namespace. For tests and reporting.
func (a *BudgetArbiter) Shares() map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int64, len(a.applied))
	for k, v := range a.applied {
		out[k] = v
	}
	return out
}

// Close stops the ticker. Namespaces keep whatever ceiling they were last given; nothing is
// unbounded as a result, which matters because the alternative - handing everything back on the
// way out - would spike memory exactly when a host is shutting down.
func (a *BudgetArbiter) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	close(a.stop)
	a.mu.Unlock()
	a.wg.Wait()
}

func (a *BudgetArbiter) loop(interval time.Duration) {
	defer a.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.Rebalance()
		}
	}
}

// Rebalance recomputes and applies every share once. Exported so a host can force a pass and so
// tests can drive the policy without waiting on a ticker.
func (a *BudgetArbiter) Rebalance() {
	for name, share := range a.plan() {
		a.mu.Lock()
		p, ok := a.parts[name]
		if ok {
			a.applied[name] = share
		}
		a.mu.Unlock()
		if ok {
			p.SetBudgetBytes(share)
		}
	}
}

// plan computes the shares that should be applied, and returns only those that have moved far
// enough to be worth applying. Demand is read here, under no lock of the arbiter's, because
// DemandBytes reaches into live stores and holding the arbiter's mutex across it would make the arbiter's
// tick contend with the namespaces it is measuring.
func (a *BudgetArbiter) plan() map[string]int64 {
	a.mu.Lock()
	names := make([]string, 0, len(a.parts))
	for n := range a.parts {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]BudgetParticipant, len(names))
	for i, n := range names {
		parts[i] = a.parts[n]
	}
	total, floor := a.total, a.floor
	reserved := make(map[string]int64, len(a.reserved))
	for k, v := range a.reserved {
		reserved[k] = v
	}
	applied := make(map[string]int64, len(a.applied))
	for k, v := range a.applied {
		applied[k] = v
	}
	a.mu.Unlock()

	if len(names) == 0 {
		return nil
	}

	// Fixed reservations come off the top; whoever is left divides the rest.
	pool := total
	arbitrated := make([]int, 0, len(names))
	for i, n := range names {
		if r, ok := reserved[n]; ok && r > 0 {
			pool -= r
			continue
		}
		arbitrated = append(arbitrated, i)
	}

	out := make(map[string]int64, len(names))
	for n, r := range reserved {
		out[n] = r
	}
	if len(arbitrated) == 0 {
		return finalize(out, applied, total)
	}
	if pool < 0 {
		pool = 0
	}

	n := int64(len(arbitrated))
	// A floor that cannot fit everyone is not a floor; fall back to an even split, which is the
	// most defensible thing to do with a pool too small to express a preference.
	if floor <= 0 || floor*n > pool {
		even := pool / n
		for _, i := range arbitrated {
			out[names[i]] = even
		}
		return finalize(out, applied, total)
	}

	demands := make([]int64, len(arbitrated))
	var sum int64
	for k, i := range arbitrated {
		d := parts[i].DemandBytes()
		if d < 0 {
			d = 0
		}
		demands[k] = d
		sum += d
	}

	spare := pool - floor*n
	if sum == 0 {
		// Nothing is holding anything yet - a freshly opened host. Split evenly rather than
		// giving the first namespace to ask for something the whole pool.
		even := pool / n
		for _, i := range arbitrated {
			out[names[i]] = even
		}
		return finalize(out, applied, total)
	}
	var handed int64
	for k, i := range arbitrated {
		share := floor + int64(float64(spare)*(float64(demands[k])/float64(sum)))
		out[names[i]] = share
		handed += share
	}
	// Rounding down N times leaves a few bytes on the table; give them to the neediest rather
	// than letting the pool quietly shrink every pass.
	if rem := pool - handed; rem > 0 {
		best, bestD := arbitrated[0], demands[0]
		for k, i := range arbitrated {
			if demands[k] > bestD {
				best, bestD = i, demands[k]
			}
		}
		out[names[best]] += rem
	}
	return finalize(out, applied, total)
}

// finalize decides which of the computed shares are actually worth applying.
//
// Hysteresis alone is not safe here. It can suppress one namespace's *decrease* while letting
// another's increase through, and the total actually installed then drifts above the pool - which
// is the single thing a shared pool exists to prevent, and it is not a reporting artifact: those
// are real ceilings on real caches. So the damping is a comfort, never a licence to exceed the
// pool. When honouring it would over-commit, this pass forgoes it entirely; `want` sums to the
// pool by construction, so applying all of it restores the ceiling.
//
// Caught by TestNineNamespacesDivideOnePool, where nine namespaces registering one at a time left
// enough suppressed decreases to put the installed total 2.8% over.
func finalize(want, applied map[string]int64, total int64) map[string]int64 {
	filtered := withHysteresis(want, applied)
	var projected int64
	for name := range want {
		if v, ok := filtered[name]; ok {
			projected += v
			continue
		}
		projected += applied[name]
	}
	if projected > total {
		return want
	}
	return filtered
}

// withHysteresis drops the shares that have not moved far enough from what is already applied to
// justify the eviction a change can cost. A namespace with nothing applied yet always passes.
func withHysteresis(want, applied map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(want))
	for name, w := range want {
		cur, ok := applied[name]
		if !ok || cur <= 0 {
			out[name] = w
			continue
		}
		delta := w - cur
		if delta < 0 {
			delta = -delta
		}
		if float64(delta) > float64(cur)*budgetHysteresisFraction {
			out[name] = w
		}
	}
	return out
}
