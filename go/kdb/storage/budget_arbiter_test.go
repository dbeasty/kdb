package storage_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
)

const mib = int64(1) << 20

// fakeParticipant is a namespace that holds whatever it is told to want.
type fakeParticipant struct {
	mu     sync.Mutex
	demand int64
	budget int64
	sets   int
}

func (f *fakeParticipant) DemandBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.demand
}

func (f *fakeParticipant) SetBudgetBytes(b int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.budget = b
	f.sets++
}

func (f *fakeParticipant) want(d int64) {
	f.mu.Lock()
	f.demand = d
	f.mu.Unlock()
}

func (f *fakeParticipant) got() (int64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.budget, f.sets
}

// A lone namespace gets the whole pool. This is the compatibility case: a host with one
// namespace under it must be proportioned exactly as that namespace would have been on its own.
func TestArbiterGivesOneNamespaceEverything(t *testing.T) {
	a := storage.NewBudgetArbiter(64*mib, 0)
	defer a.Close()
	p := &fakeParticipant{}
	a.Register("only", p)
	if got, _ := p.got(); got != 64*mib {
		t.Fatalf("lone namespace got %d bytes, want the whole %d byte pool", got, 64*mib)
	}
}

// Registering a second namespace takes room off the first at the moment it is actually claimed,
// not at the next tick.
func TestArbiterSplitsOnRegistration(t *testing.T) {
	a := storage.NewBudgetArbiter(64*mib, 0)
	defer a.Close()
	first, second := &fakeParticipant{}, &fakeParticipant{}
	a.Register("a", first)
	a.Register("b", second)
	// Neither is holding anything yet, so an even split is the only defensible answer.
	gotA, _ := first.got()
	gotB, _ := second.got()
	if gotA != 32*mib || gotB != 32*mib {
		t.Fatalf("shares were %d / %d, want an even %d each", gotA, gotB, 32*mib)
	}
}

// The whole point of the component: a busy namespace borrows the headroom an idle one is not
// using, instead of both sitting on a static half.
func TestArbiterFollowsDemand(t *testing.T) {
	a := storage.NewBudgetArbiter(100*mib, 0)
	a.SetFloorBytes(10 * mib)
	defer a.Close()
	hot, cold := &fakeParticipant{}, &fakeParticipant{}
	a.Register("hot", hot)
	a.Register("cold", cold)

	hot.want(30 * mib)
	cold.want(10 * mib)
	a.Rebalance()

	// floor 10 each, 80 spare split 3:1 by demand.
	gotHot, _ := hot.got()
	gotCold, _ := cold.got()
	if gotHot != 70*mib {
		t.Fatalf("hot namespace got %d, want %d", gotHot, 70*mib)
	}
	if gotCold != 30*mib {
		t.Fatalf("cold namespace got %d, want %d", gotCold, 30*mib)
	}
	if gotHot+gotCold != 100*mib {
		t.Fatalf("shares sum to %d, want exactly the %d pool", gotHot+gotCold, 100*mib)
	}
}

// However lopsided demand gets, a quiet namespace keeps its floor - it should be slower, not
// unable to serve a read.
func TestArbiterNeverDropsANamespaceBelowItsFloor(t *testing.T) {
	a := storage.NewBudgetArbiter(100*mib, 0)
	a.SetFloorBytes(10 * mib)
	defer a.Close()
	hot, idle := &fakeParticipant{}, &fakeParticipant{}
	a.Register("hot", hot)
	a.Register("idle", idle)

	hot.want(900 * mib)
	idle.want(0)
	a.Rebalance()

	gotIdle, _ := idle.got()
	if gotIdle != 10*mib {
		t.Fatalf("idle namespace got %d, want its %d floor", gotIdle, 10*mib)
	}
	gotHot, _ := hot.got()
	if gotHot != 90*mib {
		t.Fatalf("hot namespace got %d, want the remaining %d", gotHot, 90*mib)
	}
}

// A pool too small to give everyone a floor cannot express a preference, so it splits evenly
// rather than starving whoever sorts last.
func TestArbiterFallsBackToEvenWhenTheFloorCannotFit(t *testing.T) {
	a := storage.NewBudgetArbiter(9*mib, 0)
	a.SetFloorBytes(10 * mib)
	defer a.Close()
	one, two, three := &fakeParticipant{}, &fakeParticipant{}, &fakeParticipant{}
	a.Register("1", one)
	a.Register("2", two)
	a.Register("3", three)
	one.want(100 * mib)
	a.Rebalance()
	for name, p := range map[string]*fakeParticipant{"1": one, "2": two, "3": three} {
		if got, _ := p.got(); got != 3*mib {
			t.Fatalf("namespace %s got %d, want an even %d", name, got, 3*mib)
		}
	}
}

// Shrinking a namespace evicts, so a share that has barely moved is not worth applying. Without
// this, ordinary tick-to-tick noise would re-cut every budget every interval.
func TestArbiterHoldsStillForSmallChanges(t *testing.T) {
	a := storage.NewBudgetArbiter(100*mib, 0)
	a.SetFloorBytes(10 * mib)
	defer a.Close()
	hot, cold := &fakeParticipant{}, &fakeParticipant{}
	a.Register("hot", hot)
	a.Register("cold", cold)
	hot.want(30 * mib)
	cold.want(10 * mib)
	a.Rebalance()
	_, setsBefore := hot.got()

	// A nudge: hot's computed share moves from 70 to ~71.4 MiB, well under the 12.5% threshold.
	hot.want(31 * mib)
	a.Rebalance()
	got, setsAfter := hot.got()
	if setsAfter != setsBefore {
		t.Fatalf("a %d byte move re-cut the budget (%d -> %d applications)", 1*mib, setsBefore, setsAfter)
	}
	if got != 70*mib {
		t.Fatalf("budget drifted to %d, want it held at %d", got, 70*mib)
	}

	// A real shift does get applied.
	hot.want(200 * mib)
	a.Rebalance()
	if _, setsNow := hot.got(); setsNow == setsAfter {
		t.Fatal("a large demand change was not applied")
	}
}

// Reserve is the escape hatch: a caller who has decided what one namespace needs keeps that
// decision, and the rest divide what is left.
func TestArbiterReservationIsCarvedOffTheTop(t *testing.T) {
	a := storage.NewBudgetArbiter(100*mib, 0)
	a.SetFloorBytes(10 * mib)
	defer a.Close()
	pinned, other := &fakeParticipant{}, &fakeParticipant{}
	a.Register("pinned", pinned)
	a.Reserve("pinned", 40*mib)
	a.Register("other", other)

	other.want(500 * mib)
	a.Rebalance()

	if got, _ := pinned.got(); got != 40*mib {
		t.Fatalf("pinned namespace got %d, want its reserved %d regardless of demand elsewhere", got, 40*mib)
	}
	if got, _ := other.got(); got != 60*mib {
		t.Fatalf("other namespace got %d, want the remaining %d", got, 60*mib)
	}
}

// Closing a namespace hands its room back to the ones that are staying.
func TestArbiterReturnsRoomOnUnregister(t *testing.T) {
	a := storage.NewBudgetArbiter(64*mib, 0)
	defer a.Close()
	stay, going := &fakeParticipant{}, &fakeParticipant{}
	a.Register("stay", stay)
	a.Register("going", going)
	if got, _ := stay.got(); got != 32*mib {
		t.Fatalf("got %d, want %d before the other left", got, 32*mib)
	}
	a.Unregister("going")
	if got, _ := stay.got(); got != 64*mib {
		t.Fatalf("got %d, want the whole %d pool back", got, 64*mib)
	}
}

// Nine namespaces, one pool - the shape the component exists for. Whatever the demand, the
// shares must never add up to more than the pool; that is the invariant nine independent
// ceilings could not hold.
func TestArbiterNeverOverCommitsThePool(t *testing.T) {
	const pool = 64 * 1024 * 1024
	a := storage.NewBudgetArbiter(pool, 0)
	defer a.Close()
	parts := make([]*fakeParticipant, 9)
	names := []string{"matches", "users", "sessions", "scoring", "results", "stats", "identities", "codes", "oauth"}
	for i, n := range names {
		parts[i] = &fakeParticipant{}
		a.Register(n, parts[i])
	}
	// Wildly skewed, the way a real workload is.
	demands := []int64{400 * mib, 20 * mib, 15 * mib, 3 * mib, 8 * mib, 2 * mib, 1 * mib, 0, 0}
	for i, d := range demands {
		parts[i].want(d)
	}
	a.Rebalance()

	var total int64
	for _, share := range a.Shares() {
		total += share
	}
	if total > pool {
		t.Fatalf("shares total %d, over-committing the %d pool", total, int64(pool))
	}

	// The comparison that matters is against the alternative this replaces: a static Nth share,
	// which is what dividing the pool by hand gives you.
	staticNinth := int64(pool) / 9
	hot, _ := parts[0].got()
	if hot <= staticNinth {
		t.Fatalf("the hot namespace got %d, no better than the %d a static ninth would give it",
			hot, staticNinth)
	}
	// And the two namespaces holding nothing still keep a floor each.
	for _, i := range []int{7, 8} {
		if got, _ := parts[i].got(); got != storage.DefaultNamespaceFloorBytes {
			t.Fatalf("idle namespace %s got %d, want the %d floor",
				names[i], got, storage.DefaultNamespaceFloorBytes)
		}
	}

	// Worth seeing rather than only asserting: with the default 64 MiB pool, nine floors claim
	// 36 MiB of it and only 28 MiB is left to arbitrate. That is why DefaultHostMemoryBudgetBytes
	// says a multi-namespace deployment should name a real total rather than inherit a default
	// sized for one namespace.
	t.Logf("hot=%d floor-bound idle=%d static-ninth=%d floors=%d of pool=%d",
		hot, storage.DefaultNamespaceFloorBytes, staticNinth,
		9*storage.DefaultNamespaceFloorBytes, int64(pool))
}

// The pool is a ceiling, and hysteresis must never be allowed to breach it.
//
// This is not hypothetical: nine namespaces registering one at a time left enough suppressed
// decreases to put the installed total 2.8% over the pool, and it was TestNineNamespacesDivideOnePool
// in the embed package that caught it. The property is worth pinning here too, where it can be
// driven much harder than an integration test can.
func TestArbiterNeverExceedsThePoolUnderChurn(t *testing.T) {
	const pool = 256 * 1024 * 1024
	a := storage.NewBudgetArbiter(pool, 0)
	defer a.Close()

	parts := make([]*fakeParticipant, 12)
	for i := range parts {
		parts[i] = &fakeParticipant{}
		a.Register(fmt.Sprintf("ns-%02d", i), parts[i])
		assertWithinPool(t, a, pool, "after registering %d", i+1)
	}

	// A deterministic pseudo-random walk: demands that jump around are exactly what makes some
	// namespaces' changes land inside the hysteresis band while others' land outside.
	seed := uint64(12345)
	next := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return seed
	}
	for round := 0; round < 200; round++ {
		for _, p := range parts {
			p.want(int64(next()%512) * mib)
		}
		a.Rebalance()
		assertWithinPool(t, a, pool, "round %d", round)
	}

	// Namespaces leaving and rejoining mid-churn must not leak share either.
	for i := 0; i < 6; i++ {
		a.Unregister(fmt.Sprintf("ns-%02d", i))
		assertWithinPool(t, a, pool, "after unregistering %d", i)
	}
	for i := 0; i < 6; i++ {
		a.Register(fmt.Sprintf("ns-%02d", i), parts[i])
		assertWithinPool(t, a, pool, "after re-registering %d", i)
	}
}

func assertWithinPool(t *testing.T, a *storage.BudgetArbiter, pool int64, format string, args ...any) {
	t.Helper()
	var total int64
	for _, s := range a.Shares() {
		total += s
	}
	if total > pool {
		t.Fatalf("installed shares total %d, over the %d pool (%s)",
			total, pool, fmt.Sprintf(format, args...))
	}
}
