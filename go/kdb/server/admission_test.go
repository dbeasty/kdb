package server

import (
	"context"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

func newTestAdmission(t *testing.T, budget uint64, reserve int64) *Admission {
	t.Helper()
	g := NewMemoryGuard(budget, 0.85)
	g.Stop() // drive it by hand; no background sampler racing the test
	a := NewAdmission(g, reserve, DefaultScanRowBudget)
	if a == nil {
		t.Fatal("expected a configured Admission")
	}
	t.Cleanup(g.Stop)
	return a
}

func TestAdmissionDisabledWithoutBudget(t *testing.T) {
	g := NewMemoryGuard(0, 0.85)
	defer g.Stop()
	if a := NewAdmission(g, DefaultRescueReserveBytes, DefaultScanRowBudget); a != nil {
		t.Fatal("a guard with no budget must not produce an Admission")
	}
}

// A nil *Admission is the "governance disabled" case, and must behave exactly as the server did
// before any of this existed: admit everything, no grants, no panics.
func TestNilAdmissionAdmitsEverything(t *testing.T) {
	var a *Admission
	for _, class := range []OpClass{ClassPointRead, ClassScan, ClassWrite, ClassReplication} {
		grant, err := a.Acquire(context.Background(), class, 1<<30)
		if err != nil {
			t.Fatalf("nil Admission must admit %v, got %v", class, err)
		}
		grant.Release()
		grant.Release() // idempotent
	}
	if a.ScanRowBudget() != 0 || a.OutstandingBytes() != 0 || a.FloorHeldBytes() != 0 || a.ReserveLost() {
		t.Error("nil Admission accessors must all be zero-valued")
	}
}

func TestGrantReservesAndReleasesCapacity(t *testing.T) {
	a := newTestAdmission(t, 1<<30, 0)
	grant, err := a.Acquire(context.Background(), ClassWrite, 1000)
	if err != nil {
		t.Fatal(err)
	}
	want := a.costs.Estimate(ClassWrite, 1000)
	if got := a.OutstandingBytes(); got != want {
		t.Errorf("outstanding after acquire = %d, want %d", got, want)
	}
	if got := grant.CostBytes(); got != want {
		t.Errorf("grant cost = %d, want %d", got, want)
	}
	grant.Release()
	if got := a.OutstandingBytes(); got != 0 {
		t.Errorf("outstanding after release = %d, want 0", got)
	}
}

// Double-release would return capacity the node never took, silently inflating what it believes
// is available - the exact accounting drift that makes an over-admission bug invisible.
func TestGrantReleaseIsIdempotent(t *testing.T) {
	a := newTestAdmission(t, 1<<30, 0)
	grant, err := a.Acquire(context.Background(), ClassWrite, 1000)
	if err != nil {
		t.Fatal(err)
	}
	grant.Release()
	grant.Release()
	grant.Release()
	if got := a.OutstandingBytes(); got != 0 {
		t.Errorf("outstanding after repeated release = %d, want 0", got)
	}
}

// This is the property that separates admission control from a periodic sampler: capacity handed
// to in-flight work is capacity the next caller cannot also be given, with no sampling interval
// involved. A burst arriving between two samples cannot over-commit the node.
func TestGrantsCannotOverCommitCapacity(t *testing.T) {
	// A small budget so a handful of grants exhausts it.
	const budget = 200 << 10
	a := newTestAdmission(t, budget, 0)

	var held []*Grant
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		grant, err := a.Acquire(ctx, ClassWrite, 0)
		cancel()
		if err != nil {
			break
		}
		held = append(held, grant)
		if len(held) > 1000 {
			t.Fatal("capacity was never exhausted - grants are not actually bounded")
		}
	}
	if len(held) == 0 {
		t.Fatal("expected at least one grant to be admitted")
	}
	total := a.OutstandingBytes()
	if total > a.capacity {
		t.Fatalf("admitted %d bytes against a capacity of %d - the semaphore over-committed", total, a.capacity)
	}
	// And capacity must come back: releasing one grant admits exactly one more.
	held[0].Release()
	grant, err := a.Acquire(context.Background(), ClassWrite, 0)
	if err != nil {
		t.Fatalf("expected a freed grant to admit the next caller, got %v", err)
	}
	grant.Release()
	for _, g := range held[1:] {
		g.Release()
	}
}

func TestOversizedOperationIsResourceExhaustedNotBusy(t *testing.T) {
	a := newTestAdmission(t, 64<<10, 0)
	_, err := a.Acquire(context.Background(), ClassWrite, 10<<20) // 10MB payload, 64KB budget
	var exhausted *ResourceExhaustedError
	if !asError(err, &exhausted) {
		t.Fatalf("expected *ResourceExhaustedError, got %T: %v", err, err)
	}
	if exhausted.EstimateBytes <= exhausted.CapacityBytes {
		t.Errorf("error should show estimate exceeding capacity, got %d vs %d",
			exhausted.EstimateBytes, exhausted.CapacityBytes)
	}
	if got := a.stats.DeniedTooLarge[ClassWrite].Load(); got != 1 {
		t.Errorf("expected the denial to be counted, got %d", got)
	}
}

// The governance sim runs scenarios with 15-26MB budgets; the default 48MB reserve, unclamped,
// left those nodes with a 1-byte grant capacity that refused every operation - reads included -
// as too-large, forever, and the reserve's touched pages alone overflowed the container.
func TestReserveClampedToQuarterOfBudget(t *testing.T) {
	const budget = 16 << 20
	a := newTestAdmission(t, budget, DefaultRescueReserveBytes)
	if got, want := a.RescueReserveBytes(), int64(budget)/4; got != want {
		t.Errorf("reserve should clamp to a quarter of the budget, got %d want %d", got, want)
	}
	if a.capacity != budget-budget/4 {
		t.Errorf("capacity should be budget minus the clamped reserve, got %d", a.capacity)
	}
	// The clamp is what keeps ordinary work admissible under a tiny budget.
	grant, err := a.Acquire(context.Background(), ClassWrite, 2000)
	if err != nil {
		t.Fatalf("a 2KB write must be admissible under a 16MB budget, got %v", err)
	}
	grant.Release()
}

// A point read has no smaller form to resubmit, so ResourceExhausted is meaningless for it -
// reads degrade last (P7). An estimate above capacity is clamped to capacity instead.
func TestPointReadNeverRefusedAsTooLarge(t *testing.T) {
	a := newTestAdmission(t, 64<<10, 0)
	grant, err := a.AcquireBytes(context.Background(), ClassPointRead, 10<<20)
	if err != nil {
		t.Fatalf("an oversized point-read estimate must clamp, not refuse, got %v", err)
	}
	if grant.CostBytes() != a.capacity {
		t.Errorf("clamped point read should hold the whole capacity, got %d want %d",
			grant.CostBytes(), a.capacity)
	}
	grant.Release()
	if got := a.stats.DeniedTooLarge[ClassPointRead].Load(); got != 0 {
		t.Errorf("no too-large denial should be counted for point reads, got %d", got)
	}
}

func TestZonePolicyShedsByClass(t *testing.T) {
	a := newTestAdmission(t, 1<<30, 0)
	for _, tc := range []struct {
		zone    Zone
		admits  []OpClass
		refuses []OpClass
	}{
		{ZoneNormal, []OpClass{ClassPointRead, ClassScan, ClassWrite, ClassReplication}, nil},
		{ZoneElevated, []OpClass{ClassPointRead, ClassScan, ClassWrite, ClassReplication}, nil},
		{ZoneHigh, []OpClass{ClassPointRead, ClassReplication}, []OpClass{ClassScan, ClassWrite}},
		{ZoneCritical, []OpClass{ClassPointRead}, []OpClass{ClassScan, ClassWrite, ClassReplication}},
	} {
		for _, c := range tc.admits {
			if !admitInZone(tc.zone, c) {
				t.Errorf("%v must admit %v", tc.zone, c)
			}
		}
		for _, c := range tc.refuses {
			if admitInZone(tc.zone, c) {
				t.Errorf("%v must shed %v", tc.zone, c)
			}
		}
	}
	_ = a
}

func TestShedOperationReportsZoneAndRetryAfter(t *testing.T) {
	a := newTestAdmission(t, 1<<30, 0)
	a.guard.observe(float64(1<<30) * 0.99) // Critical
	_, err := a.Acquire(context.Background(), ClassWrite, 100)
	var pressure *MemoryPressureError
	if !asError(err, &pressure) {
		t.Fatalf("expected *MemoryPressureError, got %T: %v", err, err)
	}
	if pressure.Zone != ZoneCritical {
		t.Errorf("expected ZoneCritical, got %v", pressure.Zone)
	}
	if pressure.RetryAfterMs != retryAfterMsForZone(ZoneCritical) {
		t.Errorf("retry-after = %d, want %d", pressure.RetryAfterMs, retryAfterMsForZone(ZoneCritical))
	}
	// Deeper pressure must ask for a longer backoff: hammering a node in Critical spends the
	// very headroom the abort sequence may be about to need.
	if retryAfterMsForZone(ZoneCritical) <= retryAfterMsForZone(ZoneHigh) {
		t.Error("Critical should ask for a longer backoff than High")
	}
}

// THE DAG-GROWTH THROTTLE. The commit DAG grows monotonically by design - every write is a
// permanent commit and nothing evicts history - so memory the grant system does not govern rises
// over the life of the process. This is what converts that from an eventual OOM into a smooth
// throttle: as the non-granted floor rises, grant capacity shrinks to match, so writes get harder
// to admit rather than all succeeding right up until the kernel intervenes.
func TestNonGrantedFloorShrinksCapacityAsRetainedMemoryGrows(t *testing.T) {
	const budget = 100 << 20
	a := newTestAdmission(t, budget, 0)

	admitCount := func() int {
		var held []*Grant
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			g, err := a.Acquire(ctx, ClassWrite, 0)
			cancel()
			if err != nil {
				break
			}
			held = append(held, g)
			if len(held) > 100000 {
				break
			}
		}
		n := len(held)
		for _, g := range held {
			g.Release()
		}
		return n
	}

	// Almost nothing retained: the floor is near zero and capacity is near the whole budget.
	a.guard.observe(1 << 20)
	empty := admitCount()

	// Now simulate the DAG having grown to hold most of the budget. Nothing about the
	// configuration changed - only measured usage that no grant accounts for.
	a.guard.reset()
	a.guard.observe(float64(budget) * 0.80)
	grown := admitCount()

	if grown >= empty {
		t.Fatalf("grant capacity must shrink as ungoverned retained memory grows: admitted %d with an empty DAG, %d with a grown one", empty, grown)
	}
	if grown == 0 {
		t.Error("the throttle must still admit some work - a node that can admit nothing can never work off its backlog")
	}
	if held := a.FloorHeldBytes(); held <= 0 {
		t.Errorf("expected the floor to hold capacity back, got %d", held)
	}
	t.Logf("admitted %d writes with an empty DAG, %d with 80%% of the budget retained (floor held %d bytes)",
		empty, grown, a.FloorHeldBytes())
}

// The floor must never claim the entire semaphore, or the node stops making progress and can
// never recover on its own - that is the zombie state the whole design is trying to avoid.
func TestFloorAlwaysLeavesRoomForProgress(t *testing.T) {
	const budget = 100 << 20
	a := newTestAdmission(t, budget, 0)
	a.guard.observe(float64(budget) * 10) // absurd over-usage
	if held := a.FloorHeldBytes(); held >= a.capacity {
		t.Fatalf("floor %d must stay below capacity %d", held, a.capacity)
	}
	grant, err := a.Acquire(context.Background(), ClassPointRead, 0)
	if err != nil {
		t.Fatalf("a point read must remain admissible even under extreme floor pressure: %v", err)
	}
	grant.Release()
}

func TestFloorReturnsCapacityWhenUsageSubsides(t *testing.T) {
	const budget = 100 << 20
	a := newTestAdmission(t, budget, 0)
	a.guard.observe(float64(budget) * 0.80)
	high := a.FloorHeldBytes()
	if high <= 0 {
		t.Fatal("expected the floor to take capacity under load")
	}
	a.guard.reset()
	a.guard.observe(1 << 20)
	if low := a.FloorHeldBytes(); low >= high {
		t.Errorf("floor must give capacity back as usage subsides: %d -> %d", high, low)
	}
}

// §5.6: the reserve is dropped on entry to Critical so the abort sequence has headroom to run,
// and taken again once pressure clears.
func TestRescueReserveDropsOnCriticalAndReturnsOnNormal(t *testing.T) {
	const budget = 1 << 30
	const reserve = 8 << 20
	a := newTestAdmission(t, budget, reserve)
	if got := a.reserve.heldBytes(); got != reserve {
		t.Fatalf("reserve should be held at startup, got %d want %d", got, reserve)
	}

	a.guard.observe(float64(budget) * 0.99) // Critical
	if got := a.reserve.heldBytes(); got != 0 {
		t.Errorf("reserve must be released on entering Critical, still holding %d", got)
	}
	if got := a.stats.ReserveDrops.Load(); got != 1 {
		t.Errorf("reserve drop should be counted once, got %d", got)
	}

	// Back to Normal - escalation is immediate but recovery waits out the dwell.
	now := time.Now()
	a.guard.now = func() time.Time { return now }
	a.guard.reset()
	a.guard.observe(1 << 20)
	now = now.Add(zoneDwell + time.Millisecond)
	a.guard.observe(1 << 20)
	if a.guard.CurrentZone() != ZoneNormal {
		t.Fatalf("expected recovery to ZoneNormal, got %v", a.guard.CurrentZone())
	}
	if got := a.reserve.heldBytes(); got != reserve {
		t.Errorf("reserve must be re-taken on returning to Normal, got %d want %d", got, reserve)
	}
	if a.ReserveLost() {
		t.Error("reserve was re-allocated successfully, so ReserveLost must be false")
	}
}

func TestScanRowBudgetShrinksWithPressure(t *testing.T) {
	const budget = 1 << 30
	a := newTestAdmission(t, budget, 0)
	if got := a.ScanRowBudget(); got != DefaultScanRowBudget {
		t.Fatalf("expected the full row budget at rest, got %d", got)
	}
	for _, tc := range []struct {
		fraction float64
		want     int64
	}{
		{0.75, DefaultScanRowBudget / 2}, // Elevated
		{0.90, DefaultScanRowBudget / 4}, // High
		{0.99, DefaultScanRowBudget / 8}, // Critical
	} {
		a.guard.reset()
		a.guard.observe(float64(budget) * tc.fraction)
		if got := a.ScanRowBudget(); got != tc.want {
			t.Errorf("at %.0f%%: row budget = %d, want %d", tc.fraction*100, got, tc.want)
		}
	}
}

// Grants are taken and released from many goroutines at once; the accounting must not drift.
func TestConcurrentGrantsKeepAccountingExact(t *testing.T) {
	a := newTestAdmission(t, 1<<30, 0)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				g, err := a.Acquire(context.Background(), ClassPointRead, 64)
				if err != nil {
					continue
				}
				g.Release()
			}
		}()
	}
	wg.Wait()
	if got := a.OutstandingBytes(); got != 0 {
		t.Errorf("outstanding bytes must return to 0 after all grants release, got %d", got)
	}
}

func TestApplyGoMemoryLimit(t *testing.T) {
	previous := debug.SetMemoryLimit(-1) // read without changing
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	if got := ApplyGoMemoryLimit(0); got != 0 {
		t.Errorf("a zero budget must not set a limit, got %d", got)
	}
	var budget uint64 = 512 << 20
	got := ApplyGoMemoryLimit(budget)
	want := int64(float64(budget) * GoMemLimitFraction)
	if got != want {
		t.Errorf("applied limit = %d, want %d", got, want)
	}
	if actual := debug.SetMemoryLimit(-1); actual != want {
		t.Errorf("runtime soft limit = %d, want %d", actual, want)
	}
	// Ordering of the escalation ladder: shed writes first (ZoneHigh, 85%), make the GC work
	// harder second (here, 90%), drop the reserve and start the abort timer last (ZoneCritical,
	// 93%). Refusing work is cheap and reversible; burning CPU on collection degrades every
	// request, so it must not come first.
	if GoMemLimitFraction <= 0.85 {
		t.Errorf("GOMEMLIMIT fraction %v must sit above the ZoneHigh trip point of 0.85", GoMemLimitFraction)
	}
	if GoMemLimitFraction >= 93.0/85.0*0.85 {
		t.Errorf("GOMEMLIMIT fraction %v must sit below the ZoneCritical trip point of 0.93", GoMemLimitFraction)
	}
}
