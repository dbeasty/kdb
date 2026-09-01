package transaction_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// fakeClock is a manually advanced clock, so lease-expiry tests assert on semantics rather than
// on how long a sleep happened to take.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestLeaseExpiryFencesStaleWriter is the central guarantee of the lease design. Session A takes
// a lease and then stalls - a GC pause, a descheduled container, a process that simply died.
// After the lease lapses, B takes the document. When A comes back and presents the lease it
// still believes in, the commit path must refuse it: without the fence check, A's expiry would
// silently hand the document to B while A went on writing over it, which is strictly worse than
// having no locking at all.
func TestLeaseExpiryFencesStaleWriter(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	docID, _ := codec.RandomUUID()

	leaseA, err := locks.TryAcquireLease(uniqueNS, docID, "sess-a", 30*time.Second)
	if err != nil {
		t.Fatalf("A should get the lease: %v", err)
	}

	// While A's lease is live, B cannot take it.
	if _, err := locks.TryAcquireLease(uniqueNS, docID, "sess-b", 30*time.Second); err == nil {
		t.Fatal("B took a document A holds an unexpired lease on")
	}

	// A stalls past its deadline.
	clock.Advance(31 * time.Second)

	leaseB, err := locks.TryAcquireLease(uniqueNS, docID, "sess-b", 30*time.Second)
	if err != nil {
		t.Fatalf("B should get the lease once A's expired: %v", err)
	}
	if leaseB.Fence == leaseA.Fence {
		t.Fatalf("fence token was reused across holders (%d): a stale writer would be indistinguishable from a current one", leaseA.Fence)
	}
	if leaseB.Fence <= leaseA.Fence {
		t.Fatalf("fence went backwards: A=%d B=%d", leaseA.Fence, leaseB.Fence)
	}

	// A wakes up and tries to commit under the lease it still holds a copy of.
	err = locks.ValidateFences([]transaction.Lease{leaseA})
	var locked *kdberr.DocumentLockedError
	if !errors.As(err, &locked) {
		t.Fatalf("expected A's stale lease to be refused, got %v", err)
	}

	// B's lease is still good.
	if err := locks.ValidateFences([]transaction.Lease{leaseB}); err != nil {
		t.Fatalf("B's current lease should validate: %v", err)
	}
}

// TestRenewKeepsFenceAndExtendsDeadline: a renewal must not mint a new token, or the holder's
// own in-flight commit would be fenced off by its own renewal.
func TestRenewKeepsFenceAndExtendsDeadline(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	docID, _ := codec.RandomUUID()

	first, err := locks.TryAcquireLease(uniqueNS, docID, "sess-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Second)
	renewed, err := locks.Renew(uniqueNS, docID, "sess-a", 30*time.Second)
	if err != nil {
		t.Fatalf("renew within the deadline should succeed: %v", err)
	}
	if renewed.Fence != first.Fence {
		t.Fatalf("renew changed the fence (%d -> %d); the holder's own commit would be refused", first.Fence, renewed.Fence)
	}
	if !renewed.ExpiresAt.After(first.ExpiresAt) {
		t.Fatalf("renew did not extend the deadline: %v -> %v", first.ExpiresAt, renewed.ExpiresAt)
	}

	// The original lease value is still valid, because the fence did not move.
	if err := locks.ValidateFences([]transaction.Lease{first}); err != nil {
		t.Fatalf("the pre-renewal lease should still validate: %v", err)
	}
}

// TestRenewAfterExpiryFails: a holder whose lease lapsed must be told, not silently re-granted.
// Re-acquiring under a new token would leave the client believing it had held the document
// continuously, when in fact anything could have happened to it in the gap.
func TestRenewAfterExpiryFails(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	docID, _ := codec.RandomUUID()

	if _, err := locks.TryAcquireLease(uniqueNS, docID, "sess-a", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	if _, err := locks.Renew(uniqueNS, docID, "sess-a", 10*time.Second); err == nil {
		t.Fatal("renewing a lapsed lease silently succeeded")
	}
}

// TestExpiryIsLazyNotSweeperDependent: correctness must not depend on the sweeper having run.
// Every read path treats an expired record as absent on its own.
func TestExpiryIsLazyNotSweeperDependent(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	docID, _ := codec.RandomUUID()

	if _, err := locks.TryAcquireLease(uniqueNS, docID, "sess-a", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Second)

	// No Sweep() call anywhere before this point.
	if _, err := locks.TryAcquireLease(uniqueNS, docID, "sess-b", 5*time.Second); err != nil {
		t.Fatalf("an expired lease blocked a new acquirer without a sweeper run: %v", err)
	}
	if err := locks.AssertHeld(uniqueNS, docID, "sess-a"); err == nil {
		t.Fatal("AssertHeld still reports the expired holder as holding")
	}
}

// TestSweepRemovesOnlyExpired keeps Sweep honest: it is hygiene, so it must never disturb a live
// lease or an indefinite (ttl 0) hold.
func TestSweepRemovesOnlyExpired(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	expiring, _ := codec.RandomUUID()
	live, _ := codec.RandomUUID()
	forever, _ := codec.RandomUUID()

	if _, err := locks.TryAcquireLease(uniqueNS, expiring, "sess-a", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := locks.TryAcquireLease(uniqueNS, live, "sess-a", 60*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := locks.TryAcquire(uniqueNS, forever, "sess-a"); err != nil {
		t.Fatal(err)
	}

	clock.Advance(6 * time.Second)
	if removed := locks.Sweep(); removed != 1 {
		t.Fatalf("expected Sweep to remove exactly the one expired lease, removed %d", removed)
	}
	if locks.HeldCount() != 2 {
		t.Fatalf("expected 2 locks to survive the sweep, got %d", locks.HeldCount())
	}
}

// TestReleaseLeasesLeavesPreexistingHolds is the bug the commit path would otherwise have: a
// transaction that takes implicit locks and then released by session id would also drop the
// leases the client took explicitly and still believes it holds.
func TestReleaseLeasesLeavesPreexistingHolds(t *testing.T) {
	clock := newFakeClock()
	locks := transaction.NewLockManagerWithClock(clock.Now)
	explicit, _ := codec.RandomUUID()
	implicit, _ := codec.RandomUUID()

	clientLease, err := locks.TryAcquireLease(uniqueNS, explicit, "sess-a", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// A commit touching both documents re-acquires the one already held (GrantedNow false) and
	// newly acquires the other (GrantedNow true).
	reAcquired, err := locks.TryAcquireLease(uniqueNS, explicit, "sess-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if reAcquired.GrantedNow {
		t.Fatal("re-acquiring a lock the session already held reported itself as a fresh grant")
	}
	commitLease, err := locks.TryAcquireLease(uniqueNS, implicit, "sess-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !commitLease.GrantedNow {
		t.Fatal("a first-time acquisition did not report itself as a fresh grant")
	}

	locks.ReleaseLeases([]transaction.Lease{reAcquired, commitLease})

	// The commit's own lock is gone...
	if err := locks.TryAcquire(uniqueNS, implicit, "sess-b"); err != nil {
		t.Fatalf("the commit's implicit lock was not released: %v", err)
	}
	// ...but the client's explicit lease survives.
	if _, err := locks.TryAcquireLease(uniqueNS, explicit, "sess-b", time.Second); err == nil {
		t.Fatal("committing dropped the client's explicitly-held lease")
	}
	if err := locks.ValidateFences([]transaction.Lease{clientLease}); err != nil {
		t.Fatalf("the client's lease should still validate after a commit: %v", err)
	}
}
