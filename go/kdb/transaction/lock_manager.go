package transaction

import (
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

type lockKey struct {
	namespaceID string
	docID       codec.UUID
}

// lockRecord is one held lock. A zero expiresAt means the lock never expires on its own - the
// pre-lease behavior, still used for the implicit locks a commit takes and drops within one
// call, where there is no round trip during which a holder could die.
type lockRecord struct {
	sessionID string
	fence     uint64
	expiresAt time.Time
}

// Lease is proof of a held lock at a point in time. Fence is the part that matters: it is
// monotonic per document, so a holder that stalls past its expiry and comes back to commit
// presents a token the manager has already moved past, and the commit is refused. Without that,
// a lease is only a hint - the expiry frees the document for someone else while the original
// holder still believes it owns it, which is a worse race than having no locking at all.
type Lease struct {
	NamespaceID string
	DocID       codec.UUID
	SessionID   string
	Fence       uint64
	ExpiresAt   time.Time
	// GrantedNow distinguishes a lock this call created from one the session already held.
	// Release paths use it so that committing a transaction does not drop a lease the client
	// took explicitly and still expects to hold.
	GrantedNow bool
}

// LockManager holds an exclusive write lock per (namespaceID, docID).
//
// A lock is held by a session either indefinitely (TryAcquire, AcquireAllForTransaction - for
// server-internal holds that begin and end inside one call) or under a lease with a deadline
// (TryAcquireLease - for holds that span client round trips, where the holder may simply never
// come back). Expiry is evaluated lazily on every lookup, so correctness never depends on the
// sweeper having run; Sweep exists only to keep the map from retaining dead entries.
type LockManager struct {
	mu    sync.Mutex
	locks map[lockKey]lockRecord
	// fences is the monotonic grant counter per document. It outlives the lock itself: a key
	// that is released and re-acquired must never reissue a token a previous holder still has.
	fences map[lockKey]uint64
	// now is the clock, injectable so lease-expiry tests are deterministic rather than timing
	// -dependent. Production uses time.Now.
	now func() time.Time
}

// NewLockManager returns an empty lock manager on the wall clock.
func NewLockManager() *LockManager {
	return NewLockManagerWithClock(time.Now)
}

// NewLockManagerWithClock returns an empty lock manager reading time from now. Tests drive lease
// expiry through this rather than by sleeping.
func NewLockManagerWithClock(now func() time.Time) *LockManager {
	if now == nil {
		now = time.Now
	}
	return &LockManager{
		locks:  make(map[lockKey]lockRecord),
		fences: make(map[lockKey]uint64),
		now:    now,
	}
}

// liveHolderLocked returns the current unexpired holder of key, evicting an expired record as a
// side effect. Callers must hold m.mu.
func (m *LockManager) liveHolderLocked(key lockKey) (lockRecord, bool) {
	rec, held := m.locks[key]
	if !held {
		return lockRecord{}, false
	}
	if !rec.expiresAt.IsZero() && !m.now().Before(rec.expiresAt) {
		delete(m.locks, key)
		return lockRecord{}, false
	}
	return rec, true
}

// grantLocked records sessionID as the holder of key, minting a new fence token only when the
// key is changing hands. A holder re-acquiring or renewing its own lock keeps its token -
// otherwise renewing would invalidate the very fence the holder is about to present at commit.
func (m *LockManager) grantLocked(key lockKey, sessionID string, ttl time.Duration, existing lockRecord, wasHeld bool) Lease {
	fence := existing.fence
	if !wasHeld || existing.sessionID != sessionID {
		m.fences[key]++
		fence = m.fences[key]
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = m.now().Add(ttl)
	}
	m.locks[key] = lockRecord{sessionID: sessionID, fence: fence, expiresAt: expiresAt}
	return Lease{
		NamespaceID: key.namespaceID,
		DocID:       key.docID,
		SessionID:   sessionID,
		Fence:       fence,
		ExpiresAt:   expiresAt,
		GrantedNow:  !wasHeld || existing.sessionID != sessionID,
	}
}

// TryAcquire locks (namespaceID, docID) for sessionID with no expiry, or returns a
// *kdberr.DocumentLockedError if another session already holds it. Re-acquiring a lock already
// held by sessionID is a no-op.
func (m *LockManager) TryAcquire(namespaceID string, docID codec.UUID, sessionID string) error {
	_, err := m.TryAcquireLease(namespaceID, docID, sessionID, 0)
	return err
}

// TryAcquireLease locks (namespaceID, docID) for sessionID until ttl elapses. A ttl of zero
// means no expiry. Returns a *kdberr.DocumentLockedError if a different session holds an
// unexpired lock.
func (m *LockManager) TryAcquireLease(namespaceID string, docID codec.UUID, sessionID string, ttl time.Duration) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := lockKey{namespaceID, docID}
	rec, held := m.liveHolderLocked(key)
	if held && rec.sessionID != sessionID {
		return Lease{}, kdberr.NewDocumentLockedError(
			"document "+docID.String()+" is locked by session "+rec.sessionID,
			namespaceID, docID.String(), rec.sessionID,
		)
	}
	return m.grantLocked(key, sessionID, ttl, rec, held), nil
}

// Renew extends sessionID's lease on (namespaceID, docID) by ttl from now, keeping its fence
// token. Returns *kdberr.DocumentLockedError if the session no longer holds the lock - which is
// exactly what a holder whose lease already lapsed and was taken by someone else must be told,
// rather than silently re-acquiring under a new token it does not know about.
func (m *LockManager) Renew(namespaceID string, docID codec.UUID, sessionID string, ttl time.Duration) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := lockKey{namespaceID, docID}
	rec, held := m.liveHolderLocked(key)
	if !held || rec.sessionID != sessionID {
		return Lease{}, kdberr.NewDocumentLockedError(
			"session "+sessionID+" no longer holds a lease on document "+docID.String(),
			namespaceID, docID.String(), rec.sessionID,
		)
	}
	return m.grantLocked(key, sessionID, ttl, rec, true), nil
}

// Release drops the lock on (namespaceID, docID) if held by sessionID.
func (m *LockManager) Release(namespaceID string, docID codec.UUID, sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := lockKey{namespaceID, docID}
	if rec, held := m.locks[key]; held && rec.sessionID == sessionID {
		delete(m.locks, key)
	}
}

// ReleaseAll drops every lock held by sessionID. Called when a session ends - including when its
// connection simply drops, which is the case a lease-free lock manager can never recover from.
func (m *LockManager) ReleaseAll(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, rec := range m.locks {
		if rec.sessionID == sessionID {
			delete(m.locks, key)
		}
	}
}

// ReleaseLeases drops only the locks in leases that this call originally granted (GrantedNow),
// leaving alone anything the session held before. This is what a commit uses: releasing by
// session id instead would silently drop leases the client took explicitly through LockAcquire
// and still believes it holds.
func (m *LockManager) ReleaseLeases(leases []Lease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range leases {
		if !l.GrantedNow {
			continue
		}
		key := lockKey{l.NamespaceID, l.DocID}
		if rec, held := m.locks[key]; held && rec.sessionID == l.SessionID && rec.fence == l.Fence {
			delete(m.locks, key)
		}
	}
}

// AssertHeld returns a *kdberr.DocumentLockedError if sessionID does not hold an unexpired lock
// on (namespaceID, docID).
func (m *LockManager) AssertHeld(namespaceID string, docID codec.UUID, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, held := m.liveHolderLocked(lockKey{namespaceID, docID})
	if !held || rec.sessionID != sessionID {
		return kdberr.NewDocumentLockedError(
			"session "+sessionID+" does not hold lock on document "+docID.String(),
			namespaceID, docID.String(), rec.sessionID,
		)
	}
	return nil
}

// ValidateFences confirms every lease is still current: same session, same fence token, not
// expired. This is the check that makes leases safe. A holder that paused past its deadline -
// GC, a stalled syscall, a descheduled container - will find its document already reassigned,
// and must be refused here rather than allowed to land a write over whoever took it.
func (m *LockManager) ValidateFences(leases []Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range leases {
		key := lockKey{l.NamespaceID, l.DocID}
		rec, held := m.liveHolderLocked(key)
		if !held || rec.sessionID != l.SessionID || rec.fence != l.Fence {
			return kdberr.NewDocumentLockedError(
				"lease on document "+l.DocID.String()+" is no longer valid for session "+l.SessionID,
				l.NamespaceID, l.DocID.String(), rec.sessionID,
			)
		}
	}
	return nil
}

// Sweep drops expired records and reports how many it removed. Purely hygiene - every read path
// already treats an expired record as absent - so it can run on any cadence, or never.
func (m *LockManager) Sweep() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	removed := 0
	for key, rec := range m.locks {
		if !rec.expiresAt.IsZero() && !now.Before(rec.expiresAt) {
			delete(m.locks, key)
			removed++
		}
	}
	return removed
}

// HeldCount reports how many unexpired locks exist (tests and metrics).
func (m *LockManager) HeldCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	n := 0
	for _, rec := range m.locks {
		if rec.expiresAt.IsZero() || now.Before(rec.expiresAt) {
			n++
		}
	}
	return n
}

// AssertUnheldByOthers returns a *kdberr.DocumentLockedError if any document in tx is held under
// an unexpired lock by a session other than sessionID.
//
// This replaces the commit path's old take-every-lock-then-release-it dance, which was actively
// harmful: writes into one runtime are already serialized by the server's write gate, so the
// locks bought no exclusion the gate did not already provide - but taking them fail-fast meant
// that while one writer sat in the gate holding locks, every other writer to the same document
// was refused outright instead of simply waiting its turn. Under real contention (several
// clients doing compare-and-swap on one document) that turned an ordinary queue into a storm of
// spurious failures no amount of retrying could clear.
//
// What the locks were genuinely for survives here: a document a *client* holds an explicit lease
// on must not be written by anyone else. That is a real exclusion the write gate cannot express,
// and it is the only one this check enforces.
func (m *LockManager) AssertUnheldByOthers(namespaceID string, sessionID string, tx document.Transaction) error {
	ids := DocumentIDsIn(tx.Operations)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, docID := range ids {
		rec, held := m.liveHolderLocked(lockKey{namespaceID, docID})
		if held && rec.sessionID != sessionID {
			return kdberr.NewDocumentLockedError(
				"document "+docID.String()+" is locked by session "+rec.sessionID,
				namespaceID, docID.String(), rec.sessionID,
			)
		}
	}
	return nil
}

// AcquireAllForTransaction locks every document referenced by tx for sessionID, in deterministic
// (sorted) order, and returns the resulting leases. ttl of zero means the locks do not expire,
// which is right for the commit path: the locks are taken and released inside one call, with no
// round trip during which the holder could vanish.
//
// If any document is held by another session, any locks newly granted by this call are released
// before the error is returned - locks sessionID already held coming in (e.g. a client's
// explicit lease) are left untouched.
func (m *LockManager) AcquireAllForTransaction(
	namespaceID string, sessionID string, tx document.Transaction, ttl time.Duration,
) ([]Lease, error) {
	ids := DocumentIDsIn(tx.Operations)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	leases := make([]Lease, 0, len(ids))
	for _, docID := range ids {
		lease, err := m.TryAcquireLease(namespaceID, docID, sessionID, ttl)
		if err != nil {
			m.ReleaseLeases(leases)
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, nil
}

// DocumentIDsIn returns the distinct document ids referenced by write/delete operations.
func DocumentIDsIn(operations []document.Op) []codec.UUID {
	seen := make(map[codec.UUID]struct{})
	var out []codec.UUID
	for _, op := range operations {
		var docID codec.UUID
		switch o := op.(type) {
		case document.WriteOp:
			docID = o.DocID
		case document.DeleteOp:
			docID = o.DocID
		default:
			continue
		}
		if _, ok := seen[docID]; ok {
			continue
		}
		seen[docID] = struct{}{}
		out = append(out, docID)
	}
	return out
}
