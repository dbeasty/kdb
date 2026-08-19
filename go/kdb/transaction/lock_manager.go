package transaction

import (
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

type lockKey struct {
	namespaceID string
	docID       codec.UUID
}

// LockManager holds an exclusive write lock per (namespaceID, docID), held by a session
// until released.
type LockManager struct {
	mu    sync.Mutex
	locks map[lockKey]string
}

// NewLockManager returns an empty lock manager.
func NewLockManager() *LockManager {
	return &LockManager{locks: make(map[lockKey]string)}
}

// TryAcquire locks (namespaceID, docID) for sessionID, or returns a *kdberr.DocumentLockedError
// if another session already holds it. Re-acquiring a lock already held by sessionID is a no-op.
func (m *LockManager) TryAcquire(namespaceID string, docID codec.UUID, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := lockKey{namespaceID, docID}
	owner, held := m.locks[key]
	switch {
	case !held:
		m.locks[key] = sessionID
		return nil
	case owner == sessionID:
		return nil
	default:
		return kdberr.NewDocumentLockedError(
			"document "+docID.String()+" is locked by session "+owner,
			namespaceID, docID.String(), owner,
		)
	}
}

// Release drops the lock on (namespaceID, docID) if held by sessionID.
func (m *LockManager) Release(namespaceID string, docID codec.UUID, sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := lockKey{namespaceID, docID}
	if m.locks[key] == sessionID {
		delete(m.locks, key)
	}
}

// ReleaseAll drops every lock held by sessionID.
func (m *LockManager) ReleaseAll(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, owner := range m.locks {
		if owner == sessionID {
			delete(m.locks, key)
		}
	}
}

// AssertHeld returns a *kdberr.DocumentLockedError if sessionID does not hold the lock on
// (namespaceID, docID).
func (m *LockManager) AssertHeld(namespaceID string, docID codec.UUID, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner := m.locks[lockKey{namespaceID, docID}]
	if owner != sessionID {
		return kdberr.NewDocumentLockedError(
			"session "+sessionID+" does not hold lock on document "+docID.String(),
			namespaceID, docID.String(), owner,
		)
	}
	return nil
}

// AcquireAllForTransaction locks every document referenced by tx for sessionID, in
// deterministic (sorted) order. If any document is held by another session, any locks newly
// granted by this call are released before the error is returned — locks sessionID already
// held coming in (e.g. from prior per-statement acquisition) are left untouched.
func (m *LockManager) AcquireAllForTransaction(namespaceID string, sessionID string, tx document.Transaction) error {
	ids := DocumentIDsIn(tx.Operations)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	var newlyAcquired []codec.UUID
	for _, docID := range ids {
		key := lockKey{namespaceID, docID}
		grantedNow, err := func() (bool, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			owner, held := m.locks[key]
			switch {
			case !held:
				m.locks[key] = sessionID
				return true, nil
			case owner == sessionID:
				return false, nil
			default:
				return false, kdberr.NewDocumentLockedError(
					"document "+docID.String()+" is locked by session "+owner,
					namespaceID, docID.String(), owner,
				)
			}
		}()
		if err != nil {
			for _, id := range newlyAcquired {
				m.Release(namespaceID, id, sessionID)
			}
			return err
		}
		if grantedNow {
			newlyAcquired = append(newlyAcquired, docID)
		}
	}
	return nil
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
