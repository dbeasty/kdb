package dag

import (
	"container/list"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// CommitOperationsLoader re-reads one commit's operations from wherever
// they are durable - for a file-backed namespace, the delta log that
// recorded the commit in the first place. See SetOperationsLoader.
type CommitOperationsLoader func(hash codec.Hash) ([]document.Op, error)

// SetOperationsLoader bounds how many bytes of commit *operations* this
// DAG keeps resident, and installs the loader that re-reads the ones it
// drops. Both together or neither: dropping operations with nothing able
// to fetch them back would silently turn history into empty commits, and a
// loader with no budget would never be consulted.
//
// Why this exists. A commit's operations carry the whole text of every
// document it wrote, and the DAG held them for every commit forever. That
// made a namespace's resident memory the arithmetic sum of every version
// of every document it had ever stored, exactly as the storage engine's
// version store did (see storage/engine.shardedDocByHashStore, which holds
// the same bytes and had the same problem). Both had to be bounded before
// either mattered: with only one fixed, the other still pinned every
// version, and the measured total did not move at all. With both, one
// 1.4MB document rewritten 463 times went from 329MB resident to 10.75MB -
// from the sum of its history to something close to its live size
// (docs/benchmarks/open-cost.md).
//
// budgetBytes <= 0, or a nil loader, restores unbounded retention.
func (d *InMemoryCommitDag) SetOperationsLoader(loader CommitOperationsLoader, budgetBytes int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if loader == nil || budgetBytes <= 0 {
		d.opsLoader.Store(nil)
		d.opsBudget = 0
		return
	}
	d.opsLoader.Store(&loader)
	d.opsBudget = budgetBytes
	d.evictOpsLocked(codec.Hash{})
}

// commitOpsBytes approximates what one commit's operations cost resident.
// Only WriteOp carries bulk; every other operation is a handful of fixed
// fields, counted through the flat per-operation constant.
func commitOpsBytes(c document.Commit) int64 {
	var total int64
	for _, op := range c.Operations {
		total += 48
		if w, ok := op.(document.WriteOp); ok {
			total += int64(len(w.Patch))
		}
	}
	return total
}

// trackOpsLocked records a newly stored commit as holding resident
// operations, and evicts down to budget. Must be called with mu held
// exclusively, from the one place a commit enters d.commits.
func (d *InMemoryCommitDag) trackOpsLocked(c document.Commit) {
	if d.opsBudget <= 0 || len(c.Operations) == 0 {
		return
	}
	// Initialized individually, never as a group: opsEvicted can already
	// hold entries while opsElem is still nil - that is exactly the state
	// RestoreCheckpoint leaves behind, having marked every restored commit
	// as needing a load without any of them holding operations to track.
	// Creating all three together on the first tracked commit wiped those
	// markings, and the whole restored history then read as commits that
	// wrote nothing.
	if d.opsElem == nil {
		d.opsElem = make(map[codec.Hash]*list.Element)
	}
	if d.opsEvicted == nil {
		d.opsEvicted = make(map[codec.Hash]struct{})
	}
	if d.opsLRU == nil {
		d.opsLRU = list.New()
	}
	if _, ok := d.opsElem[c.Hash]; ok {
		return
	}
	d.opsElem[c.Hash] = d.opsLRU.PushFront(c.Hash)
	d.opsResident += commitOpsBytes(c)
	// The commit being inserted is protected from this eviction pass. It is
	// not a branch head yet - appendCommitLocked advances the branch only
	// after putCommitLocked returns - so nothing else would keep it, and a
	// commit whose own operations exceed the budget would evict itself on
	// the way in. publishHeadLocked then snapshots it as head with no
	// operations, and the lock-free read path and the stream notifier hand
	// that out as a commit that wrote nothing.
	d.evictOpsLocked(c.Hash)
}

// opsPinnedLocked reports commits whose operations must stay resident
// whatever the budget says: branch heads and tags, which are the DAG's
// retention roots for writers, and reader pins (see Pin). These are the
// same three roots Squash and StubCommit consult, for the same reason -
// something live is still resolving against them, and the head snapshot in
// particular is copied out on the lock-free read path where there is no
// opportunity to fetch anything back.
func (d *InMemoryCommitDag) opsPinnedLocked(hash codec.Hash) bool {
	if d.pins[hash] > 0 {
		return true
	}
	for _, b := range d.branches {
		if b.HeadHash == hash {
			return true
		}
	}
	for _, t := range d.tags {
		if t.CommitHash == hash {
			return true
		}
	}
	return false
}

// evictOpsLocked drops operations from least-recently-stored commits until
// resident bytes are within budget. The commit itself stays - it is small,
// and ancestry, timestamps and tree hashes must keep answering from memory
// - only its operations go, and only where something can fetch them back.
//
// protect is a commit this pass must not touch whatever the budget says;
// pass the zero hash when there is none. See trackOpsLocked for the case
// it exists for. Overshooting the budget because everything in range is
// protected or pinned is deliberate - correctness first.
func (d *InMemoryCommitDag) evictOpsLocked(protect codec.Hash) {
	if d.opsBudget <= 0 || d.opsLRU == nil {
		return
	}
	el := d.opsLRU.Back()
	for d.opsResident > d.opsBudget && el != nil {
		prev := el.Prev()
		h := el.Value.(codec.Hash)
		if h != protect && !d.opsPinnedLocked(h) {
			if c, ok := d.commits[h]; ok {
				d.opsResident -= commitOpsBytes(c)
				c.Operations = nil
				d.commits[h] = c
				d.opsEvicted[h] = struct{}{}
			}
			d.opsLRU.Remove(el)
			delete(d.opsElem, h)
		}
		el = prev
	}
}

// opsEvictedFor reports whether hash's operations have been dropped and
// need loading before the commit can be handed to anything that reads
// them.
func (d *InMemoryCommitDag) opsEvictedFor(hash codec.Hash) bool {
	if d.opsEvicted == nil {
		return false
	}
	_, ok := d.opsEvicted[hash]
	return ok
}

// hydrate returns c with its operations loaded back. Deliberately does not
// re-admit them into d.commits: a history walk would otherwise pull the
// whole range back into memory, which is the thing the budget exists to
// prevent, and the caller already holds what it asked for.
//
// The loaded operations are checked against the commit's own hash before
// being returned. A commit hash covers its operations, so this proves the
// bytes fetched are the ones the commit was built from - worth a SHA-256
// on a path that is already doing I/O, and the difference between a
// corrupt or mismatched log surfacing here and it surfacing as a silently
// wrong merge.
func (d *InMemoryCommitDag) hydrate(c document.Commit) (document.Commit, error) {
	loader := d.opsLoader.Load()
	if loader == nil {
		return document.Commit{}, kdberr.NewVersionNotFoundError(
			"commit operations were evicted and no loader is installed to read them back",
			d.NamespaceID, c.Hash.Hex())
	}
	ops, err := (*loader)(c.Hash)
	if err != nil {
		return document.Commit{}, err
	}
	c.Operations = ops
	recomputed, err := document.ComputeCommitHash(c)
	if err != nil {
		return document.Commit{}, err
	}
	if recomputed != c.Hash {
		return document.Commit{}, NewConsistencyError(
			"operations loaded for this commit do not reproduce its hash", d.NamespaceID, &c.Hash)
	}
	return c, nil
}

// CommitOperations returns hash's operations, loading them back if the
// budget has evicted them. Prefer this over reading Operations off a
// commit obtained from GetCommit or Walk, neither of which can report the
// load failing.
func (d *InMemoryCommitDag) CommitOperations(hash codec.Hash) ([]document.Op, error) {
	c, err := d.GetCommitOrThrow(hash)
	if err != nil {
		return nil, err
	}
	return c.Operations, nil
}

// WalkWithOperations is Walk with every returned commit guaranteed to
// carry its operations, loading back any the budget evicted.
//
// Walk itself cannot do this: it has no error return, so a load that
// failed could only be reported by handing back a commit that looks like
// it wrote nothing - and callers that read operations (peer-sync
// divergence resolution, merge replay) treat an empty operation list as
// "this commit changed nothing" rather than as an error. Any caller that
// reads Operations off the result must use this instead.
func (d *InMemoryCommitDag) WalkWithOperations(from codec.Hash, until *codec.Hash, limit int) ([]TraversalEntry, error) {
	entries := d.Walk(from, until, limit)
	for i, e := range entries {
		full, ok := e.(FullEntry)
		if !ok {
			continue
		}
		d.mu.RLock()
		evicted := d.opsEvictedFor(full.Commit.Hash)
		d.mu.RUnlock()
		if !evicted {
			continue
		}
		hydrated, err := d.hydrate(full.Commit)
		if err != nil {
			return nil, err
		}
		entries[i] = FullEntry{Commit: hydrated}
	}
	return entries, nil
}

// SetOperationsBudget re-cuts how many bytes of commit operations this DAG may keep resident,
// leaving the loader alone. The resize half of SetOperationsLoader, for a BudgetArbiter re-cutting
// a namespace's share while it is running.
//
// A no-op unless a loader is already installed. That is the same rule SetOperationsLoader states
// - both together or neither - and it matters more here, because this is called on a timer by
// something that has no way to know whether the namespace behind it has a delta log to re-read
// from. Bounding a DAG that cannot fetch operations back would silently turn history into
// commits that wrote nothing.
//
// Safe to call while the DAG is being read and written; it evicts under the same lock every
// other retention path takes.
func (d *InMemoryCommitDag) SetOperationsBudget(budgetBytes int64) {
	if d == nil || budgetBytes <= 0 || d.opsLoader.Load() == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opsBudget = budgetBytes
	d.evictOpsLocked(codec.Hash{})
}

// OperationsResidentBytes is what this DAG's tracked commit operations currently cost. For the
// budget arbiter's demand signal, and for tests; not on any hot path.
func (d *InMemoryCommitDag) OperationsResidentBytes() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opsResident
}
