package dag

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Pin marks hash as in use by something that will still need to resolve it later, and returns
// the release to call when it no longer will. Pins are the DAG's retention roots for readers,
// alongside the two it already had - branch heads and tags - which are the retention roots for
// *writers*. Squash and StubCommit consult all three.
//
// This is the reader-table problem every MVCC store has to solve (LMDB's reader table,
// Postgres's xmin horizon): a reader resolves a version, then does work against it, and nothing
// about holding that hash in a local variable stops a concurrent compaction from reclaiming the
// commit out from under it. Two callers in this codebase need it:
//
//   - A SNAPSHOT session's read pin (server.KdbSession.readPin), held for the length of one
//     client transaction, which can be arbitrarily long.
//   - An in-flight commit's tx.BaseVersion (server.KdbServerRuntime.runTransaction), captured
//     before the writer queues at the write gate and not resolved until it reaches the front.
//     transaction.Engine.Commit hard-fails with BaseNotFoundError if it has gone by then, and
//     up to DefaultMaxQueuedWrites of these are outstanding at once.
//
// Pins are counted, not boolean: the same commit is routinely pinned by several sessions at
// once, and one of them ending must not drop the others' protection. The returned release is
// idempotent, so it is safe to defer and also call explicitly.
//
// A pin is a *refusal* to reclaim, not a reservation: pinning a commit that compaction then
// wants makes compaction fail loudly (CompactionSafetyError) rather than silently corrupt a
// reader. That is the same bargain the existing branch-head check makes.
func (d *InMemoryCommitDag) Pin(hash codec.Hash) (release func()) {
	d.mu.Lock()
	if d.pins == nil {
		d.pins = make(map[codec.Hash]int)
	}
	d.pins[hash]++
	d.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if n, ok := d.pins[hash]; ok {
				if n <= 1 {
					delete(d.pins, hash)
				} else {
					d.pins[hash] = n - 1
				}
			}
		})
	}
}

// IsPinned reports whether hash currently has at least one live pin.
func (d *InMemoryCommitDag) IsPinned(hash codec.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.pins[hash]
	return ok
}

// PinnedCount returns how many distinct commits are pinned right now. For tests and
// observability - a number that only ever grows is a leaked release.
func (d *InMemoryCommitDag) PinnedCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.pins)
}

// assertUnpinnedLocked refuses a reclamation that would strand a live reader. Must be called
// with mu held.
func (d *InMemoryCommitDag) assertUnpinnedLocked(hash codec.Hash, detail string) error {
	if _, ok := d.pins[hash]; !ok {
		return nil
	}
	return NewCompactionSafetyError("commit is pinned by a live reader", d.NamespaceID, hash, detail)
}
