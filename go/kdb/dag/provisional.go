package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// AppendOptions selects how AppendCommitWith appends.
type AppendOptions struct {
	// Detached skips the head compare-and-swap - see AppendCommitDetached.
	Detached bool
	// Provisional records the commit as one part of a cross-namespace group whose outcome is
	// not yet durable. See SettleProvisional.
	Provisional bool
}

// AppendCommitWith is AppendCommit (or AppendCommitDetached) with options.
//
// Marking a commit provisional has to happen under the same lock that publishes it, not after:
// a checkpoint snapshot taken in between would see the commit as an ordinary settled one, write
// it into the checkpoint, and a crash before the group is decided would then restore a commit
// that recovery is obliged to roll back.
func (d *InMemoryCommitDag) AppendCommitWith(
	tx document.Transaction,
	parentHash codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
	opts AppendOptions,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var expected *codec.Hash
	if !opts.Detached {
		expected = &parentHash
	}
	c, err := d.appendCommitLocked(tx, []codec.Hash{parentHash}, newDocumentTree, schemaHash, message, mainBranch, expected)
	if err != nil {
		return c, err
	}
	if opts.Provisional {
		if d.provisional == nil {
			d.provisional = make(map[codec.Hash]struct{})
		}
		d.provisional[c.Hash] = struct{}{}
		d.provisionalGen++
	}
	return c, nil
}

// SettleProvisional records that a provisional commit's group has been durably decided, so the
// commit is now as permanent as any other. Settling a commit that is not provisional is a no-op.
//
// A group that fails never settles its parts, on purpose: the namespace's in-memory state then
// includes a commit that recovery will roll back, and keeping the part provisional is what stops
// a checkpoint from persisting that state before the namespace is reopened.
func (d *InMemoryCommitDag) SettleProvisional(hash codec.Hash) {
	d.mu.Lock()
	delete(d.provisional, hash)
	d.mu.Unlock()
}

// HasProvisional reports whether any commit in this DAG belongs to a group that has not been
// decided yet.
func (d *InMemoryCommitDag) HasProvisional() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.provisional) > 0
}

// ProvisionalGeneration counts provisional commits ever appended. Two equal readings with
// HasProvisional false at both ends mean no provisional commit was published in between - which
// is what a checkpoint needs to know about the window between its graph snapshot and its
// live-tree capture, two reads that are not taken under one lock.
func (d *InMemoryCommitDag) ProvisionalGeneration() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.provisionalGen
}
