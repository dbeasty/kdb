package peersync

import (
	"errors"
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Snapshot bootstrap: a node that is empty, or that a peer's history cannot reach commit by
// commit (the peer retains no commits below a floor), installs the peer's state at one commit
// wholesale - every live document at that commit - and admits the commit as a shallow root. From
// there it syncs commit by commit like any other peer.
//
// Nothing is trusted: the installed documents must build exactly the tree the commit declares,
// and the commit must hash to its own hash. A tampered body fails the first check.

// ErrNotEmpty refuses installing a snapshot over a namespace that already has history of its
// own: a snapshot replaces state, and replacing a namespace's history is a decision to make on
// purpose, not a side effect of syncing.
var ErrNotEmpty = errors.New("peer sync: a snapshot can only be installed into an empty namespace")

// snapshotPage builds one page of the snapshot of env's namespace at commit at: documents with
// ids after the cursor, in id order, up to maxBytes of bodies (at least one).
func snapshotPage(env IngestEnv, at codec.Hash, after string, maxBytes int) (wire.SnapshotPageMessage, error) {
	commit, err := env.DAG.GetCommitOrThrow(at)
	if err != nil {
		return wire.SnapshotPageMessage{}, err
	}
	walker, ok := env.Storage.(storage.TreeWalker)
	if !ok {
		return wire.SnapshotPageMessage{}, fmt.Errorf("peer sync: storage for %s cannot serve snapshots", env.NamespaceID)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultPageBytes
	}
	// Ids after the cursor, sorted: a tree's walk order is only incidentally sorted, and the
	// cursor has to mean the same thing on every page.
	total := 0
	var ids []codec.UUID
	if err := walker.WalkTree(env.NamespaceID, commit.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		total++
		if id.String() > after {
			ids = append(ids, id)
		}
		return true
	}); err != nil {
		return wire.SnapshotPageMessage{}, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	page := wire.SnapshotPageMessage{Namespace: env.NamespaceID, Commit: commit, Total: total, Done: true}
	size := 0
	const chunk = 128
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		docs, err := env.Storage.GetDocuments(env.NamespaceID, ids[start:end], commit.DocumentTreeHash)
		if err != nil {
			return wire.SnapshotPageMessage{}, err
		}
		for i, d := range docs {
			if d == nil {
				return wire.SnapshotPageMessage{}, fmt.Errorf("peer sync: document %s of tree %s is not readable", ids[start+i], commit.DocumentTreeHash.Hex())
			}
			if len(page.Docs) > 0 && size+len(d.JSON) > maxBytes {
				page.Done = false
				page.Next = page.Docs[len(page.Docs)-1].DocID
				return page, nil
			}
			page.Docs = append(page.Docs, wire.SnapshotDoc{DocID: d.ID.String(), Body: d.JSON})
			size += len(d.JSON)
		}
	}
	return page, nil
}

// InstallSnapshot fetches a peer's snapshot page by page and installs it as this namespace's
// state: documents into storage, verified against the commit's declared tree, the commit
// admitted as a shallow root, main pointed at it. The namespace must be empty (main at genesis).
//
// Runs under the node's serialization and calls its Advanced hook, so indexes, unique keys and
// listeners see the installed documents; SnapshotInstalled, when set, is what makes the result
// survive a restart, since no replayable log produced it.
func InstallSnapshot(env IngestEnv, fetch func(after string) (wire.SnapshotPageMessage, error)) (document.Commit, error) {
	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	var installed document.Commit
	err := node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		head, headCommit, ok, err := env.DAG.HeadCommit()
		if err != nil {
			return err
		}
		if !ok || len(headCommit.ParentHashes) != 0 {
			return ErrNotEmpty
		}
		if env.CanInstallSnapshot != nil {
			if err := env.CanInstallSnapshot(); err != nil {
				return err
			}
		}
		tree := headCommit.DocumentTreeHash
		var target document.Commit
		var written []codec.UUID
		var applied []document.Op
		// admitted records that the root is in the DAG, so undo knows whether it has a commit
		// to take back as well as documents. Everything after PutShallowCommit can still fail.
		//
		// The DAG is undone before storage, and a failure there abandons the undo rather than
		// carrying on: documents deleted out from under a head that is still pointing at them
		// would be worse than the failure being reported.
		admitted := false
		undo := func(cause error) error {
			if admitted {
				// Main first: DropShallowCommit refuses a commit a branch head names, and this
				// is also what stops later commits being logged on top of state that nothing
				// durable stands behind. SetHead is a no-op when main never moved.
				if err := env.DAG.SetHead(mainBranch, head); err != nil {
					return fmt.Errorf("peer sync: %w (and main could not be put back: %v)", cause, err)
				}
				// Leaving the root resident is not cosmetic: a namespace holding a commit it
				// never bootstrapped from refuses every later bootstrap attempt for the life of
				// the process (embed.CanInstallSnapshot), so a failure here is reported rather
				// than swallowed - a retry that will be refused is worse than a loud failure.
				if err := env.DAG.DropShallowCommit(target.Hash); err != nil {
					return fmt.Errorf("peer sync: %w (and the snapshot root could not be taken back: %v)", cause, err)
				}
				admitted = false
			}
			for _, id := range written {
				_ = env.Storage.DeleteDocument(env.NamespaceID, id)
			}
			if _, err := env.Storage.CommitTree(env.NamespaceID, tree); err != nil {
				_ = env.Storage.DiscardPending(env.NamespaceID)
			}
			return cause
		}
		after := ""
		for first := true; ; first = false {
			page, err := fetch(after)
			if err != nil {
				return undo(err)
			}
			if first {
				target = page.Commit
				if recomputed, err := document.ComputeCommitHash(target); err != nil || recomputed != target.Hash {
					return undo(fmt.Errorf("peer sync: snapshot commit %s does not hash to itself", target.Hash.Hex()))
				}
			} else if page.Commit.Hash != target.Hash {
				return undo(fmt.Errorf("peer sync: snapshot pages disagree about their commit (%s, then %s)", target.Hash.Hex(), page.Commit.Hash.Hex()))
			}
			for _, d := range page.Docs {
				id, err := codec.ParseUUID(d.DocID)
				if err != nil {
					return undo(err)
				}
				if d.DocID <= after && after != "" {
					return undo(fmt.Errorf("peer sync: snapshot page out of order at %s", d.DocID))
				}
				if err := env.Storage.PutDocument(env.NamespaceID, document.Document{ID: id, JSON: d.Body}); err != nil {
					return undo(err)
				}
				written = append(written, id)
				applied = append(applied, document.WriteOp{DocID: id, Patch: d.Body})
				after = d.DocID
			}
			// Commit each page's documents as they arrive rather than staging the whole
			// namespace: the pending set is memory, and a snapshot is by definition all of it.
			built, err := env.Storage.CommitTree(env.NamespaceID, tree)
			if err != nil {
				return undo(err)
			}
			tree = built.TreeHash
			if page.Done {
				if built.TreeHash != target.DocumentTreeHash {
					return undo(&TreeMismatchError{CommitHash: target.Hash, Declared: target.DocumentTreeHash, Built: built.TreeHash})
				}
				env.DAG.PutDocumentTree(built)
				break
			}
			if page.Next != "" {
				after = page.Next
			}
		}
		if err := env.DAG.PutShallowCommit(target); err != nil {
			return undo(err)
		}
		admitted = true
		if err := env.DAG.SetHead(mainBranch, target.Hash); err != nil {
			return undo(err)
		}
		if env.SnapshotInstalled != nil {
			if err := env.SnapshotInstalled(target); err != nil {
				// Nothing durable stands behind the snapshot: undo puts main back and takes the
				// root out of the DAG, leaving the namespace as empty as it was - and so still
				// able to be bootstrapped, which is the whole point of retrying.
				return undo(fmt.Errorf("peer sync: snapshot could not be made durable: %w", err))
			}
		}
		// Durable from here: the root stays whatever happens next. Advanced's error is returned
		// to the caller but never undoes a head move (see LocalNode), and this one is now backed
		// by a checkpoint on disk.
		admitted = false
		installed = target
		return node.Advanced(AdvanceStep{Commits: []document.Commit{target}, Applied: applied})
	})
	return installed, err
}

// needsSnapshot reports whether pulling remote into env has to start with a snapshot: this
// namespace is empty and the peer cannot send its history from the beginning - it holds a
// shallow root or a retention floor - or the caller asked for one.
func needsSnapshot(env IngestEnv, remote wire.NamespaceRefs, prefer bool) bool {
	_, head, ok, err := env.DAG.HeadCommit()
	if err != nil || !ok || len(head.ParentHashes) != 0 {
		return false
	}
	mainHex, has := remote.Branches[mainBranch]
	if !has || mainHex == head.Hash.Hex() {
		return false
	}
	return prefer || len(remote.Shallow) > 0 || remote.HistoryFloorHex != ""
}
