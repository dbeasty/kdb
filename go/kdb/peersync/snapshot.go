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
//
// A page costs the page. The state's tree is a trie ordered by id, so the page seeks to the
// cursor and walks forward from there (document.DocumentTree.WalkAfter). It used to walk the whole
// tree and sort every id after the cursor, on every page: O(D) per page and O(D²/page) for a
// whole snapshot, which at a million documents was most of what a bootstrap cost the host.
// snapshotPageScan is that algorithm, kept for a state whose tree cannot be resolved as a trie and
// for a cursor that is not a canonical id; both serve identical pages.
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
	state, err := stateAt(env, walker, commit)
	if err != nil {
		return wire.SnapshotPageMessage{}, err
	}
	if state.tree != nil {
		tree, ok, err := state.tree()
		if err != nil {
			return wire.SnapshotPageMessage{}, err
		}
		if ok {
			if cursor, ok := snapshotCursor(after); ok {
				return snapshotPageSeek(env, state, tree, commit, cursor, maxBytes)
			}
		}
	}
	return snapshotPageScan(env, state, commit, after, maxBytes)
}

// snapshotChunk is how many documents a page reads from storage at a time.
const snapshotChunk = 128

// snapshotCursor parses a page cursor as the id it names. ok is false for one that is not an id's
// canonical form: the cursor is compared as a string, and only for the canonical form (lowercase
// hex) does string order agree with the trie's byte order. A peer only ever sends back the id it
// was given, so in practice this is "" or canonical.
func snapshotCursor(after string) (*codec.UUID, bool) {
	if after == "" {
		return nil, true
	}
	id, err := codec.ParseUUID(after)
	if err != nil || id.String() != after {
		return nil, false
	}
	return &id, true
}

// snapshotPageSeek is snapshotPage over an ordered tree: it seeks to the cursor and reads forward,
// so the work is the page's documents plus the trie's depth, whatever the namespace's size.
func snapshotPageSeek(env IngestEnv, state snapshotState, tree document.DocumentTree, commit document.Commit, cursor *codec.UUID, maxBytes int) (wire.SnapshotPageMessage, error) {
	page := wire.SnapshotPageMessage{Namespace: env.NamespaceID, Commit: commit, Total: tree.Size(), Done: true}
	size := 0
	ids := make([]codec.UUID, 0, snapshotChunk)
	collect := func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return len(ids) < snapshotChunk
	}
	for {
		ids = ids[:0]
		if cursor == nil {
			tree.WalkOrdered(collect)
		} else {
			tree.WalkAfter(*cursor, collect)
		}
		if len(ids) == 0 {
			return page, nil
		}
		docs, err := state.read(ids)
		if err != nil {
			return wire.SnapshotPageMessage{}, err
		}
		for i, d := range docs {
			if d == nil {
				return wire.SnapshotPageMessage{}, fmt.Errorf("peer sync: document %s of tree %s is not readable", ids[i], commit.DocumentTreeHash.Hex())
			}
			if len(page.Docs) > 0 && size+len(d.JSON) > maxBytes {
				page.Done = false
				page.Next = page.Docs[len(page.Docs)-1].DocID
				return page, nil
			}
			page.Docs = append(page.Docs, wire.SnapshotDoc{DocID: d.ID.String(), Body: d.JSON})
			size += len(d.JSON)
		}
		last := ids[len(ids)-1]
		cursor = &last
	}
}

// snapshotPageScan is the original paging: walk the whole tree, sort every id after the cursor,
// read from the front. O(D) per page; the fallback for a tree that cannot be walked in order.
func snapshotPageScan(env IngestEnv, state snapshotState, commit document.Commit, after string, maxBytes int) (wire.SnapshotPageMessage, error) {
	// Ids after the cursor, sorted: a tree's walk order is only incidentally sorted, and the
	// cursor has to mean the same thing on every page.
	total := 0
	var ids []codec.UUID
	if err := state.walk(func(id codec.UUID, _ codec.Hash) bool {
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
	for start := 0; start < len(ids); start += snapshotChunk {
		end := min(start+snapshotChunk, len(ids))
		docs, err := state.read(ids[start:end])
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
// Two phases, so the write gate is not held across the network:
//
//  1. Transfer, outside the node's serialization. Pages are checked as they arrive (the commit
//     hashes to itself, every page names it, ids ascend) and staged on disk (pageSpool), so
//     memory holds one page whatever the namespace's size. Nothing local changes.
//  2. Install, under the node's serialization (for the server, its write gate). The namespace is
//     checked to be empty again, then the staged documents are applied in page-sized batches,
//     the tree they built checked against the commit's, the root admitted and main moved.
//
// The gate cannot be let go between batches of phase 2: storage has one live tree per namespace
// (CommitTree builds on it, not on the tree it is passed), so a local commit taken between two
// batches would build on half a snapshot. Staging to disk is what keeps the gate out of the
// transfer instead.
//
// Semantics for a concurrent local write: it is not blocked by the transfer, and it wins. A local
// commit landing while the pages are in flight leaves the namespace with history of its own, and
// phase 2 refuses the snapshot with ErrNotEmpty, leaving the local write and nothing of the
// snapshot - exactly what happens when the local write lands first. A local write arriving during
// phase 2 waits for it, as for any other writer, and then lands on the installed state.
//
// Runs Advanced once the head has moved, so indexes, unique keys and listeners see the installed
// documents; its Applied arrive as AdvanceStep.AppliedPages, read back from the staging file
// rather than held. SnapshotInstalled, when set, is what makes the result survive a restart, since
// no replayable log produced it.
func InstallSnapshot(env IngestEnv, fetch func(after string) (wire.SnapshotPageMessage, error)) (document.Commit, error) {
	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	// Refused before any page is fetched when it would be refused anyway; checked again under the
	// serialization, which is the check that counts.
	if err := snapshotTargetEmpty(env); err != nil {
		return document.Commit{}, err
	}
	spool, err := newPageSpool(env.StagingDir)
	if err != nil {
		return document.Commit{}, err
	}
	defer spool.close()
	target, err := fetchSnapshotPages(fetch, func(d wire.SnapshotDoc) error { return spool.add(d.DocID, d.Body) })
	if err != nil {
		return document.Commit{}, err
	}

	var installed document.Commit
	err = node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		head, err := env.DAG.Head()
		if err != nil {
			return err
		}
		if err := snapshotTargetEmpty(env); err != nil {
			return err
		}
		_, headCommit, _, err := env.DAG.HeadCommit()
		if err != nil {
			return err
		}
		tree := headCommit.DocumentTreeHash
		// committed counts the staged documents that reached a committed tree, in spool order,
		// so undo can take exactly those back out without a list of their ids.
		committed := 0
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
			// A batch that failed part-way left puts pending; they never reached a tree.
			_ = env.Storage.DiscardPending(env.NamespaceID)
			_ = spool.each(DefaultPageBytes, committed, func(docs []wire.SnapshotDoc) error {
				for _, d := range docs {
					if id, err := codec.ParseUUID(d.DocID); err == nil {
						_ = env.Storage.DeleteDocument(env.NamespaceID, id)
					}
				}
				if _, err := env.Storage.CommitTree(env.NamespaceID, tree); err != nil {
					_ = env.Storage.DiscardPending(env.NamespaceID)
				}
				return nil
			})
			return cause
		}
		// Applied in page-sized batches, each committed as it goes rather than staging the whole
		// namespace: the pending set is memory, and a snapshot is by definition all of it.
		var built document.DocumentTree
		applyErr := spool.each(DefaultPageBytes, -1, func(docs []wire.SnapshotDoc) error {
			for _, d := range docs {
				id, err := codec.ParseUUID(d.DocID)
				if err != nil {
					return err
				}
				if err := env.Storage.PutDocument(env.NamespaceID, document.Document{ID: id, JSON: d.Body}); err != nil {
					return err
				}
			}
			b, err := env.Storage.CommitTree(env.NamespaceID, tree)
			if err != nil {
				return err
			}
			built, tree = b, b.TreeHash
			committed += len(docs)
			return nil
		})
		if applyErr != nil {
			return undo(applyErr)
		}
		if spool.docs == 0 {
			// Nothing staged: the tree is whatever the empty namespace already has.
			if built, err = env.Storage.CommitTree(env.NamespaceID, tree); err != nil {
				return undo(err)
			}
		}
		if built.TreeHash != target.DocumentTreeHash {
			return undo(&TreeMismatchError{CommitHash: target.Hash, Declared: target.DocumentTreeHash, Built: built.TreeHash})
		}
		env.DAG.PutDocumentTree(built)
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
		return node.Advanced(AdvanceStep{Commits: []document.Commit{target}, AppliedPages: spoolOps(spool)})
	})
	return installed, err
}

// snapshotTargetEmpty refuses a namespace a snapshot cannot be installed into: one with history
// of its own (main past genesis), or one that says it could not hold the snapshot durably.
func snapshotTargetEmpty(env IngestEnv) error {
	_, headCommit, ok, err := env.DAG.HeadCommit()
	if err != nil {
		return err
	}
	if !ok || len(headCommit.ParentHashes) != 0 {
		return ErrNotEmpty
	}
	if env.CanInstallSnapshot != nil {
		return env.CanInstallSnapshot()
	}
	return nil
}

// fetchSnapshotPages pulls every page of a snapshot through fetch, checking each as it arrives -
// the commit hashes to itself, every page names the same one, ids strictly ascend - and handing
// each document to keep. Returns the snapshot's commit. Holds one page at a time.
func fetchSnapshotPages(fetch func(after string) (wire.SnapshotPageMessage, error), keep func(wire.SnapshotDoc) error) (document.Commit, error) {
	var target document.Commit
	after := ""
	for first := true; ; first = false {
		page, err := fetch(after)
		if err != nil {
			return document.Commit{}, err
		}
		if first {
			target = page.Commit
			if recomputed, err := document.ComputeCommitHash(target); err != nil || recomputed != target.Hash {
				return document.Commit{}, fmt.Errorf("peer sync: snapshot commit %s does not hash to itself", target.Hash.Hex())
			}
		} else if page.Commit.Hash != target.Hash {
			return document.Commit{}, fmt.Errorf("peer sync: snapshot pages disagree about their commit (%s, then %s)", target.Hash.Hex(), page.Commit.Hash.Hex())
		}
		for _, d := range page.Docs {
			if _, err := codec.ParseUUID(d.DocID); err != nil {
				return document.Commit{}, err
			}
			if d.DocID <= after && after != "" {
				return document.Commit{}, fmt.Errorf("peer sync: snapshot page out of order at %s", d.DocID)
			}
			if err := keep(d); err != nil {
				return document.Commit{}, err
			}
			after = d.DocID
		}
		if page.Done {
			return target, nil
		}
		if page.Next != "" {
			after = page.Next
		}
	}
}

// spoolOps reads a spool back as the write operations that installed it, for AdvanceStep.
func spoolOps(spool *pageSpool) func(fn func([]document.Op) error) error {
	return func(fn func([]document.Op) error) error {
		ops := make([]document.Op, 0, snapshotChunk)
		return spool.each(DefaultPageBytes, -1, func(docs []wire.SnapshotDoc) error {
			ops = ops[:0]
			for _, d := range docs {
				id, err := codec.ParseUUID(d.DocID)
				if err != nil {
					return err
				}
				ops = append(ops, document.WriteOp{DocID: id, Patch: d.Body})
			}
			err := fn(ops)
			clear(ops)
			return err
		})
	}
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
