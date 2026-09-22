package peersync

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Graft: taking in a peer's history that is rooted where this node's is not
// (docs/kdb-distributed-self-healing-research.md, Phase 11; git's grafts and
// --allow-unrelated-histories).
//
// A node bootstrapped from a snapshot holds its root commit without the root's parents. Any node
// that does not hold those parents either cannot store the root - the history it would be taking
// in reaches down to a commit whose ancestry it has no way to check - and the sync fails as an
// UnrelatedHistoryError. Deepen fixes that when some peer still has the history. When none does
// (the source the snapshot came from is gone), a namespace whose resolution chain allows unrelated
// histories grafts the root instead: it fetches the root's state as a snapshot, checks it against
// the root's tree hash, and stores the root as a shallow root of its own - exactly as the peer
// holds it. Everything above the root then stores normally, and the merge of the two heads, which
// share no commit, takes the empty tree as its base (resolveDivergedLocked).
//
// Durability, in order - each step leaves a namespace that opens correctly if the process dies
// right after it:
//  1. the root's tree and every body are written to the blob store and synced; nothing names
//     them yet;
//  2. the namespace marker names the root as a graft (GraftRecorded), so a replay that meets it in
//     the log admits it without its parents;
//  3. the root is logged. It is not on main, so a replay stores it without applying it.
// The graft is never on main by itself: main reaches it only through the merge that follows, which
// writes every document the two sides disagree on, so a replay applying that merge onto this
// node's own head rebuilds the merged tree without the root's state.

// ErrUnrelatedNotAllowed is a graft asked of a namespace whose resolution chain does not allow
// merging unrelated histories.
var ErrUnrelatedNotAllowed = errors.New("peer sync: the namespace's resolution chain does not allow unrelated histories (allowUnrelated)")

// GraftResult is what one Graft did.
type GraftResult struct {
	Root codec.Hash
	// Documents is how many documents the root's tree holds.
	Documents int
}

// Graft fetches root's state through fetch (paged, as for InstallSnapshot), verifies it and
// stores root as a shallow root of this namespace. A root already held is left alone.
//
// The whole state is held in memory until it is verified: a tree cannot be checked against its
// hash until the last page is in.
func Graft(env IngestEnv, root codec.Hash, fetch func(after string) (wire.SnapshotPageMessage, error)) (GraftResult, error) {
	res := GraftResult{Root: root}
	if !env.Resolution.Chain.AllowsUnrelated() {
		return res, ErrUnrelatedNotAllowed
	}
	store, ok := env.Storage.(storage.ForeignTreeStore)
	if !ok {
		return res, fmt.Errorf("peer sync: storage for %s cannot hold a grafted tree", env.NamespaceID)
	}
	if env.DAG.HasCommit(root) {
		return res, nil
	}
	var commit document.Commit
	bodies := map[codec.UUID]string{}
	entries := map[codec.UUID]codec.Hash{}
	after := ""
	for first := true; ; first = false {
		page, err := fetch(after)
		if err != nil {
			return res, err
		}
		if first {
			commit = page.Commit
			if commit.Hash != root {
				return res, fmt.Errorf("peer sync: graft: asked for %s, the peer sent %s", root.Hex(), commit.Hash.Hex())
			}
			if recomputed, err := document.ComputeCommitHash(commit); err != nil || recomputed != commit.Hash {
				return res, fmt.Errorf("peer sync: graft: commit %s does not hash to itself", root.Hex())
			}
		} else if page.Commit.Hash != commit.Hash {
			return res, fmt.Errorf("peer sync: graft: pages disagree about their commit (%s, then %s)", commit.Hash.Hex(), page.Commit.Hash.Hex())
		}
		for _, d := range page.Docs {
			id, err := codec.ParseUUID(d.DocID)
			if err != nil {
				return res, err
			}
			if after != "" && d.DocID <= after {
				return res, fmt.Errorf("peer sync: graft: page out of order at %s", d.DocID)
			}
			h, err := (document.Document{ID: id, JSON: d.Body}).ContentHash()
			if err != nil {
				return res, err
			}
			bodies[id], entries[id] = d.Body, h
			after = d.DocID
		}
		if page.Done {
			break
		}
		if page.Next != "" {
			after = page.Next
		}
	}
	tree, err := document.BuildDocumentTree(entries)
	if err != nil {
		return res, err
	}
	if tree.TreeHash != commit.DocumentTreeHash {
		return res, &TreeMismatchError{CommitHash: commit.Hash, Declared: commit.DocumentTreeHash, Built: tree.TreeHash}
	}
	res.Documents = len(entries)

	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	var durable []func() error
	err = node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		if env.DAG.HasCommit(root) {
			return nil
		}
		if err := env.checkSkew(commit); err != nil {
			return err
		}
		if err := store.StoreForeignTree(env.NamespaceID, tree, bodies); err != nil {
			return fmt.Errorf("peer sync: graft: storing %s's tree: %w", root.Hex(), err)
		}
		if env.GraftRecorded != nil {
			if err := env.GraftRecorded(root); err != nil {
				return err
			}
		}
		if err := env.DAG.PutShallowCommit(commit); err != nil {
			return fmt.Errorf("peer sync: graft: storing %s: %w", root.Hex(), err)
		}
		env.DAG.PutDocumentTree(tree)
		durable, err = env.persistAll([]document.Commit{commit})
		return err
	})
	for _, wait := range durable {
		if werr := wait(); werr != nil && err == nil {
			err = fmt.Errorf("peer sync: graft: persisting %s: %w", root.Hex(), werr)
		}
	}
	return res, err
}
