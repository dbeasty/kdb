package peersync

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Serving a snapshot at a commit whose tree this node never built.
//
// A node builds a tree for every commit its own main passes through. A commit it only stored - a
// peer's commit that it merged rather than fast-forwarded onto - names a tree this node never
// built: the merge applied the difference between the two sides, not the peer's commit. Asking
// for a snapshot at such a commit used to fail with "no tree". That is exactly what a node
// bootstrapped from a snapshot asks for when a peer's history forks from behind its root
// (boundary.go): the fork point is usually another writer's commit, which the node it
// bootstrapped from merged.
//
// The state is still here, as history: the commit's first-parent chain reaches a commit whose tree
// this node does hold (its own main, at the latest at the merge base), and every commit on the way
// carries its operations. Folding them forward from there gives the tree, which is checked against
// the hash the commit declares before anything is served, and the documents those operations
// wrote. Nothing is stored: the fold is served from memory and kept for the next page of the same
// snapshot.

// maxSnapshotFold bounds how many commits a fold walks back looking for a tree it can start from.
// The fork points boundary.go asks for are a few commits off a main this node built; a walk this
// long means the state is not reasonably derivable here, and failing says so.
const maxSnapshotFold = 10_000

// snapshotState is the state at one commit, as a snapshot page reads it.
type snapshotState struct {
	walk func(visit func(codec.UUID, codec.Hash) bool) error
	read func(ids []codec.UUID) ([]*document.Document, error)
}

// stateAt resolves commit's state: straight from storage when it holds the tree, else folded
// forward from the nearest first-parent ancestor whose tree it holds.
func stateAt(env IngestEnv, walker storage.TreeWalker, commit document.Commit) (snapshotState, error) {
	direct := snapshotState{
		walk: func(visit func(codec.UUID, codec.Hash) bool) error {
			return walker.WalkTree(env.NamespaceID, commit.DocumentTreeHash, visit)
		},
		read: func(ids []codec.UUID) ([]*document.Document, error) {
			return env.Storage.GetDocuments(env.NamespaceID, ids, commit.DocumentTreeHash)
		},
	}
	probe := walker.WalkTree(env.NamespaceID, commit.DocumentTreeHash, func(codec.UUID, codec.Hash) bool { return false })
	if probe == nil {
		return direct, nil
	}
	if f, ok := foldCache.get(env, commit.Hash); ok {
		return f.state(env), nil
	}
	f, err := foldState(env, walker, commit)
	if err != nil {
		return snapshotState{}, fmt.Errorf("%w (and its state could not be rebuilt from its history here: %v)", probe, err)
	}
	foldCache.put(env, commit.Hash, f)
	return f.state(env), nil
}

// foldedState is a commit's state rebuilt from a base tree storage holds plus the documents the
// commits above it wrote.
type foldedState struct {
	tree document.DocumentTree
	base codec.Hash // a tree storage holds; documents not in written are read at it
	// written holds every document the folded commits left behind; nil for a deleted one.
	written map[codec.UUID]*document.Document
}

func (f *foldedState) state(env IngestEnv) snapshotState {
	return snapshotState{
		walk: func(visit func(codec.UUID, codec.Hash) bool) error {
			f.tree.Walk(visit)
			return nil
		},
		read: func(ids []codec.UUID) ([]*document.Document, error) {
			out := make([]*document.Document, len(ids))
			var rest []codec.UUID
			var at []int
			for i, id := range ids {
				if d, ok := f.written[id]; ok {
					if d != nil {
						cp := *d
						out[i] = &cp
					}
					continue
				}
				rest = append(rest, id)
				at = append(at, i)
			}
			if len(rest) > 0 {
				docs, err := env.Storage.GetDocuments(env.NamespaceID, rest, f.base)
				if err != nil {
					return nil, err
				}
				for j, d := range docs {
					out[at[j]] = d
				}
			}
			return out, nil
		},
	}
}

func foldState(env IngestEnv, walker storage.TreeWalker, commit document.Commit) (*foldedState, error) {
	empty := document.EmptyDocumentTree().TreeHash
	held := func(tree codec.Hash) bool {
		return tree == empty || walker.WalkTree(env.NamespaceID, tree, func(codec.UUID, codec.Hash) bool { return false }) == nil
	}
	// Back along first parents to a tree storage holds; path runs from commit downwards.
	path := []codec.Hash{commit.Hash}
	base := empty
	cursor := commit
	for found := false; !found; {
		if len(path) > maxSnapshotFold {
			return nil, fmt.Errorf("no tree this node holds within %d first-parent ancestors", maxSnapshotFold)
		}
		if len(cursor.ParentHashes) == 0 {
			break // genesis: the fold starts from the empty tree
		}
		if env.DAG.IsShallow(cursor.Hash) {
			return nil, fmt.Errorf("its history ends at shallow root %s, whose tree is not here", cursor.Hash.Hex())
		}
		parent, ok := env.DAG.GetCommit(cursor.ParentHashes[0])
		if !ok {
			return nil, fmt.Errorf("its ancestor %s is not here", cursor.ParentHashes[0].Hex())
		}
		if held(parent.DocumentTreeHash) {
			base, found = parent.DocumentTreeHash, true
			break
		}
		path = append(path, parent.Hash)
		cursor = parent
	}

	entries := map[codec.UUID]codec.Hash{}
	if base != empty {
		if err := walker.WalkTree(env.NamespaceID, base, func(id codec.UUID, h codec.Hash) bool {
			entries[id] = h
			return true
		}); err != nil {
			return nil, err
		}
	}
	written := map[codec.UUID]*document.Document{}
	for i := len(path) - 1; i >= 0; i-- {
		ops, err := env.DAG.CommitOperations(path[i])
		if err != nil {
			return nil, err
		}
		// The write path's staging, per commit: a document's last operation wins, deletes are
		// applied before writes, and a write is stored as the document it parses to - the same
		// fold embed's tree rebuild does, or the hash would not be the one the commit names.
		puts := map[codec.UUID]document.Document{}
		deletes := map[codec.UUID]struct{}{}
		for _, op := range ops {
			switch o := op.(type) {
			case document.WriteOp:
				doc, err := document.FromJSONWithID(o.DocID, o.Patch)
				if err != nil {
					doc = document.Document{ID: o.DocID, JSON: o.Patch}
				}
				puts[o.DocID] = doc
				delete(deletes, o.DocID)
			case document.DeleteOp:
				deletes[o.DocID] = struct{}{}
				delete(puts, o.DocID)
			}
		}
		for id := range deletes {
			delete(entries, id)
			written[id] = nil
		}
		for id, doc := range puts {
			h, err := doc.ContentHash()
			if err != nil {
				return nil, err
			}
			entries[id] = h
			d := doc
			written[id] = &d
		}
	}
	tree, err := document.BuildDocumentTree(entries)
	if err != nil {
		return nil, err
	}
	if tree.TreeHash != commit.DocumentTreeHash {
		return nil, &TreeMismatchError{CommitHash: commit.Hash, Declared: commit.DocumentTreeHash, Built: tree.TreeHash}
	}
	return &foldedState{tree: tree, base: base, written: written}, nil
}

// foldCache keeps the last few folded states, so the pages of one snapshot fold once rather than
// once a page.
var foldCache = &foldStates{}

type foldStates struct {
	mu      sync.Mutex
	entries []foldEntry
}

type foldEntry struct {
	node   *dag.InMemoryCommitDag // whose node: a state is read through that node's storage
	ns     string
	commit codec.Hash
	state  *foldedState
}

const foldCacheSize = 4

func (c *foldStates) get(env IngestEnv, commit codec.Hash) (*foldedState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if e.node == env.DAG && e.ns == env.NamespaceID && e.commit == commit {
			return e.state, true
		}
	}
	return nil, false
}

func (c *foldStates) put(env IngestEnv, commit codec.Hash, s *foldedState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, foldEntry{node: env.DAG, ns: env.NamespaceID, commit: commit, state: s})
	if len(c.entries) > foldCacheSize {
		c.entries = c.entries[len(c.entries)-foldCacheSize:]
	}
}
