package engine

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// treeRebuilder reconstructs one historical document tree, named by its
// hash. See SetTreeRebuilder.
type treeRebuilder func(want codec.Hash) (document.DocumentTree, bool, error)

// RestoreLiveTree installs tree as this namespace's current committed
// state, in place of replaying the commits that built it.
//
// Pins every content hash the tree names, exactly as CommitTree would have
// as it built the tree commit by commit, so reads of live data stay in
// memory rather than taking the cold path. Pinning a hash whose document
// is not resident is meaningful and intended: the pin is a refusal to
// evict, recorded against the hash, and it is already in place by the time
// the version store first loads that document back.
func (e *ServerEngine) RestoreLiveTree(namespaceID string, tree document.DocumentTree) {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	tree.Walk(func(_ codec.UUID, h codec.Hash) bool {
		e.docsByHash.Pin(h)
		return true
	})
	e.tree = tree
	e.treesMu.Lock()
	e.publishTreeLocked(tree)
	e.treesMu.Unlock()
}

// RegisterHistoricalTree records a tree that was not restored as the live
// one, so a read at that tree's hash can resolve.
func (e *ServerEngine) RegisterHistoricalTree(tree document.DocumentTree) {
	e.treesByHash.Put(tree)
}

// CachedTree reports whether a tree is currently resident, without
// counting as a use of it.
//
// The rebuild path needs this to find the nearest ancestor it can fold
// forward from; going through Get would promote every tree it probed to
// most-recently-used and evict the ones it is walking towards.
func (e *ServerEngine) CachedTree(h codec.Hash) (document.DocumentTree, bool) {
	if s := e.latestTree.Load(); s != nil && s.hash == h {
		return s.tree, true
	}
	return e.treesByHash.Peek(h)
}

// SetTreeRebuilder installs the fallback that reconstructs historical
// document trees on the first read that needs one.
//
// A checkpoint restores the live tree and the commit graph, not every tree
// the namespace has ever had: the trees are derivable from the delta log,
// and persisting all of them would put an artifact on disk that grows with
// history times documents - the shape this whole line of work exists to
// remove. So they are rebuilt instead, at most once per process, and only
// if something actually reads at a historical commit. A namespace only
// ever read at its head never pays.
//
// This is the third fallback of the same shape, after the version store's
// and the DAG's: keep live data in memory, keep history on disk, go and
// get it when asked.
func (e *ServerEngine) SetTreeRebuilder(fn treeRebuilder) {
	e.treeRebuildMu.Lock()
	e.treeRebuild = fn
	e.treeRebuildMu.Unlock()
}

// rebuildTree reconstructs the tree named by want, if a rebuilder is
// installed.
//
// Runs on every miss rather than once per process. The once-per-process
// version could not coexist with a bounded store: after it had run, an
// evicted tree had no way back at all, and while it ran it produced every
// tree in history into a store that could hold only some of them -
// evicting its own output before the caller ever saw it.
func (e *ServerEngine) rebuildTree(want codec.Hash) (document.DocumentTree, bool, error) {
	e.treeRebuildMu.Lock()
	fn := e.treeRebuild
	e.treeRebuildMu.Unlock()
	if fn == nil {
		return document.DocumentTree{}, false, nil
	}
	return fn(want)
}

// LiveTree returns the current committed document tree - what a checkpoint
// records so a restore does not have to rebuild it commit by commit.
func (e *ServerEngine) LiveTree() document.DocumentTree {
	if s := e.latestTree.Load(); s != nil {
		return s.tree
	}
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	return e.tree
}

// GetTree implements dag.DocumentTreeStore, resolving hash through the same
// chain every other historical read uses: the live snapshot, then the
// bounded store, then the tree objects, and only then a fold back out of
// the delta log.
//
// It used to stop after the bounded store, and that made the DAG's own
// history operations fail on any file-backed namespace. A checkpoint
// restores the live tree and the commit graph, not every tree the
// namespace has ever had - the rest are derivable from the log and are
// rebuilt on demand (see SetTreeRebuilder). But the rebuild hung off
// treeAt, which only the storage read path called, so a caller coming
// through the DAG got a plain miss for a tree that was merely not resident
// yet. dag.Diff is the visible casualty: it resolves both commits' trees
// through here, so after a restart it failed with "from tree missing" for
// every commit but the newest, which is exactly when someone wants to look
// at history.
//
// The rebuild is not free - it folds commits forward from the nearest
// resident ancestor - but it is bounded, it caches its result, and a hash
// no commit claims is refused cheaply before any of it starts
// (rebuildTreeByFolding's CommitForTree check). The live tree still
// answers from the atomic snapshot without touching the bounded store, so
// the hot path is unchanged.
//
// The dag.DocumentTreeStore signature has no error to return, so a rebuild
// that fails reports a miss - the same answer the caller got before, and
// the callers already treat a missing tree as a real, reportable outcome.
func (e *ServerEngine) GetTree(hash codec.Hash) (document.DocumentTree, bool) {
	tree, ok, err := e.treeAt(hash)
	if err != nil {
		return document.DocumentTree{}, false
	}
	return tree, ok
}

// PutTree implements dag.DocumentTreeStore.
func (e *ServerEngine) PutTree(tree document.DocumentTree) { e.treesByHash.Put(tree) }

// HistoryTreesResidentBytes is what the historical tree store currently
// holds. For tests and reporting.
func (e *ServerEngine) HistoryTreesResidentBytes() int64 { return e.treesByHash.ResidentBytes() }

// HistoryTreesResident is how many historical trees are held. For tests.
func (e *ServerEngine) HistoryTreesResident() int { return e.treesByHash.Len() }
