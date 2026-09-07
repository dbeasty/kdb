package engine

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// treeRebuilder repopulates the historical document trees a checkpoint
// restore deliberately left out. See SetTreeRebuilder.
type treeRebuilder func() error

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
// one, so a read at that tree's hash can resolve. Used by the rebuilder.
func (e *ServerEngine) RegisterHistoricalTree(tree document.DocumentTree) {
	e.treesMu.Lock()
	e.treesByHash[tree.TreeHash] = tree
	e.treesMu.Unlock()
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
	e.treeRebuildOnce = &sync.Once{}
	e.treeRebuildMu.Unlock()
}

// rebuildTreesOnce runs the tree rebuilder at most once and reports
// whether anything ran. A rebuilder that fails is not retried; the error
// is remembered and returned to every subsequent caller, because a second
// full scan of the log is unlikely to produce a different answer and very
// likely to be expensive.
func (e *ServerEngine) rebuildTreesOnce() error {
	e.treeRebuildMu.Lock()
	fn, once := e.treeRebuild, e.treeRebuildOnce
	e.treeRebuildMu.Unlock()
	if fn == nil || once == nil {
		return nil
	}
	once.Do(func() { e.treeRebuildErr = fn() })
	return e.treeRebuildErr
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
