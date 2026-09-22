package engine

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ErrForeignTreeNeedsObjects refuses a graft on a namespace whose history strategy keeps no tree
// objects (replay, and so every history=none namespace): there a tree is resolvable only by
// folding the log, which holds nothing of a tree that came from elsewhere.
var ErrForeignTreeNeedsObjects = errors.New("engine: grafting a foreign tree needs history strategy \"objects\"; deepen from a peer that holds the history instead")

// StoreForeignTree makes tree - one this namespace did not build, a peer's state at a commit it
// grafts in (docs/kdb-distributed-self-healing-research.md, Phase 11) - durably resolvable by hash,
// with its documents' bodies, without touching the live tree: after this, TreeAt, WalkTree and
// GetDocument at tree.TreeHash work, across restarts.
//
// Every body is checked against the content hash the tree names before anything is written. The
// bodies and one full tree object go to the blob store, which is flushed and synced before this
// returns - so a caller may refer to the tree (admit the commit naming it, log it) only after.
func (e *ServerEngine) StoreForeignTree(_ string, tree document.DocumentTree, bodies map[codec.UUID]string) error {
	if e.memTable == nil {
		return errors.New("engine: no blob store to hold a foreign tree")
	}
	if e.HistoryStrategy() != storage.HistoryStrategyObjects {
		return ErrForeignTreeNeedsObjects
	}
	puts := make([]TreeChange, 0, tree.Size())
	var bad error
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		body, ok := bodies[id]
		if !ok {
			bad = fmt.Errorf("engine: foreign tree %s names document %s, whose body was not supplied", tree.TreeHash.Hex(), id)
			return false
		}
		got, err := (document.Document{ID: id, JSON: body}).ContentHash()
		if err != nil || got != h {
			bad = fmt.Errorf("engine: foreign tree %s: document %s's body does not match its content hash", tree.TreeHash.Hex(), id)
			return false
		}
		puts = append(puts, TreeChange{DocID: id, ContentHash: h})
		return true
	})
	if bad != nil {
		return bad
	}
	for _, p := range puts {
		e.memTable.Put(p.ContentHash, []byte(bodies[p.DocID]))
	}
	e.memTable.Put(tree.TreeHash, encodeTreeObject(treeObject{
		kind:     treeObjectKindFull,
		baseHash: document.EmptyDocumentTree().TreeHash,
		puts:     puts,
	}))
	if _, err := e.memTable.Flush(0); err != nil {
		return err
	}
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}
	// Some bodies now live only in the blob store; cold reads must look there.
	e.bodiesExternal.Store(true)
	e.treesByHash.Put(tree)
	return nil
}
