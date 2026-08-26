package embed

import (
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// MaterializeCommit replays one commit's document operations into store, so the commit's
// documents become visible to SqlExec/Query/GetDocument immediately - mirrors Kotlin's
// materializeSingleCommit (kdb-embed/EmbedOperations.kt). Needed because dag.PutCommit (used by
// peersync when ingesting a commit that didn't originate from this node's own
// transaction.Engine.Commit) only updates DAG bookkeeping (the commit object and its document
// tree hash) - it never touches storage.Adapter, so without this a peer's writes would be
// reachable from the DAG but invisible to every query that reads through storage.
//
// Only WriteOp/DeleteOp affect storage; FileWriteOp/SchemaMigrationOp are no-ops here just as in
// the Kotlin reference (schema propagation is handled separately, see syncEmbedSchema).
func MaterializeCommit(store storage.Adapter, d *dag.InMemoryCommitDag, namespaceID string, commit document.Commit) error {
	if len(commit.ParentHashes) == 0 {
		// Genesis commit: no prior tree to anchor CommitTree on, and by construction carries no
		// operations either.
		return nil
	}
	parent, err := d.GetCommitOrThrow(commit.ParentHashes[0])
	if err != nil {
		return err
	}
	for _, op := range commit.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			if err := store.PutDocument(namespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				return err
			}
		case document.DeleteOp:
			if err := store.DeleteDocument(namespaceID, o.DocID); err != nil {
				return err
			}
		}
	}
	_, err = store.CommitTree(namespaceID, parent.DocumentTreeHash)
	return err
}
