package embed

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// putJSONBatch is PutJSONDocument for many documents at once: every body stored, the tree
// committed once, and one commit on main carrying a write per document - an ordinary transaction
// with many operations, so what it builds is an ordinary namespace (log, checkpoint, graph), in a
// fraction of the commits. For building large namespaces in tests (TestMemoryProfile's
// KDB_MEMPROFILE_BATCH); a later write to the same id in the batch wins, as it would in sequence.
func putJSONBatch(rt *EmbeddedKdbRuntime, namespaceID string, jsonTexts []string) error {
	ops := make([]document.Op, 0, len(jsonTexts))
	for _, text := range jsonTexts {
		id, supplied, err := document.ResolveID(text)
		if err != nil {
			return err
		}
		if !supplied {
			if id, err = codec.RandomUUID(); err != nil {
				return err
			}
		}
		doc, err := document.FromJSONWithID(id, text)
		if err != nil {
			return err
		}
		if err := rt.Storage.PutDocument(namespaceID, doc); err != nil {
			return err
		}
		ops = append(ops, document.WriteOp{DocID: doc.ID, Patch: doc.JSON})
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return err
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return err
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   ops,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
	_, err = rt.DAG.AppendCommit(tx, head, tree, nil, "")
	return err
}
