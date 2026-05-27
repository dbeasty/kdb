package embed

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// PutJSONDocument stores a JSON object as a document and appends a commit on main.
func PutJSONDocument(rt *EmbeddedKdbRuntime, namespaceID, jsonText string) (codec.Hash, error) {
	dagImpl, ok := rt.DAG.(*dag.InMemoryCommitDag)
	if !ok {
		return codec.Hash{}, fmt.Errorf("embed: put requires in-memory DAG")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &root); err != nil {
		return codec.Hash{}, fmt.Errorf("invalid json: %w", err)
	}
	doc, err := document.FromJSON(jsonText)
	if err != nil {
		return codec.Hash{}, err
	}
	if idRaw, ok := root["id"]; ok {
		var idStr string
		if err := json.Unmarshal(idRaw, &idStr); err == nil && idStr != "" {
			if uid, err := codec.ParseUUID(idStr); err == nil {
				doc, err = document.FromJSONWithID(uid, jsonText)
				if err != nil {
					return codec.Hash{}, err
				}
			}
		}
	}
	if err := rt.Storage.PutDocument(namespaceID, doc); err != nil {
		return codec.Hash{}, err
	}
	head, err := dagImpl.Head()
	if err != nil {
		return codec.Hash{}, err
	}
	commit, err := dagImpl.GetCommitOrThrow(head)
	if err != nil {
		return codec.Hash{}, err
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, err
	}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   []document.Op{document.WriteOp{DocID: doc.ID, Patch: doc.JSON}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
	c, err := dagImpl.AppendCommit(tx, head, tree, nil, "")
	if err != nil {
		return codec.Hash{}, err
	}
	return c.Hash, nil
}
