package embed

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// PutResult is the outcome of storing a JSON document and appending a commit.
type PutResult struct {
	DocID  codec.UUID
	Commit codec.Hash
}

// PutJSONDocument stores a JSON object as a document and appends a commit on main.
func PutJSONDocument(rt *EmbeddedKdbRuntime, namespaceID, jsonText string) (PutResult, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &root); err != nil {
		return PutResult{}, fmt.Errorf("invalid json: %w", err)
	}
	docID, err := resolveDocID(root)
	if err != nil {
		return PutResult{}, err
	}
	stored, err := document.EnsureIDInJSON(jsonText, docID)
	if err != nil {
		return PutResult{}, err
	}
	doc, err := document.FromJSONWithID(docID, stored)
	if err != nil {
		return PutResult{}, err
	}
	if err := rt.Storage.PutDocument(namespaceID, doc); err != nil {
		return PutResult{}, err
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return PutResult{}, err
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return PutResult{}, err
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return PutResult{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return PutResult{}, err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return PutResult{}, err
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   []document.Op{document.WriteOp{DocID: doc.ID, Patch: doc.JSON}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
	c, err := rt.DAG.AppendCommit(tx, head, tree, nil, "")
	if err != nil {
		return PutResult{}, err
	}
	return PutResult{DocID: doc.ID, Commit: c.Hash}, nil
}

func resolveDocID(root map[string]json.RawMessage) (codec.UUID, error) {
	idRaw, ok := root["id"]
	if !ok {
		return codec.RandomUUID()
	}
	var idStr string
	if err := json.Unmarshal(idRaw, &idStr); err != nil {
		// A caller-supplied "id" that isn't a JSON string (a number, an object, ...) is a
		// mistake worth surfacing, not silently overwritten with a fresh random id the caller
		// never asked for and won't be expecting.
		return codec.UUID{}, fmt.Errorf("kdb: \"id\" field must be a string, got %s: %w", idRaw, err)
	}
	if idStr == "" {
		return codec.UUID{}, fmt.Errorf("kdb: \"id\" field must not be empty")
	}
	return codec.ParseUUID(idStr)
}
