package embed

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// PutResult is the outcome of storing a JSON document and appending a commit.
type PutResult struct {
	DocID  codec.UUID
	Commit codec.Hash
}

// PutJSONDocument stores a JSON object as a document and appends a commit on main.
//
// The body is stored byte-exact (kdb-spec-layer16 §9.4): no "id" is injected and no key is
// reordered. A top-level "id" in the body is honoured as the document's identity - a UUID string
// directly, any other non-empty string through codec.DerivedUUID - and when there is none the
// engine mints a random UUID and reports it in PutResult.DocID; see document.ResolveID.
func PutJSONDocument(rt *EmbeddedKdbRuntime, namespaceID, jsonText string) (PutResult, error) {
	docID, supplied, err := document.ResolveID(jsonText)
	if err != nil {
		return PutResult{}, err
	}
	if !supplied {
		docID, err = codec.RandomUUID()
		if err != nil {
			return PutResult{}, err
		}
	}
	doc, err := document.FromJSONWithID(docID, jsonText)
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
