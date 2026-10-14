package document

import "github.com/limidus/kdb/go/kdb/codec"

// Transaction is an in-flight transaction (not persisted until part of a Commit).
type Transaction struct {
	ID            codec.UUID
	BaseVersion   codec.Hash
	Operations    []Op
	Timestamp     codec.Timestamp
	AuthorNodeID  codec.UUID
	ResultVersion *codec.Hash

	// Preconditions are per-operation assertions about the state each write lands on, evaluated
	// against the target tree inside the server's write serialization. Empty means "no
	// assertions", which is every transaction built before preconditions existed. Not part of
	// the commit: see the Precondition doc comment.
	Preconditions []Precondition

	// Attachments, when set, is the attachment table after this transaction, and the DAG records
	// its root as the commit's AttachmentTreeHash. nil (the usual case) lets the DAG derive it:
	// the first parent's table with Operations applied. Not part of the commit itself.
	Attachments *AttachmentTable
}
