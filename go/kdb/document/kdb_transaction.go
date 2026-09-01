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
}
