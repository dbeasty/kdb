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
}
