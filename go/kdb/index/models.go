package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// IndexType classifies physical index implementations.
type IndexType int

const (
	IndexTypeHash IndexType = iota
	IndexTypeBTree
	IndexTypeFullText
	IndexTypeVector
)

// HintAction is a replicated index update action.
type HintAction int

const (
	HintActionPut HintAction = iota
	HintActionDelete
)

// Descriptor is an immutable index description.
type Descriptor struct {
	IndexID       codec.UUID
	NamespaceID   string
	FieldName     string
	Fields        []string
	Type          IndexType
	Unique        bool
	SchemaVersion int
	CreatedAtHash codec.Hash
}

// Entry is one index row at a commit.
type Entry struct {
	DocID       codec.UUID
	Key         Key
	CommitHash  codec.Hash
}

// RankedResult is a vector search hit.
type RankedResult struct {
	DocID codec.UUID
	Score float32
}

// Hint is a pre-computed index update for replication.
type Hint struct {
	IndexID    codec.UUID
	FieldName  string
	Type       IndexType
	Action     HintAction
	DocID      codec.UUID
	Key        Key
	CommitHash codec.Hash
}

// StoreFactory creates physical index stores.
type StoreFactory interface {
	Create(descriptor Descriptor) Store
}
