package index

import (
	"fmt"
	"strings"

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

// String returns the catalog spelling of the type (HASH, BTREE, FULLTEXT, VECTOR).
func (t IndexType) String() string {
	switch t {
	case IndexTypeHash:
		return "HASH"
	case IndexTypeBTree:
		return "BTREE"
	case IndexTypeFullText:
		return "FULLTEXT"
	case IndexTypeVector:
		return "VECTOR"
	default:
		return fmt.Sprintf("IndexType(%d)", int(t))
	}
}

// ParseIndexType is the inverse of IndexType.String (case-insensitive).
func ParseIndexType(s string) (IndexType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "HASH":
		return IndexTypeHash, nil
	case "BTREE":
		return IndexTypeBTree, nil
	case "FULLTEXT":
		return IndexTypeFullText, nil
	case "VECTOR":
		return IndexTypeVector, nil
	default:
		return 0, fmt.Errorf("unknown index type: %q", s)
	}
}

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
	// Options carries Layer 16 index options: "index_name", "weights"
	// ("title=3,description=1"), "dimensions", "metric" (cosine|l2|inner_product), "m",
	// "ef_construction", "ef_search". Hash/btree descriptors derived from a schema also carry
	// "field_type" (the schema field's codec type label) so key extraction can produce typed
	// keys (TimestampKey, Int32Key) rather than the JSON value's natural key.
	Options map[string]string
}

// Entry is one index row at a commit.
type Entry struct {
	DocID      codec.UUID
	Key        Key
	CommitHash codec.Hash
}

// RankedResult is a scored search hit (full-text, vector, or fused).
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

// StoreFactory creates physical index stores from descriptors.
type StoreFactory interface {
	Create(descriptor Descriptor) (Store, error)
}

// StoreFactoryFunc adapts a function to StoreFactory.
type StoreFactoryFunc func(descriptor Descriptor) (Store, error)

func (f StoreFactoryFunc) Create(descriptor Descriptor) (Store, error) { return f(descriptor) }

// SortRanked orders results by score descending, then document id ascending (canonical UUID
// string order) - the tie rule every ranked API in this layer shares.
func SortRanked(results []RankedResult) {
	sortRanked(results)
}
