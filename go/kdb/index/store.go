package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Store is the physical index interface (Component 8).
type Store interface {
	Descriptor() Descriptor

	Put(entry Entry) error
	Delete(docID codec.UUID, atCommit codec.Hash) error
	BulkLoad(entries []Entry) error

	Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error)
	Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error)
	Search(query string, atCommit *codec.Hash, limit int) ([]codec.UUID, error)
	NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]RankedResult, error)

	Rebuild(entries []Entry) error
	Clear() error
	IsValid(atCommit codec.Hash) (bool, error)
	Snapshot() ([]byte, error)
	RestoreSnapshot(data []byte) error
}
