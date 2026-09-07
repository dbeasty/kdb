package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Store is the physical index interface (Component 8).
//
// Search and NearestNeighbours return ranked results sorted by score descending, then document
// id ascending (canonical UUID string order), so two stores over the same corpus - or the Go
// and Kotlin engines - return the same list. Lookup and Range return document ids only.
type Store interface {
	Descriptor() Descriptor

	Put(entry Entry) error
	Delete(docID codec.UUID, atCommit codec.Hash) error
	BulkLoad(entries []Entry) error

	Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error)
	Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error)
	Search(query string, atCommit *codec.Hash, limit int) ([]RankedResult, error)
	NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]RankedResult, error)

	Rebuild(entries []Entry) error
	Clear() error
	IsValid(atCommit codec.Hash) (bool, error)
	Snapshot() ([]byte, error)
	RestoreSnapshot(data []byte) error
}

// DocumentStore is a Store that derives its own entries from a JSON document, which is what
// the commit path needs (Layer 16 §10): the engine hands every registered index the document
// and lets the index extract its key, its analyzed text, or its vector.
//
// The two-phase form exists so that a document's fault (a vector of the wrong length) rejects
// the commit before anything is mutated: PrepareDocument does all extraction and validation and
// touches no state; the returned PreparedPut applies the result once the commit hash is known.
type DocumentStore interface {
	Store
	PrepareDocument(docID codec.UUID, jsonText string) (PreparedPut, error)
	PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error
}

// PreparedPut is a validated, not-yet-applied document update for one store.
type PreparedPut interface {
	// Apply records the update under commitHash and returns the hint that replicates it. A
	// prepared put may apply as a delete (the document no longer carries the indexed field);
	// the hint's Action says which.
	Apply(commitHash codec.Hash) (Hint, error)
}
