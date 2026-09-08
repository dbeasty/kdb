package storage

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Adapter is the core storage interface for document and tree reads/writes.
//
// A note on the atCommit parameters below, because the name is a trap: they take a **document
// tree hash**, not a commit hash. The implementation matches the value against the live tree
// snapshot and the tree store, both keyed by tree hash (see engine.ServerEngine.treeAt), so a
// commit hash passed here does not error - it simply resolves nothing, and the read returns "no
// such document" for a document that is plainly there. Callers hold a commit and want
// commit.DocumentTreeHash; KdbServerRuntime.getDocumentAt is the reference for how to do it.
//
// The name is kept because it is load-bearing across both language trees and every implementation
// of this interface; this comment is the cheaper fix.
type Adapter interface {
	Capabilities() CapabilitySet

	GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error)
	GetDocumentOrThrow(namespaceID string, docID codec.UUID, atCommit codec.Hash) (document.Document, error)
	GetDocuments(namespaceID string, docIDs []codec.UUID, atCommit codec.Hash) ([]*document.Document, error)
	ScanDocuments(namespaceID string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error

	PutDocument(namespaceID string, doc document.Document) error
	DeleteDocument(namespaceID string, docID codec.UUID) error

	// DiscardPending drops any PutDocument/DeleteDocument calls made since the last
	// CommitTree for this namespace, restoring the last-committed visible state. Used to
	// roll back a transaction whose write phase failed partway through.
	DiscardPending(namespaceID string) error

	CommitTree(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error)

	Flush(namespaceID string) error

	ReadBlob(contentHash codec.Hash) ([]byte, error)
	WriteBlob(bytes []byte) (codec.Hash, error)

	IngestDeltaSegment(segment DeltaSegmentRef) error
}

// TreePinner is implemented by adapters that cache historical document trees under a budget,
// and so can be asked to keep one resolvable for as long as a caller still needs it.
//
// Optional on purpose. An adapter with an unbounded tree cache - anything memory-backed, where
// nothing is ever evicted - needs no pins, and an adapter that rebuilds a tree cheaply enough
// not to care is free not to implement it. Callers probe for it and skip the pin when it is
// absent; a missing pin costs performance, never correctness.
//
// treeHash is a document tree hash, the same key space as Adapter's atCommit parameter.
//
// Pins are counted: N calls to PinTree need N calls to UnpinTree. Pinning a tree that is not
// currently cached is legal and is the common case for a writer whose base version has already
// been evicted - the pin protects the tree once the rebuild stores it.
type TreePinner interface {
	PinTree(treeHash codec.Hash)
	UnpinTree(treeHash codec.Hash)
}
