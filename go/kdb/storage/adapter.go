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
