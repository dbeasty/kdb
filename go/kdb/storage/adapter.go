package storage

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Adapter is the core storage interface for document and tree reads/writes.
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
