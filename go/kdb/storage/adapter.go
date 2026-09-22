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

// SegmentSkim is what a damage-tolerant read of one delta segment found - see
// DamageTolerantReader.
type SegmentSkim struct {
	// PhysicalSize is the segment file's length, damaged frames and all.
	PhysicalSize int64
	// LastCommitHash is the last commit any intact frame holds.
	LastCommitHash codec.Hash
	// DamagedFrames are the offsets of complete frames that failed their CRC or would not parse.
	DamagedFrames []int64
	// LastFrameDamaged is set when the segment's final complete frame is one of them.
	LastFrameDamaged bool
}

// DamageTolerantReader reads delta segments past damaged frames. A DeltaSegmentReader stops at the
// first damaged frame, as replay must; this is for telling a damaged log from a replaced one, and
// for recovering what the damage did not touch.
type DamageTolerantReader interface {
	SkimSegment(segment DeltaSegmentRef) (SegmentSkim, error)
	StreamCommitsPastDamage(segment DeltaSegmentRef, fn func(document.Commit, int64) error) error
}

// TreeResolver resolves a whole document tree by hash - for Merkle comparison between peers
// (document.DocumentTree.Node), which needs the trie rather than a walk of it. ok is false for a
// tree the store does not hold.
type TreeResolver interface {
	TreeAt(treeHash codec.Hash) (document.DocumentTree, bool, error)
}

// ForeignTreeStore is implemented by adapters that can hold a document tree they did not build - a
// peer's state at a commit this namespace grafts in (peersync.Graft) - resolvable by hash, with
// its documents, without it becoming the live tree. Every body is checked against the content
// hash the tree names, and the tree is durable when the call returns.
type ForeignTreeStore interface {
	StoreForeignTree(namespaceID string, tree document.DocumentTree, bodies map[codec.UUID]string) error
}

// TreeWalker is implemented by adapters that can stream a tree's (doc id, content hash) entries
// without loading document bodies - what a snapshot needs to page through a namespace in id
// order without reading every body for every page. treeHash is a document tree hash, as for
// Adapter's atCommit. A tree the adapter cannot resolve is an error, not an empty walk.
type TreeWalker interface {
	WalkTree(namespaceID string, treeHash codec.Hash, visit func(docID codec.UUID, contentHash codec.Hash) bool) error
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
