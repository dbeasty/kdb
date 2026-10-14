package storage

import (
	"errors"
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
	// FramesEnd is where the last complete frame - intact or damaged - ends. Unlike PhysicalSize
	// it does not count a preallocated segment's zero-filled remainder.
	FramesEnd int64
	// TrailingUnreadable is set when bytes that are neither a frame nor zero padding follow
	// FramesEnd with no intact frame after them: a torn tail, or a final frame damaged so badly
	// (its header) that it no longer reads as a frame at all.
	TrailingUnreadable bool
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

// DocumentCounter is implemented by adapters that can say how many documents a tree holds
// without reading any of them. ok is false when they cannot do so cheaply (the tree is not
// resident) or when the count would not match what a read returns - the server's expiry layer
// answers ok=false while documents are being hidden, because a hidden document is still in the
// tree. A caller that gets ok=false reads the documents instead; it must never rebuild a tree to
// get a count.
type DocumentCounter interface {
	CountDocuments(namespaceID string, treeHash codec.Hash) (n int, ok bool, err error)
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

// ErrOrderedScanUnsupported: see OrderedScanner.
var ErrOrderedScanUnsupported = errors.New("storage: ordered scan not supported by this adapter")

// OrderedScanner is ScanDocuments in ascending document id order, starting after a given id - what
// a paged SELECT needs to resume where its last page ended instead of re-reading every row before
// it. after == nil starts at the first document. Optional: a caller that finds it missing pages by
// offset, as before.
type OrderedScanner interface {
	// ScanDocumentsAfter returns ErrOrderedScanUnsupported when an adapter that routes to others
	// (engine.MultiplexAdapter) finds the one it routes to cannot do it.
	ScanDocumentsAfter(namespaceID string, atCommit codec.Hash, after *codec.UUID, batchSize int, onBatch func([]document.Document) error) error
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

// ThroughReader is implemented by adapters that can read documents without caching them: what a
// snapshot page wants, since it reads each document of the namespace once. treeHash is a document
// tree hash, as for Adapter's atCommit; the result is GetDocuments'. Optional: a caller that finds
// it missing uses GetDocuments.
type ThroughReader interface {
	ReadDocumentsThrough(namespaceID string, docIDs []codec.UUID, treeHash codec.Hash) ([]*document.Document, error)
}

// SnapshotApplier is implemented by adapters that can take a peer's snapshot into an empty
// namespace in batches more cheaply than PutDocument and CommitTree: ApplySnapshotBatch is
// PutDocument for each of docs followed by CommitTree, returning the tree, for a caller that adds
// documents in ascending id order to a namespace that holds nothing else. Optional: a caller that
// finds it missing uses PutDocument and CommitTree.
type SnapshotApplier interface {
	ApplySnapshotBatch(namespaceID string, docs []document.Document) (document.DocumentTree, error)
}

// ErrTreeBodiesUnsupported: see TreeBodyStreamer.
var ErrTreeBodiesUnsupported = errors.New("storage: this adapter cannot stream a tree's bodies")

// TreeBodyStreamer is implemented by adapters that can hand over the bodies a tree names in
// whatever order is cheapest for them to read - for the engine, the order of its log - rather
// than one random read per document. fn sees each (document, body) the tree names that the
// adapter finds that way, possibly more than once and never in any promised order; a document it
// does not find is read the ordinary way. A body is fn's to keep: it shares no buffer that would
// keep anything else alive. ErrTreeBodiesUnsupported when it cannot do this at all.
type TreeBodyStreamer interface {
	StreamTreeBodies(namespaceID string, tree document.DocumentTree, fn func(docID codec.UUID, body string) error) error
}
