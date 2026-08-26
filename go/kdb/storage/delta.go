package storage

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// DeltaSegmentRef identifies a sealed delta segment on durable storage.
type DeltaSegmentRef struct {
	SegmentID   codec.UUID
	NamespaceID string
	// SequenceNumber is the segment's position in namespace-wide commit
	// order, assigned monotonically at open time and encoded directly in
	// the segment's file name (see io.SegmentNameBuilder.DeltaSequenced).
	// This - not SegmentID, which is only a display identity - is what
	// replay sorts and reasons about: see kdb-spec-layer13 Component 47
	// for why sorting by a random UUID (the pre-Layer-13 behavior) made a
	// multi-segment namespace unopenable after enough restarts.
	SequenceNumber  int64
	FirstCommitHash codec.Hash
	LastCommitHash  codec.Hash
	SizeBytes       int64
	Compression     CompressionCodec
}

// DocumentPatch is one document change in a delta record.
type DocumentPatch struct {
	DocID            codec.UUID
	Before           *document.Document
	After            *document.Document
	ContentHashAfter *codec.Hash
}
