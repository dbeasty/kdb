package storage

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// DeltaSegmentRef identifies a sealed delta segment on durable storage.
type DeltaSegmentRef struct {
	SegmentID       codec.UUID
	NamespaceID     string
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
