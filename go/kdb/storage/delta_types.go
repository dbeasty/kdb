package storage

import (
	"bytes"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// DeltaAuthorshipEnvelope is present in every delta record.
type DeltaAuthorshipEnvelope struct {
	Principal     string
	Timestamp     codec.Timestamp
	RightsToken   string
	ClientContext string
}

// DeltaRecord is one commit frame in a delta segment.
type DeltaRecord struct {
	CommitHash      codec.Hash
	NamespaceID     string
	Authorship      DeltaAuthorshipEnvelope
	CommitPayload   []byte
	DocumentPatches []DocumentPatch
}

// DeltaSegmentWriter appends framed commit payloads to a segment.
type DeltaSegmentWriter interface {
	NamespaceID() string
	SegmentID() codec.UUID
	CurrentSizeBytes() int64
	IsSealed() bool
	Append(record DeltaRecord) (int64, error)
	Flush() error
	Seal() (DeltaSegmentRef, error)
}

// DeltaSegmentReader reads sealed delta segments.
type DeltaSegmentReader interface {
	NamespaceID() string
	ReadAll(segment DeltaSegmentRef) ([]DeltaRecord, error)
	ReadRange(segment DeltaSegmentRef, sinceCommit, untilCommit codec.Hash) ([]DeltaRecord, error)
	ListSegments() ([]DeltaSegmentRef, error)
}

func deltaRecordEqual(a, b DeltaRecord) bool {
	if a.CommitHash != b.CommitHash || a.NamespaceID != b.NamespaceID {
		return false
	}
	if a.Authorship != b.Authorship {
		return false
	}
	if !bytes.Equal(a.CommitPayload, b.CommitPayload) {
		return false
	}
	if len(a.DocumentPatches) != len(b.DocumentPatches) {
		return false
	}
	for i := range a.DocumentPatches {
		pa, pb := a.DocumentPatches[i], b.DocumentPatches[i]
		if pa.DocID != pb.DocID {
			return false
		}
		if !documentEqual(pa.Before, pb.Before) || !documentEqual(pa.After, pb.After) {
			return false
		}
		if !hashPtrEqual(pa.ContentHashAfter, pb.ContentHashAfter) {
			return false
		}
	}
	return true
}

func documentEqual(a, b *document.Document) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.ID == b.ID && a.JSON == b.JSON
}

func hashPtrEqual(a, b *codec.Hash) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
