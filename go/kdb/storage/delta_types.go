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

// DeltaCommitStreamer is an optional capability of a DeltaSegmentReader:
// it hands a segment's commits to fn one at a time as they are decoded,
// rather than returning all of them at once.
//
// DeltaSegmentReader.ReadAll materializes a whole segment - every version
// of every document it records - and returns them as DeltaRecords, which
// carry the commit as re-encoded payload bytes. A caller that wanted
// commits back therefore paid three full materializations of the segment:
// the decode inside ReadAll, the re-encode into the record, and its own
// decode of that payload. Replay, which reads every segment a namespace
// has ever written, paid it over the whole history at once.
//
// Implementations that can stream should; callers should type-assert for
// this and fall back to ReadAll, so a reader that cannot stream still
// works. *delta.DefaultReader implements it.
type DeltaCommitStreamer interface {
	// StreamCommits hands each commit in segment to fn along with the byte
	// offset of the frame it came from. The offset is what lets a caller
	// build an index now and come back for one commit later without
	// re-reading the rest; callers that just want the commits ignore it.
	StreamCommits(segment DeltaSegmentRef, fn func(commit document.Commit, frameOffset int64) error) error
	// ReadCommitAt decodes the single commit framed at frameOffset, an
	// offset a previous StreamCommits reported for this same segment.
	ReadCommitAt(segment DeltaSegmentRef, frameOffset int64) (document.Commit, error)
}

// DeltaSegmentSequencer reports the sequence number of the segment a
// writer is currently appending to. Everything below it is sealed and can
// never gain another commit, which is what lets a checkpoint say "segments
// up to here are fully accounted for" - see embed.saveCheckpoint.
type DeltaSegmentSequencer interface {
	SequenceNumber() int64
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
