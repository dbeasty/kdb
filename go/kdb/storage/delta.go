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
	// Tail says what follows the segment's last intact frame, which ends at SizeBytes. A reader
	// that cannot tell (an in-memory one, which has no damage) leaves it TailClean.
	Tail SegmentTail
}

// TailKind classifies the bytes after a delta segment's last intact frame - the one rule every
// reader of the log (replay, checkpoint matching, the loaders, kdb-inspect verify and repair)
// applies, so they cannot disagree about whether a segment lost data.
//
// The rule is the one from #81: a torn tail - the shape an interrupted append leaves - can only
// be bytes at the very end of a segment with nothing intact after them. Damage followed by any
// intact frame is never a torn tail, because an append that never completed cannot be followed by
// one that did: appends to a segment are sequential, and OpenWriter always starts a fresh segment
// rather than resuming an old one.
type TailKind uint8

const (
	// TailClean: the file ends at the last intact frame, or only zero bytes follow it (a
	// preallocated segment's unused space).
	TailClean TailKind = iota
	// TailTorn: bytes follow the last intact frame, but no intact frame follows them. The
	// expected shape of an unclean shutdown, in the newest segment or - once a restart has opened
	// a newer one - in an older one; what it drops was never acknowledged.
	TailTorn
	// TailDamaged: an intact frame follows bytes that are not one, or a complete frame whose body
	// passes its CRC will not decode. Not a torn tail: the bytes were changed in place, and
	// stopping at them would drop committed history.
	TailDamaged
)

func (k TailKind) String() string {
	switch k {
	case TailClean:
		return "clean"
	case TailTorn:
		return "torn"
	case TailDamaged:
		return "damaged"
	}
	return "unknown"
}

// SegmentTail describes where a delta segment's intact frames stop and what follows them.
type SegmentTail struct {
	Kind TailKind
	// Offset is where the intact frames stop: the segment's readable size.
	Offset int64
	// Reason says why the walk stopped there, for TailTorn and TailDamaged.
	Reason string
	// NextIntactFrame is the offset of the first intact frame after Offset, or -1 when there is
	// none. Set for TailDamaged.
	NextIntactFrame int64
}

// DocumentPatch is one document change in a delta record.
type DocumentPatch struct {
	DocID            codec.UUID
	Before           *document.Document
	After            *document.Document
	ContentHashAfter *codec.Hash
}
