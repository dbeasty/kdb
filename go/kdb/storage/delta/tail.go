package delta

import (
	"bytes"
	"fmt"

	"github.com/limidus/kdb/go/kdb/storage"
)

// How a segment's frames end, and whether anything intact follows the point where they stop -
// the one rule (storage.TailKind) every reader of the delta log applies (N5).
//
// Before this, every scanner stopped a segment at its first garbled header or CRC mismatch and
// recorded only the bytes before it, so damage in the middle of a segment read exactly like a
// torn tail at its end: replay dropped every commit after it, with no error, and its "corrupt and
// not the most recent segment" refusal could never fire.

// ScanSegment walks a whole segment's bytes, decoding every frame, and classifies how its intact
// frames end (see storage.TailKind). Damage is reported in the tail rather than as an error, and
// commits holds every commit before the stop.
func ScanSegment(raw []byte) ([]ScannedCommit, storage.SegmentTail) {
	var commits []ScannedCommit
	stop, err := scanFrames(raw, func(s ScannedCommit) error {
		commits = append(commits, s)
		return nil
	})
	tail := storage.SegmentTail{Offset: int64(stop), NextIntactFrame: -1}
	if corrupt, ok := err.(*CorruptFrameError); ok {
		tail.Reason = corrupt.Reason
		tail.NextIntactFrame = int64(corrupt.NextIntactFrame)
		tail.Kind = storage.TailDamaged
		if corrupt.Torn() {
			tail.Kind = storage.TailTorn
		}
		return commits, tail
	}
	if stop < len(raw) && !allZero(raw[stop:]) {
		tail.Kind = storage.TailTorn
		tail.Reason = describeStop(raw[stop:])
	}
	return commits, tail
}

// describeStop says why a walk could not read a frame at the start of rest.
func describeStop(rest []byte) string {
	switch {
	case len(rest) < frameHeaderSize:
		return fmt.Sprintf("%d byte(s) after the last frame, too few for a frame header", len(rest))
	case !isKdbpFrame(rest, 0):
		return "no frame header here (magic missing)"
	case readIntBE(rest, 8) <= 0:
		return "frame header declares an empty body"
	default:
		return fmt.Sprintf("frame declares %d body byte(s) but only %d follow its header",
			readIntBE(rest, 8), len(rest)-frameHeaderSize)
	}
}

var kdbpMagic = []byte{0x4B, 0x44, 0x42, 0x50}

// validFrameHeader reports whether header (at least frameHeaderSize bytes) is a well-formed v2
// frame header, and the body length it declares.
func validFrameHeader(header []byte) (int, bool) {
	if len(header) < frameHeaderSize || !isKdbpFrame(header, 0) {
		return 0, false
	}
	if header[4] != PageFormatVersion ||
		(header[5] != pageCodecNone && header[5] != pageCodecZSTD) ||
		header[6] != 0 || header[7] != 0 {
		return 0, false
	}
	n := readIntBE(header, 8)
	if n <= 0 || n > maxScannedSegmentBytes {
		return 0, false
	}
	return n, true
}

// intactFrameAt reports whether a complete frame with a well-formed header and a body matching
// its CRC starts at offset, and where it ends.
//
// Stricter than frameBounds, which is asked "where does the frame that should be here end". This
// is asked "is this really a frame" of bytes that may be anything, so it also checks the version,
// codec and reserved bytes and refuses an empty body. A stray "KDBP" inside other bytes then only
// passes if its next 16 bytes happen to form a valid header and the bytes after them happen to
// match the CRC it declares.
func intactFrameAt(raw []byte, offset int) (int, bool) {
	if offset < 0 || offset+frameHeaderSize > len(raw) {
		return 0, false
	}
	n, ok := validFrameHeader(raw[offset : offset+frameHeaderSize])
	if !ok || n > len(raw)-offset-frameHeaderSize {
		return 0, false
	}
	end := offset + frameHeaderSize + n
	if verifyFrameCRC(raw[offset:end], offset) != nil {
		return 0, false
	}
	return end, true
}

// NextIntactFrame returns the offset of the first intact frame (see intactFrameAt) starting
// strictly after from, or -1 if there is none. It searches for the frame magic byte by byte
// rather than following frame lengths, so it finds frames past a garbled header as well as past a
// damaged body.
func NextIntactFrame(raw []byte, from int) int {
	i := from + 1
	if i < 0 {
		i = 0
	}
	for i+frameHeaderSize <= len(raw) {
		j := bytes.Index(raw[i:], kdbpMagic)
		if j < 0 {
			return -1
		}
		c := i + j
		if _, ok := intactFrameAt(raw, c); ok {
			return c
		}
		i = c + 1
	}
	return -1
}

// countIntactFramesFrom counts the intact frames from start on (start itself included; -1 means
// none), stepping past damage between them.
func countIntactFramesFrom(raw []byte, start int) int {
	n := 0
	off := start
	for off >= 0 && off < len(raw) {
		if end, ok := intactFrameAt(raw, off); ok {
			n++
			off = end
			continue
		}
		off = NextIntactFrame(raw, off)
	}
	return n
}

// newCorruptFrameError builds the error for a fault at offset in raw, counting what follows it.
func newCorruptFrameError(raw []byte, offset int, reason string, undecodable bool) *CorruptFrameError {
	next := NextIntactFrame(raw, offset)
	return &CorruptFrameError{
		Offset:            offset,
		Reason:            reason,
		IntactFramesAfter: countIntactFramesFrom(raw, next),
		NextIntactFrame:   next,
		Undecodable:       undecodable,
	}
}

var zeroBlock = make([]byte, 64<<10)

func allZero(b []byte) bool {
	for len(b) > 0 {
		n := len(b)
		if n > len(zeroBlock) {
			n = len(zeroBlock)
		}
		if !bytes.Equal(b[:n], zeroBlock[:n]) {
			return false
		}
		b = b[n:]
	}
	return true
}

// DamagedSegmentError reports a delta segment whose intact frames stop at damage that is not a
// torn tail: intact frames follow it, or the frame there passes its CRC and will not decode, or
// (BuiltOn) commits in later segments descend from what its unreadable tail held. Replaying past
// it would silently drop committed history, so open refuses instead. Detect with errors.As.
type DamagedSegmentError struct {
	NamespaceID string
	Sequence    int64
	// Offset is where the segment's intact frames stop.
	Offset int64
	Reason string
	// NextIntactFrame is the offset of the first intact frame after the damage, or -1.
	NextIntactFrame int64
	// BuiltOn is set when the damage was found only because later commits name parents the log
	// no longer holds: the segment's tail looked torn, but a torn tail is never built on.
	BuiltOn bool
	// Err is the underlying fault, when there is one.
	Err error
}

func (e *DamagedSegmentError) Error() string {
	var what string
	switch {
	case e.BuiltOn:
		what = "its last frame(s) cannot be read, and commits in later segments build on them, so this is not a torn tail"
	case e.NextIntactFrame >= 0:
		what = fmt.Sprintf("intact frames follow the damage (the next at byte %d), so this is not a torn tail", e.NextIntactFrame)
	default:
		what = "the frame there is complete and passes its checksum but will not decode, so this is not a torn tail"
	}
	msg := fmt.Sprintf(
		"kdb: namespace %q: delta segment %d is damaged at byte %d (%s); %s, and opening past it "+
			"would silently drop committed history. Run 'kdb-inspect verify --data-dir <dir> --namespace %s' "+
			"to see the damage, then restore the namespace from a backup or a peer ('kdb-inspect restore'), "+
			"or 'kdb-inspect repair-segments', which repairs only what it can prove loses nothing",
		e.NamespaceID, e.Sequence, e.Offset, e.Reason, what, e.NamespaceID)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *DamagedSegmentError) Unwrap() error { return e.Err }
