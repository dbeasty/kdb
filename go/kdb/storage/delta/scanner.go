package delta

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
)

const frameHeaderSize = PageFrameHeaderSize

// ScannedCommit is one commit frame in a delta segment.
type ScannedCommit struct {
	CommitHash  codec.Hash
	Commit      document.Commit
	FrameOffset int
}

// CorruptFrameError reports a delta frame whose stored CRC32 (written by
// PageCodec.Frame) does not match its actual body bytes, or whose body
// fails to parse despite fitting entirely within the scanned range - i.e.
// a frame that is not simply truncated (see the frameEnd > len(bytes)
// case in ScanSegmentBytes, handled separately and silently, since a
// short/missing tail is the expected shape of an unclean shutdown).
//
// A CRC mismatch on a frame that otherwise looks complete means either
// real corruption, or (also plausible after an unclean shutdown - see
// kdb-spec-layer13 Component 47 §4.3) a frame whose length header landed
// on disk before its body did. ScanSegmentBytes cannot tell those apart
// by itself; it reports the offset and lets the caller decide based on
// context it doesn't have (is this the most recently written segment?).
type CorruptFrameError struct {
	Offset int
	Reason string
}

func (e *CorruptFrameError) Error() string {
	return fmt.Sprintf("delta segment: corrupt frame at offset %d: %s", e.Offset, e.Reason)
}

// ScanSegmentBytes scans delta segment bytes (sequential KDBP frames). Each
// frame records its own codec (see PageCodec), so no codec argument is needed
// or accepted - a segment may even mix codecs.
//
// On a CorruptFrameError, the returned slice still holds every commit
// scanned successfully *before* the corrupt frame - callers that want
// torn-tail-tolerant behavior (see CorruptFrameError's doc comment) use
// that partial result rather than discarding it.
func ScanSegmentBytes(bytes []byte) ([]ScannedCommit, error) {
	var out []ScannedCommit
	var codec PageCodec
	offset := 0
	for offset+frameHeaderSize <= len(bytes) {
		if !isKdbpFrame(bytes, offset) {
			break
		}
		compressedSize := readIntBE(bytes, offset+8)
		if compressedSize < 0 {
			// A negative length can only come from a garbled/torn header -
			// there is no valid frame here or after it in this segment.
			break
		}
		frameEnd := offset + frameHeaderSize + compressedSize
		if frameEnd > len(bytes) {
			// Declared frame doesn't fully fit: a torn tail, the expected
			// shape of an unclean shutdown (the write that created this
			// frame never completed). Not an error - stop cleanly with
			// whatever scanned before it.
			break
		}
		frame := bytes[offset:frameEnd]
		storedCRC := uint32(readIntBE(frame, 16))
		actualCRC := compression.CRC32All(frame[frameHeaderSize:])
		if actualCRC != storedCRC {
			return out, &CorruptFrameError{
				Offset: offset,
				Reason: fmt.Sprintf("crc mismatch: stored=%08x actual=%08x", storedCRC, actualCRC),
			}
		}
		payload, err := codec.Parse(frame)
		if err != nil {
			return out, &CorruptFrameError{Offset: offset, Reason: err.Error()}
		}
		commit, err := document.FromPayloadBytes(payload)
		if err != nil {
			return out, &CorruptFrameError{Offset: offset, Reason: err.Error()}
		}
		out = append(out, ScannedCommit{
			CommitHash:  commit.Hash,
			Commit:      commit,
			FrameOffset: offset,
		})
		offset = frameEnd
	}
	return out, nil
}

func isKdbpFrame(bytes []byte, offset int) bool {
	return bytes[offset] == 0x4B && bytes[offset+1] == 0x44 &&
		bytes[offset+2] == 0x42 && bytes[offset+3] == 0x50
}

func readIntBE(bytes []byte, offset int) int {
	return (int(bytes[offset])&0xFF)<<24 |
		(int(bytes[offset+1])&0xFF)<<16 |
		(int(bytes[offset+2])&0xFF)<<8 |
		int(bytes[offset+3])&0xFF
}
