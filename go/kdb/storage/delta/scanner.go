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
//
// Holds the whole segment's commits - every version of every document it
// records - in memory at once. Callers that consume commits one at a time
// should use ScanSegmentFrames instead; see its doc comment for why that
// matters on the replay path.
func ScanSegmentBytes(bytes []byte) ([]ScannedCommit, error) {
	var out []ScannedCommit
	err := ScanSegmentFrames(bytes, func(s ScannedCommit) error {
		out = append(out, s)
		return nil
	})
	return out, err
}

// ScanSegmentFrames walks segment's frames in order, decoding each into a
// document.Commit and handing it to fn. ScanSegmentBytes is exactly this
// with an fn that appends to a slice.
//
// Callers that consume commits as they arrive use this instead, so a
// segment's worth of decoded commits never has to be resident at once. On
// the replay path that is the difference between allocating the arithmetic
// sum of every version a document has ever had and allocating one commit
// at a time - a store holding a single 1.4MB document rewritten 463 times
// opened with 382MB live and 7.7GB of churn before this existed
// (docs/benchmarks/open-cost.md).
//
// Error semantics match ScanSegmentBytes exactly: a torn tail (a frame
// whose declared length runs past the end of bytes, or a garbled header)
// stops the walk cleanly with a nil error, while a CRC mismatch or a frame
// that is complete but will not parse returns *CorruptFrameError. In every
// case each frame already handed to fn stands - that partial progress is
// what makes torn-tail tolerance work. An error returned by fn stops the
// walk and is propagated unwrapped.
func ScanSegmentFrames(bytes []byte, fn func(ScannedCommit) error) error {
	var codec PageCodec
	offset := 0
	for offset+frameHeaderSize <= len(bytes) {
		frameEnd, ok := frameBounds(bytes, offset)
		if !ok {
			// Torn tail or garbled header: no valid frame here or after
			// it. Not an error - stop cleanly with what came before.
			break
		}
		frame := bytes[offset:frameEnd]
		if err := verifyFrameCRC(frame, offset); err != nil {
			return err
		}
		payload, err := codec.Parse(frame)
		if err != nil {
			return &CorruptFrameError{Offset: offset, Reason: err.Error()}
		}
		commit, err := document.FromPayloadBytes(payload)
		if err != nil {
			return &CorruptFrameError{Offset: offset, Reason: err.Error()}
		}
		if err := fn(ScannedCommit{
			CommitHash:  commit.Hash,
			Commit:      commit,
			FrameOffset: offset,
		}); err != nil {
			return err
		}
		offset = frameEnd
	}
	return nil
}

// frameBounds returns the end offset of the frame starting at offset, and
// whether one is there at all. A false means "stop scanning": either the
// magic is absent, the declared length is garbled, or the frame runs past
// the end of bytes - all of them the expected shape of a torn tail rather
// than corruption (see CorruptFrameError).
func frameBounds(bytes []byte, offset int) (frameEnd int, ok bool) {
	if !isKdbpFrame(bytes, offset) {
		return 0, false
	}
	compressedSize := readIntBE(bytes, offset+8)
	if compressedSize < 0 {
		return 0, false
	}
	frameEnd = offset + frameHeaderSize + compressedSize
	if frameEnd > len(bytes) {
		return 0, false
	}
	return frameEnd, true
}

// verifyFrameCRC checks frame's body against the CRC32 its header records.
// Costs a pass over the compressed bytes and allocates nothing, which is
// what lets ScanSegmentBounds validate a whole segment without decoding it.
func verifyFrameCRC(frame []byte, offset int) error {
	storedCRC := uint32(readIntBE(frame, 16))
	actualCRC := compression.CRC32All(frame[frameHeaderSize:])
	if actualCRC != storedCRC {
		return &CorruptFrameError{
			Offset: offset,
			Reason: fmt.Sprintf("crc mismatch: stored=%08x actual=%08x", storedCRC, actualCRC),
		}
	}
	return nil
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

// FrameWalk reports the frames in a byte window without decoding any of
// them: how far the walk got, and where the first and last complete frames
// in the window start. offsetBase is added to the reported offsets so a
// caller scanning a segment in windows can report positions in the segment
// rather than in the window.
//
// Every frame's CRC is checked, which is what makes a windowed scan as
// strict as a whole-segment one; it costs a pass over the compressed bytes
// and allocates nothing.
type FrameWalk struct {
	FirstOffset int64
	LastOffset  int64
	Count       int
	// ConsumedEnd is the offset just past the last complete frame, which is
	// where the next window must start. A window that ends mid-frame leaves
	// this at the start of that frame rather than skipping it.
	ConsumedEnd int64
}

// WalkFrames scans window for complete frames, verifying each one's CRC.
// Torn or garbled frames stop the walk cleanly, as everywhere else.
func WalkFrames(window []byte, offsetBase int64) (FrameWalk, error) {
	w := FrameWalk{FirstOffset: -1, LastOffset: -1, ConsumedEnd: offsetBase}
	offset := 0
	for offset+frameHeaderSize <= len(window) {
		frameEnd, ok := frameBounds(window, offset)
		if !ok {
			break
		}
		if err := verifyFrameCRC(window[offset:frameEnd], int(offsetBase)+offset); err != nil {
			return w, err
		}
		if w.FirstOffset < 0 {
			w.FirstOffset = offsetBase + int64(offset)
		}
		w.LastOffset = offsetBase + int64(offset)
		w.Count++
		offset = frameEnd
		w.ConsumedEnd = offsetBase + int64(offset)
	}
	return w, nil
}

// SegmentBounds is what a DeltaSegmentRef needs to know about a segment:
// its first and last commit, and how many bytes of it are real frames.
type SegmentBounds struct {
	First      document.Commit
	Last       document.Commit
	FrameCount int
	ScannedEnd int
}

// ScanSegmentBounds walks every frame in bytes validating its CRC, but
// decodes only the first and the last into commits - which is all a
// storage.DeltaSegmentRef records.
//
// Listing a namespace's segments used to go through ScanSegmentBytes,
// decoding every commit in every segment purely to read two hashes off the
// ends, and then replay would read and decode all of them again. On a
// namespace whose history is large next to its live data that doubled the
// most expensive part of opening it for nothing.
//
// What this deliberately does not do is notice a frame that survives its
// CRC but will not parse. That combination means the writer framed
// something invalid rather than the disk losing bytes, and the paths that
// actually consume commits - ScanSegmentFrames, and ReadAll through it -
// still decode every frame and still report it, with the is-this-the-last-
// segment context needed to tell a torn tail from real corruption. Bounds
// scanning trades that one detection for not paying a full decode twice.
//
// Torn tails stop the walk cleanly, exactly as in ScanSegmentFrames; a CRC
// mismatch returns *CorruptFrameError. A segment with no frames at all
// returns a zero SegmentBounds and a nil error.
func ScanSegmentBounds(bytes []byte) (SegmentBounds, error) {
	var pc PageCodec
	var bounds SegmentBounds
	firstOffset, lastOffset := -1, -1
	offset := 0
	for offset+frameHeaderSize <= len(bytes) {
		frameEnd, ok := frameBounds(bytes, offset)
		if !ok {
			break
		}
		if err := verifyFrameCRC(bytes[offset:frameEnd], offset); err != nil {
			return bounds, err
		}
		if firstOffset < 0 {
			firstOffset = offset
		}
		lastOffset = offset
		bounds.FrameCount++
		offset = frameEnd
	}
	bounds.ScannedEnd = offset
	if firstOffset < 0 {
		return bounds, nil
	}
	first, err := decodeFrameAt(&pc, bytes, firstOffset)
	if err != nil {
		return bounds, err
	}
	bounds.First = first
	bounds.Last = first
	if lastOffset != firstOffset {
		last, err := decodeFrameAt(&pc, bytes, lastOffset)
		if err != nil {
			return bounds, err
		}
		bounds.Last = last
	}
	return bounds, nil
}

// DecodeFrame decodes exactly one KDBP frame that starts at bytes[0],
// validating its CRC. offset is only used to report where a fault was, so
// a caller that read the frame out of a larger segment can name the real
// position. See DefaultReader.ReadCommitAt.
func DecodeFrame(bytes []byte, offset int) (document.Commit, error) {
	frameEnd, ok := frameBounds(bytes, 0)
	if !ok {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: "no complete frame at this offset"}
	}
	if err := verifyFrameCRC(bytes[:frameEnd], offset); err != nil {
		return document.Commit{}, err
	}
	var pc PageCodec
	payload, err := pc.Parse(bytes[:frameEnd])
	if err != nil {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: err.Error()}
	}
	commit, err := document.FromPayloadBytes(payload)
	if err != nil {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: err.Error()}
	}
	return commit, nil
}

func decodeFrameAt(pc *PageCodec, bytes []byte, offset int) (document.Commit, error) {
	frameEnd, ok := frameBounds(bytes, offset)
	if !ok {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: "frame vanished between passes"}
	}
	payload, err := pc.Parse(bytes[offset:frameEnd])
	if err != nil {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: err.Error()}
	}
	commit, err := document.FromPayloadBytes(payload)
	if err != nil {
		return document.Commit{}, &CorruptFrameError{Offset: offset, Reason: err.Error()}
	}
	return commit, nil
}
