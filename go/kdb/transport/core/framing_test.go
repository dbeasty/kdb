package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"testing"

	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// prefixOnly returns just a 4-byte little-endian length prefix - the smallest input an attacker
// can send that forces the parser to commit to a frame length.
func prefixOnly(length int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(length))
	return buf
}

// rawFrame builds a frame whose 4-byte prefix is `length` but whose actual buffer is `total`
// bytes, so tests can construct both honest frames (length == total) and lying ones.
func rawFrame(length int32, total int) []byte {
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf, uint32(length))
	for i := 4; i < total; i++ {
		buf[i] = byte(i) // recognizable non-zero body so corruption would show
	}
	return buf
}

// encodedTestFrame builds a real wire frame the way every production sender does (via
// wire.EncodeFrameOnly), so the round-trip test exercises the actual encode format rather than a
// hand-rolled imitation of it.
func encodedTestFrame(t *testing.T, correlationID int, payload []byte) []byte {
	t.Helper()
	frame, err := wire.EncodeFrameOnly(wire.Header{
		MessageType:     wire.MsgPositionAck,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   correlationID,
	}, payload)
	if err != nil {
		t.Fatalf("EncodeFrameOnly: %v", err)
	}
	return frame
}

// TestFrameRoundTripThroughStreamReader is the basic contract: a frame produced by the real
// encoder, fed to the stream reader in one piece, comes back byte-identical and decodes to the
// same header and payload. Everything else in this file is a variation on how those bytes arrive.
func TestFrameRoundTripThroughStreamReader(t *testing.T) {
	payload := []byte(`{"namespace":"app/data"}`)
	frame := encodedTestFrame(t, 42, payload)

	reader := NewFrameStreamReader(0)
	frames, err := reader.Feed(frame)
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !bytes.Equal(frames[0], frame) {
		t.Fatalf("frame not returned byte-identical:\n got %x\nwant %x", frames[0], frame)
	}
	header, err := wire.DecodeHeader(frames[0])
	if err != nil {
		t.Fatalf("DecodeHeader on round-tripped frame: %v", err)
	}
	if header.MessageType != wire.MsgPositionAck || header.CorrelationID != 42 {
		t.Fatalf("header round trip: got type=%v corr=%d", header.MessageType, header.CorrelationID)
	}
	if header.PayloadLength != len(payload) {
		t.Fatalf("payload length: got %d, want %d", header.PayloadLength, len(payload))
	}
	if !bytes.Equal(frames[0][wire.FrameHeaderSize:], payload) {
		t.Fatalf("payload bytes differ after round trip")
	}
	if reader.BufferedBytes() != 0 {
		t.Fatalf("reader kept %d bytes after a complete frame", reader.BufferedBytes())
	}
}

// TestFrameSplitAcrossReads feeds a frame one byte at a time - the worst possible TCP
// fragmentation, including splits inside the 4-byte length prefix itself. The reader must emit
// nothing until the very last byte and then exactly one intact frame. This is the partial-delivery
// property real sockets rely on.
func TestFrameSplitAcrossReads(t *testing.T) {
	frame := encodedTestFrame(t, 7, []byte("split-me"))
	reader := NewFrameStreamReader(0)
	for i := 0; i < len(frame)-1; i++ {
		frames, err := reader.Feed(frame[i : i+1])
		if err != nil {
			t.Fatalf("Feed byte %d: %v", i, err)
		}
		if len(frames) != 0 {
			t.Fatalf("frame emitted early after %d of %d bytes", i+1, len(frame))
		}
		if reader.BufferedBytes() != i+1 {
			t.Fatalf("BufferedBytes after %d bytes: got %d", i+1, reader.BufferedBytes())
		}
	}
	frames, err := reader.Feed(frame[len(frame)-1:])
	if err != nil {
		t.Fatalf("Feed final byte: %v", err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], frame) {
		t.Fatalf("final byte did not complete the frame intact (got %d frames)", len(frames))
	}
	if reader.BufferedBytes() != 0 {
		t.Fatalf("reader kept %d bytes after completing the frame", reader.BufferedBytes())
	}
}

// TestMultipleFramesInOneChunk covers the opposite of fragmentation: coalescing. One read can
// deliver several complete frames plus the beginning of the next; the reader must emit all
// complete frames in order and hold the remainder for the next Feed.
func TestMultipleFramesInOneChunk(t *testing.T) {
	a := encodedTestFrame(t, 1, []byte("first"))
	b := encodedTestFrame(t, 2, []byte("second"))
	c := encodedTestFrame(t, 3, []byte("third"))

	chunk := append(append(append([]byte{}, a...), b...), c[:5]...)
	reader := NewFrameStreamReader(0)
	frames, err := reader.Feed(chunk)
	if err != nil {
		t.Fatalf("Feed coalesced chunk: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("got %d frames from coalesced chunk, want 2", len(frames))
	}
	if !bytes.Equal(frames[0], a) || !bytes.Equal(frames[1], b) {
		t.Fatalf("frames emitted out of order or corrupted")
	}
	if reader.BufferedBytes() != 5 {
		t.Fatalf("remainder: got %d buffered bytes, want 5", reader.BufferedBytes())
	}
	frames, err = reader.Feed(c[5:])
	if err != nil {
		t.Fatalf("Feed rest of third frame: %v", err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], c) {
		t.Fatalf("third frame not reassembled from remainder")
	}
}

// TestOversizedFrameRejected: a prefix just over the configured limit must fail with
// FrameTooLargeError before any body arrives, and must drop the poisoned buffer so the error is
// not re-reported forever. This is the primary DoS guard for a hostile peer.
func TestOversizedFrameRejected(t *testing.T) {
	const max = 64
	reader := NewFrameStreamReader(max)
	_, err := reader.Feed(prefixOnly(max + 1))
	if err == nil {
		t.Fatal("oversized prefix accepted")
	}
	var tooLarge *wire.FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("got %T (%v), want *wire.FrameTooLargeError", err, err)
	}
	if tooLarge.Length != max+1 || tooLarge.Max != max {
		t.Fatalf("error fields: got length=%d max=%d, want %d/%d", tooLarge.Length, tooLarge.Max, max+1, max)
	}
	if reader.BufferedBytes() != 0 {
		t.Fatalf("poisoned bytes retained after error: %d", reader.BufferedBytes())
	}

	// A frame exactly at the limit is legal - the bound is inclusive.
	frames, err := reader.Feed(rawFrame(max, max))
	if err != nil {
		t.Fatalf("frame exactly at max rejected: %v", err)
	}
	if len(frames) != 1 || len(frames[0]) != max {
		t.Fatalf("at-limit frame not delivered")
	}
}

// TestGiantLengthPrefixDoesNotAllocate is the over-allocation guard: 4 bytes claiming a ~2GiB
// frame must produce an error, not a 2GiB make([]byte). We assert both that the error is
// immediate (no waiting for a body that will never come) and that heap allocation stayed sane -
// TotalAlloc is monotonic, so a giant up-front allocation cannot hide from it.
func TestGiantLengthPrefixDoesNotAllocate(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	reader := NewFrameStreamReader(0) // default 16 MiB limit
	_, err := reader.Feed(prefixOnly(0x7FFFFFFF))
	if err == nil {
		t.Fatal("2GiB length prefix accepted")
	}

	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 16*1024*1024 {
		t.Fatalf("rejecting a giant prefix allocated %d bytes", delta)
	}
}

// TestNegativeLengthPrefixRejected: the prefix is decoded as a signed int32, so 0xFFFFFFFF is -1.
// A negative "length" must be a clean error, never a panic or a negative-size allocation.
func TestNegativeLengthPrefixRejected(t *testing.T) {
	reader := NewFrameStreamReader(0)
	_, err := reader.Feed(prefixOnly(-1))
	if err == nil {
		t.Fatal("negative length prefix accepted")
	}
}

// TestZeroAndSubHeaderLengthRejected: the frame length counts the 12-byte header itself, so any
// prefix below wire.FrameHeaderSize describes an impossible frame. Accepting length 4 (or 0)
// would make drainCompleteFrames loop forever emitting empty frames from the same bytes.
func TestZeroAndSubHeaderLengthRejected(t *testing.T) {
	for _, length := range []int32{0, 1, 4, int32(wire.FrameHeaderSize) - 1} {
		reader := NewFrameStreamReader(0)
		_, err := reader.Feed(prefixOnly(length))
		if err == nil {
			t.Fatalf("length prefix %d accepted; minimum legal frame is %d bytes", length, wire.FrameHeaderSize)
		}
	}
}

// TestHeaderOnlyMinimumFrame: a frame of exactly wire.FrameHeaderSize (empty payload) is the
// smallest legal frame and must pass through.
func TestHeaderOnlyMinimumFrame(t *testing.T) {
	frame := encodedTestFrame(t, 9, nil)
	if len(frame) != wire.FrameHeaderSize {
		t.Fatalf("encoder produced %d bytes for empty payload, want %d", len(frame), wire.FrameHeaderSize)
	}
	reader := NewFrameStreamReader(0)
	frames, err := reader.Feed(frame)
	if err != nil {
		t.Fatalf("Feed header-only frame: %v", err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], frame) {
		t.Fatalf("header-only frame not delivered intact")
	}
}

// TestDefaultMaxFrameBytesApplied: passing 0 must fall back to wire.DefaultMaxFrameBytes rather
// than "no limit" - a reader accidentally constructed with the zero value must still be bounded.
func TestDefaultMaxFrameBytesApplied(t *testing.T) {
	reader := NewFrameStreamReader(0)
	if _, err := reader.Feed(prefixOnly(int32(wire.DefaultMaxFrameBytes) + 1)); err == nil {
		t.Fatal("frame above wire.DefaultMaxFrameBytes accepted by zero-configured reader")
	}
}

// TestResetDiscardsPartialFrame: Reset is what a transport calls on reconnect - buffered bytes
// from the dead connection must not be prepended to the new one's stream.
func TestResetDiscardsPartialFrame(t *testing.T) {
	frame := encodedTestFrame(t, 5, []byte("stale"))
	reader := NewFrameStreamReader(0)
	if _, err := reader.Feed(frame[:7]); err != nil {
		t.Fatalf("Feed partial: %v", err)
	}
	reader.Reset()
	if reader.BufferedBytes() != 0 {
		t.Fatalf("Reset left %d buffered bytes", reader.BufferedBytes())
	}
	// A fresh, complete frame after Reset must parse cleanly, not be corrupted by stale bytes.
	frames, err := reader.Feed(frame)
	if err != nil || len(frames) != 1 || !bytes.Equal(frames[0], frame) {
		t.Fatalf("frame after Reset not delivered intact (err=%v, frames=%d)", err, len(frames))
	}
}

// TestValidateOutgoingFrame covers the send-side guard used by the tcp and ws transports: it must
// reject buffers too short to even hold a prefix, prefixes that disagree with the actual buffer
// size (a corrupted or mis-encoded frame), and frames over the limit.
func TestValidateOutgoingFrame(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		frame := encodedTestFrame(t, 1, []byte("ok"))
		if err := ValidateOutgoingFrame(frame, wire.DefaultMaxFrameBytes); err != nil {
			t.Fatalf("valid frame rejected: %v", err)
		}
	})
	t.Run("shorter than prefix", func(t *testing.T) {
		for _, n := range []int{0, 1, 3} {
			if err := ValidateOutgoingFrame(make([]byte, n), wire.DefaultMaxFrameBytes); err == nil {
				t.Fatalf("%d-byte buffer accepted", n)
			}
		}
	})
	t.Run("prefix disagrees with buffer size", func(t *testing.T) {
		// Prefix says 16 bytes but the buffer holds 20 - sending it would desync the peer's
		// stream, so it must be caught before it hits the wire.
		if err := ValidateOutgoingFrame(rawFrame(16, 20), wire.DefaultMaxFrameBytes); err == nil {
			t.Fatal("prefix/buffer length mismatch accepted")
		}
	})
	t.Run("oversized", func(t *testing.T) {
		if err := ValidateOutgoingFrame(rawFrame(128, 128), 64); err == nil {
			t.Fatal("frame above max accepted")
		}
	})
}

// chunkScript returns a readChunk func for ReadFrame that serves the given chunks in order and
// then reports EOF (nil), recording how many reads were made.
func chunkScript(chunks ...[]byte) (func() ([]byte, error), *int) {
	i := 0
	reads := 0
	return func() ([]byte, error) {
		reads++
		if i >= len(chunks) {
			return nil, nil
		}
		c := chunks[i]
		i++
		return c, nil
	}, &reads
}

// TestReadFrameAcrossChunkedReads: ReadFrame must keep pulling chunks until one frame is
// complete, tolerating a split inside the length prefix.
func TestReadFrameAcrossChunkedReads(t *testing.T) {
	frame := encodedTestFrame(t, 11, []byte("chunked"))
	readChunk, _ := chunkScript(frame[:2], frame[2:6], frame[6:])
	got, err := ReadFrame(0, readChunk)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("reassembled frame differs from sent frame")
	}
}

// TestReadFrameCleanEOF: EOF with nothing buffered is a normal connection close - (nil, nil), not
// an error.
func TestReadFrameCleanEOF(t *testing.T) {
	readChunk, _ := chunkScript()
	got, err := ReadFrame(0, readChunk)
	if err != nil {
		t.Fatalf("clean EOF returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("clean EOF returned a frame: %x", got)
	}
}

// TestReadFrameEOFMidFrame: EOF with a partial frame buffered means the peer died (or lied about
// the length) mid-message. That must surface as ConnectionClosedError carrying the buffered byte
// count - silently returning nil would hide data truncation.
func TestReadFrameEOFMidFrame(t *testing.T) {
	frame := encodedTestFrame(t, 3, []byte("truncated"))
	partial := frame[:len(frame)-4]
	readChunk, _ := chunkScript(partial)
	_, err := ReadFrame(0, readChunk)
	if err == nil {
		t.Fatal("EOF mid-frame returned no error")
	}
	var closed *kdberr.ConnectionClosedError
	if !errors.As(err, &closed) {
		t.Fatalf("got %T (%v), want *kdberr.ConnectionClosedError", err, err)
	}
	if closed.BufferedBytes != len(partial) {
		t.Fatalf("BufferedBytes: got %d, want %d", closed.BufferedBytes, len(partial))
	}
}

// TestReadFrameOversizedFailsBeforeMoreReads: a hostile length prefix must abort ReadFrame on the
// spot - it must not keep issuing reads trying to satisfy a 2GiB frame.
func TestReadFrameOversizedFailsBeforeMoreReads(t *testing.T) {
	readChunk, reads := chunkScript(prefixOnly(0x7FFFFFFF), []byte("should never be read"))
	_, err := ReadFrame(0, readChunk)
	if err == nil {
		t.Fatal("giant prefix accepted by ReadFrame")
	}
	if *reads != 1 {
		t.Fatalf("ReadFrame kept reading after a fatal prefix: %d reads", *reads)
	}
}

// TestReadFramePropagatesReadError: an I/O error from the underlying read must come back
// unwrapped-or-wrapped but non-nil - never be swallowed into a nil frame.
func TestReadFramePropagatesReadError(t *testing.T) {
	boom := fmt.Errorf("socket exploded")
	_, err := ReadFrame(0, func() ([]byte, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("read error not propagated: got %v", err)
	}
}

// TestDefaultConnectOptions pins the default frame cap to the wire package's constant so a
// refactor cannot silently decouple the two limits.
func TestDefaultConnectOptions(t *testing.T) {
	opts := DefaultConnectOptions()
	if opts.MaxFrameBytes != wire.DefaultMaxFrameBytes {
		t.Fatalf("MaxFrameBytes: got %d, want %d", opts.MaxFrameBytes, wire.DefaultMaxFrameBytes)
	}
}
