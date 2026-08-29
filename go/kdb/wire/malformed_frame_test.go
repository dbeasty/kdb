package wire_test

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

// framePrefix builds a syntactically valid 12-byte header in a buffer of bufLen bytes, whose
// length prefix declares declaredLen. When declaredLen != bufLen the frame is malformed in
// exactly the way a hostile or corrupt peer produces.
func framePrefix(t *testing.T, bufLen, declaredLen int, msgType wire.MessageType) []byte {
	t.Helper()
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf[0:], uint32(int32(declaredLen)))
	binary.LittleEndian.PutUint16(buf[4:], uint16(msgType))
	binary.LittleEndian.PutUint16(buf[6:], wire.KdbWireProtocolVersion)
	binary.LittleEndian.PutUint32(buf[8:], 7)
	return buf
}

// A frame whose prefix claims far more bytes than it carries must be a decode error, not a
// slice-bounds panic. Every caller derives the payload slice from PayloadLength, which is
// computed from the declared length, so this used to panic and - with no recover() anywhere on
// the connection-handling path - take the whole process down with it.
func TestDecodeRejectsDeclaredLengthLongerThanBuffer(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	frame := framePrefix(t, 20, 1000, wire.MsgHandshake)

	msg, err := c.Decode(frame)
	if err == nil {
		t.Fatalf("expected a decode error, got message %#v", msg)
	}
	if !strings.Contains(err.Error(), "declared length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeHeaderRejectsDeclaredLengthLongerThanBuffer(t *testing.T) {
	if _, err := wire.DecodeHeader(framePrefix(t, 20, 1000, wire.MsgHandshake)); err == nil {
		t.Fatal("expected a decode error for a header declaring more bytes than the buffer holds")
	}
	// One byte short is still short: the boundary is exact, not approximate.
	if _, err := wire.DecodeHeader(framePrefix(t, 19, 20, wire.MsgHandshake)); err == nil {
		t.Fatal("expected a decode error for a buffer one byte shorter than declared")
	}
	// An exact-length frame with a real payload byte still decodes its header fine.
	exact := framePrefix(t, 13, 13, wire.MsgHandshake)
	h, err := wire.DecodeHeader(exact)
	if err != nil {
		t.Fatalf("exact-length frame: %v", err)
	}
	if h.PayloadLength != 1 {
		t.Fatalf("payloadLength: got %d, want 1", h.PayloadLength)
	}
	// A buffer longer than its declared length is legal for DecodeHeader - the frame readers
	// hand over exact buffers, but nothing about the header itself is wrong.
	if _, err := wire.DecodeHeader(framePrefix(t, 64, 13, wire.MsgHandshake)); err != nil {
		t.Fatalf("over-long buffer with a valid declared length: %v", err)
	}
}

// A frame declaring a zero-length payload while carrying trailing bytes gets past the
// "too short for payload" guard and would slice frame[13:12] - low above high, which panics
// exactly like an out-of-range high bound.
func TestDecodeRejectsEmptyDeclaredPayload(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	if _, err := c.Decode(framePrefix(t, 64, wire.FrameHeaderSize, wire.MsgHandshake)); err == nil {
		t.Fatal("expected a decode error for a frame declaring an empty payload")
	}
}

// DecodePayloadEnvelope takes its header from the caller instead of re-deriving it, so it has
// to re-check the bound itself: a header that does not belong to the frame it is passed with
// must be an error rather than a panic. kdb-inspect pairs the two from separate calls.
func TestDecodePayloadEnvelopeRejectsMismatchedHeader(t *testing.T) {
	frame := framePrefix(t, 64, 64, wire.MsgHandshake)
	header, err := wire.DecodeHeader(frame)
	if err != nil {
		t.Fatal(err)
	}
	header.PayloadLength = 10_000 // as if it came from a different, larger frame
	if _, err := wire.DecodePayloadEnvelope(frame, header); err == nil {
		t.Fatal("expected an error for a header whose payload does not fit the frame")
	}
	header.PayloadLength = 0
	if _, err := wire.DecodePayloadEnvelope(frame, header); err == nil {
		t.Fatal("expected an error for a header declaring an empty payload")
	}
}

// The unknown-type and oversized rejections should stay ahead of any payload handling, so a
// garbage frame is refused on its header alone.
func TestDecodeHeaderRejectsUnknownMessageType(t *testing.T) {
	frame := framePrefix(t, 32, 32, wire.MessageType(0x7FFF))
	if _, err := wire.DecodeHeader(frame); err == nil {
		t.Fatal("expected an error for an unknown message type code")
	}
}

func TestDecodeHeaderRejectsNegativeAndOversizedLengths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared int
	}{
		{"negative", -1},
		{"below header size", 4},
		{"above max frame bytes", wire.DefaultMaxFrameBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := wire.DecodeHeader(framePrefix(t, 32, tc.declared, wire.MsgHandshake)); err == nil {
				t.Fatalf("expected an error for declared length %d", tc.declared)
			}
		})
	}
}
