package wire

import (
	"encoding/binary"
)

// ValidateFrameLength checks total frame size bounds.
func ValidateFrameLength(length, maxFrameBytes int) error {
	if length < FrameHeaderSize || length > maxFrameBytes {
		return newFrameTooLarge(length, maxFrameBytes)
	}
	return nil
}

func readInt32LE(buf []byte, offset int) int32 {
	return int32(binary.LittleEndian.Uint32(buf[offset:]))
}

func readInt16LE(buf []byte, offset int) int16 {
	return int16(binary.LittleEndian.Uint16(buf[offset:]))
}

func writeInt32LE(buf []byte, offset int, value int32) {
	binary.LittleEndian.PutUint32(buf[offset:], uint32(value))
}

func writeInt16LE(buf []byte, offset int, value int16) {
	binary.LittleEndian.PutUint16(buf[offset:], uint16(value))
}

// DecodeHeader parses the fixed 12-byte frame header.
func DecodeHeader(frame []byte) (Header, error) {
	if len(frame) < FrameHeaderSize {
		return Header{}, newDecodeError("frame shorter than header")
	}
	frameLength := int(readInt32LE(frame, 0))
	if err := ValidateFrameLength(frameLength, DefaultMaxFrameBytes); err != nil {
		return Header{}, err
	}
	// The declared length has to be checked against the buffer we were actually handed, not
	// just against the protocol maximum. PayloadLength is derived from frameLength and every
	// caller slices the payload out with it, so a frame whose prefix claims more bytes than it
	// carries used to panic with a slice-bounds error rather than return a decode error. The
	// TCP/stream framing reader never produces such a buffer (it only emits a frame once
	// frameLength bytes have arrived), but a WebSocket message is delivered whole and
	// unvalidated, and captured frames fed to kdb-inspect can be truncated by whatever wrote
	// them - both reach here with an attacker- or corruption-controlled prefix.
	if len(frame) < frameLength {
		return Header{}, newDecodeError("frame shorter than its declared length")
	}
	typeCode := uint16(readInt16LE(frame, 4))
	msgType, ok := MessageTypeFromCode(typeCode)
	if !ok {
		return Header{}, newDecodeError("unknown message type")
	}
	protocolVersion := int(readInt16LE(frame, 6))
	correlationID := int(readInt32LE(frame, 8))
	payloadLength := frameLength - FrameHeaderSize
	return Header{
		MessageType:     msgType,
		ProtocolVersion: protocolVersion,
		CorrelationID:   correlationID,
		PayloadLength:   payloadLength,
	}, nil
}

// EncodeFrameOnly writes a complete length-prefixed frame with the given header and payload.
func EncodeFrameOnly(header Header, payload []byte) ([]byte, error) {
	totalLength := FrameHeaderSize + len(payload)
	if err := ValidateFrameLength(totalLength, DefaultMaxFrameBytes); err != nil {
		return nil, err
	}
	out := make([]byte, totalLength)
	writeInt32LE(out, 0, int32(totalLength))
	writeInt16LE(out, 4, int16(header.MessageType))
	writeInt16LE(out, 6, int16(header.ProtocolVersion))
	writeInt32LE(out, 8, int32(header.CorrelationID))
	copy(out[FrameHeaderSize:], payload)
	return out, nil
}

func encodingTag(enc PayloadEncoding) byte {
	if enc == EncodingJSON {
		return 1
	}
	return 0
}
