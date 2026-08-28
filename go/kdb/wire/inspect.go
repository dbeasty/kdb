package wire

// PayloadEnvelope is the JSON payload body inside a wire frame.
type PayloadEnvelope = payloadEnvelope

// HandshakeDTO is the handshake section of a payload envelope.
type HandshakeDTO = handshakeDto

// DecodePayloadEnvelope extracts the JSON envelope from a complete frame.
func DecodePayloadEnvelope(frame []byte, header Header) (PayloadEnvelope, error) {
	payloadOffset := FrameHeaderSize
	if len(frame) < payloadOffset+1 {
		return PayloadEnvelope{}, newDecodeError("frame too short for payload")
	}
	// header is supplied by the caller rather than re-derived here, so it is not necessarily
	// consistent with frame - re-check the bound DecodeHeader would have enforced before
	// slicing, or a mismatched pair panics instead of erroring.
	if header.PayloadLength < 1 || payloadOffset+header.PayloadLength > len(frame) {
		return PayloadEnvelope{}, newDecodeError("payload length does not fit the frame")
	}
	body := frame[payloadOffset+1 : payloadOffset+header.PayloadLength]
	var env PayloadEnvelope
	if err := unmarshalJSON(body, &env); err != nil {
		return PayloadEnvelope{}, newDecodeError("invalid payload json")
	}
	return env, nil
}

func (env PayloadEnvelope) Summary() string {
	switch env.Kind {
	case "handshake":
		if env.Handshake != nil {
			return "handshake node=" + env.Handshake.NodeID
		}
	case "handshakeAck":
		return "handshakeAck"
	case "deltaCommit":
		if env.DeltaCommit != nil {
			return "deltaCommit ns=" + env.DeltaCommit.Namespace
		}
	case "commitFetch":
		return "commitFetch"
	case "commitPush":
		return "commitPush"
	case "positionAck":
		return "positionAck"
	default:
		return env.Kind
	}
	return env.Kind
}
