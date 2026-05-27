package wire

import "encoding/json"

// Codec encodes and decodes length-prefixed wire frames.
type Codec interface {
	Encoding() PayloadEncoding
	Encode(message Message) ([]byte, error)
	Decode(frame []byte) (Message, error)
	EncodeFrameOnly(header Header, payload []byte) ([]byte, error)
	DecodeHeader(frame []byte) (Header, error)
}

// DefaultCodec is the standard JSON-envelope wire codec (v1).
type DefaultCodec struct {
	encoding PayloadEncoding
}

// NewCodec returns the default wire codec for the given payload encoding tag.
func NewCodec(encoding PayloadEncoding) *DefaultCodec {
	return &DefaultCodec{encoding: encoding}
}

func (c *DefaultCodec) Encoding() PayloadEncoding { return c.encoding }

func (c *DefaultCodec) Encode(message Message) ([]byte, error) {
	env, err := messageToEnvelope(message)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 1+len(body))
	payload[0] = encodingTag(c.encoding)
	copy(payload[1:], body)
	h := message.Header()
	h.PayloadLength = len(payload)
	return EncodeFrameOnly(h, payload)
}

func (c *DefaultCodec) Decode(frame []byte) (Message, error) {
	header, err := DecodeHeader(frame)
	if err != nil {
		return nil, err
	}
	payloadOffset := FrameHeaderSize
	if len(frame) < payloadOffset+1 {
		return nil, newDecodeError("frame too short for payload")
	}
	tag := frame[payloadOffset]
	body := frame[payloadOffset+1 : payloadOffset+header.PayloadLength]
	if tag != 0 && tag != 1 {
		return nil, newDecodeError("unsupported encoding tag")
	}
	var env PayloadEnvelope
	if err := unmarshalJSON(body, &env); err != nil {
		return nil, newDecodeError("invalid payload json")
	}
	return envelopeToMessage(header, env)
}

func (c *DefaultCodec) EncodeFrameOnly(header Header, payload []byte) ([]byte, error) {
	return EncodeFrameOnly(header, payload)
}

func (c *DefaultCodec) DecodeHeader(frame []byte) (Header, error) {
	return DecodeHeader(frame)
}
