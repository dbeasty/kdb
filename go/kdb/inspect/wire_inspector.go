package inspect

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/wire"
)

// WireFrameInspector produces debug JSON views of wire frames (non-authoritative).
type WireFrameInspector struct {
	codec *wire.DefaultCodec
}

// NewWireFrameInspector returns an inspector using the default wire codec.
func NewWireFrameInspector() *WireFrameInspector {
	return &WireFrameInspector{codec: wire.NewCodec(wire.EncodingKdbBinary)}
}

// DumpFrame decodes a wire frame and returns JSON for inspection.
func (w *WireFrameInspector) DumpFrame(frame []byte, pretty bool) (string, error) {
	header, err := w.codec.DecodeHeader(frame)
	if err != nil {
		return "", err
	}
	msg, err := w.codec.Decode(frame)
	if err != nil {
		return "", err
	}
	out := map[string]any{
		"header": map[string]any{
			"messageType":     header.MessageType.String(),
			"protocolVersion": header.ProtocolVersion,
			"correlationId":   header.CorrelationID,
			"payloadLength":   header.PayloadLength,
		},
		"body": map[string]any{
			"type":          "wire",
			"direction":     "capture",
			"messageType":   header.MessageType.String(),
			"correlationId": header.CorrelationID,
			"message":       msg,
		},
	}
	if pretty {
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DumpFrameOrError returns JSON or an error object string.
func DumpFrameOrError(frame []byte, pretty bool) string {
	s, err := NewWireFrameInspector().DumpFrame(frame, pretty)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return s
}
