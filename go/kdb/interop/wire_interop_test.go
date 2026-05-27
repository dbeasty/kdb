package interop

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

func TestWireHandshakeInteropShape(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	hash := strings.Repeat("00", 64)
	msg := wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: 1, CorrelationID: 1},
		Request: wire.HandshakePayload{
			NodeID:     "node-a",
			Namespaces: []string{"app/data"},
			LocalHeads: map[string]string{"app/data": hash},
			ClientMode: wire.ClientStreamReadOnly,
		},
	}
	enc, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := c.Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	back, ok := dec.(wire.HandshakeMessage)
	if !ok || back.Request.NodeID != "node-a" {
		t.Fatalf("got %T %+v", dec, dec)
	}
}
