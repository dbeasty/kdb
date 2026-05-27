package inspect_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/inspect"
	"github.com/limidus/kdb/go/kdb/wire"
)

func TestDumpWireHandshakeFrame(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	msg := wire.HandshakeMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   1,
		},
		Request: wire.HandshakePayload{
			NodeID:     "node-a",
			Namespaces: []string{"app/data"},
			ClientMode: wire.ClientStreamReadOnly,
		},
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	out, err := inspect.NewWireFrameInspector().DumpFrame(frame, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 20 {
		t.Fatalf("short output: %q", out)
	}
}
