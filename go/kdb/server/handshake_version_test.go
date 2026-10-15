package server

import (
	"slices"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/version"
	"github.com/limidus/kdb/go/kdb/wire"
)

// The ack names the server's release so a client published for one release (@limidus/kdb-client) can
// refuse to talk to another.
func TestHandshakeAckCarriesServerVersion(t *testing.T) {
	ack := handshakeAck(wire.HandshakeMessage{}, true, "", nil)
	if ack.Response.ServerVersion == "" || ack.Response.ServerVersion != version.Get().Version {
		t.Fatalf("ServerVersion = %q, want %q", ack.Response.ServerVersion, version.Get().Version)
	}
}

// Binary frames (B4) are agreed per connection: a client that offers no wire features - every
// client before this release - gets the JSON envelope exactly as before, and one that offers them
// gets them in the ack and binary replies after it. Requests are decoded by their own tag, so a
// JSON request on a binary connection is still served.
func TestHandshakeNegotiatesBinaryFramesPerConnection(t *testing.T) {
	runtime, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rt := NewKdbServerRuntime(runtime)
	jsonCodec := wire.NewCodec(wire.EncodingJSON)
	get := jsonCodec.WithWireFeatures(nil)
	for _, offer := range [][]string{nil, {"some-future-feature"}, wire.SupportedWireFeatures} {
		h := newSqlWireConnHandler(jsonCodec, rt)
		hs, err := jsonCodec.Encode(wire.HandshakeMessage{
			H:       wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
			Request: wire.HandshakePayload{ClientMode: wire.ClientSQL, WireFeatures: offer},
		})
		if err != nil {
			t.Fatal(err)
		}
		ackFrame, err := h.handleFrame(hs)
		if err != nil {
			t.Fatal(err)
		}
		if tag := ackFrame[wire.FrameHeaderSize]; tag != 1 {
			t.Fatalf("offer %v: the ack went out with tag %d", offer, tag)
		}
		ack, err := jsonCodec.Decode(ackFrame)
		if err != nil {
			t.Fatal(err)
		}
		want := wire.NegotiateWireFeatures(offer)
		if got := ack.(wire.HandshakeAckMessage).Response; !slices.Equal(got.WireFeatures, want) || got.NegotiatedEncoding != wire.EncodingJSON {
			t.Fatalf("offer %v: ack features %v encoding %v", offer, got.WireFeatures, got.NegotiatedEncoding)
		}
		req, err := get.Encode(wire.DocumentGetMessage{
			H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 2},
			Namespace: "app/data", DocID: "6f1c2c4e-0000-4000-8000-000000000001",
		})
		if err != nil {
			t.Fatal(err)
		}
		reply, err := h.handleFrame(req)
		if err != nil {
			t.Fatal(err)
		}
		wantTag := byte(1)
		if len(want) > 0 {
			wantTag = 2
		}
		if tag := reply[wire.FrameHeaderSize]; tag != wantTag {
			t.Fatalf("offer %v: reply tag %d, want %d", offer, tag, wantTag)
		}
		if _, err := jsonCodec.Decode(reply); err != nil {
			t.Fatal(err)
		}
	}
}
