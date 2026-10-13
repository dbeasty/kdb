package client_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/wire"
)

// A server that predates binary frames - an older Go release, or the Kotlin server - acks a
// handshake without wireFeatures. Against it the client must stay on the JSON envelope: such a
// server rejects any encoding tag but 0 and 1.
func TestClientStaysOnJSONWithAServerThatAcksNoFeatures(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var (
		mu      sync.Mutex
		tags    []byte
		offered []string
	)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		codec := wire.NewCodec(wire.EncodingJSON)
		for {
			var prefix [4]byte
			if _, err := io.ReadFull(conn, prefix[:]); err != nil {
				return
			}
			frame := make([]byte, binary.LittleEndian.Uint32(prefix[:]))
			copy(frame, prefix[:])
			if _, err := io.ReadFull(conn, frame[4:]); err != nil {
				return
			}
			mu.Lock()
			tags = append(tags, frame[wire.FrameHeaderSize])
			mu.Unlock()
			msg, err := codec.Decode(frame)
			if err != nil {
				return
			}
			var reply wire.Message
			switch m := msg.(type) {
			case wire.HandshakeMessage:
				mu.Lock()
				offered = m.Request.WireFeatures
				mu.Unlock()
				reply = wire.HandshakeAckMessage{H: m.H, Response: wire.HandshakeAckPayload{Accepted: true, NegotiatedEncoding: wire.EncodingJSON, ProtocolVersion: 1}}
			case wire.DocumentGetMessage:
				body := `{"a":1}`
				reply = wire.DocumentGetResultMessage{H: wire.Header{MessageType: wire.MsgDocumentGetResult, ProtocolVersion: 1, CorrelationID: m.H.CorrelationID}, Namespace: m.Namespace, DocID: m.DocID, JSON: &body, CommitHex: "00"}
			default:
				return
			}
			out, err := codec.Encode(reply)
			if err != nil {
				return
			}
			if _, err := conn.Write(out); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, "tcp://"+ln.Addr().String(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if f := c.WireFeatures(); f != nil {
		t.Fatalf("negotiated %v with a server that acked none", f)
	}
	body, _, err := c.GetJSON(ctx, "app/data", "6f1c2c4e-0000-4000-8000-000000000001")
	if err != nil || string(body) != `{"a":1}` {
		t.Fatalf("get: %s %v", body, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(offered) == 0 {
		t.Fatal("the client offered no wire features")
	}
	for i, tag := range tags {
		if tag > 1 {
			t.Fatalf("frame %d went out with tag %d to a JSON-only server", i, tag)
		}
	}
}
