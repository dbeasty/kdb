package tcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

func listenAndAccept(t *testing.T) (addr string, accepted <-chan stream.ConnectionHandle, cleanup func()) {
	t.Helper()
	transport := NewTransport(core.DefaultConnectOptions())
	ln, err := transport.ListenBound("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatalf("ListenBound: %v", err)
	}
	ch := make(chan stream.ConnectionHandle, 8)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) {
			ch <- conn
		})
	}()
	return fmt.Sprintf("tcp://%s", ln.Addr().String()), ch, func() {
		cancel()
		_ = ln.Close()
	}
}

// TestReadLoopDeliversBurstWithoutDroppingFrames is the regression test for the finding
// recorded in docs/kdb-finish-up-plan.md as 1-G12: readLoop used to push each decoded frame onto
// incoming (a 32-slot buffered channel) with `select { case incoming <- frame: default: }`,
// silently dropping any frame that arrived while the buffer was full - a lost request on the
// server side, a lost reply on the client side, with the caller only finding out via its own
// timeout, never an error at the point the frame was actually lost. Sends a burst well past the
// buffer's capacity before the receiving side ever calls Incoming(), and asserts every single
// frame still arrives.
func TestReadLoopDeliversBurstWithoutDroppingFrames(t *testing.T) {
	addr, accepted, cleanup := listenAndAccept(t)
	defer cleanup()

	transport := NewTransport(core.DefaultConnectOptions())
	client, err := transport.Connect(addr)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	const burst = 200 // well past the 32-slot buffer
	wireCodec := wire.NewCodec(wire.EncodingJSON)
	for i := 0; i < burst; i++ {
		msg := wire.PositionAckMessage{
			H:          wire.Header{MessageType: wire.MsgPositionAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: i},
			Namespace:  "app/data",
			CommitHash: codec.Hash{},
		}
		frame, err := wireCodec.Encode(msg)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if err := client.Send(frame); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	var server stream.ConnectionHandle
	select {
	case server = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	received := make(map[int]bool, burst)
	deadline := time.After(5 * time.Second)
	for len(received) < burst {
		select {
		case frame, ok := <-server.Incoming():
			if !ok {
				t.Fatalf("Incoming() closed early after %d/%d frames", len(received), burst)
			}
			msg, err := wireCodec.Decode(frame)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			received[msg.Header().CorrelationID] = true
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d frames - some were dropped", len(received), burst)
		}
	}
	for i := 0; i < burst; i++ {
		if !received[i] {
			t.Fatalf("frame with correlation id %d was never delivered", i)
		}
	}
}

// TestCloseDuringBlockedSendDoesNotPanic is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G13: Close used to close(c.incoming) directly while readLoop
// (a different goroutine) could concurrently be sending on that same channel - a classic "send
// on closed channel" panic race. Fills the buffer, sends one more frame to push readLoop into
// its blocking send, then calls Close from another goroutine while that send is in flight -
// under -race, this must neither panic nor deadlock, and Incoming() must observably close.
func TestCloseDuringBlockedSendDoesNotPanic(t *testing.T) {
	addr, accepted, cleanup := listenAndAccept(t)
	defer cleanup()

	transport := NewTransport(core.DefaultConnectOptions())
	client, err := transport.Connect(addr)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	var server stream.ConnectionHandle
	select {
	case server = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	wireCodec := wire.NewCodec(wire.EncodingJSON)
	frameFor := func(i int) []byte {
		msg := wire.PositionAckMessage{
			H:          wire.Header{MessageType: wire.MsgPositionAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: i},
			Namespace:  "app/data",
			CommitHash: codec.Hash{},
		}
		frame, err := wireCodec.Encode(msg)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		return frame
	}

	// Nobody ever reads server.Incoming() below, so once the 32-slot buffer fills, readLoop's
	// next send blocks - exactly the state Close must be able to interrupt safely.
	for i := 0; i < 40; i++ {
		if err := client.Send(frameFor(i)); err != nil {
			// The client side may see the connection close mid-burst once the server closes -
			// that's fine, this test only cares that the server side doesn't panic.
			break
		}
	}
	time.Sleep(50 * time.Millisecond) // let readLoop settle into its blocked send

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return - readLoop likely deadlocked instead of observing done")
	}

	// Incoming() must eventually close (readLoop's deferred close(c.incoming)) rather than
	// leaving any consumer's range loop hanging forever.
	closedOK := false
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case _, ok := <-server.Incoming():
			if !ok {
				closedOK = true
				break drain
			}
		case <-deadline:
			break drain
		}
	}
	if !closedOK {
		t.Fatal("expected Incoming() to close after Close(), it never did")
	}
}
