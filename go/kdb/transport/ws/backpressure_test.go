package ws

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
)

// TestReadLoopDeliversBurstWithoutDroppingFrames is the regression test for the finding
// recorded in docs/kdb-finish-up-plan.md as 1-G12: readLoop used to push each decoded frame onto
// incoming (a 32-slot buffered channel) with `select { case incoming <- frame: default: }`,
// silently dropping any frame that arrived while the buffer was full. Uses fakeWsServer's echo
// behavior: sends a burst of frames well past the buffer's capacity before ever reading
// Incoming(), and asserts every single echo still arrives.
func TestReadLoopDeliversBurstWithoutDroppingFrames(t *testing.T) {
	server := startFakeWsServer(t)
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	const burst = 200 // well past the 32-slot buffer
	// %04d, not %d: wireShapedFrame's total (4-byte length prefix + payload) must reach the
	// real wire header's minimum size (FrameHeaderSize, 12 bytes) or ValidateOutgoingFrame
	// rejects it as too short - a bare "frame-0" (7 bytes) falls under that floor.
	for i := 0; i < burst; i++ {
		payload := wireShapedFrame([]byte(fmt.Sprintf("frame-%04d", i)))
		if err := conn.Send(payload); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	received := make(map[string]bool, burst)
	deadline := time.After(5 * time.Second)
	for len(received) < burst {
		select {
		case frame, ok := <-conn.Incoming():
			if !ok {
				t.Fatalf("Incoming() closed early after %d/%d frames", len(received), burst)
			}
			received[string(frame[4:])] = true
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d frames - some were dropped", len(received), burst)
		}
	}
	for i := 0; i < burst; i++ {
		want := fmt.Sprintf("frame-%04d", i)
		if !received[want] {
			t.Fatalf("frame %q was never delivered", want)
		}
	}
}

// TestCloseDuringBlockedSendDoesNotPanic is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G13: Close used to close(c.incoming) directly while readLoop
// (a different goroutine) could concurrently be sending on that same channel - a "send on closed
// channel" panic race. Lets the echo server fill the buffer past capacity without ever reading
// Incoming(), then calls Close from another goroutine while readLoop is blocked trying to
// deliver the next echoed frame - under -race, this must neither panic nor deadlock, and
// Incoming() must observably close.
func TestCloseDuringBlockedSendDoesNotPanic(t *testing.T) {
	server := startFakeWsServer(t)
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	for i := 0; i < 40; i++ {
		payload := wireShapedFrame(bytes.Repeat([]byte{byte(i)}, 8))
		if err := conn.Send(payload); err != nil {
			break
		}
	}
	time.Sleep(50 * time.Millisecond) // let readLoop settle into its blocked send

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := conn.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return - readLoop likely deadlocked instead of observing done")
	}

	closedOK := false
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case _, ok := <-conn.Incoming():
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
