package tcp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// A peer that stops reading must not hold Send forever: the write deadline fails the send, the
// connection is closed, and later sends fail fast instead of queueing behind the stalled one.
func TestSendToAStalledReaderHitsTheWriteDeadlineAndCloses(t *testing.T) {
	opts := core.DefaultConnectOptions()
	opts.WriteTimeoutMs = 300
	transport := NewTransport(opts)
	ln, err := transport.ListenBound("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan stream.ConnectionHandle, 1)
	go func() {
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) { accepted <- conn })
	}()

	// The client connects and never reads.
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	server := <-accepted

	frame, err := wire.EncodeFrameOnly(wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion},
		make([]byte, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var sendErr error
	for i := 0; i < 64 && sendErr == nil; i++ {
		sendErr = server.Send(frame)
	}
	if sendErr == nil {
		t.Fatal("64 x 4 MiB to a peer that never reads all succeeded")
	}
	var ne net.Error
	if !errors.As(sendErr, &ne) || !ne.Timeout() {
		t.Fatalf("want a timeout error, got %v", sendErr)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("send blocked %v; the deadline is 300ms", elapsed)
	}
	// The connection is closed: incoming ends, and the next send fails immediately.
	select {
	case _, ok := <-server.Incoming():
		if ok {
			t.Fatal("unexpected frame")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed after the write deadline")
	}
	t0 := time.Now()
	if err := server.Send(frame); err == nil {
		t.Fatal("send on a connection closed by a write timeout succeeded")
	}
	if time.Since(t0) > time.Second {
		t.Fatal("send after close did not fail fast")
	}
}

func TestWriteTimeoutResolution(t *testing.T) {
	cases := map[int64]time.Duration{0: core.DefaultWriteTimeout, -1: 0, 1500: 1500 * time.Millisecond}
	for ms, want := range cases {
		if got := (core.TransportConnectOptions{WriteTimeoutMs: ms}).WriteTimeout(); got != want {
			t.Errorf("WriteTimeoutMs=%d: got %v want %v", ms, got, want)
		}
	}
}
