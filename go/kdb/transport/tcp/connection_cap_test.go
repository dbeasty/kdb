package tcp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// serveWithCap starts a listener with the given connection cap and returns its address plus a
// stop func. Accepted connections are parked (not closed) so the cap can be observed.
func serveWithCap(t *testing.T, maxConns int) (string, func()) {
	t.Helper()
	opts := core.DefaultConnectOptions()
	opts.MaxConnections = maxConns
	transport := NewTransport(opts)
	ln, err := transport.ListenBound("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var handles []stream.ConnectionHandle
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) {
			mu.Lock()
			handles = append(handles, conn)
			mu.Unlock()
			<-ctx.Done()
		})
	}()
	addr := ln.Addr().String()
	return addr, func() {
		cancel()
		mu.Lock()
		for _, h := range handles {
			_ = h.Close()
		}
		mu.Unlock()
		wg.Wait()
	}
}

// isClosedByPeer reports whether the server hung up on this connection - what a client sees when
// it is refused past the cap.
func isClosedByPeer(t *testing.T, c net.Conn) bool {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err := c.Read(buf)
	return err == io.EOF
}

// kdb-spec-layer13 Component 49 §6.5. Without a cap, every accepted connection is a goroutine
// stack and a frame buffer that admission control cannot see or refuse - a way to consume the
// memory budget without ever submitting a request.
func TestConnectionsBeyondCapAreRefusedAtAccept(t *testing.T) {
	addr, stop := serveWithCap(t, 2)
	defer stop()

	var kept []net.Conn
	defer func() {
		for _, c := range kept {
			_ = c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("connection %d within the cap should be accepted: %v", i, err)
		}
		kept = append(kept, c)
	}
	// Let the accept loop register both before testing the third.
	time.Sleep(100 * time.Millisecond)

	third, err := net.Dial("tcp", addr)
	if err != nil {
		return // refused at the TCP level is also a valid refusal
	}
	defer third.Close()
	if !isClosedByPeer(t, third) {
		t.Error("a connection past the cap must be closed at accept time, not served")
	}
}

// The cap must be a live count, not a lifetime total: closing a connection has to free its slot,
// or a long-running server would refuse everything after its first N clients ever disconnected.
func TestClosingAConnectionFreesItsCapSlot(t *testing.T) {
	addr, stop := serveWithCap(t, 1)
	defer stop()

	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	blocked, err := net.Dial("tcp", addr)
	if err == nil {
		if !isClosedByPeer(t, blocked) {
			t.Error("expected the second connection to be refused while the first is live")
		}
		_ = blocked.Close()
	}

	// Free the slot and confirm a new connection is served rather than refused.
	_ = first.Close()
	time.Sleep(300 * time.Millisecond)

	third, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("expected a freed slot to accept a new connection: %v", err)
	}
	defer third.Close()
	_ = third.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := third.Read(buf); err == io.EOF {
		t.Error("the connection should have been served, not closed - the cap slot was never freed")
	}
}

func TestZeroCapMeansUnlimited(t *testing.T) {
	addr, stop := serveWithCap(t, 0)
	defer stop()
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < 12; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("connection %d should be accepted when the cap is 0 (unlimited): %v", i, err)
		}
		conns = append(conns, c)
	}
	time.Sleep(100 * time.Millisecond)
	last := conns[len(conns)-1]
	if isClosedByPeer(t, last) {
		t.Error("no connection should be refused when the cap is 0")
	}
}

// The per-connection frame queue is a real, unaccounted memory commitment: queue depth times
// MaxFrameBytes times connection count. It was 32 frames (512MB per connection at the 16MB frame
// ceiling); §5.4 requires it be brought down to a small number.
func TestIncomingQueueDefaultIsSmall(t *testing.T) {
	if core.DefaultIncomingQueueFrames > 4 {
		t.Errorf("per-connection frame queue default is %d; §5.4 requires 2-4", core.DefaultIncomingQueueFrames)
	}
	if core.DefaultIncomingQueueFrames < 1 {
		t.Fatal("a zero-length queue would make every read block on a consumer")
	}
	conn := newSocketConnection(&nopConn{}, core.DefaultConnectOptions(), nil)
	defer conn.Close()
	if got := cap(conn.incoming); got != core.DefaultIncomingQueueFrames {
		t.Errorf("connection queue capacity = %d, want %d", got, core.DefaultIncomingQueueFrames)
	}
}

func TestIncomingQueueIsConfigurable(t *testing.T) {
	opts := core.DefaultConnectOptions()
	opts.IncomingQueueFrames = 2
	conn := newSocketConnection(&nopConn{}, opts, nil)
	defer conn.Close()
	if got := cap(conn.incoming); got != 2 {
		t.Errorf("connection queue capacity = %d, want 2", got)
	}
}

// nopConn is a net.Conn that blocks on Read until closed - enough to construct a
// socketConnection without a real socket.
type nopConn struct {
	mu     sync.Mutex
	closed chan struct{}
	once   sync.Once
}

func (c *nopConn) ensure() chan struct{} {
	c.once.Do(func() { c.closed = make(chan struct{}) })
	return c.closed
}
func (c *nopConn) Read(b []byte) (int, error)  { <-c.ensure(); return 0, io.EOF }
func (c *nopConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *nopConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ensure():
	default:
		close(c.closed)
	}
	return nil
}
func (c *nopConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *nopConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *nopConn) SetDeadline(t time.Time) error      { return nil }
func (c *nopConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *nopConn) SetWriteDeadline(t time.Time) error { return nil }
