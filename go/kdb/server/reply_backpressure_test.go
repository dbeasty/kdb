package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/wire"
)

// gatedConn is a FrameConn whose peer reads nothing after the first free replies until opened.
type gatedConn struct {
	in    chan []byte
	gate  chan struct{}
	free  int
	mu    sync.Mutex
	sends int
	got   [][]byte
}

func (c *gatedConn) Incoming() <-chan []byte { return c.in }

func (c *gatedConn) Send(frame []byte) error {
	c.mu.Lock()
	free := c.sends < c.free
	c.sends++
	c.mu.Unlock()
	if !free {
		<-c.gate
	}
	c.mu.Lock()
	c.got = append(c.got, append([]byte(nil), frame...))
	c.mu.Unlock()
	return nil
}

func (c *gatedConn) received() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

// B6: past MaxQueuedReplyBytes of unwritten replies, the connection's reader stops taking frames
// until they drain - well before MaxInFlightFrames - and resumes once the peer reads again.
func TestReaderStopsWhileReplyBytesAreQueued(t *testing.T) {
	rt := newTestRuntime(t)
	rt.MaxQueuedReplyBytesPerConn = 1 << 20
	id := mustRandomUUID(t)
	if _, err := rt.Upsert("app/data", id, fmt.Sprintf(`{"pad":%q}`, strings.Repeat("x", 512<<10)), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	codec := wire.NewCodec(wire.EncodingJSON)
	h := newSqlWireConnHandler(codec, rt)
	// The handshake reply and one read go through, so the connection has seen a reply's size;
	// then the peer stops reading.
	conn := &gatedConn{in: make(chan []byte), gate: make(chan struct{}), free: 2}
	done := make(chan struct{})
	go func() { h.run(conn); close(done) }()

	encode := func(m wire.Message) []byte {
		f, err := codec.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	conn.in <- encode(wire.HandshakeMessage{H: header(1, wire.MsgHandshake),
		Request: wire.HandshakePayload{NodeID: "bp", Namespaces: []string{"app/data"}, ClientMode: wire.ClientSQL}})

	get := func(cid int) []byte {
		return encode(wire.DocumentGetMessage{H: header(cid, wire.MsgDocumentGet), Namespace: "app/data", DocID: id.String()})
	}
	conn.in <- get(2)
	for deadline := time.Now().Add(5 * time.Second); conn.received() < 2; {
		if time.Now().After(deadline) {
			t.Fatal("first read was not answered")
		}
		time.Sleep(time.Millisecond)
	}

	const total = 40
	pushed := 0
	for ; pushed < total; pushed++ {
		frame := encode(wire.DocumentGetMessage{H: header(100+pushed, wire.MsgDocumentGet), Namespace: "app/data", DocID: id.String()})
		select {
		case conn.in <- frame:
			continue
		case <-time.After(500 * time.Millisecond):
		}
		break
	}
	if pushed >= total || pushed >= MaxInFlightFrames {
		t.Fatalf("reader took all %d frames while ~%d KiB of replies sat unwritten", pushed, pushed*512)
	}
	// Each dispatched frame reserves the ~512 KiB a reply on this connection has cost, so the
	// third takes the budget past 1 MiB and the reader stops there.
	if pushed > 3 {
		t.Fatalf("reader took %d frames of 512 KiB replies against a 1 MiB budget", pushed)
	}
	if _, waits := h.replies.snapshot(); waits == 0 {
		t.Fatal("reader never waited on the reply budget")
	}

	// The peer reads again: everything drains and every request is answered.
	close(conn.gate)
	for ; pushed < total; pushed++ {
		conn.in <- encode(wire.DocumentGetMessage{H: header(100+pushed, wire.MsgDocumentGet), Namespace: "app/data", DocID: id.String()})
	}
	close(conn.in)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not finish after the peer resumed reading")
	}
	if got := conn.received(); got != total+2 {
		t.Fatalf("got %d replies, want %d", got, total+2)
	}
	if queued, _ := h.replies.snapshot(); queued != 0 {
		t.Fatalf("%d reply bytes still charged after everything was sent", queued)
	}
}

func TestReplyBudgetUnboundedNeverWaits(t *testing.T) {
	b := newReplyBudget(0)
	b.charge(1 << 30)
	b.waitBelowLimit()
	b.release(1 << 30)
}

func TestMaxQueuedReplyBytesResolution(t *testing.T) {
	rt := newTestRuntime(t)
	if got := rt.MaxQueuedReplyBytes(); got != DefaultMaxQueuedReplyBytes {
		t.Fatalf("default: %d", got)
	}
	rt.MaxQueuedReplyBytesPerConn = -1
	if got := rt.MaxQueuedReplyBytes(); got != 0 {
		t.Fatalf("disabled: %d", got)
	}
	rt.MaxQueuedReplyBytesPerConn = 5
	if got := rt.MaxQueuedReplyBytes(); got != 5 {
		t.Fatalf("explicit: %d", got)
	}
}
