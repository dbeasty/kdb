package syncnode

import (
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestRefsWatchReportsHeadMoves: a session that sends REFS_WATCH gets the watched namespace's refs
// at once, then an unsolicited REFS_CHANGED, under the watch's correlation id, each time a commit
// moves the head - and a burst of commits is reported as few frames, each with the latest head.
func TestRefsWatchReportsHeadMoves(t *testing.T) {
	cloud, srv := cloudOverHTTP(t)
	const ns = "zolik/u/1"
	cloud.put(t, ns, mustID(t), `{"v":0}`)
	conn, err := ws.NewTransport(core.DefaultConnectOptions()).Connect(strings.Replace(srv.URL, "http://", "ws://", 1) + "/kdb/sync")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	codec := wire.NewCodec(wire.EncodingJSON)
	send := func(m wire.Message) {
		t.Helper()
		frame, err := codec.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Send(frame); err != nil {
			t.Fatal(err)
		}
	}
	recv := func(within time.Duration) wire.Message {
		t.Helper()
		select {
		case frame := <-conn.Incoming():
			m, err := codec.Decode(frame)
			if err != nil {
				t.Fatal(err)
			}
			return m
		case <-time.After(within):
			return nil
		}
	}
	hdr := func(mt wire.MessageType, c int) wire.Header {
		return wire.Header{MessageType: mt, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c}
	}
	token := "phone-token"
	send(wire.SyncHelloMessage{H: hdr(wire.MsgSyncHello, 1), NodeID: "watcher", Protocol: wire.SyncProtocolVersion,
		Capabilities: []string{wire.SyncCapWatch}, Namespaces: []string{ns}, Token: &token})
	ack, ok := recv(5 * time.Second).(wire.SyncHelloAckMessage)
	if !ok || !ack.Accepted || !strings.Contains(strings.Join(ack.Capabilities, ","), wire.SyncCapWatch) {
		t.Fatalf("hello: %+v", ack)
	}
	send(wire.RefsWatchMessage{H: hdr(wire.MsgRefsWatch, 7), Namespaces: []string{ns}})
	first, ok := recv(5 * time.Second).(wire.RefsChangedMessage)
	if !ok || first.H.CorrelationID != 7 || len(first.Refs) != 1 || first.Refs[0].Branches["main"] == "" {
		t.Fatalf("watch reply: %+v", first)
	}
	if m := recv(200 * time.Millisecond); m != nil {
		t.Fatalf("nothing moved, yet the host sent %T", m)
	}

	for i := 0; i < 20; i++ {
		cloud.put(t, ns, mustID(t), `{"burst":true}`)
	}
	want := headOf(t, cloud, ns)
	frames := 0
	for {
		m := recv(2 * time.Second)
		if m == nil {
			t.Fatalf("no REFS_CHANGED with the head %s after %d frames", want, frames)
		}
		changed, ok := m.(wire.RefsChangedMessage)
		if !ok || changed.H.CorrelationID != 7 {
			t.Fatalf("unexpected %+v", m)
		}
		frames++
		if changed.Refs[0].Branches["main"] == want {
			break
		}
	}
	if frames > 10 {
		t.Errorf("20 commits in a burst were reported in %d frames; they should coalesce", frames)
	}
}

func headOf(t *testing.T, n testNode, ns string) string {
	t.Helper()
	rt, ok := n.set.Get(ns)
	if !ok {
		t.Fatalf("no namespace %s", ns)
	}
	h, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h.Hex()
}
