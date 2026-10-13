package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// writeFarBehind writes ~20 MiB of documents, one per commit and more commits than per-commit
// catch-up reaches, so a subscriber at the returned position is caught up by tree delta - one
// frame of more than 16 MiB, before the split.
func writeFarBehind(t *testing.T, rt *KdbServerRuntime) codec.Hash {
	t.Helper()
	at, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("z", 256<<10)
	for i := 0; i < maxPerCommitCatchUp+16; i++ {
		if _, err := rt.Upsert(rt.Runtime.DefaultNamespace, mustRandomUUID(t), fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	return at
}

// Bug 2: a far-behind subscriber whose catch-up exceeds the 16 MiB frame used to be dropped -
// the single treeDelta frame failed to encode - and failed identically on every reconnect. The
// catch-up is now split into frames bounded by bytes; applying them in order reaches the head
// with the parent check holding on every frame.
func TestStreamFarBehindOverTheFrameLimitCatchesUp(t *testing.T) {
	rt := newTestRuntime(t)
	hub := hubFor(t, rt)
	at := writeFarBehind(t, rt)
	c := newRecordingConn(t, at, nil)
	subscribeRecording(t, hub, c, &at, "")
	head, _ := rt.dag.Head()
	c.waitAt(head, liveDocs(t, rt, nil))
	if c.frames < 5 {
		t.Fatalf("expected the ~20 MiB catch-up to be split across several frames, got %d", c.frames)
	}
}

// The same over a real socket, with the Go stream client: it reaches the head without a desync
// error, which is what the Kotlin subscriber's identical parent check needs too.
func TestStreamFarBehindOverTheFrameLimitGoSubscriber(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	hub, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	rt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }
	at := writeFarBehind(t, rt)
	head, _ := rt.dag.Head()

	sub := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), nil)
	conn, err := sub.Connect(stream.SubscriberConfig{NamespaceID: ns, NodeID: "far", CoordinatorURI: "tcp://" + listener.Addr().String(), ResumeFrom: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case ev := <-sub.Events():
			switch ev.Kind {
			case stream.EventError, stream.EventDisconnected:
				t.Fatalf("subscriber %v: %v", ev.Kind, ev.Cause)
			}
		case <-deadline:
			t.Fatal("subscriber never reached the head")
		case <-time.After(10 * time.Millisecond):
		}
		if pos := conn.Position(); pos != nil && *pos == head {
			return
		}
	}
}

func TestSplitOpsByBytes(t *testing.T) {
	if parts := splitOpsByBytes(nil, 100); len(parts) != 1 || len(parts[0]) != 0 {
		t.Fatalf("no operations must still make one (empty) frame, got %v", parts)
	}
	var ops []document.Op
	for i := 0; i < 50; i++ {
		ops = append(ops, document.WriteOp{DocID: codec.UUID{}, Patch: strings.Repeat("a", i*37)})
		if i%7 == 0 {
			ops = append(ops, document.DeleteOp{})
		}
	}
	ops = append(ops, document.WriteOp{Patch: strings.Repeat("b", 5000)}) // larger than the limit alone
	const limit = 2000
	parts := splitOpsByBytes(ops, limit)
	var joined []document.Op
	for _, p := range parts {
		if len(p) == 0 {
			t.Fatal("empty part in a non-empty split")
		}
		size := 0
		for _, op := range p {
			size += opEncodedBytes(op)
		}
		if size > limit && len(p) > 1 {
			t.Fatalf("part of %d ops is %d bytes, over %d", len(p), size, limit)
		}
		joined = append(joined, p...)
	}
	if len(joined) != len(ops) {
		t.Fatalf("split lost operations: %d of %d", len(joined), len(ops))
	}
	for i := range ops {
		if fmt.Sprint(joined[i]) != fmt.Sprint(ops[i]) {
			t.Fatalf("operation %d out of order", i)
		}
	}
	// The per-op estimate bounds what the op really adds to an encoded deltaCommit.
	codec := wire.NewCodec(wire.EncodingJSON)
	frameOf := func(ops []document.Op) int {
		f, err := codec.Encode(wire.DeltaCommitMessage{H: header(1, wire.MsgDeltaCommit), Payload: wire.DeltaCommitPayload{Operations: ops}})
		if err != nil {
			t.Fatal(err)
		}
		return len(f)
	}
	base := frameOf(nil)
	for _, op := range ops[:10] {
		if got, est := frameOf([]document.Op{op})-base, opEncodedBytes(op); got > est {
			t.Fatalf("%T adds %d bytes, estimated %d", op, got, est)
		}
	}
}
