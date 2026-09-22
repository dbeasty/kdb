package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// recordingConn is a subscriber that applies every delta it is sent, and checks each one names
// the previous as its parent - what a real subscriber holds, and whether it could hold it.
type recordingConn struct {
	t      *testing.T
	mu     sync.Mutex
	docs   map[codec.UUID]string
	pos    codec.Hash
	frames int
	broken string
}

func newRecordingConn(t *testing.T, at codec.Hash, docs map[codec.UUID]string) *recordingConn {
	if docs == nil {
		docs = map[codec.UUID]string{}
	}
	return &recordingConn{t: t, docs: docs, pos: at}
}

func (c *recordingConn) Send(frame []byte) error {
	msg, err := wire.NewCodec(wire.EncodingJSON).Decode(frame)
	if err != nil {
		return err
	}
	d, ok := msg.(wire.DeltaCommitMessage)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.Payload.ParentHash != c.pos && c.broken == "" {
		c.broken = fmt.Sprintf("frame %d names parent %s, subscriber is at %s", c.frames, d.Payload.ParentHash.Hex(), c.pos.Hex())
	}
	for _, op := range d.Payload.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			c.docs[o.DocID] = o.Patch
		case document.DeleteOp:
			delete(c.docs, o.DocID)
		}
	}
	c.pos = d.Payload.CommitHash
	c.frames++
	return nil
}
func (c *recordingConn) Incoming() <-chan []byte { return nil }
func (c *recordingConn) Close() error            { return nil }
func (c *recordingConn) TryPoll() []byte         { return nil }

// waitAt waits until the subscriber is at want, then checks it holds exactly expect.
func (c *recordingConn) waitAt(want codec.Hash, expect map[codec.UUID]string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		pos, broken := c.pos, c.broken
		c.mu.Unlock()
		if broken != "" {
			c.t.Fatal(broken)
		}
		if pos == want {
			break
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("subscriber stuck at %s, head is %s", pos.Hex(), want.Hex())
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.docs) != len(expect) {
		c.t.Fatalf("subscriber holds %d documents, expected %d", len(c.docs), len(expect))
	}
	for id, body := range expect {
		if c.docs[id] != body {
			c.t.Fatalf("subscriber holds %s = %q, expected %q", id, c.docs[id], body)
		}
	}
}

// liveDocs is every document at rt's head that keep accepts.
func liveDocs(t *testing.T, rt *KdbServerRuntime, keep func(string) bool) map[codec.UUID]string {
	t.Helper()
	_, head, _, err := rt.dag.HeadCommit()
	if err != nil {
		t.Fatal(err)
	}
	out := map[codec.UUID]string{}
	var ids []codec.UUID
	if err := rt.Runtime.Storage.(storage.TreeWalker).WalkTree(rt.Runtime.DefaultNamespace, head.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		body, _, _, _ := rt.GetDocument(rt.Runtime.DefaultNamespace, id)
		if keep == nil || keep(body) {
			out[id] = body
		}
	}
	return out
}

func subscribeRecording(t *testing.T, hub *StreamHub, c *recordingConn, resume *codec.Hash, filter string) {
	t.Helper()
	req := wire.HandshakePayload{NodeID: "rec", Namespaces: []string{hub.namespaceID}, ClientMode: wire.ClientStreamReadOnly}
	if resume != nil {
		req.LocalHeads = map[string]string{hub.namespaceID: resume.Hex()}
	}
	if filter != "" {
		req.Filter = &filter
	}
	_, start := hub.handleHandshake(c, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}, Request: req,
	})
	if start == nil {
		t.Fatal("handshake refused")
	}
	start()
	t.Cleanup(func() { hub.unregister(c) })
}

func hubFor(t *testing.T, rt *KdbServerRuntime) *StreamHub {
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), rt.Runtime.DefaultNamespace, rt)
	rt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }
	return hub
}

// TestStreamResumeSendsWhatWasMissed: a subscriber that reconnects with its position gets every
// commit it missed, in order, over a real socket - it used to be registered and then sent only
// what came after, so its first frame failed its own parent check and it could never recover.
func TestStreamResumeSendsWhatWasMissed(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)
	hub, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	rt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }
	at, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	var missed []codec.Hash
	for i := 0; i < 3; i++ {
		c, err := rt.Upsert(ns, mustRandomUUID(t), fmt.Sprintf(`{"i":%d}`, i), auth.Principal{})
		if err != nil {
			t.Fatal(err)
		}
		missed = append(missed, c.Hash)
	}
	sub := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), nil)
	conn, err := sub.Connect(stream.SubscriberConfig{NamespaceID: ns, NodeID: "resumer", CoordinatorURI: "tcp://" + listener.Addr().String(), ResumeFrom: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect()
	var got []codec.Hash
	deadline := time.After(5 * time.Second)
	for len(got) < len(missed) {
		select {
		case ev := <-sub.Events():
			switch ev.Kind {
			case stream.EventDeltaReceived:
				got = append(got, ev.CommitHash)
			case stream.EventError:
				t.Fatalf("subscriber error: %v", ev.Cause)
			}
		case <-deadline:
			t.Fatalf("received %d of the %d missed commits", len(got), len(missed))
		}
	}
	for i := range missed {
		if got[i] != missed[i] {
			t.Fatalf("commit %d: got %s, want %s", i, got[i].Hex(), missed[i].Hex())
		}
	}
	if pos := conn.Position(); pos == nil || *pos != missed[len(missed)-1] {
		t.Fatal("the subscriber's position is not the head")
	}
}

// TestStreamResumeFromUnknownPositionIsRefused: a position this namespace never had (or has
// truncated away) cannot be caught up from; the handshake says so instead of accepting a
// subscriber whose state nothing can repair.
func TestStreamResumeFromUnknownPositionIsRefused(t *testing.T) {
	rt := newTestRuntime(t)
	_, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	bogus := codec.Hash{Bytes: [32]byte{1, 2, 3}}
	sub := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), nil)
	defer sub.Disconnect()
	_, err = sub.Connect(stream.SubscriberConfig{NamespaceID: "app/data", NodeID: "x", CoordinatorURI: "tcp://" + listener.Addr().String(), ResumeFrom: &bogus})
	if err == nil || !strings.Contains(err.Error(), "resume position") {
		t.Fatalf("expected the resume to be refused, got %v", err)
	}
}

// TestStreamLongAbsenceCatchesUpInOneFrame: further behind than per-commit catch-up reaches, a
// subscriber gets the difference between its tree and the head's as one frame, and holds
// exactly the head's documents afterwards - deletes included.
func TestStreamLongAbsenceCatchesUpInOneFrame(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	hub := hubFor(t, rt)
	doomed := mustRandomUUID(t)
	if _, err := rt.Upsert(ns, doomed, `{"v":"gone soon"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	at, _ := rt.dag.Head()
	before := liveDocs(t, rt, nil)
	for i := 0; i < maxPerCommitCatchUp+10; i++ {
		if _, err := rt.Upsert(ns, mustRandomUUID(t), fmt.Sprintf(`{"i":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rt.Commit(ns, deleteTx(t, rt, doomed), "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	c := newRecordingConn(t, at, before)
	subscribeRecording(t, hub, c, &at, "")
	head, _ := rt.dag.Head()
	c.waitAt(head, liveDocs(t, rt, nil))
	if c.frames != 1 {
		t.Fatalf("expected one catch-up frame, got %d", c.frames)
	}
}

// TestStreamFollowsPeerMerges: commits adopted from a peer - fast-forwards and merges, whichever
// parent a merge puts first - reach a subscriber as frames it can apply, and it ends holding the
// merged tree. Publishing each commit against its first parent used to desync a subscriber on
// every merge whose first parent was the peer's side.
func TestStreamFollowsPeerMerges(t *testing.T) {
	a, b := newTestRuntime(t), newTestRuntime(t)
	ns := a.Runtime.DefaultNamespace
	hub := hubFor(t, a)
	at, _ := a.dag.Head()
	c := newRecordingConn(t, at, liveDocs(t, a, nil))
	subscribeRecording(t, hub, c, nil, "")
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	for round := 0; round < 6; round++ {
		putDoc(t, a, fmt.Sprintf(`{"from":"a","round":%d}`, round))
		putDoc(t, b, fmt.Sprintf(`{"from":"b","round":%d}`, round))
		client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), a.dag, a.Runtime.Storage)
		session, err := client.Connect(peersync.ClientConfig{NamespaceID: ns, NodeID: "a", PeerURI: "tcp://" + ln.Addr().String(), Node: a.PeerSyncNode(), ApplyToStorage: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.SyncBidirectional(); err != nil {
			t.Fatal(err)
		}
		client.Disconnect()
		head, _ := a.dag.Head()
		c.waitAt(head, liveDocs(t, a, nil))
	}
}

// TestStreamFilterAndReadChecks: a filtered subscription gets only matching documents, a
// document leaving the filter or unreadable to the subscriber arrives as a delete, and nothing
// it may not read is ever sent.
func TestStreamFilterAndReadChecks(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	secret := mustRandomUUID(t)
	rt.AuthEngine = hideDocs{hidden: map[string]bool{secret.String(): true}}
	hub := hubFor(t, rt)
	at, _ := rt.dag.Head()
	c := newRecordingConn(t, at, nil)
	subscribeRecording(t, hub, c, nil, `region = 'EU'`)

	eu, us := mustRandomUUID(t), mustRandomUUID(t)
	for id, body := range map[codec.UUID]string{eu: `{"region":"EU"}`, us: `{"region":"US"}`, secret: `{"region":"EU","secret":true}`} {
		if _, err := rt.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := rt.dag.Head()
	c.waitAt(head, map[codec.UUID]string{eu: liveDocs(t, rt, nil)[eu]})

	if _, err := rt.Upsert(ns, eu, `{"region":"US"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	head, _ = rt.dag.Head()
	c.waitAt(head, map[codec.UUID]string{})
}
