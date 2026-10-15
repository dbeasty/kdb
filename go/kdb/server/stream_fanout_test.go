package server

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// capturingConn keeps a copy of every frame it is sent. sync makes it a core.SynchronousSender,
// which the hub answers with a pooled buffer it reuses after Send - so the copy is required.
type capturingConn struct {
	sync   bool
	mu     sync.Mutex
	frames [][]byte
	pos    codec.Hash
}

func (c *capturingConn) Send(frame []byte) error {
	msg, err := wire.NewCodec(wire.EncodingJSON).Decode(frame)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames = append(c.frames, bytes.Clone(frame))
	if d, ok := msg.(wire.DeltaCommitMessage); ok {
		c.pos = d.Payload.CommitHash
	}
	return nil
}
func (c *capturingConn) Incoming() <-chan []byte { return nil }
func (c *capturingConn) Close() error            { return nil }
func (c *capturingConn) TryPoll() []byte         { return nil }

type syncCapturingConn struct{ *capturingConn }

func (syncCapturingConn) SendCompletesSynchronously() bool { return true }

func (c *capturingConn) at() codec.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pos
}

func (c *capturingConn) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.frames)
}

// legacyStreamFrames is the hub's per-subscriber path as it was before frames were shared (kept
// as the oracle): the subscriber's view of each commit on head's first-parent line after from -
// filter, then read check, in one pass - encoded for it alone, correlation id 0.
func legacyStreamFrames(t *testing.T, h *StreamHub, filter sql.Expr, checkReads bool, principal auth.Principal, from, head codec.Hash) [][]byte {
	t.Helper()
	var chain []document.Commit
	for at := head; at != from; {
		c, ok := h.runtime.dag.GetCommit(at)
		if !ok {
			t.Fatalf("commit %s missing", at.Hex())
		}
		ops, err := h.runtime.dag.CommitOperations(at)
		if err != nil {
			t.Fatal(err)
		}
		c.Operations = ops
		chain = append(chain, c)
		at = c.ParentHashes[0]
	}
	slices.Reverse(chain)
	var out [][]byte
	for _, c := range chain {
		var vis []document.Op
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				if filter != nil && !sql.EvalPredicate(filter, document.Document{ID: o.DocID, JSON: o.Patch}, schema.None(), nil) ||
					checkReads && h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal,
						auth.DocumentReadAction{Namespace: h.namespaceID, DocID: o.DocID.String()}) != nil {
					vis = append(vis, document.DeleteOp{DocID: o.DocID})
					continue
				}
				vis = append(vis, o)
			case document.DeleteOp:
				vis = append(vis, o)
			default:
				if filter == nil {
					vis = append(vis, op)
				}
			}
		}
		if vis == nil && (filter != nil || checkReads) {
			vis = []document.Op{}
		}
		frame, err := h.wire.Encode(wire.DeltaCommitMessage{
			H: wire.Header{MessageType: wire.MsgDeltaCommit, ProtocolVersion: wire.KdbWireProtocolVersion},
			Payload: wire.DeltaCommitPayload{
				Namespace: h.namespaceID, CommitHash: c.Hash, ParentHash: c.ParentHashes[0],
				TimestampMicros: c.Timestamp.EpochMicros(), Operations: vis,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, frame)
	}
	return out
}

type fanoutSub struct {
	name      string
	filter    string
	anonymous bool
	sync      bool
	conn      *capturingConn
	handle    stream.ConnectionHandle
}

// TestStreamFanoutMatchesPerSubscriberEncoding: many subscribers - unfiltered and filtered,
// sharing filters and not, anonymous and read-checked, with a document the read check hides -
// each receive byte for byte the frames the per-subscriber path encoded for them, under
// correlation ids that are unique across the hub and increasing per subscriber. Two writers
// commit concurrently, so -race sees the shared cache under contention.
func TestStreamFanoutMatchesPerSubscriberEncoding(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	secret := mustRandomUUID(t)
	rt.AuthEngine = hideDocs{hidden: map[string]bool{secret.String(): true}}
	hub := hubFor(t, rt)
	start, _ := rt.dag.Head()

	var subs []*fanoutSub
	add := func(name, filter string, anonymous, sync bool) {
		s := &fanoutSub{name: name, filter: filter, anonymous: anonymous, sync: sync, conn: &capturingConn{}}
		s.handle = s.conn
		if sync {
			s.handle = syncCapturingConn{s.conn}
		}
		subs = append(subs, s)
	}
	for i := 0; i < 4; i++ {
		add(fmt.Sprintf("all-%d", i), "", false, i%2 == 0)
		add(fmt.Sprintf("eu-%d", i), `region = 'EU'`, false, i%2 == 1)
	}
	add("all-anon", "", true, true)
	add("eu-anon", `region = 'EU'`, true, false)
	add("us", `region = 'US'`, false, true)
	add("eu-respelled", `region='EU'`, false, false)

	for _, s := range subs {
		hub.SetAllowAnonymous(s.anonymous)
		req := wire.HandshakePayload{NodeID: s.name, Namespaces: []string{ns}, ClientMode: wire.ClientStreamReadOnly}
		if s.filter != "" {
			f := s.filter
			req.Filter = &f
		}
		_, begin := hub.handleHandshake(s.handle, wire.HandshakeMessage{
			H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}, Request: req,
		})
		if begin == nil {
			t.Fatalf("%s: handshake refused", s.name)
		}
		begin()
		handle := s.handle
		t.Cleanup(func() { hub.unregister(handle) })
	}
	hub.SetAllowAnonymous(false)

	// Writers: upserts moving documents in and out of each filter, the hidden document, and
	// multi-operation commits with deletes.
	eu, us := mustRandomUUID(t), mustRandomUUID(t)
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				region := []string{"EU", "US"}[(i+w)%2]
				var err error
				switch i % 4 {
				case 0:
					_, err = rt.Upsert(ns, eu, fmt.Sprintf(`{"region":%q,"i":%d}`, region, i), auth.Principal{})
				case 1:
					_, err = rt.Upsert(ns, secret, fmt.Sprintf(`{"region":%q,"secret":"s%d"}`, region, i), auth.Principal{})
				case 2:
					_, err = rt.Upsert(ns, us, fmt.Sprintf(`{"region":%q,"w":%d}`, region, w), auth.Principal{})
				case 3:
					err = commitOps(rt, ns, []document.Op{
						document.WriteOp{DocID: mustRandomUUID(t), Patch: fmt.Sprintf(`{"region":%q,"batch":%d}`, region, i)},
						document.WriteOp{DocID: mustRandomUUID(t), Patch: `{"region":"EU","batch":true}`},
					})
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	head, _ := rt.dag.Head()
	deadline := time.Now().Add(10 * time.Second)
	for _, s := range subs {
		for s.conn.at() != head {
			if time.Now().After(deadline) {
				t.Fatalf("%s stuck short of the head", s.name)
			}
			time.Sleep(time.Millisecond)
		}
	}

	seen := map[int]string{}
	for _, s := range subs {
		var filter sql.Expr
		if s.filter != "" {
			f, err := sql.ParseFilter(s.filter)
			if err != nil {
				t.Fatal(err)
			}
			filter = f
		}
		want := legacyStreamFrames(t, hub, filter, !s.anonymous, auth.Principal{}, start, head)
		got := s.conn.snapshot()
		if len(got) != len(want) {
			t.Fatalf("%s: %d frames, legacy path sends %d", s.name, len(got), len(want))
		}
		last := -1
		for i := range got {
			h, err := wire.DecodeHeader(got[i])
			if err != nil {
				t.Fatal(err)
			}
			if h.CorrelationID <= last {
				t.Fatalf("%s: frame %d has correlation id %d after %d", s.name, i, h.CorrelationID, last)
			}
			last = h.CorrelationID
			if other, dup := seen[h.CorrelationID]; dup {
				t.Fatalf("correlation id %d sent to both %s and %s", h.CorrelationID, other, s.name)
			}
			seen[h.CorrelationID] = s.name
			masked := bytes.Clone(got[i])
			wire.SetCorrelationID(masked, 0)
			if !bytes.Equal(masked, want[i]) {
				t.Fatalf("%s: frame %d differs from the per-subscriber encoding:\n got %s\nwant %s", s.name, i, masked[13:], want[i][13:])
			}
			if !s.anonymous && bytes.Contains(got[i], []byte(`secret`)) {
				t.Fatalf("%s: was sent the hidden document: %s", s.name, got[i][13:])
			}
		}
	}
}

func commitOps(rt *KdbServerRuntime, ns string, ops []document.Op) error {
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	_, err = rt.Commit(ns, document.Transaction{ID: id, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()}, "", auth.Principal{})
	return err
}

// TestStreamFanoutRBACSubscribersShareOnlyWhatTheyMayRead: under RBAC, subscribers with read
// access and one (rw2) whose every document read is denied, all authenticated and subscribed
// through the real handshake: the readers get every body, rw2 none - shared frames are never
// handed to a subscriber whose own read check denies a write in them.
func TestStreamFanoutRBACSubscribersShareOnlyWhatTheyMayRead(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	if err := store.CreateRole("rw", []string{"read:app/data", "write:app/data"}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"rw", "rw2"} {
		if err := store.CreateUser(u, "pw", []string{"rw"}); err != nil {
			t.Fatal(err)
		}
	}
	rt.AuthEngine = denyReadsFor{inner: engine, user: "rw2"}
	ns := rt.Runtime.DefaultNamespace
	hub := hubFor(t, rt)
	readers := []*capturingConn{handshakeAs(t, hub, "rw"), handshakeAs(t, hub, "rw")}
	restricted := handshakeAs(t, hub, "rw2")

	user, pw := "rw", "pw"
	writer, err := engine.Authenticator().Authenticate(context.Background(), auth.Credentials{User: &user, Password: &pw})
	if err != nil {
		t.Fatal(err)
	}
	var last codec.Hash
	for i := 0; i < 5; i++ {
		c, err := rt.Upsert(ns, mustRandomUUID(t), fmt.Sprintf(`{"body":"visible-%d"}`, i), writer)
		if err != nil {
			t.Fatal(err)
		}
		last = c.Hash
	}
	for _, c := range append(readers, restricted) {
		deadline := time.Now().Add(30 * time.Second)
		for c.at() != last {
			if time.Now().After(deadline) {
				t.Fatal("subscriber stuck")
			}
			time.Sleep(time.Millisecond)
		}
	}
	for _, c := range readers {
		if n := len(c.snapshot()); n != 5 {
			t.Fatalf("reader got %d frames for 5 commits", n)
		}
		for _, f := range c.snapshot() {
			if !bytes.Contains(f, []byte("visible-")) {
				t.Fatalf("reader missed a body: %s", f[13:])
			}
		}
	}
	if n := len(restricted.snapshot()); n != 5 {
		t.Fatalf("restricted subscriber got %d frames for 5 commits", n)
	}
	for _, f := range restricted.snapshot() {
		if bytes.Contains(f, []byte("visible-")) {
			t.Fatalf("restricted subscriber was sent a body: %s", f[13:])
		}
	}
}

// handshakeAs subscribes as user (password "pw").
func handshakeAs(t *testing.T, hub *StreamHub, user string) *capturingConn {
	t.Helper()
	c := &capturingConn{}
	pw := "pw"
	_, begin := hub.handleHandshake(syncCapturingConn{c}, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Request: wire.HandshakePayload{NodeID: user, Namespaces: []string{hub.namespaceID}, ClientMode: wire.ClientStreamReadOnly,
			User: &user, Password: &pw},
	})
	if begin == nil {
		t.Fatalf("handshake as %s refused", user)
	}
	begin()
	t.Cleanup(func() { hub.unregister(syncCapturingConn{c}) })
	return c
}

// denyReadsFor is inner with every document read by user refused.
type denyReadsFor struct {
	inner auth.Engine
	user  string
}

func (d denyReadsFor) Authenticator() auth.Authenticator { return d.inner.Authenticator() }
func (d denyReadsFor) Authorizer() auth.Authorizer       { return d }
func (d denyReadsFor) Authorize(ctx context.Context, p auth.Principal, a auth.Action) error {
	if _, ok := a.(auth.DocumentReadAction); ok && p.ID == d.user {
		return fmt.Errorf("%s may not read", d.user)
	}
	return d.inner.Authorizer().Authorize(ctx, p, a)
}

// TestStreamFanoutSlowSubscriberDoesNotHoldUpOthers: one subscriber stops reading in the middle
// of the shared commits; every other one still reaches the head, and the stuck one catches up
// once released.
func TestStreamFanoutSlowSubscriberDoesNotHoldUpOthers(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	hub := hubFor(t, rt)
	stuck := newStuckConn()
	handshakeStreamSubscriber(t, hub, stuck, ns)
	t.Cleanup(func() { hub.unregister(stuck) })
	var fast []*capturingConn
	for i := 0; i < 8; i++ {
		c := &capturingConn{}
		var h stream.ConnectionHandle = c
		if i%2 == 0 {
			h = syncCapturingConn{c}
		}
		handshakeStreamSubscriber(t, hub, h, ns)
		t.Cleanup(func() { hub.unregister(h) })
		fast = append(fast, c)
	}
	for i := 0; i < 20; i++ {
		if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"i":1}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := rt.dag.Head()
	deadline := time.Now().Add(30 * time.Second)
	for _, c := range fast {
		for c.at() != head {
			if time.Now().After(deadline) {
				t.Fatal("a fast subscriber was held up behind a stuck one")
			}
			time.Sleep(time.Millisecond)
		}
		if n := len(c.snapshot()); n != 20 {
			t.Fatalf("fast subscriber got %d frames for 20 commits", n)
		}
	}
	if stuck.sent.Load() != 0 {
		t.Fatal("stuck subscriber sent something before release")
	}
	close(stuck.release)
	for stuck.last() != head {
		if time.Now().After(deadline) {
			t.Fatal("stuck subscriber never caught up once released")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFanoutCacheIsBounded: the cache holds at most fanoutCacheMaxEntries commits and
// fanoutCacheMaxBytes bytes, evicts least recently used first, does not keep a commit bigger
// than its whole budget, and a hit does not reload.
func TestFanoutCacheIsBounded(t *testing.T) {
	c := newFanoutCache()
	loads := 0
	commit := func(i int, body int) (codec.Hash, func() (document.Commit, error)) {
		var h codec.Hash
		h.Bytes[0], h.Bytes[1] = byte(i), byte(i>>8)
		return h, func() (document.Commit, error) {
			loads++
			return document.Commit{Hash: h, ParentHashes: []codec.Hash{{}}, Operations: []document.Op{
				document.WriteOp{Patch: strings.Repeat("x", body)},
			}}, nil
		}
	}
	for i := 0; i < fanoutCacheMaxEntries+10; i++ {
		h, load := commit(i, 10)
		if _, err := c.get(h, load); err != nil {
			t.Fatal(err)
		}
	}
	if c.order.Len() != fanoutCacheMaxEntries || len(c.byHash) != fanoutCacheMaxEntries {
		t.Fatalf("holds %d entries, bound is %d", c.order.Len(), fanoutCacheMaxEntries)
	}
	loads = 0
	h, load := commit(fanoutCacheMaxEntries+9, 10) // most recent: a hit
	if _, _ = c.get(h, load); loads != 0 {
		t.Fatal("a hit reloaded")
	}
	h, load = commit(0, 10) // the oldest: evicted
	if _, _ = c.get(h, load); loads != 1 {
		t.Fatal("an evicted commit was not reloaded")
	}

	// Bytes: commits of a quarter of the budget each; at most four fit.
	c = newFanoutCache()
	for i := 0; i < 10; i++ {
		h, load := commit(1000+i, fanoutCacheMaxBytes/4-1024)
		if _, err := c.get(h, load); err != nil {
			t.Fatal(err)
		}
		if c.bytes > fanoutCacheMaxBytes {
			t.Fatalf("holds %d bytes, bound is %d", c.bytes, fanoutCacheMaxBytes)
		}
	}
	if c.order.Len() > 4 {
		t.Fatalf("holds %d quarter-budget commits", c.order.Len())
	}
	h, load = commit(2000, fanoutCacheMaxBytes+1)
	e, err := c.get(h, load)
	if err != nil || e == nil || e.resident || c.byHash[h] != nil {
		t.Fatal("an over-budget commit was cached (or not returned)")
	}
	// A view's frames are charged to the cache, and charging past the budget evicts.
	before := c.bytes
	front := c.order.Front().Value.(*fanoutEntry)
	c.charge(front, 1000)
	if c.bytes != before+1000 && c.bytes > fanoutCacheMaxBytes {
		t.Fatalf("charge: %d -> %d", before, c.bytes)
	}
}
