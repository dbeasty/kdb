package server

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ListenStream starts a TCP stream listener bound to addr, serving Mode 1 (read-only delta
// fan-out) and Mode 2 (write-back) subscribers for runtime's namespace (kdb-spec.md §8.1) -
// go/kdb/stream's own Coordinator only ever wires up a server handler for *InMemoryTransport
// (see its Start/Publish), so it has never had a real network listener. This is that listener,
// modeled structurally on ListenSqlWire/ListenPeerSync's own tcp.Transport accept-loop shape,
// but with a real per-connection subscriber registry (StreamHub) rather than SQL's per-connection
// session or peer-sync's namespace-wide DAG state - fan-out needs to reach every connected
// subscriber, not just the one that sent a given frame. Mirrors Kotlin's StreamBroadcastHub,
// which is the component actually wired into a running kdb-service today.
//
// The returned *StreamHub's Publish method is what wakes every connected subscriber to be caught
// up to the new head - wire it via KdbServerRuntime.CommitListener (see that field's doc comment)
// to fire automatically on every write, the way go/cmd/kdb-service does.
func ListenStream(addr string, runtime *KdbServerRuntime, namespaceID string) (*StreamHub, *Listener, error) {
	return ListenStreamTLS(addr, runtime, namespaceID, nil)
}

// ListenStreamTLS is ListenStream with TLS settings for a tcps:// addr - see
// core.TransportTlsSettings. Pass nil for plaintext (equivalent to ListenStream).
func ListenStreamTLS(addr string, runtime *KdbServerRuntime, namespaceID string, tlsSettings *core.TransportTlsSettings) (*StreamHub, *Listener, error) {
	opts := core.DefaultConnectOptions()
	opts.TLS = tlsSettings
	transport := tcp.NewTransport(opts)
	ln, err := transport.ListenBound(addr)
	if err != nil {
		return nil, nil, err
	}
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), namespaceID, runtime)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &Listener{ln: ln, cancel: cancel, done: done}
	go func() {
		defer close(done)
		_ = transport.Serve(ctx, ln, hub.run)
	}()
	return hub, l, nil
}

// maxPerCommitCatchUp is how far behind a subscriber may be and still be sent the commits it
// missed one by one. Further behind, it gets their net effect as one frame instead: the per-commit
// history is not what a subscriber catching up needs, and walking it costs the hub.
const maxPerCommitCatchUp = 64

type registeredSubscriber struct {
	nodeID  string
	conn    stream.ConnectionHandle
	lastAck *codec.Hash
	// principal is who this connection's handshake authenticated as; write-back replays run as
	// it, and it is who each document is checked against before a subscriber sees it.
	principal auth.Principal
	// filter limits the subscription to the documents matching it; nil is every document.
	filter sql.Expr
	// checkReads is whether each document is checked against principal before it is sent. Off
	// for an anonymous hub, which is documented to show everything to everyone.
	checkReads bool

	// position is the commit this subscriber holds, as far as the hub knows: its resume point,
	// then the last frame sent. Owned by sendLoop.
	position codec.Hash
	// wake is poked by Publish. One pending poke is enough: sendLoop always catches up to the
	// head as it finds it, so any number of commits while it was busy cost one catch-up.
	wake chan struct{}
	// stop is closed exactly once, when this subscriber is unregistered, to retire sendLoop.
	stop     chan struct{}
	stopOnce sync.Once
}

func (s *registeredSubscriber) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *registeredSubscriber) retire() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// sendLoop is one goroutine per subscriber, and the only place its fan-out frames are sent. It
// brings the subscriber from its position to the head each time it is woken, so a subscriber
// that is slow, or was away, is never left with a gap: it gets what it missed, not a subset of
// it. A send that fails means the connection is gone.
func (h *StreamHub) sendLoop(sub *registeredSubscriber) {
	for {
		select {
		case <-sub.stop:
			return
		case <-sub.wake:
		}
		if err := h.catchUp(sub); err != nil {
			slog.Warn("stream: subscriber dropped", "node", sub.nodeID, "namespace", h.namespaceID, "error", err)
			_ = sub.conn.Close()
			h.unregister(sub.conn)
			return
		}
	}
}

// catchUp sends sub everything between its position and the head. When the head's first-parent
// line leads back to the position within maxPerCommitCatchUp commits, each commit goes as it was
// made; otherwise - a merge that put the position on a side branch, a long absence, commits whose
// operations were archived - one frame carries the difference between the two trees, named for
// the head. Either way, applying the frames to what the subscriber held at its position gives
// the head, and the subscriber's parent check holds frame to frame.
func (h *StreamHub) catchUp(sub *registeredSubscriber) error {
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil || head == sub.position {
		return err
	}
	if chain, ok := h.firstParentChain(head, sub.position); ok {
		for _, c := range chain {
			if err := h.sendDelta(sub, c.Hash, sub.position, c.Timestamp.EpochMicros(), c.Operations); err != nil {
				return err
			}
			sub.position = c.Hash
		}
		return nil
	}
	ops, ts, err := h.treeDelta(sub.position, head)
	if err != nil {
		return err
	}
	if err := h.sendDelta(sub, head, sub.position, ts, ops); err != nil {
		return err
	}
	sub.position = head
	return nil
}

// firstParentChain lists, oldest first and with their operations, the commits on head's
// first-parent line down to (not including) from - if from is on it within reach and every one
// of them still has its operations.
func (h *StreamHub) firstParentChain(head, from codec.Hash) ([]document.Commit, bool) {
	d := h.runtime.dag
	if d == nil {
		return nil, false
	}
	var chain []document.Commit
	for at := head; at != from; {
		if len(chain) == maxPerCommitCatchUp || d.HasStub(at) {
			return nil, false
		}
		c, ok := d.GetCommit(at)
		if !ok || len(c.ParentHashes) == 0 {
			return nil, false
		}
		ops, err := d.CommitOperations(at)
		if err != nil {
			return nil, false
		}
		c.Operations = ops
		chain = append(chain, c)
		at = c.ParentHashes[0]
	}
	slices.Reverse(chain)
	return chain, true
}

// treeDelta is the difference between the trees at from and to, as operations: the final body
// of every document added or changed, a delete for every one removed.
func (h *StreamHub) treeDelta(from, to codec.Hash) ([]document.Op, int64, error) {
	rt := h.runtime.Runtime
	diff, err := embed.DiffCommits(rt, from.Hex(), to.Hex())
	if err != nil {
		return nil, 0, err
	}
	target, err := rt.DAG.GetCommitOrThrow(to)
	if err != nil {
		return nil, 0, err
	}
	var ops []document.Op
	var changed []codec.UUID
	for _, e := range diff.Entries {
		switch x := e.(type) {
		case dag.DiffAdded:
			changed = append(changed, x.DocID)
		case dag.DiffModified:
			changed = append(changed, x.DocID)
		case dag.DiffRemoved:
			ops = append(ops, document.DeleteOp{DocID: x.DocID})
		}
	}
	const chunk = 256
	for start := 0; start < len(changed); start += chunk {
		ids := changed[start:min(start+chunk, len(changed))]
		docs, err := rt.Storage.GetDocuments(h.namespaceID, ids, target.DocumentTreeHash)
		if err != nil {
			return nil, 0, err
		}
		for i, d := range docs {
			if d == nil {
				ops = append(ops, document.DeleteOp{DocID: ids[i]})
				continue
			}
			ops = append(ops, document.WriteOp{DocID: d.ID, Patch: d.JSON})
		}
	}
	return ops, target.Timestamp.EpochMicros(), nil
}

// visible is what sub may see of ops: a write of a document outside its filter, or that it may
// not read, becomes a delete - it may have held the document before, and must not keep it.
// Operations that name no document pass only on an unfiltered subscription.
func (h *StreamHub) visible(sub *registeredSubscriber, ops []document.Op) []document.Op {
	if sub.filter == nil && !sub.checkReads {
		return ops
	}
	out := make([]document.Op, 0, len(ops))
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			if sub.filter != nil && !sql.EvalPredicate(sub.filter, document.Document{ID: o.DocID, JSON: o.Patch}, schema.None(), nil) ||
				sub.checkReads && h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), sub.principal,
					auth.DocumentReadAction{Namespace: h.namespaceID, DocID: o.DocID.String()}) != nil {
				out = append(out, document.DeleteOp{DocID: o.DocID})
				continue
			}
			out = append(out, o)
		case document.DeleteOp:
			out = append(out, o)
		default:
			if sub.filter == nil {
				out = append(out, op)
			}
		}
	}
	return out
}

func (h *StreamHub) sendDelta(sub *registeredSubscriber, commit, parent codec.Hash, ts int64, ops []document.Op) error {
	h.mu.Lock()
	cid := h.correlation
	h.correlation++
	h.mu.Unlock()
	frame, err := h.wire.Encode(wire.DeltaCommitMessage{
		H: wire.Header{MessageType: wire.MsgDeltaCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: cid},
		Payload: wire.DeltaCommitPayload{
			Namespace: h.namespaceID, CommitHash: commit, ParentHash: parent,
			TimestampMicros: ts, Operations: h.visible(sub, ops),
		},
	})
	if err != nil {
		return err
	}
	return sub.conn.Send(frame)
}

// StreamHub serves Mode 1/2 subscribers for one namespace - the network-listening counterpart to
// go/kdb/stream's Coordinator - and Mode 2's TransactionReplay.
//
// A subscription is a filtered projection that is not stored (distributed plan, Phase 7.5): each
// subscriber is followed from its position to the head, with the same filter and per-document
// read checks a projection gets. The frames come from the DAG, not from what Publish was handed,
// which is what makes a subscription resumable: a subscriber that reconnects with its position
// (LocalHeads) gets exactly what it missed, and one that falls behind is caught up rather than
// dropped frames. Safe for concurrent use: Publish only wakes sender goroutines, and the
// subscriber list is only touched under mu.
type StreamHub struct {
	wire        wire.Codec
	namespaceID string
	runtime     *KdbServerRuntime

	mu          sync.Mutex
	subscribers []*registeredSubscriber
	correlation int

	// allowAnonymous skips authenticating the handshake: every subscriber is the anonymous
	// principal, as every subscriber was before the handshake authenticated at all. Off by
	// default; only an explicit operator opt-in (kdb-service --stream-allow-anonymous) turns it
	// on, since under RBAC it lets anyone read the whole namespace's commit stream. Atomic
	// because the hub is already accepting connections when a caller sets it.
	allowAnonymous atomic.Bool
}

// SetAllowAnonymous turns anonymous subscription on or off - see StreamHub.allowAnonymous.
func (h *StreamHub) SetAllowAnonymous(on bool) { h.allowAnonymous.Store(on) }

// NewStreamHub creates a stream hub for namespaceID, backed by runtime for both head lookups
// (handshake responses) and TransactionReplay (write-back). Exported so a caller that already
// has its own transport (e.g. a test using InMemoryTransport-style wiring, or a future
// WebSocket listener) can drive one without going through ListenStream's TCP-specific setup.
func NewStreamHub(w wire.Codec, namespaceID string, runtime *KdbServerRuntime) *StreamHub {
	return &StreamHub{wire: w, namespaceID: namespaceID, runtime: runtime, correlation: 5000}
}

// Publish tells every subscriber the namespace has moved, and returns at once: each subscriber's
// own goroutine then catches it up from the DAG. The commit is not what gets sent - a subscriber
// is sent everything between its position and the head, however many commits that is - so it is
// accepted only for the CommitListener signature. A slow or dead subscriber cannot block or
// crash the publisher, and never loses a commit: it is caught up when it drains.
func (h *StreamHub) Publish(stream.PublishedCommit) {
	h.mu.Lock()
	targets := append([]*registeredSubscriber(nil), h.subscribers...)
	h.mu.Unlock()
	for _, sub := range targets {
		sub.poke()
	}
}

func (h *StreamHub) run(conn stream.ConnectionHandle) {
	defer h.unregister(conn)
	for frame := range conn.Incoming() {
		msg, err := h.wire.Decode(frame)
		if err != nil {
			continue
		}
		if hs, ok := msg.(wire.HandshakeMessage); ok {
			// The ack goes before any delta: the subscriber is only started once it is sent.
			ack, start := h.handleHandshake(conn, hs)
			if ack != nil {
				if err := conn.Send(ack); err != nil {
					return
				}
			}
			if start != nil {
				start()
			}
			continue
		}
		response := h.handleFrame(conn, msg)
		if response == nil {
			continue
		}
		if err := conn.Send(response); err != nil {
			return
		}
	}
}

func (h *StreamHub) handleFrame(conn stream.ConnectionHandle, msg wire.Message) []byte {
	switch m := msg.(type) {
	case wire.PositionAckMessage:
		h.updateLastAck(conn, m.CommitHash)
		return nil
	case wire.TransactionReplayMessage:
		return h.handleTransactionReplay(conn, m)
	default:
		return nil
	}
}

// handleHandshake registers a subscriber and returns the ack, and a start function to call once
// the ack is sent (nil when the handshake is refused).
func (h *StreamHub) handleHandshake(conn stream.ConnectionHandle, msg wire.HandshakeMessage) ([]byte, func()) {
	mode := msg.Request.ClientMode
	if mode != wire.ClientStreamReadOnly && mode != wire.ClientStreamWriteBack {
		return h.encodeHandshakeReject(msg, "STREAM_READ_ONLY or STREAM_WRITE_BACK required"), nil
	}
	if !slices.Contains(msg.Request.Namespaces, h.namespaceID) {
		return h.encodeHandshakeReject(msg, "namespace mismatch"), nil
	}
	// The handshake authenticates (D11). It used not to at all, so under RBAC any client could
	// subscribe and receive every commit's full operations, and write back as nobody in
	// particular.
	principal := auth.Principal{}
	anonymous := h.allowAnonymous.Load()
	if !anonymous {
		creds := auth.Credentials{User: msg.Request.User, Password: msg.Request.Password, Token: msg.Request.Token}
		p, err := h.runtime.AuthEngine.Authenticator().Authenticate(context.Background(), creds)
		if err != nil {
			return h.encodeHandshakeReject(msg, err.Error()), nil
		}
		if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), p, auth.StreamSubscribeAction{Namespace: h.namespaceID}); err != nil {
			return h.encodeHandshakeReject(msg, err.Error()), nil
		}
		principal = p
	}
	var filter sql.Expr
	if msg.Request.Filter != nil && strings.TrimSpace(*msg.Request.Filter) != "" {
		f, err := sql.ParseFilter(*msg.Request.Filter)
		if err != nil {
			return h.encodeHandshakeReject(msg, "filter: "+err.Error()), nil
		}
		filter = f
	}
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil {
		return h.encodeHandshakeReject(msg, err.Error()), nil
	}
	// A subscriber with no position starts at the head: it asked for what happens from now on.
	// One with a position is caught up from it - if this namespace's main has it in its past.
	position := head
	if hex, ok := msg.Request.LocalHeads[h.namespaceID]; ok && hex != "" {
		p, err := codec.HashFromHex(hex)
		if err != nil || h.runtime.dag == nil || !h.runtime.dag.HasCommit(p) || (p != head && !h.runtime.dag.IsAncestor(p, head)) {
			return h.encodeHandshakeReject(msg, fmt.Sprintf(
				"resume position %s is not in %s's history here (never was, or truncated away); subscribe without one and reload", hex, h.namespaceID)), nil
		}
		position = p
	}
	sub := &registeredSubscriber{
		nodeID:     msg.Request.NodeID,
		conn:       conn,
		lastAck:    &position,
		principal:  principal,
		filter:     filter,
		checkReads: !anonymous,
		position:   position,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
	}
	h.mu.Lock()
	h.removeConnLocked(conn)
	h.subscribers = append(h.subscribers, sub)
	h.mu.Unlock()

	ack := wire.HandshakeAckMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
		Response: wire.HandshakeAckPayload{
			Accepted:           true,
			NegotiatedEncoding: wire.EncodingJSON,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        map[string]string{h.namespaceID: head.Hex()},
		},
	}
	frame, err := h.wire.Encode(ack)
	if err != nil {
		return nil, nil
	}
	return frame, func() {
		go h.sendLoop(sub)
		sub.poke() // a resumed subscriber is caught up at once, not at the next commit
	}
}

func (h *StreamHub) encodeHandshakeReject(msg wire.HandshakeMessage, reason string) []byte {
	ack := wire.HandshakeAckMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
		Response: wire.HandshakeAckPayload{
			Accepted:           false,
			NegotiatedEncoding: wire.EncodingJSON,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        map[string]string{},
			RejectionReason:    &reason,
		},
	}
	frame, err := h.wire.Encode(ack)
	if err != nil {
		return nil
	}
	return frame
}

// handleTransactionReplay serves Mode 2 write-back, as the principal this connection's handshake
// authenticated. A connection that never completed a handshake has no principal and is refused
// unless the hub allows anonymous access, in which case it is the anonymous principal - a no-op
// against auth.AllowAll and fail-closed under RBAC.
func (h *StreamHub) handleTransactionReplay(conn stream.ConnectionHandle, msg wire.TransactionReplayMessage) []byte {
	if msg.Namespace != h.namespaceID {
		return nil
	}
	principal, ok := h.principalFor(conn)
	var reply wire.Message
	if !ok && !h.allowAnonymous.Load() {
		reply = sqlResultError(msg.H.CorrelationID, msg.Namespace, "", "stream write-back requires a completed handshake")
	} else if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: msg.Namespace}); err != nil {
		reply = sqlResultError(msg.H.CorrelationID, msg.Namespace, "", (&AuthorizationError{Cause: err}).Error())
	} else {
		reply = replayTransaction(h.runtime, principal, msg)
	}
	frame, err := h.wire.Encode(reply)
	if err != nil {
		return nil
	}
	return frame
}

func (h *StreamHub) principalFor(conn stream.ConnectionHandle) (auth.Principal, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range h.subscribers {
		if sub.conn == conn {
			return sub.principal, true
		}
	}
	return auth.Principal{}, false
}

func (h *StreamHub) updateLastAck(conn stream.ConnectionHandle, commitHash codec.Hash) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range h.subscribers {
		if sub.conn == conn {
			sub.lastAck = &commitHash
		}
	}
}

func (h *StreamHub) unregister(conn stream.ConnectionHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeConnLocked(conn)
}

func (h *StreamHub) removeConnLocked(conn stream.ConnectionHandle) {
	kept := h.subscribers[:0]
	for _, sub := range h.subscribers {
		if sub.conn != conn {
			kept = append(kept, sub)
			continue
		}
		// Retire the sender goroutine along with the registration, or a re-handshake on the same
		// connection (removeConnLocked's other caller) would leave the old one running and two
		// goroutines writing interleaved frames to the same socket.
		sub.retire()
	}
	h.subscribers = kept
}
