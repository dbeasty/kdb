package server

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
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
// The returned *StreamHub's Publish method is what turns a local commit into a DeltaCommit frame
// for every connected subscriber - wire it via KdbServerRuntime.CommitListener (see that field's
// doc comment) to fire automatically on every write, the way go/cmd/kdb-service does.
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

// subscriberQueueDepth is how many DeltaCommit frames one subscriber may fall behind before
// Publish starts dropping them. Sized to absorb a normal burst of commits (a batch import, a
// compaction's worth of deltas) without ever letting one connection's socket backpressure reach
// the writer that produced the commit.
const subscriberQueueDepth = 256

type registeredSubscriber struct {
	nodeID  string
	conn    stream.ConnectionHandle
	lastAck *codec.Hash

	// outbound is this subscriber's own queue, drained by its own goroutine (sendLoop). Publish
	// only ever hands frames to this channel, never to conn.Send: socketConnection.Send does a
	// blocking write on the TCP socket while holding that connection's mutex, so fanning out
	// inline meant one subscriber whose receive window had filled up stalled the whole fan-out -
	// and with it the goroutine that had just committed, which calls Publish through
	// KdbServerRuntime.CommitListener while still holding its admission grant. "Best-effort,
	// non-blocking" was the documented contract; this is what actually makes it true.
	outbound chan []byte
	// stop is closed exactly once, when this subscriber is unregistered, to retire sendLoop.
	stop     chan struct{}
	stopOnce sync.Once
	// dropped counts frames Publish could not queue because outbound was full. A subscriber that
	// cannot keep up misses frames by design - but silently missing them is not the same as
	// missing them visibly, and this is the only signal that a client's view has a hole in it
	// that it needs to resync (reconnect with LocalHeads) to close.
	dropped atomic.Int64
}

// enqueue hands frame to this subscriber's sender goroutine without ever blocking. A full queue
// means the subscriber is not keeping up: the frame is dropped and counted rather than allowed
// to hold up every other subscriber and the committing writer behind it.
func (s *registeredSubscriber) enqueue(frame []byte) {
	select {
	case <-s.stop:
	case s.outbound <- frame:
	default:
		s.dropped.Add(1)
	}
}

func (s *registeredSubscriber) retire() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// sendLoop is one goroutine per subscriber, and the only place conn.Send is called for fan-out
// frames. A send that fails means the connection is gone: unregister it (which retires this
// loop) rather than spinning on a dead socket.
func (h *StreamHub) sendLoop(sub *registeredSubscriber) {
	for {
		select {
		case <-sub.stop:
			return
		case frame := <-sub.outbound:
			if err := sub.conn.Send(frame); err != nil {
				h.unregister(sub.conn)
				return
			}
		}
	}
}

// StreamHub fans out DeltaCommit frames to every currently-connected Mode 1/2 subscriber for one
// namespace, and serves Mode 2's TransactionReplay - the network-listening counterpart to
// go/kdb/stream's Coordinator. Safe for concurrent use: Publish (called from whatever goroutine
// just committed) and run (one per accepted connection) both only ever touch subscribers under
// mu.
type StreamHub struct {
	wire        wire.Codec
	namespaceID string
	runtime     *KdbServerRuntime

	mu          sync.Mutex
	subscribers []*registeredSubscriber
	correlation int
}

// NewStreamHub creates a stream hub for namespaceID, backed by runtime for both head lookups
// (handshake responses) and TransactionReplay (write-back). Exported so a caller that already
// has its own transport (e.g. a test using InMemoryTransport-style wiring, or a future
// WebSocket listener) can drive one without going through ListenStream's TCP-specific setup.
func NewStreamHub(w wire.Codec, namespaceID string, runtime *KdbServerRuntime) *StreamHub {
	return &StreamHub{wire: w, namespaceID: namespaceID, runtime: runtime, correlation: 5000}
}

// Publish broadcasts commit as a DeltaCommit frame to every currently-registered subscriber and
// returns without waiting for any of them - a slow or dead subscriber must not block or crash the
// publisher, matching StreamBroadcastHub.publish's own best-effort fan-out. Each subscriber has
// its own bounded queue and its own sender goroutine; a subscriber that cannot drain fast enough
// loses frames (counted in DroppedFrames) instead of applying backpressure to the writer.
func (h *StreamHub) Publish(commit stream.PublishedCommit) {
	h.mu.Lock()
	cid := h.correlation
	h.correlation++
	targets := append([]*registeredSubscriber(nil), h.subscribers...)
	h.mu.Unlock()

	msg := wire.DeltaCommitMessage{
		H: wire.Header{MessageType: wire.MsgDeltaCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: cid},
		Payload: wire.DeltaCommitPayload{
			Namespace:       h.namespaceID,
			CommitHash:      commit.CommitHash,
			ParentHash:      commit.ParentHash,
			TimestampMicros: commit.TimestampMicros,
			Operations:      commit.Operations,
			IndexHints:      commit.IndexHints,
		},
	}
	frame, err := h.wire.Encode(msg)
	if err != nil {
		return
	}
	for _, sub := range targets {
		sub.enqueue(frame)
	}
}

// DroppedFrames reports how many DeltaCommit frames have been dropped across all subscribers
// currently registered, because they could not keep up with the fan-out. Non-zero means at least
// one client's view has a gap in it and needs to reconnect (with its LocalHeads) to resync.
func (h *StreamHub) DroppedFrames() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var total int64
	for _, sub := range h.subscribers {
		total += sub.dropped.Load()
	}
	return total
}

func (h *StreamHub) run(conn stream.ConnectionHandle) {
	defer h.unregister(conn)
	for frame := range conn.Incoming() {
		response := h.handleFrame(conn, frame)
		if response == nil {
			continue
		}
		if err := conn.Send(response); err != nil {
			return
		}
	}
}

func (h *StreamHub) handleFrame(conn stream.ConnectionHandle, frame []byte) []byte {
	msg, err := h.wire.Decode(frame)
	if err != nil {
		return nil
	}
	switch m := msg.(type) {
	case wire.HandshakeMessage:
		return h.handleHandshake(conn, m)
	case wire.PositionAckMessage:
		h.updateLastAck(conn, m.CommitHash)
		return nil
	case wire.TransactionReplayMessage:
		return h.handleTransactionReplay(m)
	default:
		return nil
	}
}

func (h *StreamHub) handleHandshake(conn stream.ConnectionHandle, msg wire.HandshakeMessage) []byte {
	mode := msg.Request.ClientMode
	if mode != wire.ClientStreamReadOnly && mode != wire.ClientStreamWriteBack {
		return h.encodeHandshakeReject(msg, "STREAM_READ_ONLY or STREAM_WRITE_BACK required")
	}
	if !slices.Contains(msg.Request.Namespaces, h.namespaceID) {
		return h.encodeHandshakeReject(msg, "namespace mismatch")
	}
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil {
		return h.encodeHandshakeReject(msg, err.Error())
	}
	var resume *codec.Hash
	if hex, ok := msg.Request.LocalHeads[h.namespaceID]; ok && hex != "" {
		if parsed, err := codec.HashFromHex(hex); err == nil {
			resume = &parsed
		}
	}
	sub := &registeredSubscriber{
		nodeID:   msg.Request.NodeID,
		conn:     conn,
		lastAck:  resume,
		outbound: make(chan []byte, subscriberQueueDepth),
		stop:     make(chan struct{}),
	}
	h.mu.Lock()
	h.removeConnLocked(conn)
	h.subscribers = append(h.subscribers, sub)
	h.mu.Unlock()
	go h.sendLoop(sub)

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
		return nil
	}
	return frame
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

// handleTransactionReplay serves Mode 2 write-back. Unlike the SQL_CLIENT entry point
// (wire_listen.go's handleTransactionReplay), a stream connection has no per-connection
// authenticated principal - its handshake never authenticates, matching Kotlin's
// StreamBroadcastHub.handleHandshake exactly - so this authorizes an anonymous auth.Principal{}
// directly against runtime.AuthEngine: a no-op against the default auth.AllowAll, and a
// deliberate fail-closed choice if RBAC is enabled (kdb-service's --rbac), rather than silently
// bypassing it the way an unauthenticated caller reaching straight into runtime.Replay would.
func (h *StreamHub) handleTransactionReplay(msg wire.TransactionReplayMessage) []byte {
	if msg.Namespace != h.namespaceID {
		return nil
	}
	var reply wire.Message
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), auth.Principal{}, auth.TxCommitAction{Namespace: msg.Namespace}); err != nil {
		reply = sqlResultError(msg.H.CorrelationID, msg.Namespace, "", (&AuthorizationError{Cause: err}).Error())
	} else {
		reply = replayTransaction(h.runtime, auth.Principal{}, msg)
	}
	frame, err := h.wire.Encode(reply)
	if err != nil {
		return nil
	}
	return frame
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
