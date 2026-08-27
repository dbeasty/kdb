package server

import (
	"context"
	"slices"
	"sync"

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

type registeredSubscriber struct {
	nodeID  string
	conn    stream.ConnectionHandle
	lastAck *codec.Hash
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

// Publish broadcasts commit as a DeltaCommit frame to every currently-registered subscriber,
// unregistering any connection whose Send fails - a slow or dead subscriber must not block or
// crash the publisher, matching StreamBroadcastHub.publish's own best-effort fan-out.
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
		if err := sub.conn.Send(frame); err != nil {
			h.unregister(sub.conn)
		}
	}
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
	h.mu.Lock()
	h.removeConnLocked(conn)
	h.subscribers = append(h.subscribers, &registeredSubscriber{nodeID: msg.Request.NodeID, conn: conn, lastAck: resume})
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
		}
	}
	h.subscribers = kept
}
