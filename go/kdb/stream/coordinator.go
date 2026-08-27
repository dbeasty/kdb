package stream

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TransactionReplayer handles an incoming TransactionReplay frame from a Mode 2 (write-back
// stream) subscriber, returning the wire response to send back - a wire.SqlResultMessage on
// success or error, a wire.ConflictReportMessage on a genuine conflict (kdb-spec.md §8.1's Mode
// 2 definition; §8.5's TransactionReplay/ConflictReport message types). Mirrors Kotlin's
// StreamBroadcastHub transactionReplayer callback: a plain function rather than a direct
// KdbServerRuntime dependency, since kdb-stream must not depend on kdb-server (kdb-server already
// depends on kdb-stream) - whoever wires both together (go/cmd/kdb-service) supplies the closure,
// typically calling KdbServerRuntime.Replay.
type TransactionReplayer func(wire.TransactionReplayMessage) wire.Message

// Coordinator publishes commits and tracks subscribers.
type Coordinator interface {
	Start(session SessionConfig) error
	Stop() error
	Publish(commit PublishedCommit) error
	Subscribers() <-chan SubscriberState
	// SetTransactionReplayer wires (or clears, with nil) the handler for Mode 2 write-back
	// TransactionReplay frames - see TransactionReplayer's doc comment. Unset (nil) is the
	// default: TransactionReplay is then rejected with an explicit error response rather than
	// silently dropped.
	SetTransactionReplayer(replayer TransactionReplayer)
}

type defaultCoordinator struct {
	wire        wire.Codec
	transport   Transport
	correlation int
	session     *SessionConfig
	subscribers chan SubscriberState
	lastSub     *SubscriberState
	replayer    TransactionReplayer
	mu          sync.Mutex
}

// NewCoordinator creates a stream coordinator backed by in-memory transport when applicable.
func NewCoordinator(w wire.Codec, transport Transport) Coordinator {
	return &defaultCoordinator{
		wire:        w,
		transport:   transport,
		correlation: 1000,
		subscribers: make(chan SubscriberState, 16),
	}
}

func (c *defaultCoordinator) Subscribers() <-chan SubscriberState { return c.subscribers }

func (c *defaultCoordinator) SetTransactionReplayer(replayer TransactionReplayer) {
	c.mu.Lock()
	c.replayer = replayer
	c.mu.Unlock()
}

func (c *defaultCoordinator) Start(session SessionConfig) error {
	c.mu.Lock()
	c.session = &session
	c.mu.Unlock()
	if _, ok := c.transport.(*InMemoryTransport); !ok {
		return nil
	}
	hub := HubFor(session.NamespaceID)
	hub.ServerHandler = func(frame []byte) {
		c.handleServerFrame(session, frame, hub)
	}
	return nil
}

func (c *defaultCoordinator) Stop() error {
	c.mu.Lock()
	ns := ""
	if c.session != nil {
		ns = c.session.NamespaceID
	}
	c.session = nil
	c.mu.Unlock()
	if ns == "" {
		return nil
	}
	HubFor(ns).ServerHandler = nil
	return nil
}

func (c *defaultCoordinator) Publish(commit PublishedCommit) error {
	c.mu.Lock()
	cfg := c.session
	correlation := c.correlation
	c.correlation++
	c.mu.Unlock()
	if cfg == nil {
		return nil
	}
	msg := wire.DeltaCommitMessage{
		H: wire.Header{
			MessageType:     wire.MsgDeltaCommit,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   correlation,
		},
		Payload: wire.DeltaCommitPayload{
			Namespace:       cfg.NamespaceID,
			CommitHash:      commit.CommitHash,
			ParentHash:      commit.ParentHash,
			TimestampMicros: commit.TimestampMicros,
			Operations:      commit.Operations,
			IndexHints:      commit.IndexHints,
		},
	}
	frame, err := c.wire.Encode(msg)
	if err != nil {
		return err
	}
	if _, ok := c.transport.(*InMemoryTransport); ok {
		HubFor(cfg.NamespaceID).ServerSend(frame)
	}
	return nil
}

func (c *defaultCoordinator) handleServerFrame(session SessionConfig, frame []byte, hub *Hub) {
	message, err := c.wire.Decode(frame)
	if err != nil {
		return
	}
	switch msg := message.(type) {
	case wire.HandshakeMessage:
		ack := wire.HandshakeAckPayload{
			Accepted:           true,
			NegotiatedEncoding: wire.EncodingKdbBinary,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        msg.Request.LocalHeads,
		}
		ackMsg := wire.HandshakeAckMessage{
			H: wire.Header{
				MessageType:     wire.MsgHandshake,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   msg.H.CorrelationID,
			},
			Response: ack,
		}
		if frame, err := c.wire.Encode(ackMsg); err == nil {
			hub.ServerSend(frame)
		}
		mode := ClientReadOnly
		if msg.Request.ClientMode == wire.ClientStreamWriteBack {
			mode = ClientWriteBack
		}
		head, _ := session.HeadProvider()
		c.updateSubscriber(msg.Request.NodeID, mode, &head)
	case wire.PositionAckMessage:
		c.mu.Lock()
		existing := c.lastSub
		c.mu.Unlock()
		if existing != nil {
			hash := msg.CommitHash
			c.updateSubscriber(existing.NodeID, existing.Mode, &hash)
		}
	case wire.TransactionReplayMessage:
		if msg.Namespace != session.NamespaceID {
			return
		}
		c.mu.Lock()
		replayer := c.replayer
		c.mu.Unlock()
		var response wire.Message
		if replayer != nil {
			response = replayer(msg)
		} else {
			errMsg := "write-back replay is not enabled on this stream host"
			response = wire.SqlResultMessage{
				H:         wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
				Namespace: msg.Namespace,
				Error:     &errMsg,
			}
		}
		if frame, err := c.wire.Encode(response); err == nil {
			hub.ServerSend(frame)
		}
	}
}

func (c *defaultCoordinator) updateSubscriber(nodeID string, mode ClientMode, lastAck *codec.Hash) {
	state := SubscriberState{NodeID: nodeID, Mode: mode, LastAck: lastAck}
	c.mu.Lock()
	c.lastSub = &state
	c.mu.Unlock()
	select {
	case c.subscribers <- state:
	default:
	}
}
