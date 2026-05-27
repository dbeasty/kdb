package stream

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Coordinator publishes commits and tracks subscribers.
type Coordinator interface {
	Start(session SessionConfig) error
	Stop() error
	Publish(commit PublishedCommit) error
	Subscribers() <-chan SubscriberState
}

type defaultCoordinator struct {
	wire        wire.Codec
	transport   Transport
	correlation int
	session     *SessionConfig
	subscribers chan SubscriberState
	lastSub     *SubscriberState
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
