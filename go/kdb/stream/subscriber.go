package stream

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Subscriber connects to a stream coordinator and receives delta commits.
type Subscriber interface {
	Connect(cfg SubscriberConfig) (*Connection, error)
	Disconnect() error
	Events() <-chan Event
}

type defaultSubscriber struct {
	wire          wire.Codec
	transport     Transport
	hintApplier   IndexHintApplier
	conn          ConnectionHandle
	position      *codec.Hash
	config        *SubscriberConfig
	correlation   int
	events        chan Event
	mu            sync.Mutex
	stopIncoming  chan struct{}
}

// NewSubscriber creates a stream subscriber.
func NewSubscriber(w wire.Codec, transport Transport, hintApplier IndexHintApplier) Subscriber {
	if hintApplier == nil {
		hintApplier = DefaultHintApplier()
	}
	return &defaultSubscriber{
		wire:        w,
		transport:   transport,
		hintApplier: hintApplier,
		correlation: 1,
		events:      make(chan Event, 32),
	}
}

func (s *defaultSubscriber) Events() <-chan Event { return s.events }

func (s *defaultSubscriber) Connect(cfg SubscriberConfig) (*Connection, error) {
	if cfg.Mode == ClientWriteBack {
		// v1: write-back requires transaction engine (not wired in Go port yet).
	}
	conn, err := s.transport.Connect(cfg.CoordinatorURI)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.conn = conn
	s.config = &cfg
	s.position = cfg.ResumeFrom
	s.stopIncoming = make(chan struct{})
	s.mu.Unlock()

	wireMode := wire.ClientStreamReadOnly
	if cfg.Mode == ClientWriteBack {
		wireMode = wire.ClientStreamWriteBack
	}
	localHeads := map[string]string{}
	if cfg.ResumeFrom != nil {
		localHeads[cfg.NamespaceID] = cfg.ResumeFrom.Hex()
	}
	hs := wire.HandshakeMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   s.nextCorrelation(),
		},
		Request: wire.HandshakePayload{
			NodeID:     cfg.NodeID,
			Namespaces: []string{cfg.NamespaceID},
			LocalHeads: localHeads,
			ClientMode: wireMode,
		},
	}
	go s.readLoop(conn)
	frame, err := s.wire.Encode(hs)
	if err != nil {
		return nil, err
	}
	if err := conn.Send(frame); err != nil {
		return nil, err
	}
	return &Connection{
		NamespaceID: cfg.NamespaceID,
		Mode:        cfg.Mode,
		Position:    s.currentPosition,
	}, nil
}

func (s *defaultSubscriber) Disconnect() error {
	s.mu.Lock()
	conn := s.conn
	stop := s.stopIncoming
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if stop != nil {
		close(stop)
	}
	s.emit(Event{Kind: EventDisconnected})
	return nil
}

func (s *defaultSubscriber) readLoop(conn ConnectionHandle) {
	incoming := conn.Incoming()
	for {
		select {
		case <-s.stopIncoming:
			return
		case frame, ok := <-incoming:
			if !ok {
				return
			}
			s.handleFrame(frame)
		}
	}
}

func (s *defaultSubscriber) handleFrame(frame []byte) {
	s.mu.Lock()
	cfg := s.config
	conn := s.conn
	s.mu.Unlock()
	if cfg == nil {
		return
	}
	msg, err := s.wire.Decode(frame)
	if err != nil {
		s.emit(Event{Kind: EventError, Cause: err})
		return
	}
	switch m := msg.(type) {
	case wire.HandshakeAckMessage:
		if !m.Response.Accepted {
			reason := "handshake rejected"
			if m.Response.RejectionReason != nil {
				reason = *m.Response.RejectionReason
			}
			s.emit(Event{Kind: EventError, Cause: fmt.Errorf(reason)})
			return
		}
		s.emit(Event{Kind: EventConnected, Encoding: m.Response.NegotiatedEncoding})
	case wire.DeltaCommitMessage:
		p := m.Payload
		if p.Namespace != cfg.NamespaceID {
			return
		}
		s.mu.Lock()
		expected := s.position
		s.mu.Unlock()
		if expected != nil && p.ParentHash != *expected {
			s.emit(Event{Kind: EventError, Cause: NewDesyncError(expected.Hex(), p.ParentHash.Hex())})
			return
		}
		s.hintApplier.Apply(cfg.NamespaceID, p.IndexHints)
		s.mu.Lock()
		s.position = &p.CommitHash
		s.mu.Unlock()
		s.emit(Event{Kind: EventDeltaReceived, CommitHash: p.CommitHash, HintCount: len(p.IndexHints)})
		s.emit(Event{Kind: EventPositionUpdated, CommitHash: p.CommitHash})
		if conn != nil {
			ack := wire.PositionAckMessage{
				H: wire.Header{
					MessageType:     wire.MsgPositionAck,
					ProtocolVersion: wire.KdbWireProtocolVersion,
					CorrelationID:   m.H.CorrelationID,
				},
				Namespace:  cfg.NamespaceID,
				CommitHash: p.CommitHash,
			}
			if frame, err := s.wire.Encode(ack); err == nil {
				_ = conn.Send(frame)
			}
		}
	case wire.CompactionNoticeMessage:
		s.emit(Event{Kind: EventCompactionWarning, Boundary: m.Intent.Boundary})
	case wire.IceArchiveNoticeMessage:
		s.emit(Event{Kind: EventIceArchived, OriginalHash: m.OriginalHash, ArchiveLoc: m.ArchiveLocation})
	case wire.ConflictReportMessage:
		s.emit(Event{Kind: EventError, Cause: fmt.Errorf("conflict reported")})
	}
}

func (s *defaultSubscriber) currentPosition() *codec.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position
}

func (s *defaultSubscriber) nextCorrelation() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.correlation
	s.correlation++
	return id
}

func (s *defaultSubscriber) emit(ev Event) {
	select {
	case s.events <- ev:
	default:
	}
}

// SubmitTransaction sends a transaction replay frame (v1 async stub).
func SubmitTransaction(s Subscriber, tx document.Transaction) ReplayResult {
	reason := "async replay not awaited in v1"
	return ReplayResult{Rejected: &reason}
}
