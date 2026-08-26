package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// replayTimeout bounds how long SubmitTransaction waits for a TransactionReplay response before
// giving up with ReplayResult.Rejected - matches Kotlin's REPLAY_TIMEOUT_MILLIS.
const replayTimeout = 30 * time.Second

// Subscriber connects to a stream coordinator and receives delta commits.
type Subscriber interface {
	Connect(cfg SubscriberConfig) (*Connection, error)
	Disconnect() error
	Events() <-chan Event
}

type defaultSubscriber struct {
	wire         wire.Codec
	transport    Transport
	hintApplier  IndexHintApplier
	conn         ConnectionHandle
	position     *codec.Hash
	config       *SubscriberConfig
	correlation  int
	events       chan Event
	mu           sync.Mutex
	stopIncoming chan struct{}
	// pendingReplays tracks in-flight SubmitTransaction calls by correlation id, mirroring
	// go/kdb/client's own request/response pattern (client.go's `pending` map) - handleFrame
	// delivers a matching SqlResultMessage/ConflictReportMessage here instead of treating it as
	// an unsolicited stream event. Reset fresh on every Connect (Kotlin's own pendingReplays is
	// per-DefaultStreamSubscriber-instance, but this Go subscriber is reused across reconnects,
	// so a stale entry from a prior connection must not linger).
	pendingReplays map[int]chan wire.Message
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
	conn, err := s.transport.Connect(cfg.CoordinatorURI)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.conn = conn
	s.config = &cfg
	s.position = cfg.ResumeFrom
	s.stopIncoming = make(chan struct{})
	s.pendingReplays = make(map[int]chan wire.Message)
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
		SubmitTransaction: func(tx document.Transaction) ReplayResult {
			return s.submitTransaction(cfg, tx)
		},
	}, nil
}

func (s *defaultSubscriber) Disconnect() error {
	s.mu.Lock()
	conn := s.conn
	stop := s.stopIncoming
	s.conn = nil
	stale := s.pendingReplays
	s.pendingReplays = nil
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if stop != nil {
		close(stop)
	}
	// Don't leave any in-flight SubmitTransaction call hanging until its timeout - the
	// connection it was waiting on is gone. Closing (not sending on) each channel makes the
	// waiting submitTransaction's `ok` check false, which it reports as this same rejection.
	for _, ch := range stale {
		close(ch)
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
			s.emit(Event{Kind: EventError, Cause: errors.New(reason)})
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
	case wire.SqlResultMessage:
		// The response shape a real server's TransactionReplay handler actually sends back
		// (go/kdb/server's handleTransactionReplay, mirroring Kotlin's SqlWireHost.
		// handleTransactionReplay) - not a dedicated ack type, so this must be recognized as
		// one. Never sent unsolicited on a stream connection, so silently drop it if there's no
		// matching SubmitTransaction call waiting.
		s.resolveReplay(m.H.CorrelationID, m)
	case wire.ConflictReportMessage:
		// A real TransactionReplay conflict response - deliver it to the waiting
		// SubmitTransaction call if there is one; only treat it as an unsolicited stream error
		// otherwise (this message type has no other current use on a stream connection, but
		// the fallback keeps this defensive rather than assuming).
		if !s.resolveReplay(m.H.CorrelationID, m) {
			s.emit(Event{Kind: EventError, Cause: errors.New("conflict reported")})
		}
	}
}

// resolveReplay delivers msg to the pending SubmitTransaction call awaiting correlationID, if
// any, and reports whether one was found - mirrors Kotlin's pendingReplays map/
// CompletableDeferred.complete.
func (s *defaultSubscriber) resolveReplay(correlationID int, msg wire.Message) bool {
	s.mu.Lock()
	ch, ok := s.pendingReplays[correlationID]
	if ok {
		delete(s.pendingReplays, correlationID)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	ch <- msg
	return true
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

// submitTransaction is Mode 2 (write-back stream)'s write path (kdb-spec.md §8.1): send tx as a
// TransactionReplay frame and block until the coordinator's response arrives or replayTimeout
// elapses, exposed to callers via Connection.SubmitTransaction. Mirrors Kotlin's
// DefaultStreamSubscriber.submitTransaction, and the request/await-with-timeout shape of
// go/kdb/client's own request() method (same pattern, this package's own correlation map
// instead of a dedicated one, since handleFrame already owns frame dispatch for this
// connection).
func (s *defaultSubscriber) submitTransaction(cfg SubscriberConfig, tx document.Transaction) ReplayResult {
	s.mu.Lock()
	conn := s.conn
	base := tx.BaseVersion
	if s.position != nil {
		base = *s.position
	}
	s.mu.Unlock()
	if conn == nil {
		reason := "not connected"
		return ReplayResult{Rejected: &reason}
	}

	txBytes, err := wire.EncodeTransaction(tx)
	if err != nil {
		reason := "encode transaction: " + err.Error()
		return ReplayResult{Rejected: &reason}
	}
	correlationID := s.nextCorrelation()
	msg := wire.TransactionReplayMessage{
		H: wire.Header{
			MessageType:     wire.MsgTransactionReplay,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   correlationID,
		},
		Namespace:        cfg.NamespaceID,
		BaseVersion:      base,
		TransactionBytes: txBytes,
	}
	frame, err := s.wire.Encode(msg)
	if err != nil {
		reason := "encode frame: " + err.Error()
		return ReplayResult{Rejected: &reason}
	}

	replyCh := make(chan wire.Message, 1)
	s.mu.Lock()
	if s.pendingReplays == nil {
		s.mu.Unlock()
		reason := "not connected"
		return ReplayResult{Rejected: &reason}
	}
	s.pendingReplays[correlationID] = replyCh
	s.mu.Unlock()

	if err := conn.Send(frame); err != nil {
		s.mu.Lock()
		delete(s.pendingReplays, correlationID)
		s.mu.Unlock()
		reason := "send: " + err.Error()
		return ReplayResult{Rejected: &reason}
	}

	select {
	case reply, ok := <-replyCh:
		if !ok {
			reason := "disconnected while awaiting replay response"
			return ReplayResult{Rejected: &reason}
		}
		return replayResultFrom(reply)
	case <-time.After(replayTimeout):
		s.mu.Lock()
		delete(s.pendingReplays, correlationID)
		s.mu.Unlock()
		reason := "timed out waiting for replay response"
		return ReplayResult{Rejected: &reason}
	}
}

// replayResultFrom translates a TransactionReplay response frame into a ReplayResult - a
// SqlResultMessage is the real server's ack/error shape (see wire_listen.go's
// handleTransactionReplay doc comment for why it isn't a dedicated ack type), a
// ConflictReportMessage is a genuine conflict.
func replayResultFrom(msg wire.Message) ReplayResult {
	switch m := msg.(type) {
	case wire.SqlResultMessage:
		if m.Error != nil {
			return ReplayResult{Rejected: m.Error}
		}
		hash, err := codec.HashFromHex(m.ResolvedCommitHex)
		if err != nil {
			reason := "invalid resolved commit hash: " + err.Error()
			return ReplayResult{Rejected: &reason}
		}
		return ReplayResult{Applied: &hash}
	case wire.ConflictReportMessage:
		var report kdberr.ConflictReport
		if err := json.Unmarshal(m.ReportBytes, &report); err != nil {
			reason := "undecodable conflict report: " + err.Error()
			return ReplayResult{Rejected: &reason}
		}
		return ReplayResult{Conflict: &report}
	default:
		reason := fmt.Sprintf("unexpected reply type %T", msg)
		return ReplayResult{Rejected: &reason}
	}
}
