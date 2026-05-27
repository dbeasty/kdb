package stream

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ClientMode is the stream subscriber role.
type ClientMode int

const (
	ClientReadOnly ClientMode = iota
	ClientWriteBack
)

// SessionConfig configures a stream coordinator session.
type SessionConfig struct {
	NamespaceID  string
	NodeID       string
	HeadProvider func() (codec.Hash, error)
}

// SubscriberConfig configures a stream subscriber connection.
type SubscriberConfig struct {
	NamespaceID    string
	NodeID         string
	Mode           ClientMode
	CoordinatorURI string
	ResumeFrom     *codec.Hash
}

// PublishedCommit is a commit broadcast to subscribers.
type PublishedCommit struct {
	CommitHash      codec.Hash
	ParentHash      codec.Hash
	Operations      []document.Op
	IndexHints      []wire.IndexHint
	TimestampMicros int64
}

// Connection is an active subscriber session handle.
type Connection struct {
	NamespaceID string
	Mode        ClientMode
	Position    func() *codec.Hash
}

// ReplayResult is the outcome of submitting a transaction for replay.
type ReplayResult struct {
	Applied     *codec.Hash
	Conflict    *kdberr.ConflictReport
	Rejected    *string
}

// EventKind classifies stream subscriber events.
type EventKind int

const (
	EventConnected EventKind = iota
	EventDeltaReceived
	EventPositionUpdated
	EventCompactionWarning
	EventIceArchived
	EventDisconnected
	EventError
)

// Event is one subscriber lifecycle notification.
type Event struct {
	Kind          EventKind
	Encoding      wire.PayloadEncoding
	CommitHash    codec.Hash
	HintCount     int
	Boundary      codec.Hash
	ArchiveLoc    string
	OriginalHash  codec.Hash
	Cause         error
}

// SubscriberState tracks one connected subscriber.
type SubscriberState struct {
	NodeID  string
	Mode    ClientMode
	LastAck *codec.Hash
}
