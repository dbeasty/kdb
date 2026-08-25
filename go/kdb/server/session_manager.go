package server

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// ReadConsistency mirrors dev.kdb.query.hybrid.ReadConsistency.
type ReadConsistency int

const (
	ReadCommitted ReadConsistency = iota
	ReadYourWrites
	Snapshot
)

// SessionID identifies a server-side SQL/session context.
type SessionID struct {
	Value string
}

// KdbSession holds per-connection session state (skeleton).
type KdbSession struct {
	ID              SessionID
	NamespaceID     string
	BaseVersion     codec.Hash
	ReadPin         *codec.Hash
	ReadConsistency ReadConsistency
	Pending         *transaction.Builder
	Principal       auth.Principal
}

// SessionManager tracks active sessions for a server runtime.
type SessionManager struct {
	server   *KdbServerRuntime
	mu       sync.Mutex
	sessions map[string]*KdbSession
	idSeq    atomic.Int32
}

// NewSessionManager creates a session manager for the given server runtime.
func NewSessionManager(server *KdbServerRuntime) *SessionManager {
	return &SessionManager{
		server:   server,
		sessions: make(map[string]*KdbSession),
	}
}

// Begin opens a new session pinned to a base DAG version.
func (m *SessionManager) Begin(
	namespaceID string,
	readConsistency ReadConsistency,
	baseVersionHex string,
	sessionID string,
	principal auth.Principal,
) (*KdbSession, error) {
	head, err := m.server.Runtime.DAG.Head()
	if err != nil {
		return nil, err
	}
	if baseVersionHex != "" {
		h, err := codec.HashFromHex(baseVersionHex)
		if err != nil {
			return nil, err
		}
		if _, ok := m.server.Runtime.DAG.GetCommit(h); !ok {
			return nil, fmt.Errorf("unknown base version: %s", baseVersionHex)
		}
		head = h
	}
	var readPin *codec.Hash
	if readConsistency == Snapshot {
		readPin = &head
	}
	id := sessionID
	if id == "" {
		id = fmt.Sprintf("sess-%d", m.idSeq.Add(1))
	}
	sess := &KdbSession{
		ID:              SessionID{Value: id},
		NamespaceID:     namespaceID,
		BaseVersion:     head,
		ReadPin:         readPin,
		ReadConsistency: readConsistency,
		Principal:       principal,
	}
	m.mu.Lock()
	m.sessions[id] = sess
	m.mu.Unlock()
	return sess, nil
}

// Get returns a session by id.
func (m *SessionManager) Get(sessionID string) (*KdbSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	return s, ok
}

// End removes a session.
func (m *SessionManager) End(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

// ClearPending clears in-flight transaction builder state.
func (m *SessionManager) ClearPending(sess *KdbSession) {
	sess.Pending = nil
}

// parseReadConsistency maps a wire ReadConsistency name to its enum value, matching Kotlin's
// dev.kdb.query.hybrid.ReadConsistency.valueOf naming (defaults to ReadCommitted for an
// unrecognized/empty name rather than erroring, since it only affects read-pin behavior).
func parseReadConsistency(name string) ReadConsistency {
	switch name {
	case "SNAPSHOT":
		return Snapshot
	case "READ_YOUR_WRITES":
		return ReadYourWrites
	default:
		return ReadCommitted
	}
}

func (c ReadConsistency) String() string {
	switch c {
	case Snapshot:
		return "SNAPSHOT"
	case ReadYourWrites:
		return "READ_YOUR_WRITES"
	default:
		return "READ_COMMITTED"
	}
}

// PendingBuilder returns sess's in-flight transaction builder, creating one anchored at the
// session's base version if this is the first buffered write.
func (m *SessionManager) PendingBuilder(sess *KdbSession) *transaction.Builder {
	if sess.Pending == nil {
		sess.Pending = &transaction.Builder{
			NamespaceID: sess.NamespaceID,
			BaseVersion: sess.BaseVersion,
			Schema:      m.server.Schema(),
		}
	}
	return sess.Pending
}
