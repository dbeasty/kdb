package server

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
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
	Pending         any // *transaction.Builder when ported
	Principal       any
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
