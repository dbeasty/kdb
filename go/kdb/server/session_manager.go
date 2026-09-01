package server

import (
	"fmt"
	"sync"

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

	// leaseMu guards leases. Frames on one connection are dispatched concurrently (see
	// SqlWireListen's pipelining), so a client can have a LockAcquire and a TxCommit in flight
	// at once against the same session.
	leaseMu sync.Mutex
	// leases are the document leases this session took explicitly through LockAcquire, keyed by
	// document id. Tracked here - rather than only in the LockManager - because a commit has to
	// tell the leases a client is holding across round trips from the implicit locks the commit
	// itself takes and drops: releasing by session id would silently revoke the former.
	leases map[codec.UUID]transaction.Lease
}

// TrackLease records an explicitly acquired lease on the session.
func (s *KdbSession) TrackLease(lease transaction.Lease) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.leases == nil {
		s.leases = make(map[codec.UUID]transaction.Lease)
	}
	s.leases[lease.DocID] = lease
}

// UntrackLease forgets an explicitly acquired lease.
func (s *KdbSession) UntrackLease(docID codec.UUID) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	delete(s.leases, docID)
}

// ClearLeases forgets every tracked lease. Pairs with LockManager.ReleaseAll: the session's own
// view of what it holds must not outlive the manager's.
func (s *KdbSession) ClearLeases() {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.leases = nil
}

// LeasesFor returns the tracked leases covering docIDs, in the order given. Documents the
// session holds no explicit lease on are skipped - the commit path takes its own implicit lock
// for those.
func (s *KdbSession) LeasesFor(docIDs []codec.UUID) []transaction.Lease {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	var out []transaction.Lease
	for _, id := range docIDs {
		if l, ok := s.leases[id]; ok {
			out = append(out, l)
		}
	}
	return out
}

// SessionManager tracks active sessions for a server runtime.
type SessionManager struct {
	server   *KdbServerRuntime
	mu       sync.Mutex
	sessions map[string]*KdbSession
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
		// Minted from the *runtime's* counter, not this manager's. Each connection gets its own
		// SessionManager, so a per-manager counter handed every connection its own "sess-1" -
		// harmless while session ids were only ever looked up within their own connection, but
		// the document lock manager is runtime-global and keys ownership by session id. Two
		// connections both calling themselves "sess-1" were therefore treated as one holder:
		// each could take locks the other held, and either could release the other's.
		id = fmt.Sprintf("sess-%d", m.server.nextSessionOrdinal())
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

// All returns every live session, for connection-scoped cleanup. Each connection gets its own
// SessionManager (see newSqlWireConnHandler), so this is exactly the set belonging to one
// client.
func (m *SessionManager) All() []*KdbSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*KdbSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
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
