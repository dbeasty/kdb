package server

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
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
	ReadConsistency ReadConsistency
	Pending         *transaction.Builder
	Principal       auth.Principal

	// versionMu guards baseVersion and readPin. Both move at a transaction boundary while
	// reads on the same session may be in flight: frames on one connection are dispatched
	// concurrently (see SqlWireListen's pipelining), so a TxCommit advancing the session's
	// version genuinely races a SELECT resolving the version to read at.
	versionMu sync.RWMutex
	// baseVersion is the commit this session's next write anchors on.
	baseVersion codec.Hash
	// readPin is the snapshot the session's current transaction reads at, and is set only for
	// Snapshot consistency - ReadCommitted and ReadYourWrites deliberately follow the live
	// head. It is re-taken at every transaction boundary (session begin, commit, rollback),
	// which is what makes this snapshot isolation rather than a single session-lifetime
	// snapshot: holding one pin forever means a session cannot observe its own committed
	// writes, which is neither useful nor what SNAPSHOT means.
	readPin *codec.Hash
	// pinRelease drops the DAG retention pin taken for readPin, and is guarded by versionMu
	// alongside it. Holding the hash in readPin is not by itself protection: nothing stopped a
	// compaction reclaiming that commit while a session read at it, because a read pin is not a
	// branch head and there was no registry to look one up in (dag.Pin's doc comment). Nil
	// whenever readPin is - a non-SNAPSHOT session reads the live head, which is a branch head
	// and therefore already a retention root.
	pinRelease func()
	// dag is the concrete DAG this session pins against, nil for a runtime that has none (see
	// KdbServerRuntime.dag). Pinning is skipped entirely when nil - the same degradation the
	// commit path already accepts there.
	dag *dag.InMemoryCommitDag

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

// BaseVersion returns the commit this session's next write anchors on.
func (s *KdbSession) BaseVersion() codec.Hash {
	s.versionMu.RLock()
	defer s.versionMu.RUnlock()
	return s.baseVersion
}

// ReadHead resolves the commit this session's reads must run at. Snapshot sessions read at
// the pin taken when their current transaction started; every other consistency reads the
// live head, which liveHead supplies.
//
// Before this existed, SessionManager.Begin computed a pin that execRead never consulted, so
// a SNAPSHOT session's reads silently behaved as READ_COMMITTED (kdb-finish-up-plan.md 1-G8).
// Reading the pin fixed that but introduced the opposite failure: the pin was taken once and
// never moved, so a session could commit a write and then not see it. Both are covered by
// tests in read_consistency_test.go.
func (s *KdbSession) ReadHead(liveHead func() (codec.Hash, error)) (codec.Hash, error) {
	if s.ReadConsistency == Snapshot {
		s.versionMu.RLock()
		pin := s.readPin
		s.versionMu.RUnlock()
		if pin != nil {
			return *pin, nil
		}
	}
	return liveHead()
}

// startTransactionAt opens the session's next transaction at head: writes anchor there, and a
// Snapshot session's reads are pinned there until the transaction ends. Called at session
// begin and again at every commit and rollback.
func (s *KdbSession) startTransactionAt(head codec.Hash) {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	s.baseVersion = head
	s.releasePinLocked()
	if s.ReadConsistency == Snapshot {
		pinned := head
		s.readPin = &pinned
		if s.dag != nil {
			s.pinRelease = s.dag.Pin(head)
		}
	}
}

// releasePinLocked drops the current transaction's read pin, if any, and clears it. Must be
// called with versionMu held exclusively. Every path that ends a transaction goes through here:
// starting the next one (above) and ending the session (EndTransaction).
func (s *KdbSession) releasePinLocked() {
	if s.pinRelease != nil {
		s.pinRelease()
		s.pinRelease = nil
	}
	s.readPin = nil
}

// EndTransaction drops this session's read pin without opening another - what session teardown
// needs, as distinct from the transaction boundaries that immediately re-pin. A session whose
// connection dropped mid-transaction must not go on holding a commit against compaction
// forever, which is the read-side twin of the document leases closeAllSessions already releases.
func (s *KdbSession) EndTransaction() {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	s.releasePinLocked()
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
		ReadConsistency: readConsistency,
		Principal:       principal,
		dag:             m.server.dag,
	}
	// The session's first transaction starts here, at the version it was opened against.
	sess.startTransactionAt(head)
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

// End removes a session and drops the read pin it was holding - without this a session whose
// connection dropped would hold a commit against compaction for the process lifetime, the
// read-side twin of the document locks closeAllSessions releases.
//
// The release happens after the map delete and outside m.mu, deliberately: it takes the DAG's
// lock, and holding the session map's lock across that would nest SessionManager.mu inside
// dag.mu for no reason. The delete-then-release order is safe because the delete is what claims
// the right to release - a concurrent End finds nothing and releases nothing.
func (m *SessionManager) End(sessionID string) {
	m.mu.Lock()
	sess, ok := m.sessions[sessionID]
	delete(m.sessions, sessionID)
	m.mu.Unlock()
	if ok {
		sess.EndTransaction()
	}
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
			BaseVersion: sess.BaseVersion(),
			Schema:      m.server.Schema(),
		}
	}
	return sess.Pending
}
