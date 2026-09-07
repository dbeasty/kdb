package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/wire"
)

// DefaultWriteTimeout bounds how long a commit may wait queued behind other commits before
// giving up with a *DeadlineExceededError - see writeGate. Deliberately short: a client that
// hasn't heard back this long has likely already moved on (kdb-spec-layer13 Component 49 §6.2).
const DefaultWriteTimeout = 5 * time.Second

// DefaultMaxQueuedWrites bounds how many commits may be waiting at once before new ones are
// rejected outright with *BusyError instead of joining an unbounded queue - see writeGate.
const DefaultMaxQueuedWrites = 64

// KdbServerRuntime wraps an embedded runtime with write coordination. One instance serves one
// namespace (matching the Kotlin KdbServerRuntime's per-namespace TransactionEngine cache
// described in component 38 spec §3 - the multi-namespace registry lives in
// ServerRuntimeRegistry below, not here).
type KdbServerRuntime struct {
	Runtime *embed.EmbeddedKdbRuntime

	TransactionEngine transaction.Engine
	// UpsertEngine commits with ConflictPolicyLastWrite, backing Upsert below - component 40
	// spec §5: "Upsert never conflicts and never needs a BaseVersion", distinct from Commit's
	// STRICT/CAS semantics. A separate transaction.Engine instance, not a per-call policy
	// override, because Engine bakes its conflict policy in at construction.
	UpsertEngine  transaction.Engine
	DocumentLocks *transaction.LockManager

	// sqlEngineMu guards sqlEngine and sqlIndexProvider: SetSQLIndexProvider rebuilds the engine
	// while connections may be executing statements against the previous one.
	sqlEngineMu sync.RWMutex
	// sqlEngine reads through an expiryHidingAdapter (see expiry.go): head reads never return an
	// expired document, historical reads are untouched. Built by rebuildSQLEngine.
	sqlEngine sql.Engine
	// sqlIndexProvider backs MATCH/SIMILARITY/FUSE and indexed lookups, and - when it also
	// implements sql.IndexDDLExecutor - CREATE INDEX / DROP INDEX. nil (the default) means no
	// indexes: every query is a full scan and index DDL is refused, which is exactly the
	// behaviour before Layer 16. See SetSQLIndexProvider.
	sqlIndexProvider sql.IndexProvider

	// searchProvider serves SEARCH frames (kdb-spec-layer16 §11); see SetSearchProvider. nil -
	// the default - answers every search with an UNSUPPORTED "no index configured for search"
	// error. Behind an accessor rather than an exported field because the wiring that owns the
	// fulltext/vector indexes may install it after listeners are already accepting.
	searchProviderMu sync.RWMutex
	searchProvider   SearchProvider

	// AuthEngine authenticates/authorizes both at the wire layer (handshake/session-begin, see
	// wire_listen.go) and at commit time (see Commit) - one engine for both, so RBAC changes
	// (e.g. a revoked grant) take effect for the very next commit, not just the next connection.
	// Defaults to auth.AllowAll; set to auth.NewRegistryAuthEngine(store) to enable RBAC
	// (component 38 spec §4, sub-phase C).
	AuthEngine auth.Engine

	// CommitListener, if set, is invoked with namespaceID and the new commit after every
	// successful Commit/Upsert/Replay - the cross-write notification bridge a stream hub's
	// Publish needs to fan a live write out to Mode 1/2 subscribers without polling (Kotlin's
	// Component 44: EmbeddedKdbRuntime.addCommitListener/notifyCommit, wired to
	// streamHub.publish(...) in KdbServiceMain.kt). Called synchronously, after persistence, from
	// inside runTransaction's success path - keep it fast and non-blocking (e.g. StreamHub.Publish
	// itself only does a best-effort non-blocking fan-out). nil (the default) means no
	// notification; existing callers see no behavior change.
	CommitListener func(namespaceID string, commit document.Commit)

	refCount atomic.Int32
	closeMu  sync.Mutex

	// sessionSeq mints session ids that are unique across every connection this runtime serves.
	// It lives here rather than on SessionManager because a manager is per-connection while the
	// document lock manager, which keys ownership by session id, is per-runtime.
	sessionSeq atomic.Int64

	// schemaMu guards Runtime.Schema: CREATE TABLE (executed via SqlExec) updates it after
	// construction, and that update must not race with concurrent readers on other
	// connections. Use Schema()/SetSchema() rather than touching Runtime.Schema directly.
	schemaMu sync.RWMutex

	// writeGate serializes calls into TransactionEngine.Commit - the same load-bearing
	// exclusion a bare sync.Mutex would give (transaction.Engine.Commit reads the DAG head, runs
	// conflict detection against it, and only then appends the new commit, none of it holding
	// the DAG lock), plus a bounded queue and a per-caller deadline a bare mutex can't express
	// (see writeGate's own doc comment, kdb-spec-layer13 Component 49 §6.2 - "start only what we
	// can finish" applied to time).
	//
	// It is no longer the only thing standing between two racing writers and a lost write:
	// InMemoryCommitDag.AppendCommit now compare-and-swaps the branch head against the anchor it
	// was given, so a writer that lost the race gets a *dag.HeadConflictError instead of an
	// acknowledged-but-orphaned commit. The gate is still what keeps that from being the normal
	// outcome under load - it makes writers queue rather than collide - and it is what an
	// embedded caller driving Engine.Commit on a shared *InMemoryCommitDag still wants for
	// throughput, but the correctness floor is in the DAG itself now.
	writeGate *writeGate
	// WriteTimeout bounds how long a commit may wait queued before *DeadlineExceededError.
	// Defaults to DefaultWriteTimeout; safe to change at any time.
	WriteTimeout time.Duration
	// PeerSyncConflictPolicy selects how the peer-sync listener resolves a same-document
	// divergence pushed by a peer (see peersync.ResolutionOptions.Policy). Zero value
	// (ConflictPolicyAppendOnly) keeps the safe default: disjoint-document histories
	// auto-merge, same-document divergence returns a ConflictReport for the operator to
	// resolve. Set ConflictPolicyLastWrite for symmetric later-timestamp-wins convergence
	// (kdb-service's --peer-conflict-policy last-write).
	PeerSyncConflictPolicy transaction.ConflictPolicy

	// draining is set by an orderly shutdown (kdb-spec-layer13 Component 50) to reject every
	// new write immediately with *UnavailableError, ahead of even the memory-pressure check -
	// once draining, there is no budget left to spend on partial work.
	draining atomic.Bool

	// dag is the concrete DAG transaction.Engine's Commit/Replay/Merge/Validate require -
	// unwrapped once here from Runtime.DAG, whether that's a bare *dag.InMemoryCommitDag (memory
	// runtimes) or a *embed.PersistingCommitDAG's delegate (file-backed runtimes). nil only if
	// Runtime.DAG is some third, unsupported dag.CommitDAG implementation.
	dag *dag.InMemoryCommitDag
	// persister is non-nil only for file-backed runtimes. commitWith calls Engine.Commit against
	// dag directly (Engine needs the concrete type; PersistingCommitDAG can't be passed to it -
	// see Persist's own doc comment), which bypasses PersistingCommitDAG.AppendCommit and
	// therefore the durability it provides - persister.Persist is called explicitly afterward to
	// restore it. Before this field existed, a file-backed runtime's DAG never matched the old
	// direct *dag.InMemoryCommitDag type assertion at all, so every commit failed outright with
	// "commit requires an InMemoryCommitDag" - the native Go server's --data-dir mode was
	// entirely unable to write.
	persister *embed.PersistingCommitDAG

	// memGuard is nil by default (no memory limit configured, never rejects) - opt in via
	// SetMemoryLimit. See MemoryGuard's own doc comment for why this exists: an uncompacted
	// in-memory commit DAG grows without bound by design, so a constrained deployment needs to
	// reject new writes gracefully once it nears its budget, not get OOM-killed with no signal
	// to the client.
	memGuard *MemoryGuard
	// admission is nil by default, and non-nil exactly when memGuard has a configured budget -
	// see SetMemoryBudget. It is what turns the guard from a periodic sampler into real
	// admission control: work reserves the memory it is expected to hold before it starts,
	// rather than being waved through on the strength of a reading up to one poll interval old.
	admission *Admission
	// MaxConnections caps concurrently-accepted connections on listeners this runtime serves
	// (kdb-spec-layer13 Component 49 §6.5); 0 means unlimited. Set before calling ListenSqlWire.
	MaxConnections int

	// UniqueKeys enforces unique-declared schema fields across every writer on this runtime.
	// Shared by TransactionEngine and UpsertEngine - see NewKdbServerRuntime.
	UniqueKeys *transaction.UniqueKeyRegistry
	// UniqueKeyRebuildError records a failed registry rebuild (data already violating a declared
	// constraint, or an unreadable document). Non-nil means the registry may be incomplete, so
	// enforcement is best-effort until an operator resolves it; surfaced rather than swallowed
	// because silently degrading a correctness guarantee is worse than the violation itself.
	UniqueKeyRebuildError error

	// sweeperState holds document-expiry configuration and the sweeper goroutine (expiry.go).
	sweeperState

	// hookMu guards beforeSqlExec, which a test may set while the listener is already running.
	hookMu sync.RWMutex
	// beforeSqlExec, when set by a test, runs at the top of every SqlExec dispatch - the way a
	// test blocks one session's statement to prove other frames on the connection keep flowing
	// (kdb-spec-layer16 §12). Never set in production.
	beforeSqlExec func(msg wire.SqlExecMessage)
}

// SetBeforeSqlExecHookForTest installs a hook run at the top of every SqlExec dispatch, for
// tests that need to hold one statement open while other frames arrive on the same connection.
// Exported for tests in this package's own suite; never called in production.
func (s *KdbServerRuntime) SetBeforeSqlExecHookForTest(hook func(msg wire.SqlExecMessage)) {
	s.hookMu.Lock()
	s.beforeSqlExec = hook
	s.hookMu.Unlock()
}

func (s *KdbServerRuntime) beforeSqlExecHook() func(msg wire.SqlExecMessage) {
	s.hookMu.RLock()
	defer s.hookMu.RUnlock()
	return s.beforeSqlExec
}

// NewKdbServerRuntime creates a server runtime with ref-count 1, wiring the transaction and SQL
// engines against rt's DAG/storage. rt.DAG must be a *dag.InMemoryCommitDag or a
// *embed.PersistingCommitDAG wrapping one (true of every runtime
// embed.OpenMemoryRuntime/OpenFileRuntime construct today); Commit/Query return an error
// otherwise rather than panicking.
func NewKdbServerRuntime(rt *embed.EmbeddedKdbRuntime) *KdbServerRuntime {
	var d *dag.InMemoryCommitDag
	var persister *embed.PersistingCommitDAG
	switch concrete := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		d = concrete
	case *embed.PersistingCommitDAG:
		d = concrete.Delegate()
		persister = concrete
	}
	// One registry, shared by both engines. Commit and Upsert write documents into the same
	// namespace, so two registries would each be blind to the other's claims and neither would
	// be authoritative - a client could take an email through Upsert that Commit believed free.
	uniqueKeys := transaction.NewUniqueKeyRegistry()
	engineOpts := transaction.EngineOptions{UniqueKeys: uniqueKeys, Preconditions: true}
	s := &KdbServerRuntime{
		Runtime:           rt,
		TransactionEngine: transaction.NewEngineWithOptions(transaction.ConflictPolicyStrict, nil, engineOpts),
		UpsertEngine:      transaction.NewEngineWithOptions(transaction.ConflictPolicyLastWrite, nil, engineOpts),
		DocumentLocks:     transaction.NewLockManager(),
		UniqueKeys:        uniqueKeys,
		AuthEngine:        auth.AllowAll,
		dag:               d,
		persister:         persister,
		writeGate:         newWriteGate(DefaultMaxQueuedWrites),
		WriteTimeout:      DefaultWriteTimeout,
	}
	// The SQL engine reads through the expiry filter, not the raw adapter - this is the one place
	// the read side of §9.5 is applied to SQL, since go/kdb/sql knows nothing about expiry.
	s.rebuildSQLEngine()
	s.refCount.Store(1)
	// Populate the registry from what is already on disk. A failure here is reported by
	// RebuildUniqueKeys' own caller, not swallowed - but it must not prevent the runtime from
	// being constructed: a namespace whose stored data already violates a unique constraint is
	// an operator problem to see and fix, and refusing to open the runtime at all would remove
	// every tool for fixing it. Writes that would compound the violation are still rejected,
	// because a duplicate present in the rebuild is a claimed key like any other.
	if err := s.RebuildUniqueKeys(); err != nil {
		s.UniqueKeyRebuildError = err
	}
	return s
}

// SQLEngine returns the engine SQL statements execute against. Safe for concurrent use with
// SetSQLIndexProvider.
func (s *KdbServerRuntime) SQLEngine() sql.Engine {
	s.sqlEngineMu.RLock()
	defer s.sqlEngineMu.RUnlock()
	return s.sqlEngine
}

// SetSQLIndexProvider installs (or, with nil, removes) the index provider the SQL engine plans
// and executes against - kdb-spec-layer16 §9.1/§9.3: MATCH, SIMILARITY and FUSE, indexed exact
// and range lookups, and, when the provider also implements sql.IndexDDLExecutor, CREATE INDEX /
// DROP INDEX. The engine is rebuilt around it, so this is safe to call while connections are
// live: statements already executing finish against the previous engine, the next one uses this.
func (s *KdbServerRuntime) SetSQLIndexProvider(provider sql.IndexProvider) {
	s.sqlEngineMu.Lock()
	s.sqlIndexProvider = provider
	s.sqlEngineMu.Unlock()
	s.rebuildSQLEngine()
}

// SQLIndexProvider returns the configured index provider, or nil when queries are full scans.
func (s *KdbServerRuntime) SQLIndexProvider() sql.IndexProvider {
	s.sqlEngineMu.RLock()
	defer s.sqlEngineMu.RUnlock()
	return s.sqlIndexProvider
}

// rebuildSQLEngine constructs the SQL engine over the expiry-filtering storage view and the
// current index provider.
func (s *KdbServerRuntime) rebuildSQLEngine() {
	s.sqlEngineMu.Lock()
	defer s.sqlEngineMu.Unlock()
	store := &expiryHidingAdapter{Adapter: s.Runtime.Storage, runtime: s}
	if s.sqlIndexProvider == nil {
		// Deliberately NewEngine, not NewEngineWithIndexes(.., nil): a nil provider stored in a
		// non-nil interface would make the engine believe it has one.
		s.sqlEngine = sql.NewEngine(store, s.dag)
		return
	}
	s.sqlEngine = sql.NewEngineWithIndexes(store, s.dag, s.sqlIndexProvider)
}

// SetSearchProvider installs (or, with nil, removes) the provider that serves SEARCH frames -
// kdb-spec-layer16 §11, Component 69. Safe to call at any time, including while connections are
// live: the next SEARCH uses the new provider. Until one is set every search is refused with
// ErrSearchNotConfigured (wire.ErrorCodeUnsupported).
func (s *KdbServerRuntime) SetSearchProvider(provider SearchProvider) {
	s.searchProviderMu.Lock()
	s.searchProvider = provider
	s.searchProviderMu.Unlock()
}

// SearchProvider returns the provider serving SEARCH frames, or nil when none is configured.
func (s *KdbServerRuntime) SearchProvider() SearchProvider {
	s.searchProviderMu.RLock()
	defer s.searchProviderMu.RUnlock()
	return s.searchProvider
}

// RebuildUniqueKeys repopulates the unique-key registry from the namespace's documents at the
// current head. Called at construction and again whenever the schema changes, since a migration
// that turns a field unique has to be validated against data written before the constraint
// existed.
func (s *KdbServerRuntime) RebuildUniqueKeys() error {
	if s.UniqueKeys == nil {
		return nil
	}
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	commit, ok := s.Runtime.DAG.GetCommit(head)
	if !ok {
		return nil
	}
	return s.UniqueKeys.Rebuild(
		s.Runtime.DefaultNamespace, s.Runtime.Storage, commit.DocumentTreeHash, s.Schema(),
	)
}

// nextSessionOrdinal returns the next runtime-unique session ordinal.
func (s *KdbServerRuntime) nextSessionOrdinal() int64 { return s.sessionSeq.Add(1) }

// Schema returns the runtime's current schema (safe for concurrent use with SetSchema).
func (s *KdbServerRuntime) Schema() schema.KdbSchema {
	s.schemaMu.RLock()
	defer s.schemaMu.RUnlock()
	return s.Runtime.Schema
}

// SetSchema updates the runtime's schema, e.g. after a CREATE TABLE (safe for concurrent use
// with Schema).
func (s *KdbServerRuntime) SetSchema(sch schema.KdbSchema) {
	s.schemaMu.Lock()
	s.Runtime.Schema = sch
	s.schemaMu.Unlock()
	// The new schema may declare a field unique that was not before. The registry is keyed by
	// the schema's unique fields, so it has to be rebuilt against the new one - otherwise the
	// constraint would only bind documents written from now on, and every pre-existing duplicate
	// would stay invisible.
	if err := s.RebuildUniqueKeys(); err != nil {
		s.UniqueKeyRebuildError = err
	}
}

// SetSchemaChecked is SetSchema that refuses a schema whose unique constraints the existing data
// already violates, leaving the previous schema in place. This is the migration path: turning a
// field unique when two documents already share a value is a change that cannot be honored, and
// applying it anyway would leave the namespace permanently inconsistent with its own schema.
func (s *KdbServerRuntime) SetSchemaChecked(sch schema.KdbSchema) error {
	previous := s.Schema()
	s.schemaMu.Lock()
	s.Runtime.Schema = sch
	s.schemaMu.Unlock()
	if err := s.RebuildUniqueKeys(); err != nil {
		s.schemaMu.Lock()
		s.Runtime.Schema = previous
		s.schemaMu.Unlock()
		// Restore the registry to match the schema we just rolled back to, so a rejected
		// migration leaves no trace.
		if rebuildErr := s.RebuildUniqueKeys(); rebuildErr != nil {
			s.UniqueKeyRebuildError = rebuildErr
		}
		return err
	}
	s.UniqueKeyRebuildError = nil
	return nil
}

// SetWriteQueueCapacityForTest replaces the write gate with one of the given queue capacity -
// exported only for other packages' tests (e.g. go/kdb/client's end-to-end BUSY test) that need
// to deterministically fill the queue over a real wire connection; production code should use
// the constructor-provided default (DefaultMaxQueuedWrites) or set it via a future config surface
// rather than calling this directly.
func (s *KdbServerRuntime) SetWriteQueueCapacityForTest(maxQueued int) {
	s.writeGate = newWriteGate(maxQueued)
}

// AcquireWriteSlotForTest occupies one write-gate slot (queue or running) exactly as commitWith
// would, returning a release func - exported only for other packages' tests that need to drive
// the gate to capacity from outside this package. Blocks (respecting no deadline) until a queue
// slot is available; use a background goroutine plus a short sleep to occupy the running slot
// first if the test needs a queued (not just running) waiter.
func (s *KdbServerRuntime) AcquireWriteSlotForTest() (release func(), err error) {
	return s.writeGate.acquire(context.Background())
}

// AcquireWriteSlotWithContextForTest is AcquireWriteSlotForTest with a caller-supplied context,
// for tests that need a bounded wait instead of blocking forever.
func (s *KdbServerRuntime) AcquireWriteSlotWithContextForTest(ctx context.Context) (release func(), err error) {
	return s.writeGate.acquire(ctx)
}

// BeginDraining rejects every subsequent write immediately with *UnavailableError - the first
// step of an orderly shutdown (kdb-spec-layer13 Component 50): stop admitting new work before
// doing anything else, so nothing new can start while draining, flushing, and closing happen.
// Idempotent; does not affect reads, and does not itself wait for in-flight writes to finish -
// callers that need that should wait on their own tracking of outstanding commitWith calls, or
// rely on WriteTimeout to bound how long any of them can still be running.
func (s *KdbServerRuntime) BeginDraining() {
	s.draining.Store(true)
}

// IsDraining reports whether BeginDraining has been called - consulted by the admin endpoint's
// /readyz so load balancers stop routing to a server that is refusing new writes.
func (s *KdbServerRuntime) IsDraining() bool {
	return s.draining.Load()
}

// WaitForWritesToDrain blocks until every already-admitted write has finished (the write gate
// is empty) or timeout elapses, returning true if the gate quiesced in time. Call BeginDraining
// first - without it, new writes keep being admitted and this can never settle. Polling (10ms)
// rather than a broadcast keeps writeGate's acquire/release fast path untouched for the one
// call site that only runs once per process shutdown.
func (s *KdbServerRuntime) WaitForWritesToDrain(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if s.writeGate.quiesced() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Retain increments the reference count.
func (s *KdbServerRuntime) Retain() {
	s.refCount.Add(1)
}

// Release decrements the reference count; once it reaches zero, stops the memory guard and
// closes the underlying embedded runtime - flushing and sealing the active delta segment (see
// EmbeddedKdbRuntime.Close, kdb-spec-layer13 Component 47 §4.5). This is what makes an ordinary
// process shutdown (a service's SIGTERM handler calling Release, not just an orderly abort - see
// AbortWatchdog) actually reach that flush/seal path, rather than the process exiting with
// in-memory state never given the chance to leave the log in its cleanest, fastest-to-replay
// shape (still safe to skip entirely - a kill -9 relies on the same replay path succeeding
// without any of this having run - just slower on the next open).
func (s *KdbServerRuntime) Release() {
	if s.refCount.Add(-1) > 0 {
		return
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.refCount.Load() > 0 {
		return
	}
	// The sweeper commits through this runtime; it has to be gone before storage closes.
	s.stopSweeper()
	s.memGuard.Stop()
	if s.Runtime != nil {
		s.Runtime.Close()
	}
}

// SetMemoryLimit opts this runtime into memory-pressure backpressure with the default rescue
// reserve and scan row budget. Retained as the narrow form of SetMemoryBudget, which is what
// kdb-service calls.
func (s *KdbServerRuntime) SetMemoryLimit(limitBytes uint64, rejectFraction float64) {
	s.SetMemoryBudget(limitBytes, rejectFraction, DefaultRescueReserveBytes, DefaultScanRowBudget)
}

// SetMemoryBudget opts this runtime into kdb-spec-layer13 Component 48's memory admission: an
// operation reserves the bytes it is estimated to hold before it runs, and is rejected with a
// typed, actionable error if that reservation cannot be met. Pass limitBytes == 0 (the default,
// if this is never called) to disable entirely, restoring the pre-Component-48 behavior of
// admitting everything.
//
// rejectFraction is the fraction of limitBytes at which writes start being shed (the entry point
// of ZoneHigh; the other zones are positioned relative to it - see zoneFractionOfReject).
// reserveBytes is the rescue reserve held back for the abort sequence (§5.6), and scanRowBudget
// caps rows *examined* per scan (§5.2).
//
// Typically called once at startup with the deployment's known memory budget; safe to call again
// to change it, and safe to call concurrently with in-flight work - grants already issued against
// the previous Admission release against that same instance, since a Grant holds its own
// reference to the Admission that issued it.
func (s *KdbServerRuntime) SetMemoryBudget(limitBytes uint64, rejectFraction float64, reserveBytes int64, scanRowBudget int64) {
	s.memGuard.Stop()
	s.memGuard = NewMemoryGuard(limitBytes, rejectFraction)
	s.admission = NewAdmission(s.memGuard, reserveBytes, scanRowBudget)
}

// Admission exposes the grant system, for metrics and tests. Nil when no budget is configured.
func (s *KdbServerRuntime) Admission() *Admission { return s.admission }

// MemoryZone reports the current pressure zone - ZoneNormal when no budget is configured.
func (s *KdbServerRuntime) MemoryZone() Zone { return s.memGuard.CurrentZone() }

// Commit authorizes every operation in tx against principal, then commits it via the
// transaction engine, serialized against concurrent commits to the same runtime (see commitMu).
// namespaceID is accepted for interface parity with the component spec's illustrative signature;
// this runtime is already scoped to one namespace via Runtime.DAG, so it is not otherwise
// consulted.
//
// Re-checking here, not just at the wire layer's SqlExec/TxCommit dispatch, is deliberate
// defense in depth (component 38 spec §5): a caller with its own path into Commit - bypassing
// the wire layer entirely - still gets checked, matching the Kotlin reference's
// AuthorizingTransactionEngine, which wraps TransactionEngine itself rather than relying on the
// wire host to remember to ask. AuthEngine defaults to auth.AllowAll, so this is a no-op for a
// server that hasn't opted into RBAC.
func (s *KdbServerRuntime) Commit(namespaceID string, tx document.Transaction, sessionID string, principal auth.Principal) (document.Commit, error) {
	_ = namespaceID
	_ = sessionID
	return s.commitWith(s.TransactionEngine, tx, principal)
}

// Upsert writes docID unconditionally in namespaceID via UpsertEngine (ConflictPolicyLastWrite),
// matching component 40 spec §3/§5's Upsert contract: no BaseVersion, never conflicts. A new
// document is stored exactly as supplied; an existing one is updated by a shallow root-level
// merge of jsonBody onto the stored body (document.Document.Merge), so a key jsonBody omits
// keeps its stored value. Always anchors on the current head internally,
// since callers of Upsert by definition don't have (and don't want) one.
func (s *KdbServerRuntime) Upsert(namespaceID string, docID codec.UUID, jsonBody string, principal auth.Principal) (document.Commit, error) {
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return document.Commit{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: jsonBody}},
		Timestamp:   codec.TimestampNow(),
	}
	return s.commitWith(s.UpsertEngine, tx, principal)
}

// Replay applies tx directly on top of the current head, ignoring tx.BaseVersion - the Mode 2
// (write-back stream) counterpart to Commit, backing TransactionReplay (kdb-spec.md §8.5/§8.1's
// Mode 2 definition). A write-back client has no independent local DAG to anchor optimistic
// concurrency on, so unlike Commit there is no "did anything change since I read?" check -
// matches transaction.Engine.Replay's own base==baseline==target==replayTarget shape (Kotlin's
// DefaultTransactionEngine.replay is identical), and mirrors Kotlin's own
// SqlWireHost.handleTransactionReplay/KdbServerRuntime.replay, which always replays onto
// whatever the current head is at call time, computed by the caller (see wire_listen.go's
// handleTransactionReplay) rather than trusting a client-supplied target.
func (s *KdbServerRuntime) Replay(namespaceID string, tx document.Transaction, replayTarget codec.Hash, principal auth.Principal) (document.Commit, error) {
	_ = namespaceID
	return s.replayWith(s.TransactionEngine, tx, replayTarget, principal)
}

func (s *KdbServerRuntime) commitWith(engine transaction.Engine, tx document.Transaction, principal auth.Principal) (document.Commit, error) {
	return s.runTransaction(tx, principal, txOptions{}, func() (transactionResult, error) {
		return engine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, "")
	})
}

func (s *KdbServerRuntime) replayWith(engine transaction.Engine, tx document.Transaction, replayTarget codec.Hash, principal auth.Principal) (document.Commit, error) {
	return s.runTransaction(tx, principal, txOptions{}, func() (transactionResult, error) {
		return engine.Replay(tx, s.dag, s.Runtime.Storage, s.Schema(), replayTarget, "")
	})
}

type transactionResult = transaction.TransactionResult

// txOptions are the per-call knobs runTransaction's three public callers do not need but the
// runtime's own internal commits (the expiry sweeper) do.
type txOptions struct {
	// system marks a commit the runtime makes on its own behalf; RBAC is not consulted, because
	// there is no grant that could describe "the server enforcing its own policy". Only set from
	// inside this package, never from a principal a client presented. The commit message travels
	// with the engine call the caller builds, not here.
	system bool
}

// runTransaction holds every cross-cutting concern Commit/Upsert/Replay share (draining,
// memory-pressure rejection, authorization, the writeGate's serialization+timeout+backpressure,
// and persisting a successful result) - call does only the one thing that differs between them:
// which transaction.Engine method to invoke and with what target.
func (s *KdbServerRuntime) runTransaction(tx document.Transaction, principal auth.Principal, opts txOptions, call func() (transactionResult, error)) (document.Commit, error) {
	// Checked in cheapest-first order, before authorization or taking the write gate: a server
	// shedding load should do as little work per rejected request as possible, and a rejection
	// reason "closer to the front" (shutting down entirely) makes every later check moot anyway.
	if s.draining.Load() {
		return document.Commit{}, &UnavailableError{Reason: "server is shutting down"}
	}
	// A read-only runtime has no WAL and no delta writer at all, so a write that got this far
	// would fail deep in the storage engine with an error naming a missing component rather than
	// the actual reason. Refused at the front, in the same cheapest-first spirit as draining.
	if err := s.Runtime.AssertWritable(); err != nil {
		return document.Commit{}, err
	}
	if !opts.system {
		if err := s.authorizeOperations(tx, principal); err != nil {
			return document.Commit{}, err
		}
	}
	if s.dag == nil {
		return document.Commit{}, fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", s.Runtime.DAG)
	}
	// The base version has to survive the queue, not just the commit. It was resolved by the
	// caller before this call and is not consulted until transaction.Engine.Commit runs at the
	// front of the write gate, which can be a long way behind up to DefaultMaxQueuedWrites other
	// writers - and Commit hard-fails with BaseNotFoundError if it has been reclaimed by then.
	// Nothing else roots it: a base version is not a branch head. (Replay needs no equivalent -
	// it ignores tx.BaseVersion and targets the live head, which is a branch head already.)
	defer s.dag.Pin(tx.BaseVersion)()
	timeout := s.WriteTimeout
	if timeout <= 0 {
		timeout = DefaultWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Reserve the memory this commit is estimated to hold, for as long as it holds it. Held
	// across the whole of runTransaction - including PersistAsync's durability wait - because
	// that is genuinely how long the commit occupies memory; releasing at the write gate instead
	// would let the next writer be admitted against capacity this one has not actually given
	// back yet, which is the exact over-admission the grant system exists to prevent.
	grant, err := s.admission.Acquire(ctx, ClassWrite, transactionPayloadBytes(tx))
	if err != nil {
		return document.Commit{}, err
	}
	defer grant.Release()

	release, err := s.writeGate.acquire(ctx)
	if err != nil {
		return document.Commit{}, err
	}
	// The gate is released explicitly on the success path, as soon as this
	// commit's position in the delta log is fixed - see below. releaseOnce
	// keeps the deferred release (which covers every other path) from
	// double-releasing the gate's semaphore.
	var releaseOnce sync.Once
	releaseNow := func() { releaseOnce.Do(release) }
	defer releaseNow()

	// Index extraction and validation happen before the commit lands (Component 68): a value
	// the indexes cannot accept rejects the transaction here, rather than leaving index state
	// disagreeing with the documents it describes.
	preparedIndexes, indexProvider, err := s.prepareIndexes(tx)
	if err != nil {
		return document.Commit{}, err
	}

	result, err := call()
	if err != nil {
		return document.Commit{}, err
	}
	switch r := result.(type) {
	case transaction.ResultSuccess:
		// Applied under the gate so index state advances in commit order. The hints ride along
		// on the commit for replication.
		if indexProvider != nil {
			hints, err := indexProvider.commitToIndexes(preparedIndexes, r.Commit.Hash)
			if err != nil {
				return document.Commit{}, err
			}
			_ = hints
		}
		// Queueing happens under the gate because queue order is delta-log
		// order, and two commits racing to enqueue would desync the log from
		// the DAG's actual commit order. Waiting for the write to reach disk
		// does not: once queued, this commit's position is fixed, so the gate
		// can go to the next writer while this one's fsync is still in flight.
		// That is what lets concurrent commits share a single fsync instead of
		// each paying a full physical sync in strict sequence.
		var waitDurable func() error
		if s.persister != nil {
			waitDurable, err = s.persister.PersistAsync(r.Commit)
			if err != nil {
				return document.Commit{}, err
			}
		}
		releaseNow()
		if waitDurable != nil {
			if err := waitDurable(); err != nil {
				return document.Commit{}, err
			}
		}
		if s.CommitListener != nil {
			s.CommitListener(s.Runtime.DefaultNamespace, r.Commit)
		}
		return r.Commit, nil
	case transaction.ResultConflict:
		// Sampled here, at the moment of failure, rather than when the response is shaped: the
		// queue depth that made this transaction's base version stale is the one standing right
		// now, and it can have changed by the time the frame is written.
		return document.Commit{}, &ConflictError{Report: r.Report, RetryAfterMs: s.conflictRetryAfterMs()}
	case transaction.ResultSchemaError:
		return document.Commit{}, &SchemaError{Violations: r.Violations}
	case transaction.ResultAborted:
		return document.Commit{}, r.Cause
	default:
		return document.Commit{}, fmt.Errorf("kdb server: unrecognized transaction result %T", result)
	}
}

// GetDocument reads docID's current JSON at the DAG's current head, or (nil, false, nil) if it
// doesn't exist. Backs component 40's GetJSON - a direct point lookup, not expressible as SQL
// today (no WHERE-by-document-identity predicate; see wire_listen.go's handleDocumentGet).
// treeSizeAt returns the namespace document count at the given commit - O(1) via the document
// tree's tracked size. This is the single strongest pre-execution predictor of a scan's cost,
// and it is already resolved on every read path; 0 when the commit or tree is missing (the
// query itself will surface that as a real error).
func (s *KdbServerRuntime) treeSizeAt(head codec.Hash) int {
	commit, ok := s.Runtime.DAG.GetCommit(head)
	if !ok {
		return 0
	}
	tree, ok := s.Runtime.DAG.GetDocumentTree(commit.DocumentTreeHash)
	if !ok {
		return 0
	}
	return tree.Size()
}

func (s *KdbServerRuntime) GetDocument(namespaceID string, docID codec.UUID) (json string, commitHex string, found bool, err error) {
	// One HeadCommit rather than Head + GetCommit: those were two RLock/RUnlock pairs on the
	// DAG's shared RWMutex, i.e. four atomic read-modify-writes on one cache line for a read
	// that mutates nothing - measured at 40% of all CPU samples under 1024 concurrent
	// readers, and the reason aggregate read throughput was *below* single-threaded
	// (docs/benchmarks/workload-matrix.md, Finding 2). HeadCommit is one atomic load, and it
	// additionally guarantees head and commit describe the same instant.
	head, commit, ok, err := s.Runtime.DAG.HeadCommit()
	if err != nil {
		return "", "", false, err
	}
	if !ok {
		return "", "", false, fmt.Errorf("kdb server: head commit %s missing", head.Hex())
	}
	doc, err := s.Runtime.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil {
		return "", "", false, err
	}
	if doc == nil {
		return "", head.Hex(), false, nil
	}
	// Reads at head honour document expiry between sweeps (kdb-spec-layer16 §9.5): an expired
	// document is reported absent exactly as if the sweeper had already deleted it.
	if s.isExpiredAtHead(doc.JSON) {
		return "", head.Hex(), false, nil
	}
	return doc.JSON, head.Hex(), true, nil
}

// authorizeOperations checks every op in tx against principal via s.AuthEngine.Authorizer,
// matching kdb-auth-store's AuthorizingTransactionEngine.kt: a per-document write/delete check
// for each operation, not just one check for the transaction as a whole.
func (s *KdbServerRuntime) authorizeOperations(tx document.Transaction, principal auth.Principal) error {
	authorizer := s.AuthEngine.Authorizer()
	for _, op := range tx.Operations {
		var action auth.Action
		switch o := op.(type) {
		case document.WriteOp:
			action = auth.DocumentWriteAction{Namespace: s.Runtime.DefaultNamespace, DocID: o.DocID.String()}
		case document.DeleteOp:
			action = auth.DocumentDeleteAction{Namespace: s.Runtime.DefaultNamespace, DocID: o.DocID.String()}
		default:
			continue
		}
		if err := authorizer.Authorize(context.Background(), principal, action); err != nil {
			return &AuthorizationError{Cause: err}
		}
	}
	return nil
}

// AuthorizationError wraps an RBAC denial at commit time, distinct from a conflict (component 38
// spec §6).
type AuthorizationError struct {
	Cause error
}

func (e *AuthorizationError) Error() string { return "authorization denied: " + e.Cause.Error() }
func (e *AuthorizationError) Unwrap() error { return e.Cause }

// ConflictError wraps a transaction engine conflict report, matching the wire-level
// ConflictReport message type (component 38 spec §6).
type ConflictError struct {
	Report kdberr.ConflictReport
	// RetryAfterMs is how long the server suggests waiting before re-reading and retrying,
	// computed at the moment of failure from live write-gate pressure and jittered per response
	// (conflictRetryAfterMs). It travels with the error so that every path which turns a
	// conflict into a wire response - classifyError, and the two ConflictReportMessage call
	// sites in wire_listen.go - reports the same number from one source rather than each
	// inventing its own.
	RetryAfterMs int
}

func (e *ConflictError) Error() string {
	return "conflict: transaction " + e.Report.TransactionID + " base " + e.Report.BaseHash + " vs target " + e.Report.TargetHash
}

// SchemaError wraps schema/preflight violations from a failed commit (component 38 spec §6).
type SchemaError struct {
	Violations []transaction.OperationViolation
}

func (e *SchemaError) Error() string {
	if len(e.Violations) == 0 {
		return "schema violation"
	}
	// Name what actually failed. "3 operation(s) rejected" tells a client nothing it can act on -
	// a unique-value collision and a malformed payload need entirely different responses, and
	// both used to arrive as the same sentence.
	var parts []string
	for _, v := range e.Violations {
		for _, fv := range v.Violations {
			detail := fv.ViolationType.String()
			if fv.FieldName != "" {
				detail = fv.FieldName + ": " + detail
			}
			if fv.Detail != "" {
				detail += " (" + fv.Detail + ")"
			}
			parts = append(parts, fmt.Sprintf("op %d: %s", v.OpIndex, detail))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("schema violation: %d operation(s) rejected", len(e.Violations))
	}
	return "schema violation: " + strings.Join(parts, "; ")
}

// HasUniqueViolation reports whether any rejected operation collided with a unique constraint,
// which callers classify differently from an ordinary schema problem.
func (e *SchemaError) HasUniqueViolation() bool {
	for _, v := range e.Violations {
		for _, fv := range v.Violations {
			if fv.ViolationType == kdberr.UniqueConstraint {
				return true
			}
		}
	}
	return false
}

// ServerRuntimeRegistry holds shared server runtimes by key.
type ServerRuntimeRegistry struct {
	mu       sync.Mutex
	runtimes map[string]*KdbServerRuntime
}

// NewServerRuntimeRegistry returns an empty registry.
func NewServerRuntimeRegistry() *ServerRuntimeRegistry {
	return &ServerRuntimeRegistry{runtimes: make(map[string]*KdbServerRuntime)}
}

// GetOrOpen returns an existing runtime or opens a new one, retaining a reference for the
// caller either way - every successful call must be balanced by exactly one Release(key).
func (r *ServerRuntimeRegistry) GetOrOpen(key string, open func() (*KdbServerRuntime, error)) (*KdbServerRuntime, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.runtimes[key]; ok {
		rt.Retain()
		return rt, nil
	}
	rt, err := open()
	if err != nil {
		return nil, err
	}
	// NewKdbServerRuntime already starts refCount at 1 - that single implicit reference is this
	// first caller's own, so no additional Retain() belongs here. The previous code retained
	// twice on top of it (refCount 3 for the first caller, 2 for every later cache-hit caller),
	// so Release could never actually bring a runtime's refCount to zero.
	r.runtimes[key] = rt
	return rt, nil
}

// Release releases a registry entry reference. Once the last outstanding reference is released
// (refCount reaches zero - see KdbServerRuntime.Release, which also closes the underlying
// runtime at that point), the entry is removed from the registry so a later GetOrOpen for the
// same key reopens fresh rather than reusing an already-closed, zero-refCount instance.
func (r *ServerRuntimeRegistry) Release(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[key]
	if !ok {
		return
	}
	rt.Release()
	if rt.refCount.Load() <= 0 {
		delete(r.runtimes, key)
	}
}
