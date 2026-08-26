package server

import (
	"context"
	"fmt"
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
	SQLEngine     sql.Engine
	DocumentLocks *transaction.LockManager

	// AuthEngine authenticates/authorizes both at the wire layer (handshake/session-begin, see
	// wire_listen.go) and at commit time (see Commit) - one engine for both, so RBAC changes
	// (e.g. a revoked grant) take effect for the very next commit, not just the next connection.
	// Defaults to auth.AllowAll; set to auth.NewRegistryAuthEngine(store) to enable RBAC
	// (component 38 spec §4, sub-phase C).
	AuthEngine auth.Engine

	refCount atomic.Int32
	closeMu  sync.Mutex

	// schemaMu guards Runtime.Schema: CREATE TABLE (executed via SqlExec) updates it after
	// construction, and that update must not race with concurrent readers on other
	// connections. Use Schema()/SetSchema() rather than touching Runtime.Schema directly.
	schemaMu sync.RWMutex

	// writeGate serializes calls into TransactionEngine.Commit - the same load-bearing
	// exclusion a bare sync.Mutex would give (transaction.Engine.Commit reads the DAG head,
	// runs conflict detection against it, and only then appends the new commit -
	// InMemoryCommitDag.AppendCommit advances the branch head unconditionally, with no
	// compare-and-swap against the anchor it was given, so two goroutines racing on the same
	// stale head would silently orphan one of them from "main" instead of surfacing a conflict),
	// plus a bounded queue and a per-caller deadline a bare mutex can't express (see writeGate's
	// own doc comment, kdb-spec-layer13 Component 49 §6.2 - "start only what we can finish"
	// applied to time). Any other caller that invokes Engine.Commit concurrently on a shared
	// *InMemoryCommitDag without equivalent serialization has the same exposure (worth a
	// follow-up in the transaction/dag packages themselves).
	writeGate *writeGate
	// WriteTimeout bounds how long a commit may wait queued before *DeadlineExceededError.
	// Defaults to DefaultWriteTimeout; safe to change at any time.
	WriteTimeout time.Duration
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
	s := &KdbServerRuntime{
		Runtime:           rt,
		TransactionEngine: transaction.NewEngine(transaction.ConflictPolicyStrict, nil),
		UpsertEngine:      transaction.NewEngine(transaction.ConflictPolicyLastWrite, nil),
		SQLEngine:         sql.NewEngine(rt.Storage, d),
		DocumentLocks:     transaction.NewLockManager(),
		AuthEngine:        auth.AllowAll,
		dag:               d,
		persister:         persister,
		writeGate:         newWriteGate(DefaultMaxQueuedWrites),
		WriteTimeout:      DefaultWriteTimeout,
	}
	s.refCount.Store(1)
	return s
}

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
	defer s.schemaMu.Unlock()
	s.Runtime.Schema = sch
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
	s.memGuard.Stop()
	if s.Runtime != nil {
		s.Runtime.Close()
	}
}

// SetMemoryLimit opts this runtime into memory-pressure backpressure: once heap usage crosses
// rejectFraction of limitBytes, new writes (Commit/Upsert) are rejected with a *MemoryPressureError
// instead of being accepted - see MemoryGuard's own doc comment for why. Pass limitBytes == 0 (the
// default, if this is never called) to disable. Typically set once at startup to the deployment's
// known memory budget (e.g. a container's --memory limit); safe to call again to change it.
func (s *KdbServerRuntime) SetMemoryLimit(limitBytes uint64, rejectFraction float64) {
	s.memGuard.Stop()
	s.memGuard = NewMemoryGuard(limitBytes, rejectFraction)
}

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

// Upsert writes docID unconditionally in namespaceID - create if absent, replace if present -
// via UpsertEngine (ConflictPolicyLastWrite), matching component 40 spec §3/§5's Upsert
// contract: no BaseVersion, never conflicts. Always anchors on the current head internally,
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
	return s.runTransaction(tx, principal, func() (transaction.TransactionResult, error) {
		return engine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, "")
	})
}

func (s *KdbServerRuntime) replayWith(engine transaction.Engine, tx document.Transaction, replayTarget codec.Hash, principal auth.Principal) (document.Commit, error) {
	return s.runTransaction(tx, principal, func() (transaction.TransactionResult, error) {
		return engine.Replay(tx, s.dag, s.Runtime.Storage, s.Schema(), replayTarget, "")
	})
}

// runTransaction holds every cross-cutting concern Commit/Upsert/Replay share (draining,
// memory-pressure rejection, authorization, the writeGate's serialization+timeout+backpressure,
// and persisting a successful result) - call does only the one thing that differs between them:
// which transaction.Engine method to invoke and with what target.
func (s *KdbServerRuntime) runTransaction(tx document.Transaction, principal auth.Principal, call func() (transaction.TransactionResult, error)) (document.Commit, error) {
	// Checked in cheapest-first order, before authorization or taking the write gate: a server
	// shedding load should do as little work per rejected request as possible, and a rejection
	// reason "closer to the front" (shutting down entirely) makes every later check moot anyway.
	if s.draining.Load() {
		return document.Commit{}, &UnavailableError{Reason: "server is shutting down"}
	}
	if s.memGuard.ShouldReject() {
		return document.Commit{}, &MemoryPressureError{}
	}
	if err := s.authorizeOperations(tx, principal); err != nil {
		return document.Commit{}, err
	}
	if s.dag == nil {
		return document.Commit{}, fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", s.Runtime.DAG)
	}
	timeout := s.WriteTimeout
	if timeout <= 0 {
		timeout = DefaultWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	release, err := s.writeGate.acquire(ctx)
	if err != nil {
		return document.Commit{}, err
	}
	defer release()
	result, err := call()
	if err != nil {
		return document.Commit{}, err
	}
	switch r := result.(type) {
	case transaction.ResultSuccess:
		// commitMu-serialized here, same as the append above: two commits racing to persist in
		// the wrong order would desync the delta log from the DAG's actual commit order.
		if s.persister != nil {
			if err := s.persister.Persist(r.Commit); err != nil {
				return document.Commit{}, err
			}
		}
		return r.Commit, nil
	case transaction.ResultConflict:
		return document.Commit{}, &ConflictError{Report: r.Report}
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
func (s *KdbServerRuntime) GetDocument(namespaceID string, docID codec.UUID) (json string, commitHex string, found bool, err error) {
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return "", "", false, err
	}
	commit, ok := s.Runtime.DAG.GetCommit(head)
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
	return fmt.Sprintf("schema violation: %d operation(s) rejected", len(e.Violations))
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

// GetOrOpen returns an existing runtime or opens a new one.
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
	rt.Retain()
	r.runtimes[key] = rt
	rt.Retain()
	return rt, nil
}

// Release releases a registry entry reference.
func (r *ServerRuntimeRegistry) Release(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.runtimes[key]; ok {
		rt.Release()
	}
}
