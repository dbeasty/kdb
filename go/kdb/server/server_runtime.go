package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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

	// commitMu serializes calls into TransactionEngine.Commit. This is load-bearing, not
	// just a convenience lock: transaction.Engine.Commit reads the DAG head, runs
	// conflict detection against it, and only then appends the new commit - and
	// InMemoryCommitDag.AppendCommit advances the branch head unconditionally, with no
	// compare-and-swap against the anchor it was given. Two goroutines that both read the
	// same stale head concurrently would each append a commit parented on it, and the
	// branch would silently end up pointing at whichever one finished last, orphaning the
	// other from "main" instead of surfacing a conflict. Serializing Commit here closes
	// that race for this server; any other caller that invokes Engine.Commit concurrently
	// on a shared *InMemoryCommitDag without equivalent serialization has the same
	// exposure (worth a follow-up in the transaction/dag packages themselves).
	commitMu sync.Mutex

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

// Retain increments the reference count.
func (s *KdbServerRuntime) Retain() {
	s.refCount.Add(1)
}

// Release decrements the reference count; v1 does not tear down storage.
func (s *KdbServerRuntime) Release() {
	if s.refCount.Add(-1) > 0 {
		return
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.refCount.Load() > 0 {
		return
	}
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

func (s *KdbServerRuntime) commitWith(engine transaction.Engine, tx document.Transaction, principal auth.Principal) (document.Commit, error) {
	if err := s.authorizeOperations(tx, principal); err != nil {
		return document.Commit{}, err
	}
	if s.dag == nil {
		return document.Commit{}, fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", s.Runtime.DAG)
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	result, err := engine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, "")
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
