package server

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// NamespaceTransaction is one namespace's share of a cross-namespace transaction.
type NamespaceTransaction struct {
	Namespace string
	// Tx is committed into Namespace. A zero BaseVersion anchors it on the namespace's head at
	// the moment it commits - a blind write, with no conflict detection against anything the
	// caller read; add preconditions for compare-and-set. Tx.ID is replaced by the group id, so
	// every participant commit carries the same transaction id.
	Tx document.Transaction
	// SessionID identifies the caller for document leases: a document leased to this session may
	// be written, one leased to any other session may not. Empty means "no session" - every
	// leased document is refused.
	SessionID string
}

// NamespaceCommit is one participant's commit in a successful cross-namespace transaction.
type NamespaceCommit struct {
	Namespace string
	Commit    document.Commit
}

// CrossNamespaceResult is a committed cross-namespace transaction.
type CrossNamespaceResult struct {
	// Group is the transaction id every participant commit carries.
	Group codec.UUID
	// Commits are in namespace order.
	Commits []NamespaceCommit
}

// Commit returns the commit in namespace, if it took part.
func (r CrossNamespaceResult) Commit(namespace string) (document.Commit, bool) {
	for _, c := range r.Commits {
		if c.Namespace == namespace {
			return c.Commit, true
		}
	}
	return document.Commit{}, false
}

// CrossNamespaceError is a cross-namespace transaction refused by one participant, before anything
// was written anywhere. Err is what that namespace said - a *ConflictError, *SchemaError,
// *AuthorizationError, *BusyError and so on - and errors.As reaches it through this wrapper.
//
// The same type embed.CommitGroup reports, so an error from either layer matches either name.
type CrossNamespaceError = embed.GroupPartError

// ErrUnknownNamespace is returned for a namespace the set does not hold and cannot open.
var ErrUnknownNamespace = errors.New("kdb server: unknown namespace")

// NamespaceSet commits transactions that span namespaces - see
// docs/kdb-cross-namespace-transactions-plan.md. It holds one KdbServerRuntime per namespace, and
// for the guarantees to hold every writer to those namespaces must go through the same instances:
// the write gate and the ack barrier are per runtime, so a second KdbServerRuntime over the same
// embedded runtime would be a second, uncoordinated writer.
//
// The protocol, per transaction:
//
//  1. Take every participant's write gate, in namespace order. Ordered acquisition cannot
//     deadlock against another group, and a single-namespace commit only ever holds one gate.
//  2. Prepare every participant - every check, nothing staged. One rejection rejects the whole
//     transaction, with nothing to undo anywhere.
//  3. Apply every participant (append its commit, provisional) and queue it on its namespace's
//     log. The group becomes each namespace's ack barrier.
//  4. Release the gates. Everything left is waiting on disks, and the next writer's validation
//     overlaps it.
//  5. Wait for every part to be durable, write the group's decision record, wait for that.
type NamespaceSet struct {
	coord *embed.TxnCoordinator

	mu       sync.RWMutex
	runtimes map[string]*KdbServerRuntime
	// system holds reserved namespaces (MetaNamespace) that replicate like any other but are
	// never reachable through Get/Resolve - so never through SQL, the document wire or a
	// cross-namespace commit. Only peer sync sees them (PeerNamespaces).
	system map[string]*KdbServerRuntime
	// opener opens a namespace the set does not hold yet. create says whether a namespace that
	// does not exist on disk may be created - true for a commit, false for a read.
	opener func(namespace string, create bool) (*KdbServerRuntime, error)
	// openMu serializes opener calls, so two first touches of one namespace open it once.
	openMu sync.Mutex

	// WriteTimeout bounds how long a cross-namespace transaction may wait for all of its gates.
	// Zero uses the longest WriteTimeout among the participants.
	WriteTimeout time.Duration

	// serializeAll switches to the simple protocol - one lock for the whole set, held through
	// both fsyncs - which exists only so the benchmarks can measure what the per-namespace one
	// buys. See SetSerializedForBenchmark.
	serializeAll bool
	globalMu     sync.Mutex
	// known are namespaces that exist though they may not be open: closed for idleness, or found
	// on disk at startup (AddKnown). Peers are served these too; a sync reopens them.
	known map[string]struct{}
}

// NewNamespaceSet returns an empty set committing through coord - a host's Transactions(). nil
// uses a memory coordinator, which is only valid for runtimes with no durable state.
func NewNamespaceSet(coord *embed.TxnCoordinator) *NamespaceSet {
	if coord == nil {
		coord = embed.NewMemoryTxnCoordinator()
	}
	return &NamespaceSet{coord: coord, runtimes: make(map[string]*KdbServerRuntime)}
}

// Coordinator is the coordinator this set commits through.
func (s *NamespaceSet) Coordinator() *embed.TxnCoordinator { return s.coord }

// Add puts rt in the set under its namespace. A file-backed runtime must belong to the host whose
// coordinator the set was built with: the decision that commits a group lives with that host, and
// a namespace replayed against a different one would never find it.
func (s *NamespaceSet) Add(rt *KdbServerRuntime) error {
	if rt == nil || rt.Runtime == nil {
		return errors.New("kdb server: NamespaceSet.Add: nil runtime")
	}
	if rt.persister != nil && rt.Runtime.TxnCoordinator() != s.coord {
		return fmt.Errorf("kdb server: namespace %s belongs to a different host than this set's transaction coordinator",
			rt.Runtime.DefaultNamespace)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runtimes[rt.Runtime.DefaultNamespace]; ok && existing != rt {
		return fmt.Errorf("kdb server: namespace %s is already in the set as a different runtime", rt.Runtime.DefaultNamespace)
	}
	s.runtimes[rt.Runtime.DefaultNamespace] = rt
	return nil
}

// AddSystem puts a reserved namespace in the set for replication only - see NamespaceSet.system.
func (s *NamespaceSet) AddSystem(rt *KdbServerRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.system == nil {
		s.system = map[string]*KdbServerRuntime{}
	}
	s.system[rt.Runtime.DefaultNamespace] = rt
}

// system returns a reserved namespace's runtime, if the set holds it.
func (s *NamespaceSet) systemRuntime(ns string) (*KdbServerRuntime, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rt, ok := s.system[ns]
	return rt, ok
}

func (s *NamespaceSet) systemNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.system))
	for ns := range s.system {
		out = append(out, ns)
	}
	return out
}

// SetOpener installs the function that opens namespaces the set does not yet hold. Without one,
// only namespaces added explicitly are reachable.
func (s *NamespaceSet) SetOpener(open func(namespace string, create bool) (*KdbServerRuntime, error)) {
	s.mu.Lock()
	s.opener = open
	s.mu.Unlock()
}

// Get returns the runtime serving namespace, if the set holds it.
func (s *NamespaceSet) Get(namespace string) (*KdbServerRuntime, bool) {
	s.mu.RLock()
	rt, ok := s.runtimes[namespace]
	s.mu.RUnlock()
	if ok {
		rt.touch(time.Now())
	}
	return rt, ok
}

// Namespaces lists the namespaces the set holds, sorted.
func (s *NamespaceSet) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.runtimes))
	for ns := range s.runtimes {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// Runtimes returns every runtime the set holds, keyed by namespace.
func (s *NamespaceSet) Runtimes() map[string]*KdbServerRuntime {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*KdbServerRuntime, len(s.runtimes))
	for ns, rt := range s.runtimes {
		out[ns] = rt
	}
	return out
}

// Resolve returns the runtime for namespace, opening it through the opener when the set does not
// hold it yet. create permits opening a namespace that does not exist on disk.
func (s *NamespaceSet) Resolve(namespace string, create bool) (*KdbServerRuntime, error) {
	if rt, ok := s.Get(namespace); ok {
		return rt, nil
	}
	s.mu.RLock()
	open := s.opener
	s.mu.RUnlock()
	if open == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownNamespace, namespace)
	}
	s.openMu.Lock()
	defer s.openMu.Unlock()
	if rt, ok := s.Get(namespace); ok {
		return rt, nil
	}
	rt, err := open(namespace, create)
	if err != nil {
		return nil, err
	}
	if err := s.Add(rt); err != nil {
		return nil, err
	}
	rt.touch(time.Now())
	return rt, nil
}

// SetSerializedForBenchmark switches the set to the simple protocol: one lock over every
// namespace, held through both fsyncs. Correct, and the baseline the per-namespace protocol is
// measured against. Not for production use.
func (s *NamespaceSet) SetSerializedForBenchmark(on bool) { s.serializeAll = on }

// CommitAcross commits parts atomically: every namespace gets its commit, or none does. On
// success every participant's commit is returned. A rejection by any participant is a
// *CrossNamespaceError naming it, and nothing was written. A failure after publication - an I/O
// error writing a part or the decision - is an *embed.GroupFailedError, and the participants are
// fenced until reopened (see docs/kdb-cross-namespace-transactions-plan.md §4.5).
//
// The protocol itself is embed.CommitGroup, shared with the embedded database/sql driver; this is
// the server's side of it: admission, authorization, the write gates, leases and indexes.
func (s *NamespaceSet) CommitAcross(parts []NamespaceTransaction, principal auth.Principal) (CrossNamespaceResult, error) {
	return s.commitAcross(parts, principal, false)
}

func (s *NamespaceSet) commitAcross(parts []NamespaceTransaction, principal auth.Principal, system bool) (CrossNamespaceResult, error) {
	if len(parts) == 0 {
		return CrossNamespaceResult{}, errors.New("kdb server: cross-namespace transaction has no participants")
	}
	ps := make([]*serverParticipant, 0, len(parts))
	for _, part := range parts {
		if part.Namespace == "" {
			return CrossNamespaceResult{}, errors.New("kdb server: cross-namespace transaction names an empty namespace")
		}
		rt, err := s.Resolve(part.Namespace, true)
		if err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: part.Namespace, Err: err}
		}
		ps = append(ps, &serverParticipant{ns: part.Namespace, rt: rt, sessionID: part.SessionID, tx: rt.authored(part.Tx)})
	}

	// Cheapest-first refusals, before any gate: the same order runTransaction checks them in.
	for _, p := range ps {
		if p.rt.ProjectionOf != "" && !system {
			// A write-back projection sends each local transaction to its source on its own; a
			// group spanning it and anything else could not arrive there as one.
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns,
				Err: &ProjectionReadOnlyError{Namespace: p.ns, Source: p.rt.ProjectionOf}}
		}
		if err := p.rt.admitWrite(p.tx, principal, system); err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		if _, ok := p.rt.TransactionEngine.(transaction.Preparer); !ok {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns,
				Err: fmt.Errorf("kdb server: transaction engine %T cannot take part in a cross-namespace transaction", p.rt.TransactionEngine)}
		}
		if p.rt.dag == nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns,
				Err: fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", p.rt.Runtime.DAG)}
		}
	}

	timeout := s.WriteTimeout
	if timeout <= 0 {
		for _, p := range ps {
			t := p.rt.WriteTimeout
			if t <= 0 {
				t = DefaultWriteTimeout
			}
			if t > timeout {
				timeout = t
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	groupParts := make([]embed.GroupPart, len(ps))
	for i, p := range ps {
		p.ctx = ctx
		// Held until return, including through the durability wait: see runTransaction.
		grant, err := p.rt.admission.Acquire(ctx, ClassWrite, transactionPayloadBytes(p.tx))
		if err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		defer grant.Release()
		if p.tx.BaseVersion != (codec.Hash{}) {
			// The base's tree too, so the schema phase does not rebuild it: see runTransaction.
			defer p.rt.pinBaseTree(p.tx.BaseVersion)()
		}
		groupParts[i] = embed.GroupPart{Participant: p, Tx: p.tx}
	}

	if s.serializeAll {
		s.globalMu.Lock()
		defer s.globalMu.Unlock()
	}

	published, wait, err := embed.CommitGroup(s.coord, groupParts)
	if err != nil {
		return CrossNamespaceResult{}, err
	}
	result := CrossNamespaceResult{Group: published.Group, Commits: make([]NamespaceCommit, len(published.Commits))}
	for i, c := range published.Commits {
		result.Commits[i] = NamespaceCommit{Namespace: c.Namespace, Commit: c.Commit}
	}
	finish := func() error {
		if err := wait(); err != nil {
			return err
		}
		for _, c := range result.Commits {
			if rt, ok := s.Get(c.Namespace); ok && rt.CommitListener != nil {
				rt.CommitListener(c.Namespace, c.Commit)
			}
		}
		return nil
	}
	if !acknowledgesDurably(ps) {
		// Every participant acknowledges before durability (DurabilityAsync), so this one does
		// too. The decision still follows its parts to disk - the protocol does not change, only
		// who waits for it.
		go func() { _ = finish() }()
		return result, nil
	}
	if err := finish(); err != nil {
		return CrossNamespaceResult{}, err
	}
	return result, nil
}

// serverParticipant is one server runtime's side of embed.CommitGroup.
type serverParticipant struct {
	ns        string
	rt        *KdbServerRuntime
	sessionID string
	tx        document.Transaction
	ctx       context.Context
}

func (p *serverParticipant) Namespace() string                  { return p.ns }
func (p *serverParticipant) Runtime() *embed.EmbeddedKdbRuntime { return p.rt.Runtime }
func (p *serverParticipant) Fence(cause error)                  { p.rt.fence(cause) }

// Lock takes the runtime's write gate - the same queue, bound and deadline a single-namespace
// commit waits in.
func (p *serverParticipant) Lock() (func(), error) {
	release, err := p.rt.writeGate.acquire(p.ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	return func() {
		p.rt.writeGate.observeService(time.Since(start))
		release()
	}, nil
}

// PublishStarted and PublishFinished bracket the group's publication for Snapshot, which retries
// a read that overlapped one.
func (p *serverParticipant) PublishStarted() { p.rt.groupPublishing.Add(1) }
func (p *serverParticipant) PublishFinished() {
	p.rt.groupVersion.Add(1)
	p.rt.groupPublishing.Add(-1)
}

// Prepare runs every check a server commit runs, with the gate held.
func (p *serverParticipant) Prepare(tx document.Transaction) (embed.PreparedPart, error) {
	rt := p.rt
	if tx.BaseVersion == (codec.Hash{}) {
		head, err := rt.dag.Head()
		if err != nil {
			return nil, err
		}
		tx.BaseVersion = head
	}
	// Checked under the gate, where the commit that would land is fixed: a document someone
	// else holds a lease on is refused, exactly as handleTxCommit refuses it.
	if err := rt.DocumentLocks.AssertUnheldByOthers(p.ns, p.sessionID, tx); err != nil {
		return nil, err
	}
	// Index extraction and validation happen before anything lands (Component 68), so a value
	// the indexes cannot accept rejects the whole transaction rather than one participant after
	// the others were written.
	preparedIndexes, indexProvider, err := rt.prepareIndexes(tx)
	if err != nil {
		return nil, err
	}
	pc, result, err := rt.TransactionEngine.(transaction.Preparer).PrepareCommit(tx, rt.dag, rt.Runtime.Storage, rt.Schema())
	if err != nil {
		return nil, err
	}
	switch r := result.(type) {
	case nil:
	case transaction.ResultConflict:
		return nil, &ConflictError{Report: r.Report, RetryAfterMs: rt.conflictRetryAfterMs()}
	case transaction.ResultSchemaError:
		return nil, &SchemaError{Violations: r.Violations}
	case transaction.ResultAborted:
		return nil, r.Cause
	default:
		return nil, fmt.Errorf("kdb server: unexpected prepare result %T", result)
	}
	return &serverPrepared{p: p, pc: pc, indexes: preparedIndexes, provider: indexProvider}, nil
}

type serverPrepared struct {
	p        *serverParticipant
	pc       *transaction.PreparedCommit
	indexes  []*index.PreparedWrite
	provider *RegistryIndexProvider
}

func (sp *serverPrepared) Discard() { sp.pc.Discard() }

func (sp *serverPrepared) Apply(message string) (document.Commit, error) {
	result, err := sp.pc.Apply(transaction.ApplyOptions{Message: message, Provisional: true})
	if err != nil {
		return document.Commit{}, err
	}
	switch r := result.(type) {
	case transaction.ResultSuccess:
		if sp.provider != nil {
			// Under the gate, so index state advances in commit order - as in runTransaction.
			if _, err := sp.provider.commitToIndexes(sp.indexes, r.Commit.Hash); err != nil {
				return r.Commit, err
			}
		}
		return r.Commit, nil
	case transaction.ResultAborted:
		return document.Commit{}, r.Cause
	default:
		return document.Commit{}, fmt.Errorf("kdb server: unexpected apply result %T", result)
	}
}

// acknowledgesDurably reports whether any participant promises durability at acknowledgement.
// If one does, the transaction as a whole must: it is one transaction.
func acknowledgesDurably(ps []*serverParticipant) bool {
	for _, p := range ps {
		if p.rt.persister != nil && p.rt.persister.AcknowledgesDurably() {
			return true
		}
	}
	return false
}

// snapshotSpinLimit bounds how long Snapshot retries lock-free before taking the gates. A group's
// publication window is a handful of in-memory appends, so this is only reached when groups over
// the requested namespaces are landing back to back.
const snapshotSpinLimit = 64

// Snapshot returns the head of every named namespace as of one instant no cross-namespace
// transaction was half-published in: for any group touching two of them, either both of its
// commits are at or behind the returned heads, or neither is.
//
// Lock-free in the ordinary case - it reads, then checks nothing was published over the reads,
// and retries if something was. After snapshotSpinLimit retries it takes the namespaces' write
// gates instead, which always succeeds (unless the gates are saturated, which is reported).
func (s *NamespaceSet) Snapshot(namespaces ...string) (map[string]codec.Hash, error) {
	names := append([]string(nil), namespaces...)
	sort.Strings(names)
	rts := make([]*KdbServerRuntime, 0, len(names))
	for i, ns := range names {
		if i > 0 && names[i-1] == ns {
			continue
		}
		rt, err := s.Resolve(ns, false)
		if err != nil {
			return nil, err
		}
		rts = append(rts, rt)
	}
	versions := make([]uint64, len(rts))
	for attempt := 0; attempt < snapshotSpinLimit; attempt++ {
		busy := false
		for i, rt := range rts {
			if rt.groupPublishing.Load() != 0 {
				busy = true
				break
			}
			versions[i] = rt.groupVersion.Load()
		}
		if !busy {
			heads, err := readHeads(rts)
			if err != nil {
				return nil, err
			}
			stable := true
			for i, rt := range rts {
				if rt.groupPublishing.Load() != 0 || rt.groupVersion.Load() != versions[i] {
					stable = false
					break
				}
			}
			if stable {
				return heads, nil
			}
		}
		runtime.Gosched()
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultWriteTimeout)
	defer cancel()
	var releases []func()
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, rt := range rts {
		release, err := rt.writeGate.acquire(ctx)
		if err != nil {
			return nil, err
		}
		releases = append(releases, release)
	}
	return readHeads(rts)
}

func readHeads(rts []*KdbServerRuntime) (map[string]codec.Hash, error) {
	out := make(map[string]codec.Hash, len(rts))
	for _, rt := range rts {
		head, err := rt.Runtime.DAG.Head()
		if err != nil {
			return nil, err
		}
		out[rt.Runtime.DefaultNamespace] = head
	}
	return out, nil
}
