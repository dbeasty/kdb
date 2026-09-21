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
type CrossNamespaceError struct {
	Namespace string
	Err       error
}

func (e *CrossNamespaceError) Error() string {
	return fmt.Sprintf("cross-namespace transaction refused by namespace %s: %v", e.Namespace, e.Err)
}

func (e *CrossNamespaceError) Unwrap() error { return e.Err }

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
	defer s.mu.RUnlock()
	rt, ok := s.runtimes[namespace]
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
	return rt, nil
}

// SetSerializedForBenchmark switches the set to the simple protocol: one lock over every
// namespace, held through both fsyncs. Correct, and the baseline the per-namespace protocol is
// measured against. Not for production use.
func (s *NamespaceSet) SetSerializedForBenchmark(on bool) { s.serializeAll = on }

// participant is one namespace's state through a cross-namespace commit.
type participant struct {
	ns        string
	rt        *KdbServerRuntime
	tx        document.Transaction
	sessionID string

	release       func()
	prepared      *transaction.PreparedCommit
	indexes       []*index.PreparedWrite
	indexProvider *RegistryIndexProvider
	commit        document.Commit
}

// CommitAcross commits parts atomically: every namespace gets its commit, or none does. On
// success every participant's commit is returned. A rejection by any participant is a
// *CrossNamespaceError naming it, and nothing was written. A failure after publication - an I/O
// error writing a part or the decision - is an *embed.GroupFailedError, and the participants are
// fenced until reopened (see docs/kdb-cross-namespace-transactions-plan.md §4.5).
func (s *NamespaceSet) CommitAcross(parts []NamespaceTransaction, principal auth.Principal) (CrossNamespaceResult, error) {
	return s.commitAcross(parts, principal, false)
}

func (s *NamespaceSet) commitAcross(parts []NamespaceTransaction, principal auth.Principal, system bool) (CrossNamespaceResult, error) {
	if len(parts) == 0 {
		return CrossNamespaceResult{}, errors.New("kdb server: cross-namespace transaction has no participants")
	}
	ps := make([]*participant, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if part.Namespace == "" {
			return CrossNamespaceResult{}, errors.New("kdb server: cross-namespace transaction names an empty namespace")
		}
		if _, dup := seen[part.Namespace]; dup {
			// Two transactions into one namespace would be two commits there, the second built
			// on a base the first invalidates. The caller should merge them into one.
			return CrossNamespaceResult{}, fmt.Errorf("kdb server: namespace %s appears twice in one cross-namespace transaction; "+
				"merge its operations into one", part.Namespace)
		}
		seen[part.Namespace] = struct{}{}
		rt, err := s.Resolve(part.Namespace, true)
		if err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: part.Namespace, Err: err}
		}
		ps = append(ps, &participant{ns: part.Namespace, rt: rt, tx: part.Tx, sessionID: part.SessionID})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].ns < ps[j].ns })

	// Cheapest-first refusals, before any gate: the same order runTransaction checks them in.
	for _, p := range ps {
		if err := p.rt.admitWrite(p.tx, principal, system); err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		if _, ok := p.rt.TransactionEngine.(transaction.Preparer); !ok {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns,
				Err: fmt.Errorf("kdb server: transaction engine %T cannot take part in a cross-namespace transaction", p.rt.TransactionEngine)}
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

	for _, p := range ps {
		// Held until return, including through the durability wait: see runTransaction.
		grant, err := p.rt.admission.Acquire(ctx, ClassWrite, transactionPayloadBytes(p.tx))
		if err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		defer grant.Release()
		if p.tx.BaseVersion != (codec.Hash{}) && p.rt.dag != nil {
			// A client-supplied base has to survive the gate queue: see runTransaction.
			defer p.rt.dag.Pin(p.tx.BaseVersion)()
			defer p.rt.pinBaseTree(p.tx.BaseVersion)()
		}
	}

	if s.serializeAll {
		s.globalMu.Lock()
		defer s.globalMu.Unlock()
	}

	// 1. Gates, in namespace order.
	releaseAll := func() {
		for i := len(ps) - 1; i >= 0; i-- {
			if ps[i].release != nil {
				ps[i].release()
				ps[i].release = nil
			}
		}
	}
	defer releaseAll()
	for _, p := range ps {
		release, err := p.rt.writeGate.acquire(ctx)
		if err != nil {
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		start := time.Now()
		p.release = func() {
			p.rt.writeGate.observeService(time.Since(start))
			release()
		}
	}

	// The group is begun before preparing so every participant carries its id as the
	// transaction id; a rejection below abandons it, which writes nothing.
	namespaces := make([]string, len(ps))
	for i, p := range ps {
		namespaces[i] = p.ns
	}
	group, err := s.coord.Begin(namespaces)
	if err != nil {
		return CrossNamespaceResult{}, err
	}
	for _, p := range ps {
		p.tx.ID = group.ID
	}

	// 2. Prepare everything.
	discardAll := func() {
		for _, p := range ps {
			if p.prepared != nil {
				p.prepared.Discard()
			}
		}
	}
	for _, p := range ps {
		if err := s.prepare(p); err != nil {
			discardAll()
			group.Abandon()
			return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
	}

	// 3. Apply everything. Readers taking a Snapshot retry across this window; nothing else
	// notices it.
	for _, p := range ps {
		p.rt.groupPublishing.Add(1)
	}
	applied, applyErr := s.applyAll(ps, group)
	for _, p := range ps {
		p.rt.groupVersion.Add(1)
		p.rt.groupPublishing.Add(-1)
	}
	if applyErr != nil {
		if applied == 0 {
			group.Abandon()
			return CrossNamespaceResult{}, applyErr
		}
		err := group.Fail(applyErr)
		s.fenceAll(ps, err)
		return CrossNamespaceResult{}, err
	}

	// 4. Gates off.
	releaseAll()

	// 5. Decide.
	result := CrossNamespaceResult{Group: group.ID, Commits: make([]NamespaceCommit, len(ps))}
	for i, p := range ps {
		result.Commits[i] = NamespaceCommit{Namespace: p.ns, Commit: p.commit}
	}
	finish := func() error {
		if err := group.Finish(); err != nil {
			s.fenceAll(ps, err)
			return err
		}
		for _, p := range ps {
			if p.rt.CommitListener != nil {
				p.rt.CommitListener(p.ns, p.commit)
			}
		}
		return nil
	}
	if !s.acknowledgesDurably(ps) {
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

// prepare runs every check for one participant, with its gate held.
func (s *NamespaceSet) prepare(p *participant) error {
	rt := p.rt
	if rt.dag == nil {
		return fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", rt.Runtime.DAG)
	}
	if p.tx.BaseVersion == (codec.Hash{}) {
		head, err := rt.dag.Head()
		if err != nil {
			return err
		}
		p.tx.BaseVersion = head
	}
	// Checked under the gate, where the commit that would land is fixed: a document someone
	// else holds a lease on is refused, exactly as handleTxCommit refuses it.
	if err := rt.DocumentLocks.AssertUnheldByOthers(p.ns, p.sessionID, p.tx); err != nil {
		return err
	}
	// Index extraction and validation happen before anything lands (Component 68), so a value
	// the indexes cannot accept rejects the whole transaction rather than one participant after
	// the others were written.
	preparedIndexes, indexProvider, err := rt.prepareIndexes(p.tx)
	if err != nil {
		return err
	}
	p.indexes, p.indexProvider = preparedIndexes, indexProvider
	preparer := rt.TransactionEngine.(transaction.Preparer)
	prepared, result, err := preparer.PrepareCommit(p.tx, rt.dag, rt.Runtime.Storage, rt.Schema())
	if err != nil {
		return err
	}
	switch r := result.(type) {
	case nil:
	case transaction.ResultConflict:
		return &ConflictError{Report: r.Report, RetryAfterMs: rt.conflictRetryAfterMs()}
	case transaction.ResultSchemaError:
		return &SchemaError{Violations: r.Violations}
	case transaction.ResultAborted:
		return r.Cause
	default:
		return fmt.Errorf("kdb server: unexpected prepare result %T", result)
	}
	p.prepared = prepared
	return nil
}

// applyAll publishes every participant and queues it as a part of group. applied counts the
// participants whose commit reached their graph - the ones a failure has to fence.
func (s *NamespaceSet) applyAll(ps []*participant, group *embed.TxnGroup) (applied int, err error) {
	for _, p := range ps {
		result, err := p.prepared.Apply(transaction.ApplyOptions{Message: group.Message(), Provisional: true})
		if err != nil {
			return applied, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
		success, ok := result.(transaction.ResultSuccess)
		if !ok {
			if aborted, isAborted := result.(transaction.ResultAborted); isAborted {
				return applied, &CrossNamespaceError{Namespace: p.ns, Err: aborted.Cause}
			}
			return applied, &CrossNamespaceError{Namespace: p.ns, Err: fmt.Errorf("kdb server: unexpected apply result %T", result)}
		}
		p.commit = success.Commit
		applied++
		if p.indexProvider != nil {
			// Under the gate, so index state advances in commit order - as in runTransaction.
			if _, err := p.indexProvider.commitToIndexes(p.indexes, p.commit.Hash); err != nil {
				return applied, &CrossNamespaceError{Namespace: p.ns, Err: err}
			}
		}
		if err := group.AddPart(p.rt.Runtime, p.commit); err != nil {
			return applied, &CrossNamespaceError{Namespace: p.ns, Err: err}
		}
	}
	return applied, nil
}

// fenceAll refuses further writes on every participant. Their commit logs are already latched by
// the group; this stops the server from publishing commits it could never persist.
func (s *NamespaceSet) fenceAll(ps []*participant, cause error) {
	for _, p := range ps {
		p.rt.fence(cause)
	}
}

// acknowledgesDurably reports whether any participant promises durability at acknowledgement.
// If one does, the transaction as a whole must: it is one transaction.
func (s *NamespaceSet) acknowledgesDurably(ps []*participant) bool {
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
