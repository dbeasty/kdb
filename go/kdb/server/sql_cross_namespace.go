package server

import (
	"errors"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// SQL transactions that span namespaces.
//
// A SQL session is opened on one namespace, and until now every statement ran against it whatever
// table it named. With a NamespaceSet configured, a statement's table can name another namespace:
//
//   - A qualified name always routes: `bank.ledger` is namespace bank/ledger.
//   - An unqualified name routes to <session catalog>/<table> when that namespace exists - the
//     table-is-a-namespace mapping the Kotlin/JDBC reference uses - and otherwise stays on the
//     session's own namespace, which is what it has always meant here.
//
// Writes to other namespaces are buffered on the session exactly as writes to its own are, one
// builder per namespace, and COMMIT (or TX_COMMIT) commits every one of them atomically through
// NamespaceSet.CommitAcross. See docs/kdb-cross-namespace-transactions-plan.md §7.

// sqlTarget is the namespace one statement runs against.
type sqlTarget struct {
	rt *KdbServerRuntime
	ns string
	// own is true for the session's own namespace, which keeps its existing state (Pending, the
	// read pin, the write anchor) rather than the per-namespace state below.
	own bool
}

// crossNamespaceState is a session's transaction state in one namespace other than its own.
type crossNamespaceState struct {
	rt *KdbServerRuntime
	// readHead is where a SNAPSHOT session reads this namespace for the rest of the transaction,
	// fixed at first touch (or by BEGIN's snapshot). Nil for the other consistencies, which read
	// the live head.
	readHead   *codec.Hash
	pinRelease func()
	builder    *transaction.Builder
}

// statementTarget resolves the namespace stmt runs against. forWrite permits a qualified name to
// open a namespace that does not exist yet: a write is an explicit request for it, and the commit
// would create it anyway. A read of a namespace that does not exist is an error, not an empty
// result.
func (h *sqlWireConnHandler) statementTarget(stmt sql.Statement, forWrite bool) (sqlTarget, error) {
	own := sqlTarget{rt: h.runtime, own: true}
	set := h.runtime.Namespaces
	ref, named := sql.TargetTable(stmt)
	if set == nil || !named {
		return own, nil
	}
	ownNS := h.runtime.Runtime.DefaultNamespace
	if strings.Contains(ref.Name, ".") {
		ns := strings.ReplaceAll(ref.Name, ".", "/")
		if ns == ownNS {
			return own, nil
		}
		rt, err := set.Resolve(ns, forWrite)
		if err != nil {
			return sqlTarget{}, err
		}
		return sqlTarget{rt: rt, ns: ns}, nil
	}
	ns := embed.CatalogFromNamespace(ownNS) + "/" + ref.Name
	if ns == ownNS {
		return own, nil
	}
	rt, err := set.Resolve(ns, false)
	if err != nil {
		// No such namespace: the table keeps meaning the session's own namespace, as it always
		// has. Only a namespace that exists is routed to.
		return own, nil
	}
	return sqlTarget{rt: rt, ns: ns}, nil
}

// crossState returns the session's state in t's namespace, creating it on first touch.
func (s *KdbSession) crossState(t sqlTarget) *crossNamespaceState {
	s.crossMu.Lock()
	defer s.crossMu.Unlock()
	if s.cross == nil {
		s.cross = make(map[string]*crossNamespaceState)
	}
	st, ok := s.cross[t.ns]
	if !ok {
		st = &crossNamespaceState{rt: t.rt}
		s.cross[t.ns] = st
	}
	return st
}

// crossReadHead is where this statement reads t's namespace. A SNAPSHOT session reads every
// namespace at one point for the whole transaction: BEGIN's multi-namespace snapshot when there
// was one, otherwise the head at this transaction's first touch of the namespace, pinned so
// compaction cannot reclaim it underneath the transaction.
func (s *KdbSession) crossReadHead(t sqlTarget) (codec.Hash, error) {
	if s.ReadConsistency != Snapshot {
		return t.rt.Runtime.DAG.Head()
	}
	st := s.crossState(t)
	s.crossMu.Lock()
	defer s.crossMu.Unlock()
	if st.readHead != nil {
		return *st.readHead, nil
	}
	head, fromSnapshot := s.snapshotHeads[t.ns]
	if !fromSnapshot {
		var err error
		if head, err = t.rt.Runtime.DAG.Head(); err != nil {
			return codec.Hash{}, err
		}
	}
	if t.rt.dag != nil {
		if fromSnapshot && !t.rt.dag.HasCommit(head) {
			return codec.Hash{}, errors.New("kdb server: this transaction's snapshot of namespace " + t.ns +
				" is no longer available; roll back and retry")
		}
		st.pinRelease = t.rt.dag.Pin(head)
	}
	st.readHead = &head
	return head, nil
}

// crossBuilder is the session's pending transaction for t's namespace. Its base version is fixed
// by the first write, exactly as the session's own is (see SessionManager.PendingBuilder): the
// live head for READ_COMMITTED, the transaction's read point for SNAPSHOT.
func (s *KdbSession) crossBuilder(t sqlTarget) (*transaction.Builder, error) {
	st := s.crossState(t)
	s.crossMu.Lock()
	b := st.builder
	s.crossMu.Unlock()
	if b != nil {
		return b, nil
	}
	var base codec.Hash
	var err error
	if s.ReadConsistency == Snapshot {
		base, err = s.crossReadHead(t)
	} else {
		base, err = t.rt.Runtime.DAG.Head()
	}
	if err != nil {
		return nil, err
	}
	s.crossMu.Lock()
	defer s.crossMu.Unlock()
	if st.builder == nil {
		st.builder = &transaction.Builder{NamespaceID: t.ns, BaseVersion: base, Schema: t.rt.Schema()}
	}
	return st.builder, nil
}

// pendingCross returns the other namespaces this transaction has buffered writes for, sorted.
func (s *KdbSession) pendingCross() []string {
	s.crossMu.Lock()
	defer s.crossMu.Unlock()
	var out []string
	for ns, st := range s.cross {
		if st.builder != nil {
			out = append(out, ns)
		}
	}
	sort.Strings(out)
	return out
}

// hasPendingWrites reports whether the current transaction has buffered anything, anywhere.
func (s *KdbSession) hasPendingWrites() bool {
	return s.Pending != nil || len(s.pendingCross()) > 0
}

// clearCross ends the transaction's state in every other namespace: pending writes dropped, read
// pins released, BEGIN's snapshot forgotten.
func (s *KdbSession) clearCross() {
	s.crossMu.Lock()
	defer s.crossMu.Unlock()
	for _, st := range s.cross {
		if st.pinRelease != nil {
			st.pinRelease()
		}
	}
	s.cross = nil
	s.snapshotHeads = nil
}
