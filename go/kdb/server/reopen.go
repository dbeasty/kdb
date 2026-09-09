package server

import (
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
)

// Pointing a live server runtime at a freshly-reopened namespace.
//
// embed.Host.ReopenNamespace closes a namespace and opens it again, which
// yields a *new* EmbeddedKdbRuntime. Everything in this process that was
// serving the old one - the transaction engines, the DAG, the SQL engine -
// is derived from it and has to be rebuilt against the new one, and it has
// to happen without a request in flight observing half of the change.
//
// What is deliberately *not* rebuilt is the coordination state that belongs
// to the server rather than to the storage: the write gate, the document
// locks, the unique-key registry, the auth engine, the commit listener and
// the index and search providers. Those describe how this process serves the
// namespace, not what is on disk, and recreating them would drop sessions'
// locks and every registered provider for a change that was supposed to be
// about a cache size.

// ResumeAfterDrain starts admitting writes again after a drain that was a pause rather than a
// shutdown.
//
// BeginDraining is one-way at shutdown, deliberately - a process on its way out must not start
// taking work again. A reopen is the other case: it drains, changes something, and has to hand
// the namespace back. Separated from BeginDraining rather than making that reversible, so the
// shutdown path keeps its guarantee and only the callers that mean "pause" can undo it.
func (s *KdbServerRuntime) ResumeAfterDrain() {
	if s == nil {
		return
	}
	s.draining.Store(false)
}

// ReopenWith swaps this runtime onto a newly-opened namespace, draining
// first so nothing is mid-commit across the swap.
//
// The sequence is: stop admitting writes, wait for the admitted ones to
// finish, swap, admit again. drainTimeout bounds the wait; exceeding it is
// an error and the swap does not happen, because swapping the storage out
// from under a running commit is exactly the corruption this ordering
// exists to prevent.
//
// The caller supplies the new runtime rather than this doing the reopen
// itself: the Host owns namespace lifecycle, and a server runtime reaching
// into it would put the same operation in two places.
func (s *KdbServerRuntime) ReopenWith(next *embed.EmbeddedKdbRuntime, drainTimeout time.Duration) error {
	if s == nil {
		return fmt.Errorf("kdb: no server runtime to reopen")
	}
	if next == nil {
		return fmt.Errorf("kdb: cannot swap a server runtime onto a nil namespace")
	}

	// 1. Stop admitting, and let what is admitted finish. Draining is
	//    reversible here, unlike at shutdown: this is a pause, not an exit.
	s.BeginDraining()
	defer s.draining.Store(false)
	if !s.WaitForWritesToDrain(drainTimeout) {
		return fmt.Errorf(
			"kdb: writes did not drain within %s, so the namespace was not reopened; "+
				"it is still serving the runtime it was", drainTimeout)
	}

	// 2. Swap, under the same locks the read paths take. The SQL engine is
	//    rebuilt inside the write lock rather than after it, so no query can
	//    see a runtime and an engine that disagree about which namespace
	//    they are for.
	var d *dag.InMemoryCommitDag
	var persister *embed.PersistingCommitDAG
	switch concrete := next.DAG.(type) {
	case *dag.InMemoryCommitDag:
		d = concrete
	case *embed.PersistingCommitDAG:
		d = concrete.Delegate()
		persister = concrete
	}
	if d == nil {
		return fmt.Errorf(
			"kdb: the reopened namespace has a commit DAG this server cannot drive (%T); not swapping", next.DAG)
	}

	s.sqlEngineMu.Lock()
	s.schemaMu.Lock()
	s.Runtime = next
	s.dag = d
	s.persister = persister
	s.schemaMu.Unlock()
	s.rebuildSQLEngineLocked()
	s.sqlEngineMu.Unlock()
	return nil
}
