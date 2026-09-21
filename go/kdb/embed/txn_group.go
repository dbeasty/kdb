package embed

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// GroupFailedError reports a cross-namespace group that did not commit after at least one of its
// parts was published, and is also what a fenced namespace refuses later writes with.
type GroupFailedError struct {
	Group codec.UUID
	Cause error
}

func (e *GroupFailedError) Error() string {
	return fmt.Sprintf("kdb: cross-namespace transaction %s did not commit (the namespaces it touched refuse "+
		"further writes until reopened): %v", e.Group, e.Cause)
}

func (e *GroupFailedError) Unwrap() error { return e.Cause }

// TxnGroup is one cross-namespace transaction in flight: a commit in each participating namespace,
// committed together by one decision record.
//
// The caller's side of the protocol, in order:
//
//  1. Begin (TxnCoordinator.Begin) once every participant has been checked and nothing remains
//     that could reject the transaction.
//  2. For each participant, with that namespace's writers serialized: append the commit with
//     Message() as its message and marked provisional, then AddPart.
//  3. Release the namespaces' serialization, then Finish - which waits for every part to be on
//     disk, orders the decision behind any group ahead of this one, writes the decision and waits
//     for it.
//
// If anything fails between the first append and AddPart's return, call Fail. If nothing was
// appended at all, call Abandon.
type TxnGroup struct {
	c     *TxnCoordinator
	ID    codec.UUID
	Epoch uint64
	host  string
	parts []string

	mu      sync.Mutex
	members []groupMember
	preds   []*TxnGroup

	// enqueued is closed once the decision record is queued, or once the group has failed
	// without queuing one. A group behind this one waits for it before queuing its own: the
	// decision log is FIFO, so queued-after means durable-after.
	enqueued chan struct{}
	// earlyErr is the failure that stopped the decision from being queued. Written before
	// enqueued is closed, read only after, so the close orders the two.
	earlyErr    error
	enqueueOnce sync.Once

	done     chan struct{}
	err      error
	complete sync.Once
}

type groupMember struct {
	d         *dag.InMemoryCommitDag
	persister *PersistingCommitDAG
	hash      codec.Hash
	wait      func() error
}

// Message is the commit message every part of this group carries.
func (g *TxnGroup) Message() string {
	return GroupMarker{Host: g.host, Epoch: g.Epoch, Group: g.ID, Parts: g.parts}.Message()
}

// Done is closed once the group is decided or has failed.
func (g *TxnGroup) Done() <-chan struct{} { return g.done }

// Err is nil for a committed group. Valid once Done is closed.
func (g *TxnGroup) Err() error { return g.err }

func (g *TxnGroup) decided() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

// AddPart records commit - already appended to rt's commit graph with this group's message and
// marked provisional - as one of this group's parts, and queues it on rt's commit log.
//
// Call it while rt's writers are still serialized, straight after the append: queue order is
// commit order, and this is also where the group becomes the namespace's barrier.
func (g *TxnGroup) AddPart(rt *EmbeddedKdbRuntime, commit document.Commit) error {
	var d *dag.InMemoryCommitDag
	var p *PersistingCommitDAG
	switch concrete := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		d = concrete
	case *PersistingCommitDAG:
		d, p = concrete.Delegate(), concrete
	default:
		return fmt.Errorf("kdb: cross-namespace commit needs an in-memory or persisting commit graph, got %T", rt.DAG)
	}
	m := groupMember{d: d, persister: p, hash: commit.Hash, wait: func() error { return nil }}
	var pred *TxnGroup
	if p != nil {
		wait, predecessor, err := p.persistPart(commit, g)
		if err != nil {
			// Recorded anyway, so Fail fences this namespace too: its graph already holds the part.
			g.mu.Lock()
			g.members = append(g.members, m)
			g.mu.Unlock()
			return err
		}
		m.wait, pred = wait, predecessor
	}
	g.mu.Lock()
	g.members = append(g.members, m)
	if pred != nil && pred != g {
		g.preds = append(g.preds, pred)
	}
	g.mu.Unlock()
	return nil
}

// Finish drives the group to a decision and reports the outcome. Call it after releasing every
// participant's serialization - it is the part of the protocol that waits on disks, and holding a
// namespace's writers back through two fsyncs is exactly what the protocol is arranged to avoid.
func (g *TxnGroup) Finish() error {
	g.mu.Lock()
	members := append([]groupMember(nil), g.members...)
	preds := append([]*TxnGroup(nil), g.preds...)
	g.mu.Unlock()

	var err error
	// Phase 1: every part on disk. The waits run on each namespace's own log writer, so they
	// overlap; waiting for them one after another costs no more than the slowest.
	for _, m := range members {
		if werr := m.wait(); werr != nil && err == nil {
			err = werr
		}
	}
	// Order behind earlier groups sharing a namespace. Their decisions only need to be queued -
	// ours cannot then become durable first - so this rarely waits longer than their phase 1.
	if err == nil {
		for _, p := range preds {
			<-p.enqueued
			if p.earlyErr != nil {
				err = &GroupFailedError{Group: p.ID, Cause: p.earlyErr}
				break
			}
		}
	}
	// Phase 2: the decision.
	var waitDecision func() error
	if err == nil {
		waitDecision, err = g.c.decide(g)
	}
	g.closeEnqueued(err)
	if err == nil {
		err = waitDecision()
	}
	g.finish(members, err)
	return g.err
}

// Fail ends a group that published at least one part and cannot be decided - an append or a
// queueing failure partway through. Every namespace with a part is fenced.
func (g *TxnGroup) Fail(cause error) error {
	g.mu.Lock()
	members := append([]groupMember(nil), g.members...)
	g.mu.Unlock()
	g.closeEnqueued(cause)
	g.finish(members, cause)
	return g.err
}

// Abandon ends a group that never published anything - every participant was rejected or the
// caller gave up before the first append.
func (g *TxnGroup) Abandon() {
	g.complete.Do(func() {
		g.closeEnqueued(errGroupAbandoned)
		g.err = errGroupAbandoned
		g.c.finished(g, false, false)
		close(g.done)
	})
}

// closeEnqueued publishes whether the decision was queued (err nil) or will never be.
func (g *TxnGroup) closeEnqueued(err error) {
	g.enqueueOnce.Do(func() {
		g.earlyErr = err
		close(g.enqueued)
	})
}

var errGroupAbandoned = fmt.Errorf("kdb: cross-namespace transaction abandoned before publishing")

func (g *TxnGroup) finish(members []groupMember, cause error) {
	g.complete.Do(func() {
		if cause == nil {
			for _, m := range members {
				// Settled before the barrier drops and before done closes: anything woken by the
				// decision must find the part as permanent as any other commit.
				m.d.SettleProvisional(m.hash)
				if m.persister != nil {
					m.persister.clearBarrier(g)
				}
			}
		} else {
			failed := &GroupFailedError{Group: g.ID, Cause: cause}
			cause = failed
			for _, m := range members {
				if m.persister != nil {
					m.persister.fence(failed)
				}
			}
		}
		g.err = cause
		g.c.finished(g, cause == nil, len(members) > 0)
		close(g.done)
	})
}
