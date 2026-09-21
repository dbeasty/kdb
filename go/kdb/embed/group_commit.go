package embed

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// CommitGroup is the one implementation of a transaction spanning namespaces - see
// docs/kdb-cross-namespace-transactions-plan.md §3.2. The wire server (server.NamespaceSet) and
// the embedded database/sql driver both call it; what differs between them - how a namespace's
// writers are serialized, what checks a participant runs, how errors are shaped - is the
// GroupParticipant they pass in.
//
// The protocol:
//
//  1. Lock every participant, in namespace order. Ordered acquisition cannot deadlock against
//     another group, and a single-namespace writer only ever holds one lock.
//  2. Begin the group; every part carries its id as the transaction id.
//  3. Prepare every participant - every check, nothing staged. One refusal abandons the group
//     with nothing written anywhere.
//  4. Apply every participant: its commit is appended provisional and queued on its log.
//  5. Unlock. What is left waits on disks, and the next writer's work overlaps it.
//  6. The returned wait drives the group to its decision.
//
// A failure between the first append and the last queued part fails the group and fences every
// participant; so does a failure in the wait.
func CommitGroup(coord *TxnCoordinator, parts []GroupPart) (GroupResult, func() error, error) {
	if coord == nil {
		return GroupResult{}, nil, fmt.Errorf("kdb: CommitGroup needs a transaction coordinator")
	}
	if len(parts) == 0 {
		return GroupResult{}, nil, fmt.Errorf("kdb: cross-namespace transaction has no participants")
	}
	parts = append([]GroupPart(nil), parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Participant.Namespace() < parts[j].Participant.Namespace() })
	for i := 1; i < len(parts); i++ {
		if parts[i].Participant.Namespace() == parts[i-1].Participant.Namespace() {
			return GroupResult{}, nil, fmt.Errorf("kdb: namespace %s appears twice in one cross-namespace transaction; "+
				"merge its operations into one", parts[i].Participant.Namespace())
		}
	}

	// A caller-supplied base has to survive until its participant is prepared: nothing else
	// roots it, and PrepareCommit refuses a base the graph no longer holds.
	for _, p := range parts {
		if p.Tx.BaseVersion == (codec.Hash{}) {
			continue
		}
		if d := dagOf(p.Participant.Runtime()); d != nil {
			defer d.Pin(p.Tx.BaseVersion)()
		}
	}

	// 1. Locks.
	unlocks := make([]func(), 0, len(parts))
	unlockAll := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
		unlocks = unlocks[:0]
	}
	defer unlockAll()
	for _, p := range parts {
		unlock, err := p.Participant.Lock()
		if err != nil {
			return GroupResult{}, nil, &GroupPartError{Namespace: p.Participant.Namespace(), Err: err}
		}
		unlocks = append(unlocks, unlock)
	}

	// 2. The group.
	namespaces := make([]string, len(parts))
	for i, p := range parts {
		namespaces[i] = p.Participant.Namespace()
	}
	group, err := coord.Begin(namespaces)
	if err != nil {
		return GroupResult{}, nil, err
	}

	// 3. Prepare everything.
	prepared := make([]PreparedPart, len(parts))
	for i, p := range parts {
		tx := p.Tx
		tx.ID = group.ID
		if tx.Timestamp.EpochMicros() == 0 {
			tx.Timestamp = codec.TimestampNow()
		}
		pp, err := p.Participant.Prepare(tx)
		if err != nil {
			for _, done := range prepared[:i] {
				done.Discard()
			}
			group.Abandon()
			return GroupResult{}, nil, &GroupPartError{Namespace: p.Participant.Namespace(), Err: err}
		}
		prepared[i] = pp
	}

	// 4. Apply everything, between the publication hooks of every participant that has them.
	for _, p := range parts {
		if o, ok := p.Participant.(PublishObserver); ok {
			o.PublishStarted()
		}
	}
	commits, applied, applyErr := applyGroup(group, parts, prepared)
	for _, p := range parts {
		if o, ok := p.Participant.(PublishObserver); ok {
			o.PublishFinished()
		}
	}
	if applyErr != nil {
		if applied == 0 {
			group.Abandon()
			return GroupResult{}, nil, applyErr
		}
		failed := group.Fail(applyErr)
		fenceAll(parts, failed)
		return GroupResult{}, nil, failed
	}

	// 5. Locks off.
	unlockAll()

	result := GroupResult{Group: group.ID, Commits: make([]GroupCommit, len(parts))}
	for i, p := range parts {
		result.Commits[i] = GroupCommit{Namespace: p.Participant.Namespace(), Commit: commits[i]}
	}
	// 6. The decision, when the caller asks for it.
	wait := func() error {
		if err := group.Finish(); err != nil {
			fenceAll(parts, err)
			return err
		}
		return nil
	}
	return result, wait, nil
}

// applyGroup applies each prepared part and adds it to group. applied counts the participants
// whose commit reached their graph - the ones a failure has to fence.
func applyGroup(group *TxnGroup, parts []GroupPart, prepared []PreparedPart) (commits []document.Commit, applied int, err error) {
	commits = make([]document.Commit, len(parts))
	for i, p := range parts {
		c, err := prepared[i].Apply(group.Message())
		if err != nil {
			for _, rest := range prepared[i+1:] {
				rest.Discard()
			}
			return nil, applied, &GroupPartError{Namespace: p.Participant.Namespace(), Err: err}
		}
		commits[i] = c
		applied++
		if err := group.AddPart(p.Participant.Runtime(), c); err != nil {
			for _, rest := range prepared[i+1:] {
				rest.Discard()
			}
			return nil, applied, &GroupPartError{Namespace: p.Participant.Namespace(), Err: err}
		}
	}
	return commits, applied, nil
}

func fenceAll(parts []GroupPart, cause error) {
	for _, p := range parts {
		p.Participant.Fence(cause)
	}
}

func dagOf(rt *EmbeddedKdbRuntime) *dag.InMemoryCommitDag {
	if rt == nil {
		return nil
	}
	switch concrete := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		return concrete
	case *PersistingCommitDAG:
		return concrete.Delegate()
	}
	return nil
}

// GroupPart is one namespace's share of a cross-namespace transaction.
type GroupPart struct {
	Participant GroupParticipant
	// Tx is committed into the participant's namespace. Its ID is replaced by the group id and a
	// zero Timestamp by the current time; everything else is the participant's to interpret (the
	// server, for instance, reads a zero BaseVersion as "the head at commit time").
	Tx document.Transaction
}

// GroupParticipant is how CommitGroup reaches one namespace.
type GroupParticipant interface {
	// Namespace is the namespace this participant commits into. Parts are locked in this order.
	Namespace() string
	// Runtime is the namespace's runtime; its commit graph is where the part lands.
	Runtime() *EmbeddedKdbRuntime
	// Lock serializes this namespace's writers until the returned func runs. An error refuses
	// the transaction before anything is written.
	Lock() (unlock func(), err error)
	// Prepare runs every check the namespace applies to tx, with the lock held, and stages
	// nothing. An error refuses the transaction.
	Prepare(tx document.Transaction) (PreparedPart, error)
	// Fence refuses the namespace's writes from now on: a transaction it took part in failed
	// after publishing, so its in-memory state holds a commit recovery will roll back.
	Fence(cause error)
}

// PreparedPart is a participant that passed every check.
type PreparedPart interface {
	// Apply appends the part's commit with message, marked provisional (see
	// transaction.ApplyOptions), and returns it.
	Apply(message string) (document.Commit, error)
	// Discard abandons a part that will not be applied.
	Discard()
}

// PublishObserver is implemented by participants that need to know when a group's commits are
// being published - the server's lock-free cross-namespace snapshot does.
type PublishObserver interface {
	PublishStarted()
	PublishFinished()
}

// GroupResult is a published group. It is committed once the wait CommitGroup returned succeeds.
type GroupResult struct {
	// Group is the transaction id every part carries.
	Group codec.UUID
	// Commits are in namespace order.
	Commits []GroupCommit
}

// GroupCommit is one participant's commit.
type GroupCommit struct {
	Namespace string
	Commit    document.Commit
}

// GroupPartError is a cross-namespace transaction refused (or failed) by one participant. Err is
// what the participant said; errors.As reaches it through this wrapper.
type GroupPartError struct {
	Namespace string
	Err       error
}

func (e *GroupPartError) Error() string {
	return fmt.Sprintf("cross-namespace transaction refused by namespace %s: %v", e.Namespace, e.Err)
}

func (e *GroupPartError) Unwrap() error { return e.Err }
