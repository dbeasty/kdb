package embed

import (
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// PersistingCommitDAG wraps an in-memory DAG and persists new commits to the delta log.
type PersistingCommitDAG struct {
	delegate *dag.InMemoryCommitDag
	writer   storage.DeltaSegmentWriter
	log      *commitLogWriter

	// barrier is the most recent cross-namespace group with a part in this log whose decision is
	// not yet durable, or nil. Every commit queued behind it descends from that part, so it must
	// not be acknowledged before the group is decided - see PersistAsync. Set and read under
	// whatever serializes this namespace's writers, like the queue order itself.
	barrier atomic.Pointer[TxnGroup]
}

// NewPersistingCommitDAG persists commits with DurabilitySync - every Persist
// returns only once the commit is fsynced. See NewPersistingCommitDAGWithDurability
// for the other modes.
func NewPersistingCommitDAG(delegate *dag.InMemoryCommitDag, writer storage.DeltaSegmentWriter) *PersistingCommitDAG {
	return NewPersistingCommitDAGWithDurability(delegate, writer, storage.DurabilitySync)
}

// NewPersistingCommitDAGWithDurability chooses how much of the write-out a
// Persist call waits for - see commitLogWriter.Enqueue and storage.Durability.
func NewPersistingCommitDAGWithDurability(
	delegate *dag.InMemoryCommitDag,
	writer storage.DeltaSegmentWriter,
	durability storage.Durability,
) *PersistingCommitDAG {
	return NewPersistingCommitDAGWithAsyncInterval(delegate, writer, durability, 0)
}

// NewPersistingCommitDAGWithAsyncInterval additionally sets how often the
// commit log is physically flushed under storage.DurabilityAsync; a
// non-positive interval uses the default. Ignored under the other
// durabilities - see commitLogWriter.runAsync.
func NewPersistingCommitDAGWithAsyncInterval(
	delegate *dag.InMemoryCommitDag,
	writer storage.DeltaSegmentWriter,
	durability storage.Durability,
	asyncFlushInterval time.Duration,
) *PersistingCommitDAG {
	d := &PersistingCommitDAG{delegate: delegate, writer: writer}
	if writer != nil && durability != storage.DurabilityMemoryOnly {
		d.log = newCommitLogWriter(writer, durability, asyncFlushInterval)
	}
	return d
}

func (d *PersistingCommitDAG) GetCommit(hash codec.Hash) (document.Commit, bool) {
	return d.delegate.GetCommit(hash)
}
func (d *PersistingCommitDAG) GetCommitOrThrow(hash codec.Hash) (document.Commit, error) {
	return d.delegate.GetCommitOrThrow(hash)
}
func (d *PersistingCommitDAG) PutCommit(commit document.Commit, requireParents bool) error {
	// PutCommit is used by delta replay; do not re-persist.
	return d.delegate.PutCommit(commit, requireParents)
}
func (d *PersistingCommitDAG) GetDocumentTree(treeHash codec.Hash) (document.DocumentTree, bool) {
	return d.delegate.GetDocumentTree(treeHash)
}
func (d *PersistingCommitDAG) GetDocumentTreeOrThrow(treeHash codec.Hash) (document.DocumentTree, error) {
	return d.delegate.GetDocumentTreeOrThrow(treeHash)
}
func (d *PersistingCommitDAG) PutDocumentTree(tree document.DocumentTree) {
	d.delegate.PutDocumentTree(tree)
}
func (d *PersistingCommitDAG) Head() (codec.Hash, error) { return d.delegate.Head() }
func (d *PersistingCommitDAG) HeadCommit() (codec.Hash, document.Commit, bool, error) {
	return d.delegate.HeadCommit()
}
func (d *PersistingCommitDAG) SetHead(branchName string, hash codec.Hash) error {
	return d.delegate.SetHead(branchName, hash)
}
func (d *PersistingCommitDAG) GetBranch(name string) (document.Branch, bool) {
	return d.delegate.GetBranch(name)
}
func (d *PersistingCommitDAG) ListBranches() []document.Branch { return d.delegate.ListBranches() }
func (d *PersistingCommitDAG) CreateBranch(name string, fromHash codec.Hash) (document.Branch, error) {
	return d.delegate.CreateBranch(name, fromHash)
}
func (d *PersistingCommitDAG) Walk(from codec.Hash, until *codec.Hash, limit int) []dag.TraversalEntry {
	return d.delegate.Walk(from, until, limit)
}

func (d *PersistingCommitDAG) AppendCommit(
	tx document.Transaction,
	parentHash codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	c, err := d.delegate.AppendCommit(tx, parentHash, newDocumentTree, schemaHash, message)
	if err != nil {
		return document.Commit{}, err
	}
	if err := d.Persist(c); err != nil {
		return document.Commit{}, err
	}
	return c, nil
}

// Persist writes c to the delta log if this DAG has a writer configured - the same persistence
// AppendCommit performs internally, exposed for callers that must append the commit through the
// delegate DAG directly instead of through this wrapper. transaction.Engine.Commit/Replay/Merge
// require the concrete *dag.InMemoryCommitDag (conflict detection needs methods like HasCommit/
// CommonAncestor that aren't part of the dag.CommitDAG interface, so PersistingCommitDAG can't be
// passed to them), which means calling Engine.Commit against Delegate() bypasses AppendCommit -
// and therefore this persistence - entirely unless the caller invokes Persist itself afterward
// (see KdbServerRuntime.commitWith in go/kdb/server).
func (d *PersistingCommitDAG) Persist(c document.Commit) error {
	wait, err := d.PersistAsync(c)
	if err != nil {
		return err
	}
	return wait()
}

// PersistAsync queues c for the delta log and returns a func that waits for it
// to be durable. Callers that hold a lock fixing commit order (server's
// writeGate) should queue under it and release before calling wait, so the next
// commit's work overlaps this one's disk write and the two share an fsync.
//
// Framing, compression, the segment write and the fsync all happen on the log
// writer's own goroutine either way - see commitLogWriter.
func (d *PersistingCommitDAG) PersistAsync(c document.Commit) (wait func() error, err error) {
	// No writer at all, or DurabilityMemoryOnly: nothing is written down.
	if d.log == nil {
		return func() error { return nil }, nil
	}
	rec, err := deltaRecordFor(c)
	if err != nil {
		return nil, err
	}
	wait, err = d.log.EnqueueAsync(rec, c.DocumentTreeHash)
	if err != nil {
		return nil, err
	}
	return chainOnBarrier(wait, d.barrier.Load(), d.log.durability == storage.DurabilitySync), nil
}

// chainOnBarrier extends a durability wait with the decision of the group queued ahead of it.
//
// A commit queued behind an undecided group's part descends from that part. Should the group then
// fail to commit - a crash before its decision is durable - recovery rolls the part back and, with
// it, everything descending from it. So being on disk is not enough for this commit to be
// acknowledged: the group ahead of it must also be decided. Only under DurabilitySync, where an
// acknowledgement promises survival; an async acknowledgement promises nothing a crash cannot take.
func chainOnBarrier(wait func() error, barrier *TxnGroup, syncAck bool) func() error {
	if barrier == nil || !syncAck {
		return wait
	}
	if barrier.decided() && barrier.Err() == nil {
		return wait
	}
	return func() error {
		if err := wait(); err != nil {
			return err
		}
		<-barrier.done
		if err := barrier.Err(); err != nil {
			return &GroupFailedError{Group: barrier.ID, Cause: err}
		}
		return nil
	}
}

func deltaRecordFor(c document.Commit) (storage.DeltaRecord, error) {
	payload, err := c.ToPayloadBytes()
	if err != nil {
		return storage.DeltaRecord{}, err
	}
	return storage.DeltaRecord{
		CommitHash:  c.Hash,
		NamespaceID: c.NamespaceID,
		Authorship: storage.DeltaAuthorshipEnvelope{
			Principal:     "embedded",
			Timestamp:     c.Timestamp,
			RightsToken:   "",
			ClientContext: "",
		},
		CommitPayload: payload,
	}, nil
}

// persistPart queues one part of group g, with an acknowledgement that means "on disk" in every
// durability mode, and makes g the barrier later commits in this log chain on. Returns the group
// that was the barrier before - g's predecessor in this namespace - so g can order its decision
// behind that one's.
//
// Must be called under the same serialization as PersistAsync: the queue position and the barrier
// swap have to describe the same moment.
func (d *PersistingCommitDAG) persistPart(c document.Commit, g *TxnGroup) (wait func() error, predecessor *TxnGroup, err error) {
	predecessor = d.barrier.Load()
	if predecessor != nil && predecessor.decided() && predecessor.Err() == nil {
		predecessor = nil
	}
	if d.log == nil {
		return func() error { return nil }, predecessor, nil
	}
	rec, err := deltaRecordFor(c)
	if err != nil {
		return nil, nil, err
	}
	wait, err = d.log.EnqueueDurableAsync(rec, c.DocumentTreeHash)
	if err != nil {
		return nil, nil, err
	}
	d.barrier.Store(g)
	return wait, predecessor, nil
}

// clearBarrier drops g as this log's barrier once it is decided, unless a later group has already
// replaced it.
func (d *PersistingCommitDAG) clearBarrier(g *TxnGroup) { d.barrier.CompareAndSwap(g, nil) }

// fence refuses every later append with err. See commitLogWriter.fence.
func (d *PersistingCommitDAG) fence(err error) {
	if d.log != nil {
		d.log.fence(err)
	}
}

// Close drains and flushes everything still queued, then stops the log writer.
// Must run before the underlying segment writer is sealed or closed.
func (d *PersistingCommitDAG) Close() error {
	if d.log == nil {
		return nil
	}
	return d.log.Close()
}

// Delegate returns the concrete in-memory DAG this wrapper persists on top of - for callers that
// need a *dag.InMemoryCommitDag directly (APIs that don't accept the dag.CommitDAG interface) and
// will call Persist themselves afterward to keep durability.
func (d *PersistingCommitDAG) Delegate() *dag.InMemoryCommitDag { return d.delegate }

var _ dag.CommitDAG = (*PersistingCommitDAG)(nil)

// WalkWithOperations delegates to the in-memory DAG, so a caller holding
// the persisting wrapper does not have to unwrap it to walk history
// safely. See dag.InMemoryCommitDag.WalkWithOperations for why reading
// operations off plain Walk's result is a mistake.
func (p *PersistingCommitDAG) WalkWithOperations(from codec.Hash, until *codec.Hash, limit int) ([]dag.TraversalEntry, error) {
	return p.delegate.WalkWithOperations(from, until, limit)
}

// SetPersistListener installs a callback told where each commit landed in
// the delta log, once the append knows.
//
// The position is not knowable before the append - a batch writes several
// records and only the append says where each went - so anything keyed by
// it has to be filed afterwards. See engine.RecordCommitLocation, the one
// caller, which uses it to record where the versions a commit wrote can be
// found without keeping a second copy of them.
func (d *PersistingCommitDAG) SetPersistListener(fn func(treeHash codec.Hash, segmentSeq, frameOffset int64)) {
	if d.log != nil {
		d.log.onPersisted = fn
	}
}

// SetSealListener registers a callback invoked when a rotation seals a delta
// segment, which is the moment something new becomes reclaimable: nothing
// below the open segment can be truncated, so until one is sealed there is
// nothing for maintenance to find.
//
// Called from the log writer's own goroutine, so fn must be cheap and must
// not reclaim inline - MaintenanceScheduler.NotifySegmentSealed, the caller
// this exists for, only wakes its loop.
func (d *PersistingCommitDAG) SetSealListener(fn func()) {
	if d.log != nil {
		d.log.onSealed = fn
	}
}

// History navigation, forwarded so a persisting DAG is as browsable as the
// one it wraps. None of it writes, so none of it needs the log: these read
// the graph the delegate already holds. See dag.HistoryNavigator.
func (d *PersistingCommitDAG) ResolveRef(ref dag.CommitRef) (codec.Hash, error) {
	return d.delegate.ResolveRef(ref)
}
func (d *PersistingCommitDAG) ResolveRevision(spec string) (codec.Hash, error) {
	return d.delegate.ResolveRevision(spec)
}
func (d *PersistingCommitDAG) NthAncestor(from codec.Hash, n int) (codec.Hash, error) {
	return d.delegate.NthAncestor(from, n)
}
func (d *PersistingCommitDAG) CommitAtOrBefore(from codec.Hash, ts codec.Timestamp) (codec.Hash, error) {
	return d.delegate.CommitAtOrBefore(from, ts)
}
func (d *PersistingCommitDAG) ListCommits(from codec.Hash, skip, limit int) ([]dag.CommitInfo, error) {
	return d.delegate.ListCommits(from, skip, limit)
}
func (d *PersistingCommitDAG) CreateTag(name string, hash codec.Hash, message string) (document.Tag, error) {
	return d.delegate.CreateTag(name, hash, message)
}
func (d *PersistingCommitDAG) GetTag(name string) (document.Tag, bool) {
	return d.delegate.GetTag(name)
}
func (d *PersistingCommitDAG) ListTags() []document.Tag   { return d.delegate.ListTags() }
func (d *PersistingCommitDAG) DeleteTag(name string) bool { return d.delegate.DeleteTag(name) }

var _ dag.HistoryNavigator = (*PersistingCommitDAG)(nil)

// AcknowledgesDurably reports whether a commit's durability wait means "on disk"
// (storage.DurabilitySync). False under DurabilityAsync and for a DAG with no log at all.
func (d *PersistingCommitDAG) AcknowledgesDurably() bool {
	return d.log != nil && d.log.durability == storage.DurabilitySync
}
