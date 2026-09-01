package embed

import (
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
	payload, err := c.ToPayloadBytes()
	if err != nil {
		return nil, err
	}
	return d.log.EnqueueAsync(storage.DeltaRecord{
		CommitHash:  c.Hash,
		NamespaceID: c.NamespaceID,
		Authorship: storage.DeltaAuthorshipEnvelope{
			Principal:     "embedded",
			Timestamp:     c.Timestamp,
			RightsToken:   "",
			ClientContext: "",
		},
		CommitPayload: payload,
	})
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
