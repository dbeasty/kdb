package embed

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// PersistingCommitDAG wraps an in-memory DAG and persists new commits to the delta log.
type PersistingCommitDAG struct {
	delegate *dag.InMemoryCommitDag
	writer   storage.DeltaSegmentWriter
}

func NewPersistingCommitDAG(delegate *dag.InMemoryCommitDag, writer storage.DeltaSegmentWriter) *PersistingCommitDAG {
	return &PersistingCommitDAG{delegate: delegate, writer: writer}
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
	if d.writer == nil {
		return nil
	}
	payload, err := c.ToPayloadBytes()
	if err != nil {
		return err
	}
	if _, err := d.writer.Append(storage.DeltaRecord{
		CommitHash:  c.Hash,
		NamespaceID: c.NamespaceID,
		Authorship: storage.DeltaAuthorshipEnvelope{
			Principal:     "embedded",
			Timestamp:     c.Timestamp,
			RightsToken:   "",
			ClientContext: "",
		},
		CommitPayload: payload,
	}); err != nil {
		return err
	}
	return d.writer.Flush()
}

// Delegate returns the concrete in-memory DAG this wrapper persists on top of - for callers that
// need a *dag.InMemoryCommitDag directly (APIs that don't accept the dag.CommitDAG interface) and
// will call Persist themselves afterward to keep durability.
func (d *PersistingCommitDAG) Delegate() *dag.InMemoryCommitDag { return d.delegate }

var _ dag.CommitDAG = (*PersistingCommitDAG)(nil)
