package embed

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// snapshotSupport is what a file-backed namespace needs to make a peer-sync snapshot bootstrap
// survive restart. See CanInstallSnapshot and PersistSnapshot.
type snapshotSupport struct {
	dag                 *dag.InMemoryCommitDag
	store               storage.Adapter
	shim                storage.PlatformIOShim
	dataRoot            string
	namespaceID         string
	checkpointsDisabled bool
}

// ErrSnapshotNeedsFreshNamespace refuses a snapshot bootstrap of a namespace that already has
// history: the snapshot's root has no parents here, so the log cannot describe anything before
// it, and the only safe starting point is a log with nothing in it.
var ErrSnapshotNeedsFreshNamespace = errors.New("kdb: a snapshot can only bootstrap a namespace that has never had a commit")

// CanInstallSnapshot reports whether a peer's snapshot can be installed into this namespace
// durably - checked before anything is written, so a refusal leaves the namespace untouched.
// A memory runtime always can: there is nothing to make durable.
func (r *EmbeddedKdbRuntime) CanInstallSnapshot() error {
	s := r.snapshot
	if s == nil {
		if r.DataRoot != "" {
			return errors.New("kdb: a read-only namespace cannot be bootstrapped")
		}
		return nil
	}
	if s.checkpointsDisabled {
		return errors.New("kdb: a snapshot bootstrap is recorded only by the checkpoint, and checkpoints are disabled for this namespace")
	}
	if _, ok := s.store.(*engine.ServerEngine); !ok {
		return fmt.Errorf("kdb: namespace %s's storage cannot hold a snapshot durably", s.namespaceID)
	}
	if s.dag.CommitCount() > 1 {
		return ErrSnapshotNeedsFreshNamespace
	}
	return nil
}

// PersistSnapshot makes an installed snapshot durable. The documents and root commit are already
// in storage and the DAG (peersync.InstallSnapshot put them there); this writes the documents'
// bodies to the blob store, a checkpoint claiming them as the state before the log's first
// segment, and the namespace marker that makes open refuse to proceed without that checkpoint.
//
// In that order: a crash before the checkpoint leaves a namespace that opens empty and can be
// bootstrapped again; after it, one that opens bootstrapped.
func (r *EmbeddedKdbRuntime) PersistSnapshot(root document.Commit) error {
	s := r.snapshot
	if s == nil {
		return nil
	}
	eng, ok := s.store.(*engine.ServerEngine)
	if !ok {
		return fmt.Errorf("kdb: namespace %s's storage cannot hold a snapshot durably", s.namespaceID)
	}
	if err := eng.MaterializeLiveBodies(); err != nil {
		return err
	}
	var heads [][]byte
	for _, h := range s.dag.ShallowRoots() {
		if c, err := s.dag.GetCommitOrThrow(h); err == nil {
			if payload, err := c.ToPayloadBytes(); err == nil {
				heads = append(heads, payload)
			}
		}
	}
	if err := writeCheckpoint(s.shim, namespaceCheckpoint{
		NamespaceID:     s.namespaceID,
		ThroughSequence: -1, // the snapshot is the state before the first segment
		FloorSequence:   0,
		State:           s.dag.CheckpointSnapshot(),
		LiveTree:        eng.LiveTree(),
		HeadCommits:     heads,
	}); err != nil {
		return err
	}
	meta, _ := readNamespaceMeta(s.dataRoot, s.namespaceID)
	meta.NamespaceID = s.namespaceID
	for _, existing := range meta.ShallowRoots {
		if existing == root.Hash.Hex() {
			return nil
		}
	}
	meta.ShallowRoots = append(meta.ShallowRoots, root.Hash.Hex())
	return writeNamespaceMeta(s.dataRoot, s.namespaceID, meta)
}
