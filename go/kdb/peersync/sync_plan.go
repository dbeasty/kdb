package peersync

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// ComputeSyncPlan compares local and remote heads in the DAG.
func ComputeSyncPlan(d *dag.InMemoryCommitDag, localHead, remoteHead codec.Hash) (*DagSyncPlan, error) {
	if localHead == remoteHead {
		return &DagSyncPlan{CommonAncestor: &localHead}, nil
	}
	ancestor := d.CommonAncestor(localHead, remoteHead)
	exclude := map[codec.Hash]struct{}{}
	if ancestor != nil {
		exclude[*ancestor] = struct{}{}
	}
	localOnly := d.CommitsSince(localHead, exclude)
	var remoteOnly []codec.Hash
	if d.HasCommit(remoteHead) {
		remoteOnly = d.CommitsSince(remoteHead, exclude)
	}
	return &DagSyncPlan{CommonAncestor: ancestor, LocalOnly: localOnly, RemoteOnly: remoteOnly}, nil
}

// CommitsToPush returns commits on the local branch not yet on the remote side.
func CommitsToPush(d *dag.InMemoryCommitDag, localHead, remoteHead codec.Hash, limit int) ([]document.Commit, error) {
	if localHead == remoteHead {
		return nil, nil
	}
	if !d.HasCommit(remoteHead) {
		return nil, nil
	}
	walked := d.Walk(localHead, &remoteHead, limit)
	out := make([]document.Commit, 0, len(walked))
	for i := len(walked) - 1; i >= 0; i-- {
		if full, ok := walked[i].(dag.FullEntry); ok {
			out = append(out, full.Commit)
		}
	}
	return out, nil
}
