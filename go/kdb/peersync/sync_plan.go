package peersync

import (
	"math"

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

// CommitsToPush returns the commits reachable from localHead but not from remoteHead - the
// true set difference, in parent-before-child order, capped at limit. A single
// Walk(localHead, &remoteHead) is NOT that set: pruning only at the exact remoteHead hash
// still visits shared history reachable around it (e.g. through a merge commit's other
// parent), so everything reachable from remoteHead is excluded explicitly instead.
func CommitsToPush(d *dag.InMemoryCommitDag, localHead, remoteHead codec.Hash, limit int) ([]document.Commit, error) {
	if localHead == remoteHead {
		return nil, nil
	}
	if !d.HasCommit(remoteHead) {
		return nil, nil
	}
	remoteReach := make(map[codec.Hash]struct{})
	for _, e := range d.Walk(remoteHead, nil, math.MaxInt) {
		switch entry := e.(type) {
		case dag.FullEntry:
			remoteReach[entry.Commit.Hash] = struct{}{}
		case dag.StubbedEntry:
			remoteReach[entry.Stub.OriginalHash] = struct{}{}
		}
	}
	walked := d.Walk(localHead, nil, math.MaxInt)
	out := make([]document.Commit, 0, len(walked))
	// Walk returns newest-first; reverse so parents land before children on the remote.
	for i := len(walked) - 1; i >= 0; i-- {
		full, ok := walked[i].(dag.FullEntry)
		if !ok {
			continue
		}
		if _, shared := remoteReach[full.Commit.Hash]; shared {
			continue
		}
		out = append(out, full.Commit)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
