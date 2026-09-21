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

// MissingCommits returns what a holder of haves lacks from head: every commit reachable from
// head and not from any of haves, parents first, capped at limit - plus a stub for every archived
// parent those commits name that the holder cannot be assumed to have. A hash in haves this DAG
// does not know excludes only itself.
//
// Parents first is what makes a capped result usable on its own: each page's commits have every
// parent either in an earlier page, in the holder's history, or in the stubs sent alongside, so
// the receiver can store a page the moment it arrives.
func MissingCommits(d *dag.InMemoryCommitDag, head codec.Hash, haves []codec.Hash, limit int) ([]document.Commit, []document.CommitStub, error) {
	have := d.AncestorSetOf(haves)
	if _, ok := have[head]; ok {
		return nil, nil, nil
	}
	set := map[codec.Hash]document.Commit{}
	stubs := map[codec.Hash]document.CommitStub{}
	queue := []codec.Hash{head}
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, ok := have[h]; ok {
			continue
		}
		if _, ok := set[h]; ok {
			continue
		}
		if s, ok := d.GetStub(h); ok {
			stubs[h] = s
			continue
		}
		c, ok := d.GetCommit(h)
		if !ok {
			continue
		}
		set[h] = c
		queue = append(queue, c.ParentHashes...)
	}
	ordered := topoSort(set)
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	var needStubs []document.CommitStub
	for i := range ordered {
		ops, err := d.CommitOperations(ordered[i].Hash)
		if err != nil {
			return nil, nil, err
		}
		ordered[i].Operations = ops
		for _, p := range ordered[i].ParentHashes {
			if s, ok := stubs[p]; ok {
				needStubs = append(needStubs, s)
				delete(stubs, p)
			}
		}
	}
	return ordered, needStubs, nil
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
	// These commits are handed to the remote whole, so their operations
	// have to be present - see WalkWithOperations. The reachability walk
	// above only reads hashes and stays on plain Walk.
	walked, err := d.WalkWithOperations(localHead, nil, math.MaxInt)
	if err != nil {
		return nil, err
	}
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
