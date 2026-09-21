package peersync

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// RefsOf describes a namespace's refs as a v2 node advertises them.
func RefsOf(ns string, d *dag.InMemoryCommitDag) wire.NamespaceRefs {
	refs := wire.NamespaceRefs{Namespace: ns, Branches: map[string]string{}, Tags: map[string]string{}}
	for _, b := range d.ListBranches() {
		refs.Branches[b.Name] = b.HeadHash.Hex()
	}
	for _, t := range d.ListTags() {
		refs.Tags[t.Name] = t.CommitHash.Hex()
	}
	return refs
}

// RefUpdateResult is what ApplyRef did.
type RefUpdateResult struct {
	Outcome wire.RefUpdateOutcome
	// Head is the ref's value after the decision.
	Head codec.Hash
	// Conflict is set when Outcome is RefConflict.
	Conflict *kdberr.ConflictReport
	// Main carries the full ingest result when the ref was main.
	Main *IngestResult
}

// ApplyRef decides what a proposed ref value does to this node's ref of the same name, and does
// it. The proposed commit must already be stored.
//
//   - main goes through Adopt: fast-forward, deterministic merge, or a conflict report - the same
//     decision a v1 push or pull makes, including everything Adopt does to storage and indexes.
//   - any other branch moves only forward: created if absent, fast-forwarded if the proposal
//     descends from it, left alone if it is already ahead, and reported as a conflict if the
//     two diverged. Only main has live documents to merge; a side branch is history, and
//     merging it is a decision for whoever owns it.
//   - a tag never moves: created if absent, and a proposal naming a different commit for an
//     existing tag is a conflict. Tags are the names people restore to, so silently re-pointing
//     one would be the worst kind of replication.
func ApplyRef(env IngestEnv, kind wire.RefKind, name string, proposed codec.Hash) (RefUpdateResult, error) {
	if !env.DAG.HasCommit(proposed) {
		return RefUpdateResult{}, fmt.Errorf("peer sync: %s %q names commit %s, which was never sent", kind, name, proposed.Hex())
	}
	switch kind {
	case wire.RefTag:
		return applyTag(env, name, proposed)
	case wire.RefBranch, "":
		if name == mainBranch {
			return applyMain(env, proposed)
		}
		return applySideBranch(env, name, proposed)
	default:
		return RefUpdateResult{}, fmt.Errorf("peer sync: unknown ref kind %q", kind)
	}
}

func applyMain(env IngestEnv, proposed codec.Hash) (RefUpdateResult, error) {
	res, err := Adopt(env, proposed)
	if err != nil {
		return RefUpdateResult{}, err
	}
	out := RefUpdateResult{Head: res.Head, Main: &res}
	switch res.Outcome.Kind {
	case OutcomeFastForwarded:
		out.Outcome = wire.RefFastForwarded
	case OutcomeMerged:
		out.Outcome = wire.RefMerged
	case OutcomeConflict:
		out.Outcome = wire.RefConflict
		out.Conflict = res.Outcome.Report
	default:
		out.Outcome = wire.RefNoOp
	}
	return out, nil
}

func applySideBranch(env IngestEnv, name string, proposed codec.Hash) (RefUpdateResult, error) {
	lock := divergenceLockFor(env.NamespaceID)
	lock.Lock()
	defer lock.Unlock()
	b, ok := env.DAG.GetBranch(name)
	if !ok {
		if _, err := env.DAG.CreateBranch(name, proposed); err != nil {
			return RefUpdateResult{}, err
		}
		return RefUpdateResult{Outcome: wire.RefCreated, Head: proposed}, nil
	}
	switch ResolveHeadUpdate(env.DAG, b.HeadHash, proposed) {
	case HeadFastForward:
		if err := env.DAG.SetHead(name, proposed); err != nil {
			return RefUpdateResult{}, err
		}
		return RefUpdateResult{Outcome: wire.RefFastForwarded, Head: proposed}, nil
	case HeadAlreadyAncestor:
		return RefUpdateResult{Outcome: wire.RefNoOp, Head: b.HeadHash}, nil
	default:
		return RefUpdateResult{Outcome: wire.RefConflict, Head: b.HeadHash, Conflict: &kdberr.ConflictReport{
			TransactionID: proposed.Hex(), BaseHash: b.HeadHash.Hex(), TargetHash: proposed.Hex(),
		}}, nil
	}
}

func applyTag(env IngestEnv, name string, proposed codec.Hash) (RefUpdateResult, error) {
	lock := divergenceLockFor(env.NamespaceID)
	lock.Lock()
	defer lock.Unlock()
	if t, ok := env.DAG.GetTag(name); ok {
		if t.CommitHash == proposed {
			return RefUpdateResult{Outcome: wire.RefNoOp, Head: proposed}, nil
		}
		return RefUpdateResult{Outcome: wire.RefConflict, Head: t.CommitHash, Conflict: &kdberr.ConflictReport{
			TransactionID: proposed.Hex(), BaseHash: t.CommitHash.Hex(), TargetHash: proposed.Hex(),
		}}, nil
	}
	if _, err := env.DAG.CreateTag(name, proposed, "replicated"); err != nil {
		return RefUpdateResult{}, err
	}
	return RefUpdateResult{Outcome: wire.RefCreated, Head: proposed}, nil
}

// refTargets lists every commit a namespace's refs name, deduplicated and sorted: the wants of a
// fetch that wants everything.
func refTargets(refs wire.NamespaceRefs) ([]codec.Hash, error) {
	seen := map[codec.Hash]struct{}{}
	add := func(hex string) error {
		h, err := codec.HashFromHex(hex)
		if err != nil {
			return err
		}
		seen[h] = struct{}{}
		return nil
	}
	for _, hex := range refs.Branches {
		if err := add(hex); err != nil {
			return nil, err
		}
	}
	for _, hex := range refs.Tags {
		if err := add(hex); err != nil {
			return nil, err
		}
	}
	out := make([]codec.Hash, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hex() < out[j].Hex() })
	return out, nil
}

// localRefHeads is every commit this node's refs name - what a fetch can name as haves beyond
// main's spread ancestors.
func localRefHeads(d *dag.InMemoryCommitDag) []codec.Hash {
	var out []codec.Hash
	for _, b := range d.ListBranches() {
		out = append(out, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		out = append(out, t.CommitHash)
	}
	return out
}

// DefaultPageBytes caps one v2 page's estimated commit bytes when the requester names no cap.
const DefaultPageBytes = 4 << 20

// commitSize estimates a commit's encoded size: its operations' bodies plus a fixed allowance
// for hashes, ids and framing. An estimate is enough - it only has to keep a page near its cap,
// not at it - and encoding every commit twice to measure it exactly would double the cost of
// every fetch.
func commitSize(c document.Commit) int {
	n := 256 + len(c.Message) + 32*len(c.ParentHashes)
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			n += 48 + len(o.Patch)
		default:
			n += 48
		}
	}
	return n
}

// MissingCommitsFrom is MissingCommits for several wanted heads, paged by estimated bytes
// instead of count. done reports whether the page holds everything that was missing.
func MissingCommitsFrom(d *dag.InMemoryCommitDag, wants, haves []codec.Hash, maxBytes int) (commits []document.Commit, stubs []document.CommitStub, done bool, err error) {
	if maxBytes <= 0 {
		maxBytes = DefaultPageBytes
	}
	have := d.AncestorSetOf(haves)
	set := map[codec.Hash]document.Commit{}
	allStubs := map[codec.Hash]document.CommitStub{}
	queue := append([]codec.Hash(nil), wants...)
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
			allStubs[h] = s
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
	size := 0
	for i := range ordered {
		ops, err := d.CommitOperations(ordered[i].Hash)
		if err != nil {
			return nil, nil, false, err
		}
		ordered[i].Operations = ops
		n := commitSize(ordered[i])
		if i > 0 && size+n > maxBytes {
			ordered = ordered[:i]
			break
		}
		size += n
		for _, p := range ordered[i].ParentHashes {
			if s, ok := allStubs[p]; ok {
				stubs = append(stubs, s)
				delete(allStubs, p)
			}
		}
	}
	return ordered, stubs, len(ordered) == len(set), nil
}
