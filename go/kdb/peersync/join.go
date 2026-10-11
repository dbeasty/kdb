package peersync

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// joinSameContent settles a divergence between two heads that hold the same documents and schema
// - the only divergence left in a mesh once writes stop - without minting a merge per pair.
//
// The pairwise merge is deterministic but not confluent: merge(merge(a, b), c) and
// merge(merge(a, c), b) are different commits with the same tree, so every sync of two such heads
// made yet another head, and in a mesh whose syncs overlap those were made faster than they
// spread (see dag/join.go). Here the result depends only on the content-carrying history the two
// heads stand for between them - their joint frontier P, looking through content-free merges:
//
//   - P is one of the two heads' own commit (a content-carrying commit that already holds the
//     other side's history): that head. Local: nothing to do. Incoming: main moves to it.
//   - otherwise the canonical join of P: a content-free merge whose parents are P in hash order,
//     with the transaction id, author, message and timestamp derived from P alone. Every node
//     joining heads that stand for P builds this same commit, whatever order it learned P in,
//     and a head that already is it is simply adopted.
//
// The new head need not descend from the old one - only content-free merges are left off main's
// path, never a commit that wrote anything, and the documents do not change at all, so there is
// nothing to write to storage. For two content-carrying heads the join is exactly the merge the
// pairwise rule always made, so the two rules agree wherever both apply.
//
// ok is false when the heads differ in content; the caller merges them the ordinary way.
func joinSameContent(d *dag.InMemoryCommitDag, store storage.Adapter, namespaceID string, localHead, incomingHead codec.Hash) (CommitPushOutcome, AdvanceStep, bool, error) {
	local, err := d.GetCommitOrThrow(localHead)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, false, err
	}
	incoming, err := d.GetCommitOrThrow(incomingHead)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, false, err
	}
	if local.DocumentTreeHash != incoming.DocumentTreeHash || !sameSchema(local.SchemaHash, incoming.SchemaHash) {
		return CommitPushOutcome{}, AdvanceStep{}, false, nil
	}
	frontier := d.FrontierOf(localHead, incomingHead)
	var target codec.Hash
	switch {
	case len(frontier) == 1 && frontier[0] == localHead:
		target = localHead
	case len(frontier) == 1 && frontier[0] == incomingHead:
		target = incomingHead
	case namesExactly(d, local, frontier):
		target = localHead
	case namesExactly(d, incoming, frontier):
		target = incomingHead
	}
	if target == localHead {
		return CommitPushOutcome{Kind: OutcomeNoOp}, AdvanceStep{}, true, nil
	}
	var head document.Commit
	if target == incomingHead {
		if err := d.SetHead(mainBranch, incomingHead); err != nil {
			return CommitPushOutcome{}, AdvanceStep{}, true, err
		}
		head = incoming
	} else {
		// The live tree is local's, which is also incoming's; nothing is staged, so this only
		// hands back the tree object the commit names.
		tree, err := store.CommitTree(namespaceID, local.DocumentTreeHash)
		if err != nil {
			return CommitPushOutcome{}, AdvanceStep{}, true, err
		}
		if tree.TreeHash != local.DocumentTreeHash {
			// The live tree is not the head's - not something a join can fix. Merge as before.
			_ = store.DiscardPending(namespaceID)
			return CommitPushOutcome{}, AdvanceStep{}, false, nil
		}
		if head, err = d.AppendCommitOnto(&localHead, joinTransaction(d, frontier), frontier, tree, local.SchemaHash, mergeMessage); err != nil {
			return CommitPushOutcome{}, AdvanceStep{}, true, err
		}
	}
	// Everything main reaches now and did not before, parents first, ending at the new head: what
	// is logged and announced, as for any merge.
	commits, err := commitsBetween(d, head.Hash, localHead)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, true, err
	}
	return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &head}, AdvanceStep{Commits: commits}, true, nil
}

// joinTransaction is the canonical join of frontier: every field derived from the frontier
// alone. For two parents it is the pairwise merge's own derivation, so the join of two
// content-carrying heads is the commit the pairwise rule always made.
func joinTransaction(d *dag.InMemoryCommitDag, frontier []codec.Hash) document.Transaction {
	hexes := make([]string, len(frontier))
	var ts codec.Timestamp
	for i, h := range frontier {
		hexes[i] = h.Hex()
		if c, ok := d.GetCommit(h); ok && c.Timestamp.EpochMicros() > ts.EpochMicros() {
			ts = c.Timestamp
		}
	}
	return document.Transaction{
		ID:           codec.DerivedUUID("kdb:merge/1:" + strings.Join(hexes, ":")),
		Timestamp:    ts, // the DAG stamps it one microsecond past the latest parent
		AuthorNodeID: MergeAuthorNodeID,
	}
}

// namesExactly reports whether c is the canonical join of frontier already: a content-free merge
// whose parents are exactly frontier.
func namesExactly(d *dag.InMemoryCommitDag, c document.Commit, frontier []codec.Hash) bool {
	if len(c.ParentHashes) != len(frontier) || !d.IsContentFreeMerge(c) {
		return false
	}
	parents := append([]codec.Hash(nil), c.ParentHashes...)
	dag.SortHashes(parents)
	for i := range parents {
		if parents[i] != frontier[i] {
			return false
		}
	}
	return true
}

func sameSchema(a, b *codec.Hash) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
