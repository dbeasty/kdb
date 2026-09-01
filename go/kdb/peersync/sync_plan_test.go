package peersync

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// TestCommitsToPushIncludesMergeCommitsBothParents is the regression test for the
// missing-parent push bug found live by the 3-node e2e scenario: after a divergence auto-merge,
// the local head is a merge commit whose parents are the two sides' tips. The old
// implementation (a single Walk(localHead, &remoteHead) with break-at-until semantics) could
// dequeue remoteHead - the newer side, by timestamp - and abort the whole traversal before ever
// visiting the local-side branch, so the push carried the merge commit WITHOUT its local-side
// ancestors; the receiving peer rejected it with "missing parent".
func TestCommitsToPushIncludesMergeCommitsBothParents(t *testing.T) {
	const ns = "push/ns"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()

	docA, _ := codec.RandomUUID()
	docB, _ := codec.RandomUUID()

	// Local-side write happens FIRST (older timestamp), then the remote-side write - the
	// ordering that made the old timestamp-priority walk hit remoteHead before the local side.
	localTip := writeDocAt(t, local, ns, genesis, docA, `{"origin":"local"}`, codec.TimestampFromEpochMicros(1_000))
	remoteTip := writeDocAt(t, local, ns, genesis, docB, `{"origin":"remote"}`, codec.TimestampFromEpochMicros(2_000))

	// Both tips were appended detached, so main is wherever the last one landed. Point it at
	// the local tip, which is what "localHead" means to ResolveDivergence's real callers - they
	// pass d.Head(), and the auto-merge compare-and-swaps against it.
	if err := local.dag.SetHead("main", localTip.Hash); err != nil {
		t.Fatalf("set head to local tip: %v", err)
	}

	// The merge commit a divergence resolution would create: parents = both tips.
	outcome, err := ResolveDivergence(local.dag, local.storage, ns, localTip.Hash, remoteTip.Hash, ResolutionOptions{})
	if err != nil {
		t.Fatalf("resolve divergence: %v", err)
	}
	if outcome.MergeCommit == nil {
		t.Fatalf("expected an auto-merge, got %+v", outcome)
	}
	mergeHash := outcome.MergeCommit.Hash

	// Push plan from the merged head toward a peer that only knows remoteTip.
	commits, err := CommitsToPush(local.dag, mergeHash, remoteTip.Hash, 100)
	if err != nil {
		t.Fatalf("commits to push: %v", err)
	}

	byHash := map[codec.Hash]int{}
	for i, c := range commits {
		byHash[c.Hash] = i
	}
	localIdx, hasLocal := byHash[localTip.Hash]
	mergeIdx, hasMerge := byHash[mergeHash]
	if !hasLocal {
		t.Fatalf("push omits the merge commit's local-side parent (the live bug): %v", byHash)
	}
	if !hasMerge {
		t.Fatalf("push omits the merge commit itself: %v", byHash)
	}
	if localIdx > mergeIdx {
		t.Fatalf("parent pushed after child: local=%d merge=%d", localIdx, mergeIdx)
	}
	// And nothing the remote already has.
	if _, hasRemote := byHash[remoteTip.Hash]; hasRemote {
		t.Fatal("push includes a commit the remote already has (remoteTip)")
	}
	if _, hasGenesis := byHash[genesis]; hasGenesis {
		t.Fatal("push includes the shared genesis commit")
	}
}

// TestLastWriteMergeCommitCarriesTheWinningWrite: when the LOCAL side wins a LAST_WRITE
// divergence, the auto-merge commit must still carry the winning write as one of its own
// operations. Omitting it (the previous behavior) was self-consistent on the node creating the
// merge but not on peers: they materialize the pushed commits oldest-first, so the losing raw
// commit lands after the winner and a no-op merge leaves them on the loser's content - observed
// live as direction-dependent winners in the e2e same-document conflict scenario.
func TestLastWriteMergeCommitCarriesTheWinningWrite(t *testing.T) {
	const ns = "lw/ns"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()

	docID, _ := codec.RandomUUID()
	// Local write is LATER (wins); remote write earlier (loses).
	localTip := writeDocAt(t, local, ns, genesis, docID, `{"winner":"local"}`, codec.TimestampFromEpochMicros(2_000))
	remoteTip := writeDocAt(t, local, ns, genesis, docID, `{"winner":"remote"}`, codec.TimestampFromEpochMicros(1_000))

	if err := local.dag.SetHead("main", localTip.Hash); err != nil {
		t.Fatalf("set head to local tip: %v", err)
	}

	outcome, err := ResolveDivergence(local.dag, local.storage, ns, localTip.Hash, remoteTip.Hash, ResolutionOptions{
		Policy: transaction.ConflictPolicyLastWrite,
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if outcome.Kind != OutcomeMerged || outcome.MergeCommit == nil {
		t.Fatalf("expected merged outcome, got %+v", outcome)
	}
	var found bool
	for _, op := range outcome.MergeCommit.Operations {
		if w, ok := op.(document.WriteOp); ok && w.DocID == docID {
			found = true
			if w.Patch != `{"winner":"local"}` {
				t.Fatalf("merge op carries the losing write: %s", w.Patch)
			}
		}
	}
	if !found {
		t.Fatal("merge commit carries no operation for the conflicted document - peers materializing it keep the loser's content")
	}
}
