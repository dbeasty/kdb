package peersync

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// side is one independent DAG+storage pair - forkTwoSides gives each test two sides that share
// a deterministic genesis commit (NewInMemoryCommitDag's genesis is fixed UUIDs/timestamp/tree,
// so two independently-created dags for the same namespace hash identically), then let their
// histories diverge, mirroring PeerSyncConflictDetectionTest.kt's forkTwoSides.
type side struct {
	dag     *dag.InMemoryCommitDag
	storage storage.Adapter
}

func forkTwoSides(t *testing.T, ns string) (side, side) {
	t.Helper()
	d1, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("dag 1: %v", err)
	}
	d2, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("dag 2: %v", err)
	}
	h1, _ := d1.Head()
	h2, _ := d2.Head()
	if h1 != h2 {
		t.Fatalf("genesis hashes diverged: %s vs %s", h1.Hex(), h2.Hex())
	}
	return side{dag: d1, storage: mem.NewInMemoryStorageAdapter()}, side{dag: d2, storage: mem.NewInMemoryStorageAdapter()}
}

func writeDoc(t *testing.T, s side, ns string, parent codec.Hash, docID codec.UUID, json string) document.Commit {
	t.Helper()
	if err := s.storage.PutDocument(ns, document.Document{ID: docID, JSON: json}); err != nil {
		t.Fatalf("putDocument: %v", err)
	}
	parentCommit, err := s.dag.GetCommitOrThrow(parent)
	if err != nil {
		t.Fatalf("getCommitOrThrow(parent): %v", err)
	}
	tree, err := s.storage.CommitTree(ns, parentCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("commitTree: %v", err)
	}
	txID, _ := codec.RandomUUID()
	authorID, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: parent,
		Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: json}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	commit, err := s.dag.AppendCommit(tx, parent, tree, nil, "test write")
	if err != nil {
		t.Fatalf("appendCommit: %v", err)
	}
	return commit
}

// mergeInto replays commit (produced on another side) into dst's dag+storage, the same way
// PutCommit + a materializeCommit callback would in production - needed so dst can see remote
// commits' document content, not just their hash/ops metadata. Also registers the commit's
// resulting tree on dst's own storage adapter (via CommitTree, not just staging pending writes)
// so a later writeDoc/CommitTree call using this commit as *its* parent can find the parent tree
// - each storage.Adapter tracks its own tree registry independently, separate from the dag's.
func mergeInto(t *testing.T, dst side, ns string, commit document.Commit) {
	t.Helper()
	if !dst.dag.HasCommit(commit.Hash) {
		if err := dst.dag.PutCommit(commit, true); err != nil {
			t.Fatalf("putCommit: %v", err)
		}
	}
	for _, op := range commit.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			if err := dst.storage.PutDocument(ns, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				t.Fatalf("putDocument: %v", err)
			}
		case document.DeleteOp:
			_ = dst.storage.DeleteDocument(ns, o.DocID)
		}
	}
	if len(commit.ParentHashes) == 0 {
		return
	}
	parentCommit, err := dst.dag.GetCommitOrThrow(commit.ParentHashes[0])
	if err != nil {
		t.Fatalf("getCommitOrThrow(mergeInto parent): %v", err)
	}
	if _, err := dst.storage.CommitTree(ns, parentCommit.DocumentTreeHash); err != nil {
		t.Fatalf("commitTree (mergeInto): %v", err)
	}
}

func newUUID(t *testing.T) codec.UUID {
	t.Helper()
	u, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("randomUUID: %v", err)
	}
	return u
}

func TestResolveHeadUpdateFastForward(t *testing.T) {
	ns := "app/ff"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	docID := newUUID(t)
	c1 := writeDoc(t, local, ns, genesis, docID, `{"v":1}`)

	if got := ResolveHeadUpdate(local.dag, genesis, c1.Hash); got != HeadFastForward {
		t.Fatalf("expected HeadFastForward, got %v", got)
	}
}

func TestResolveHeadUpdateAlreadyAncestorOnEqualHeads(t *testing.T) {
	ns := "app/eq"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	if got := ResolveHeadUpdate(local.dag, genesis, genesis); got != HeadAlreadyAncestor {
		t.Fatalf("expected HeadAlreadyAncestor, got %v", got)
	}
}

func TestResolveHeadUpdateAlreadyAncestorWhenLocalIsAhead(t *testing.T) {
	ns := "app/ahead"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	docID := newUUID(t)
	c1 := writeDoc(t, local, ns, genesis, docID, `{"v":1}`)

	// local is already ahead of (an ancestor of it is) the "incoming" genesis.
	if got := ResolveHeadUpdate(local.dag, c1.Hash, genesis); got != HeadAlreadyAncestor {
		t.Fatalf("expected HeadAlreadyAncestor, got %v", got)
	}
}

func TestResolveHeadUpdateDiverged(t *testing.T) {
	ns := "app/diverge"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	localDoc := newUUID(t)
	remoteDoc := newUUID(t)
	localC := writeDoc(t, local, ns, genesis, localDoc, `{"v":"local"}`)
	remoteC := writeDoc(t, remote, ns, genesis, remoteDoc, `{"v":"remote"}`)
	mergeInto(t, local, ns, remoteC)

	if got := ResolveHeadUpdate(local.dag, localC.Hash, remoteC.Hash); got != HeadDiverged {
		t.Fatalf("expected HeadDiverged, got %v", got)
	}
}

// The core regression test for the flagged bug: disjoint documents on each side must
// auto-merge via a real two-parent commit, not silently orphan one side's history the way an
// unconditional SetHead did.
func TestResolveDivergenceMergesNonConflictingDisjointWrites(t *testing.T) {
	ns := "app/merge"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	localDoc := newUUID(t)
	remoteDoc := newUUID(t)
	localC := writeDoc(t, local, ns, genesis, localDoc, `{"v":"local"}`)
	remoteC := writeDoc(t, remote, ns, genesis, remoteDoc, `{"v":"remote"}`)
	mergeInto(t, local, ns, remoteC)

	outcome, err := ResolveDivergence(local.dag, local.storage, ns, localC.Hash, remoteC.Hash)
	if err != nil {
		t.Fatalf("ResolveDivergence: %v", err)
	}
	if outcome.Kind != OutcomeMerged {
		t.Fatalf("expected OutcomeMerged, got %v (report=%v)", outcome.Kind, outcome.Report)
	}
	if outcome.MergeCommit == nil {
		t.Fatal("expected a merge commit")
	}
	if len(outcome.MergeCommit.ParentHashes) != 2 {
		t.Fatalf("expected a two-parent merge commit, got %d parents", len(outcome.MergeCommit.ParentHashes))
	}
	if outcome.MergeCommit.ParentHashes[0] != localC.Hash || outcome.MergeCommit.ParentHashes[1] != remoteC.Hash {
		t.Fatalf("expected parents [local, remote], got %v", outcome.MergeCommit.ParentHashes)
	}
	head, _ := local.dag.Head()
	if head != outcome.MergeCommit.Hash {
		t.Fatalf("expected main to point at the merge commit, got %s", head.Hex())
	}
	// Neither side's document was dropped: the merge commit's own operations carry the delta it
	// introduces relative to its primary parent (regression: an empty operations list here would
	// silently drop the remote side's documents for a replay-based materializer).
	if len(outcome.MergeCommit.Operations) != 1 {
		t.Fatalf("expected the merge commit to carry the remote side's write, got %d ops", len(outcome.MergeCommit.Operations))
	}
}

// The other half of the regression: the SAME document written differently on each side must be
// reported as a real conflict, main must NOT move, and history must still not be lost (the
// commits are already in the DAG via mergeInto/PutCommit, independent of what head does).
func TestResolveDivergenceReportsConflictOnSameDocumentWrite(t *testing.T) {
	ns := "app/conflict"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	sharedDoc := newUUID(t)
	localC := writeDoc(t, local, ns, genesis, sharedDoc, `{"v":"local"}`)
	remoteC := writeDoc(t, remote, ns, genesis, sharedDoc, `{"v":"remote"}`)
	mergeInto(t, local, ns, remoteC)

	outcome, err := ResolveDivergence(local.dag, local.storage, ns, localC.Hash, remoteC.Hash)
	if err != nil {
		t.Fatalf("ResolveDivergence: %v", err)
	}
	if outcome.Kind != OutcomeConflict {
		t.Fatalf("expected OutcomeConflict, got %v", outcome.Kind)
	}
	if outcome.Report == nil || len(outcome.Report.Conflicts) != 1 {
		t.Fatalf("expected exactly one conflicting document, got %v", outcome.Report)
	}
	if outcome.Report.Conflicts[0].DocumentID != sharedDoc.String() {
		t.Fatalf("expected conflict on %s, got %s", sharedDoc.String(), outcome.Report.Conflicts[0].DocumentID)
	}
	if outcome.Report.Conflicts[0].OperationType != kdberr.ConcurrentWrite {
		t.Fatalf("expected CONCURRENT_WRITE, got %v", outcome.Report.Conflicts[0].OperationType)
	}
	// main must be left exactly where it was before the failed merge attempt.
	head, _ := local.dag.Head()
	if head != localC.Hash {
		t.Fatalf("expected main to stay at localC on conflict, got %s (wanted %s)", head.Hex(), localC.Hash.Hex())
	}
}

func TestResolveDivergenceClassifiesDeleteWriteConflict(t *testing.T) {
	ns := "app/delete-write"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	sharedDoc := newUUID(t)
	// Seed the document on both sides first so a delete has something to delete.
	seedC := writeDoc(t, local, ns, genesis, sharedDoc, `{"v":"seed"}`)
	mergeInto(t, remote, ns, seedC)

	// local deletes; remote writes - both starting from the seed commit.
	txID, _ := codec.RandomUUID()
	authorID, _ := codec.RandomUUID()
	if err := local.storage.DeleteDocument(ns, sharedDoc); err != nil {
		t.Fatalf("deleteDocument: %v", err)
	}
	tree, err := local.storage.CommitTree(ns, seedC.DocumentTreeHash)
	if err != nil {
		t.Fatalf("commitTree after delete: %v", err)
	}
	deleteTx := document.Transaction{
		ID: txID, BaseVersion: seedC.Hash,
		Operations:   []document.Op{document.DeleteOp{DocID: sharedDoc}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	localC, err := local.dag.AppendCommit(deleteTx, seedC.Hash, tree, nil, "test delete")
	if err != nil {
		t.Fatalf("appendCommit(delete): %v", err)
	}
	remoteC := writeDoc(t, remote, ns, seedC.Hash, sharedDoc, `{"v":"remote-write"}`)
	mergeInto(t, local, ns, remoteC)

	outcome, err := ResolveDivergence(local.dag, local.storage, ns, localC.Hash, remoteC.Hash)
	if err != nil {
		t.Fatalf("ResolveDivergence: %v", err)
	}
	if outcome.Kind != OutcomeConflict {
		t.Fatalf("expected OutcomeConflict, got %v", outcome.Kind)
	}
	if outcome.Report.Conflicts[0].OperationType != kdberr.DeleteWrite {
		t.Fatalf("expected DELETE_WRITE, got %v", outcome.Report.Conflicts[0].OperationType)
	}
}

func TestResolveDivergenceNoOpWhenLocalAlreadyAhead(t *testing.T) {
	ns := "app/noop"
	local, _ := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	docID := newUUID(t)
	c1 := writeDoc(t, local, ns, genesis, docID, `{"v":1}`)

	outcome, err := ResolveDivergence(local.dag, local.storage, ns, c1.Hash, genesis)
	if err != nil {
		t.Fatalf("ResolveDivergence: %v", err)
	}
	if outcome.Kind != OutcomeNoOp {
		t.Fatalf("expected OutcomeNoOp, got %v", outcome.Kind)
	}
	head, _ := local.dag.Head()
	if head != c1.Hash {
		t.Fatalf("expected head unchanged at c1, got %s", head.Hex())
	}
}
