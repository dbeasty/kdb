package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// pairwiseMerge builds the content-free merge of p and q the way every node did before joins
// were canonical - the history a mesh that ran an older version holds.
func pairwiseMerge(t *testing.T, s side, ns string, p, q codec.Hash) document.Commit {
	t.Helper()
	p0, p1 := mergeCommitParents(p, q)
	pc, _ := s.dag.GetCommitOrThrow(p0)
	qc, _ := s.dag.GetCommitOrThrow(p1)
	ts := pc.Timestamp
	if qc.Timestamp.EpochMicros() > ts.EpochMicros() {
		ts = qc.Timestamp
	}
	tree, err := s.storage.CommitTree(ns, pc.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID: codec.DerivedUUID("kdb:merge/1:" + p0.Hex() + ":" + p1.Hex()), Timestamp: ts, AuthorNodeID: MergeAuthorNodeID,
	}
	m, err := s.dag.AppendMergeCommitOnto(nil, tx, p0, p1, tree, nil, mergeMessage)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func ingestEnv(s side, ns string) IngestEnv {
	return IngestEnv{DAG: s.dag, Storage: s.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution: ResolutionOptions{Policy: transaction.ConflictPolicyStrict}}
}

// TestJoinLooksThroughContentFreeMerges: three commits holding the same documents, joined
// pairwise two different ways - merge(a, b) on one node, merge(a, c) on another, as nodes
// running before canonical joins made them. Joining those two merges must not make a third
// pairwise merge but the one commit naming a, b and c; the node holding merge(a, c) must then
// adopt that commit rather than merge again; and nothing further is made either way after.
func TestJoinLooksThroughContentFreeMerges(t *testing.T) {
	ns := "app/join"
	a := newSide(t, ns)
	genesis, _ := a.dag.Head()
	doc := newUUID(t)
	// Three different commits, one tree: each writes the same value.
	var tips []codec.Hash
	for i := 0; i < 3; i++ {
		tips = append(tips, writeDoc(t, a, ns, genesis, doc, `{"v":1}`).Hash)
	}
	ab := pairwiseMerge(t, a, ns, tips[0], tips[1])
	ac := pairwiseMerge(t, a, ns, tips[0], tips[2])
	if !a.dag.IsContentFreeMerge(ab) || !a.dag.IsContentFreeMerge(ac) {
		t.Fatal("a merge of three same-tree commits should count as content-free")
	}

	// b follows merge(a, c).
	setHead(t, a, ac.Hash)
	b := newSide(t, ns)
	pullInto(t, ns, b, a, transaction.ConflictPolicyStrict)
	setHead(t, a, ab.Hash)

	res, err := Adopt(ingestEnv(a, ns), ac.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome.Kind != OutcomeMerged || res.Outcome.MergeCommit == nil {
		t.Fatalf("expected a join, got %v", res.Outcome.Kind)
	}
	join := *res.Outcome.MergeCommit
	want := append([]codec.Hash(nil), tips...)
	dag.SortHashes(want)
	if len(join.ParentHashes) != 3 || join.ParentHashes[0] != want[0] || join.ParentHashes[1] != want[1] || join.ParentHashes[2] != want[2] {
		t.Fatalf("the join names %v, want the three content-carrying commits %v", join.ParentHashes, want)
	}
	if !a.dag.IsContentFreeMerge(join) || len(join.Operations) != 0 {
		t.Fatal("the join should carry no content of its own")
	}
	if a.dag.IsAncestor(ab.Hash, join.Hash) || !a.dag.Incorporates(ab.Hash, join.Hash) || !a.dag.Incorporates(ac.Hash, join.Hash) {
		t.Fatal("the join should leave the pairwise merges off main's path while incorporating them")
	}

	// b, on merge(a, c), adopts the join as it is: no commit of its own.
	before := b.dag.CommitCount()
	r := pullInto(t, ns, b, a, transaction.ConflictPolicyStrict)
	if r.Head != join.Hash {
		t.Fatalf("b went to %s, want the join %s", r.Head.Hex(), join.Hash.Hex())
	}
	if got := b.dag.CommitCount(); got != before+2 { // merge(a, b) and the join, both a's
		t.Fatalf("b holds %d commits after adopting the join, want %d: it made one of its own", got, before+2)
	}
	// And from there on there is nothing to do, in either direction.
	for _, pair := range [][2]side{{a, b}, {b, a}} {
		before := pair[0].dag.CommitCount()
		r := pullInto(t, ns, pair[0], pair[1], transaction.ConflictPolicyStrict)
		if r.Outcome.Kind != OutcomeNoOp || pair[0].dag.CommitCount() != before {
			t.Fatalf("a settled pair did something: %v", r.Outcome.Kind)
		}
	}
	// An old head is incorporated, not re-merged.
	if r, err := Adopt(ingestEnv(a, ns), ab.Hash); err != nil || r.Outcome.Kind != OutcomeNoOp || r.Head != join.Hash {
		t.Fatalf("re-offering merge(a, b): %v %v at %s", err, r.Outcome.Kind, r.Head.Hex())
	}
}

// TestJoinOfTwoContentHeadsIsThePairwiseMerge: where the pairwise rule applied - two heads that
// carry content and hold the same documents - the join is the very commit it made, so nodes on
// either side of an upgrade agree.
func TestJoinOfTwoContentHeadsIsThePairwiseMerge(t *testing.T) {
	ns := "app/join-pair"
	a := newSide(t, ns)
	genesis, _ := a.dag.Head()
	doc := newUUID(t)
	x := writeDoc(t, a, ns, genesis, doc, `{"v":1}`)
	y := writeDoc(t, a, ns, genesis, doc, `{"v":1}`)
	setHead(t, a, x.Hash)
	res, err := Adopt(ingestEnv(a, ns), y.Hash)
	if err != nil || res.Outcome.MergeCommit == nil {
		t.Fatalf("join: %v %v", err, res.Outcome.Kind)
	}
	// The commit the pairwise rule built for x and y (mergeNonConflicting, with no operations
	// since the trees agree): hash-ordered parents, the pair's derived id, the merge author and
	// message, one microsecond past the later parent.
	p0, p1 := mergeCommitParents(x.Hash, y.Hash)
	ts := x.Timestamp.EpochMicros()
	if y.Timestamp.EpochMicros() > ts {
		ts = y.Timestamp.EpochMicros()
	}
	old, err := document.BuildCommit([]codec.Hash{p0, p1}, ns, codec.DerivedUUID("kdb:merge/1:"+p0.Hex()+":"+p1.Hex()),
		codec.TimestampFromEpochMicros(ts+1), MergeAuthorNodeID, nil, x.DocumentTreeHash, nil, mergeMessage)
	if err != nil {
		t.Fatal(err)
	}
	if old.Hash != res.Outcome.MergeCommit.Hash {
		t.Fatalf("the join of two content heads %s differs from their pairwise merge %s", res.Outcome.MergeCommit.Hash.Hex(), old.Hash.Hex())
	}
	if !strings.HasPrefix(res.Outcome.MergeCommit.Message, mergeMessage) {
		t.Fatalf("join message %q", res.Outcome.MergeCommit.Message)
	}
}
