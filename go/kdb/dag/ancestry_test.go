package dag

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// ancestry.go - CommitsSince, CommonAncestor, IsAncestor, AncestorSet, AppendMergeCommit - had
// no test of its own, despite being what peer-sync uses to decide which commits to send and
// which merge base to resolve a conflict against. Walk's own truncation bug (fixed after the
// 3-node e2e scenario found it live) came from this same area.

// dagBuilder builds commit graphs with explicit shapes. Timestamps advance by a fixed step per
// commit so Walk's timestamp-ordered frontier is deterministic, and so a test that deliberately
// wants an out-of-order timestamp can say so.
type dagBuilder struct {
	t      *testing.T
	dag    *InMemoryCommitDag
	root   codec.Hash
	micros int64
	names  map[codec.Hash]string
}

func newDagBuilder(t *testing.T) *dagBuilder {
	t.Helper()
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	root, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	b := &dagBuilder{
		t:      t,
		dag:    d,
		root:   root,
		micros: 1_700_000_000_000_000,
		names:  map[codec.Hash]string{root: "root"},
	}
	return b
}

func (b *dagBuilder) tx(parent codec.Hash) document.Transaction {
	b.t.Helper()
	txID, err := codec.RandomUUID()
	if err != nil {
		b.t.Fatal(err)
	}
	author, err := codec.RandomUUID()
	if err != nil {
		b.t.Fatal(err)
	}
	b.micros += 1000
	return document.Transaction{
		ID:           txID,
		BaseVersion:  parent,
		Timestamp:    codec.TimestampFromEpochMicros(b.micros),
		AuthorNodeID: author,
	}
}

// commit appends a single-parent commit and records it under name for readable failures.
// Detached: these tests build graphs of an explicit shape - diamonds, side branches, commits
// hung off a fork point long after the branch moved past it - so "the parent is not the current
// head" is the fixture, not a lost race. AppendCommit's compare-and-swap belongs to callers
// extending the tip.
func (b *dagBuilder) commit(name string, parent codec.Hash) codec.Hash {
	b.t.Helper()
	c, err := b.dag.AppendCommitDetached(b.tx(parent), parent, document.EmptyDocumentTree(), nil, name)
	if err != nil {
		b.t.Fatalf("commit %s: %v", name, err)
	}
	b.names[c.Hash] = name
	return c.Hash
}

// merge appends a two-parent commit.
func (b *dagBuilder) merge(name string, primary, merged codec.Hash) codec.Hash {
	b.t.Helper()
	c, err := b.dag.AppendMergeCommitOnto(
		nil, b.tx(primary), primary, merged, document.EmptyDocumentTree(), nil, name)
	if err != nil {
		b.t.Fatalf("merge %s: %v", name, err)
	}
	b.names[c.Hash] = name
	return c.Hash
}

func (b *dagBuilder) name(h codec.Hash) string {
	if n, ok := b.names[h]; ok {
		return n
	}
	return h.Hex()[:8]
}

func (b *dagBuilder) names_(hs []codec.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = b.name(h)
	}
	return out
}

func (b *dagBuilder) setNames(set map[codec.Hash]struct{}) []string {
	out := make([]string, 0, len(set))
	for h := range set {
		out = append(out, b.name(h))
	}
	return out
}

// diamond builds root -> a -> {b, c} -> m, the smallest graph with a genuine merge:
//
//	  a
//	 / \
//	b   c
//	 \ /
//	  m
func (b *dagBuilder) diamond() (a, left, right, m codec.Hash) {
	b.t.Helper()
	a = b.commit("a", b.root)
	left = b.commit("b", a)
	right = b.commit("c", a)
	m = b.merge("m", left, right)
	return
}

// unknownHash builds a well-formed hash that no commit in a test dag can have, for the
// "the peer named something we have never seen" cases.
func unknownHash(t *testing.T, fill byte) codec.Hash {
	t.Helper()
	h, err := codec.HashFromHex(strings.Repeat(fmt.Sprintf("%02x", fill), 32))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func hasHash(set map[codec.Hash]struct{}, h codec.Hash) bool {
	_, ok := set[h]
	return ok
}

func containsHash(hs []codec.Hash, want codec.Hash) bool {
	for _, h := range hs {
		if h == want {
			return true
		}
	}
	return false
}

func TestAncestorSetIncludesSelfAndAllParents(t *testing.T) {
	b := newDagBuilder(t)
	a, left, right, m := b.diamond()

	set := b.dag.AncestorSet(m)
	for _, want := range []codec.Hash{m, left, right, a, b.root} {
		if !hasHash(set, want) {
			t.Errorf("ancestor set of m is missing %s: got %v", b.name(want), b.setNames(set))
		}
	}
	if len(set) != 5 {
		t.Errorf("ancestor set of m has %d entries, want 5: %v", len(set), b.setNames(set))
	}

	// A commit on one side of the diamond must not reach the other side.
	leftSet := b.dag.AncestorSet(left)
	if hasHash(leftSet, right) {
		t.Errorf("ancestor set of b reached c: %v", b.setNames(leftSet))
	}
	if !hasHash(leftSet, a) || !hasHash(leftSet, left) {
		t.Errorf("ancestor set of b is missing itself or a: %v", b.setNames(leftSet))
	}
}

func TestIsAncestorAcrossAMerge(t *testing.T) {
	b := newDagBuilder(t)
	a, left, right, m := b.diamond()

	// Both sides of a merge are ancestors of it - the property a merge exists to create, and
	// the one whose absence made peer-sync push a merge commit ahead of one of its parents.
	for _, anc := range []codec.Hash{a, left, right, b.root} {
		if !b.dag.IsAncestor(anc, m) {
			t.Errorf("%s should be an ancestor of m", b.name(anc))
		}
	}
	// A commit is its own ancestor (the closure includes its start), and the relation is not
	// symmetric.
	if !b.dag.IsAncestor(m, m) {
		t.Error("a commit should be its own ancestor")
	}
	if b.dag.IsAncestor(m, a) {
		t.Error("m must not be an ancestor of a")
	}
	if b.dag.IsAncestor(left, right) || b.dag.IsAncestor(right, left) {
		t.Error("the two sides of a diamond must not be ancestors of each other")
	}
}

func TestIsAncestorAgreesWithAncestorSet(t *testing.T) {
	b := newDagBuilder(t)
	a, left, right, m := b.diamond()
	extra := b.commit("d", m)

	// AncestorSet exists so callers can test many candidates without rebuilding the closure
	// each time; the two must not be able to disagree.
	all := []codec.Hash{b.root, a, left, right, m, extra}
	for _, descendant := range all {
		set := b.dag.AncestorSet(descendant)
		for _, candidate := range all {
			if got, want := hasHash(set, candidate), b.dag.IsAncestor(candidate, descendant); got != want {
				t.Errorf("IsAncestor(%s, %s) = %v but AncestorSet says %v",
					b.name(candidate), b.name(descendant), want, got)
			}
		}
	}
}

func TestCommonAncestorOfDiamondIsTheFork(t *testing.T) {
	b := newDagBuilder(t)
	a, left, right, _ := b.diamond()

	got := b.dag.CommonAncestor(left, right)
	if got == nil {
		t.Fatal("no common ancestor found for the two sides of a diamond")
	}
	if *got != a {
		t.Fatalf("common ancestor is %s, want a", b.name(*got))
	}
}

func TestCommonAncestorPrefersTheNearestFork(t *testing.T) {
	b := newDagBuilder(t)
	// root -> a -> x -> {p, q}: the fork at x, not the older a, is the answer.
	a := b.commit("a", b.root)
	x := b.commit("x", a)
	p := b.commit("p", x)
	q := b.commit("q", x)

	got := b.dag.CommonAncestor(p, q)
	if got == nil {
		t.Fatal("no common ancestor found")
	}
	if *got != x {
		t.Fatalf("common ancestor is %s, want the nearer fork x", b.name(*got))
	}
}

func TestCommonAncestorOfACommitAndItselfIsThatCommit(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	got := b.dag.CommonAncestor(a, a)
	if got == nil || *got != a {
		t.Fatalf("common ancestor of a with itself is %v, want a", got)
	}
}

func TestCommonAncestorOfAncestorAndDescendantIsTheAncestor(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	c := b.commit("c", a)

	// Both argument orders must give the same answer - the merge base of a linear pair is the
	// older of the two, whichever way round it is asked.
	if got := b.dag.CommonAncestor(a, c); got == nil || *got != a {
		t.Fatalf("CommonAncestor(a, c) = %v, want a", got)
	}
	if got := b.dag.CommonAncestor(c, a); got == nil || *got != a {
		t.Fatalf("CommonAncestor(c, a) = %v, want a", got)
	}
}

func TestCommonAncestorOfDisjointHistoriesIsNil(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)

	// A hash the dag has never heard of shares no history with anything in it.
	if got := b.dag.CommonAncestor(a, unknownHash(t, 0xab)); got != nil {
		t.Fatalf("expected no common ancestor with an unknown hash, got %s", b.name(*got))
	}
}

func TestCommitsSinceExcludesWhatThePeerAlreadyHas(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	c := b.commit("c", a)
	d := b.commit("d", c)

	// The peer is known to have everything up to c: only d is new.
	got := b.dag.CommitsSince(d, map[codec.Hash]struct{}{c: {}})
	if len(got) != 1 || got[0] != d {
		t.Fatalf("CommitsSince(d, exclude c) = %v, want [d]", b.names_(got))
	}

	// Excluding nothing yields the whole reachable history, including the root.
	all := b.dag.CommitsSince(d, nil)
	for _, want := range []codec.Hash{b.root, a, c, d} {
		if !containsHash(all, want) {
			t.Errorf("CommitsSince(d, nothing) is missing %s: %v", b.name(want), b.names_(all))
		}
	}

	// Excluding the head itself yields nothing left to send.
	if got := b.dag.CommitsSince(d, map[codec.Hash]struct{}{d: {}}); len(got) != 0 {
		t.Fatalf("CommitsSince(d, exclude d) = %v, want empty", b.names_(got))
	}
}

// The case that matters for peer-sync correctness: with a merge in the history, excluding one
// side must not drop the other side's commits, which are genuinely still new to the peer.
func TestCommitsSinceKeepsTheOtherSideOfAMerge(t *testing.T) {
	b := newDagBuilder(t)
	_, left, right, m := b.diamond()

	got := b.dag.CommitsSince(m, map[codec.Hash]struct{}{left: {}})
	if !containsHash(got, right) {
		t.Errorf("excluding one side of the merge dropped the other side (c): %v", b.names_(got))
	}
	if !containsHash(got, m) {
		t.Errorf("the merge commit itself was dropped: %v", b.names_(got))
	}
	if containsHash(got, left) {
		t.Errorf("the excluded side (b) came back: %v", b.names_(got))
	}
}

func TestCommitsSinceIsDeterministicallyOrdered(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	c := b.commit("c", a)

	// The result is built by ranging a map, so it is sorted before returning - without that the
	// order would vary run to run and any consumer comparing two peers' lists would see
	// spurious differences.
	first := b.dag.CommitsSince(c, nil)
	for i := 0; i < 20; i++ {
		again := b.dag.CommitsSince(c, nil)
		if len(again) != len(first) {
			t.Fatalf("length changed between calls: %d then %d", len(first), len(again))
		}
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("order changed between calls at %d: %v then %v",
					j, b.names_(first), b.names_(again))
			}
		}
	}
	for i := 1; i < len(first); i++ {
		if first[i-1].Hex() >= first[i].Hex() {
			t.Fatalf("not sorted ascending by hex at %d: %v", i, b.names_(first))
		}
	}
}

func TestAppendMergeCommitRecordsBothParentsAndMovesHead(t *testing.T) {
	b := newDagBuilder(t)
	_, left, right, m := b.diamond()

	commit, ok := b.dag.GetCommit(m)
	if !ok {
		t.Fatal("merge commit was not stored")
	}
	if len(commit.ParentHashes) != 2 {
		t.Fatalf("merge commit has %d parents, want 2", len(commit.ParentHashes))
	}
	if commit.ParentHashes[0] != left || commit.ParentHashes[1] != right {
		t.Fatalf("parents are %v, want [b c] in that order", b.names_(commit.ParentHashes))
	}
	head, err := b.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != m {
		t.Fatalf("head is %s after the merge, want m", b.name(head))
	}
}

func TestAppendMergeCommitRejectsAnUnknownParent(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	if _, err := b.dag.AppendMergeCommit(
		b.tx(a), a, unknownHash(t, 0xcd), document.EmptyDocumentTree(), nil, "bad merge",
	); err == nil {
		t.Fatal("a merge against an absent parent was accepted")
	}
}

// Walk's output has to be closed under parents when nothing truncated it: a consumer that
// treats the result as "the commits to send" needs every referenced parent to be in the batch
// too. This is the exact property whose violation made a peer reject a pushed merge commit for
// a missing parent.
func TestWalkOutputIsClosedUnderParents(t *testing.T) {
	b := newDagBuilder(t)
	_, _, _, m := b.diamond()
	tip := b.commit("d", m)

	entries := b.dag.Walk(tip, nil, 1000)
	present := make(map[codec.Hash]struct{}, len(entries))
	var commits []document.Commit
	for _, e := range entries {
		if full, ok := e.(FullEntry); ok {
			present[full.Commit.Hash] = struct{}{}
			commits = append(commits, full.Commit)
		}
	}
	if len(commits) == 0 {
		t.Fatal("walk returned no full commits")
	}
	for _, c := range commits {
		for _, p := range c.ParentHashes {
			if p == b.root {
				continue // the synthetic root has no commit body of its own
			}
			if _, ok := present[p]; !ok {
				t.Errorf("walk emitted %s but not its parent %s: %v",
					b.name(c.Hash), b.name(p), b.names_(hashesOf(entries)))
			}
		}
	}
}

// `until` prunes the branch it appears on, and must not abort the traversal: a sibling branch
// still on the frontier has to keep being walked.
func TestWalkUntilPrunesOnlyItsOwnBranch(t *testing.T) {
	b := newDagBuilder(t)
	_, left, right, m := b.diamond()

	entries := b.dag.Walk(m, &left, 1000)
	got := hashesOf(entries)
	if containsHash(got, left) {
		t.Errorf("the until commit itself was emitted: %v", b.names_(got))
	}
	if !containsHash(got, right) {
		t.Errorf("pruning one side of the merge dropped the other side: %v", b.names_(got))
	}
	if !containsHash(got, m) {
		t.Errorf("the starting merge commit was dropped: %v", b.names_(got))
	}
}

func TestWalkRespectsLimit(t *testing.T) {
	b := newDagBuilder(t)
	h := b.root
	for i := 0; i < 10; i++ {
		h = b.commit(string(rune('a'+i)), h)
	}
	for _, limit := range []int{0, 1, 3, 10} {
		if got := len(b.dag.Walk(h, nil, limit)); got > limit {
			t.Errorf("limit %d: walk returned %d entries", limit, got)
		}
	}
}

func hashesOf(entries []TraversalEntry) []codec.Hash {
	out := make([]codec.Hash, 0, len(entries))
	for _, e := range entries {
		switch v := e.(type) {
		case FullEntry:
			out = append(out, v.Commit.Hash)
		case StubbedEntry:
			out = append(out, v.Stub.OriginalHash)
		}
	}
	return out
}
