package dag

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func emptyTree() document.DocumentTree { return document.EmptyDocumentTree() }

func documentTransaction(id codec.UUID, base codec.Hash, step int64, author codec.UUID) document.Transaction {
	return document.Transaction{
		ID:           id,
		BaseVersion:  base,
		Timestamp:    codec.TimestampFromEpochMicros(1_700_000_000_000_000 + step*1000),
		AuthorNodeID: author,
	}
}

// richGraph builds a shape with every feature the pruned walk has to cope
// with: a straight chain, a diamond, a merge whose second parent is far
// back, and a commit hung off a fork long after the branch moved on.
//
//	root - a - b - c ------- m1 - d - m2
//	        \                /         /
//	         e - f - g -----'         /
//	              \                  /
//	               h - i -----------'
func richGraph(b *dagBuilder) []codec.Hash {
	a := b.commit("a", b.root)
	c1 := b.commit("b", a)
	c2 := b.commit("c", c1)
	e := b.commit("e", a)
	f := b.commit("f", e)
	g := b.commit("g", f)
	m1 := b.merge("m1", c2, g)
	d := b.commit("d", m1)
	h := b.commit("h", f)
	i := b.commit("i", h)
	m2 := b.merge("m2", d, i)
	return []codec.Hash{b.root, a, c1, c2, e, f, g, m1, d, h, i, m2}
}

// ancestryMatrix records IsAncestor for every ordered pair, so two
// configurations can be compared as whole answers rather than spot checks.
func ancestryMatrix(d *InMemoryCommitDag, hashes []codec.Hash) map[[2]codec.Hash]bool {
	out := make(map[[2]codec.Hash]bool, len(hashes)*len(hashes))
	for _, x := range hashes {
		for _, y := range hashes {
			out[[2]codec.Hash{x, y}] = d.IsAncestor(x, y)
		}
	}
	return out
}

func compareMatrices(t *testing.T, b *dagBuilder, want, got map[[2]codec.Hash]bool, label string) {
	t.Helper()
	for pair, w := range want {
		if g := got[pair]; g != w {
			t.Errorf("%s: IsAncestor(%s, %s) = %v, unpruned says %v",
				label, b.name(pair[0]), b.name(pair[1]), g, w)
		}
	}
}

// TestPrunedAncestryAgreesWithTheClosure is the differential test the whole
// generation-number scheme rests on. The pruned walk is an optimization and
// nothing more: for any pair, on any shape, it has to give the answer the
// closure gives. A generation that is wrong in the low direction shows up
// here as a walk that stopped early and missed a real ancestor.
func TestPrunedAncestryAgreesWithTheClosure(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	hashes := richGraph(b)
	// Hashes the graph has never seen, both as candidate and as target -
	// the closure treats a start it does not know as its own ancestor, and
	// the pruned walk has to do the same.
	hashes = append(hashes, unknownHash(t, 0xab), unknownHash(t, 0xcd))

	derivedIncrementally := ancestryMatrix(b.dag, hashes)

	b.dag.SetGraphSettings(GraphSettings{})
	if b.dag.GraphSettings().AncestryPruning {
		t.Fatal("pruning did not turn off")
	}
	unpruned := ancestryMatrix(b.dag, hashes)

	// Back on, which recomputes every generation from the finished graph
	// rather than deriving them commit by commit as they arrived.
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	recomputed := ancestryMatrix(b.dag, hashes)

	compareMatrices(t, b, unpruned, derivedIncrementally, "derived as commits arrived")
	compareMatrices(t, b, unpruned, recomputed, "recomputed from the finished graph")

	// A shape with no real ancestry relationships would pass all of the
	// above vacuously.
	trues := 0
	for _, v := range unpruned {
		if v {
			trues++
		}
	}
	if trues < len(hashes) {
		t.Fatalf("fixture is too weak: only %d true pairs across %d hashes", trues, len(hashes))
	}
}

// TestPrunedAncestryAgreesAfterStubbing pins the reasoning in
// ensureGenerationsLocked: taking commits away leaves generations larger
// than they need to be, which prunes less but must never answer wrongly.
func TestPrunedAncestryAgreesAfterStubbing(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	hashes := richGraph(b)

	// "h", a commit on a side branch that no branch head names.
	stubbed := hashes[9]
	if _, err := b.dag.StubCommit(stubbed, "archive://test"); err != nil {
		t.Fatalf("stub: %v", err)
	}

	pruned := ancestryMatrix(b.dag, hashes)
	b.dag.SetGraphSettings(GraphSettings{})
	unpruned := ancestryMatrix(b.dag, hashes)
	compareMatrices(t, b, unpruned, pruned, "after stubbing")
}

// TestGenerationsSurviveOutOfOrderAdmission covers the case that makes
// incremental derivation unsound on its own: RestoreCheckpoint admits
// commits in map order, so a child routinely arrives before its parent and
// is given a generation lower than the parent's.
func TestGenerationsSurviveOutOfOrderAdmission(t *testing.T) {
	source := newDagBuilder(t)
	hashes := richGraph(source)
	state := source.dag.CheckpointSnapshot()

	restored, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	restored.SetGraphSettings(GraphSettings{AncestryPruning: true})
	if err := restored.RestoreCheckpoint(state); err != nil {
		t.Fatalf("restore: %v", err)
	}

	pruned := ancestryMatrix(restored, hashes)
	restored.SetGraphSettings(GraphSettings{})
	unpruned := ancestryMatrix(restored, hashes)
	compareMatrices(t, source, unpruned, pruned, "restored from a checkpoint")
}

// TestGenerationIsOneAboveTheHighestParent checks the invariant directly,
// rather than only through the answers it produces.
func TestGenerationIsOneAboveTheHighestParent(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	hashes := richGraph(b)

	b.dag.mu.RLock()
	defer b.dag.mu.RUnlock()
	for _, h := range hashes {
		n, ok := b.dag.nodes[h]
		if !ok {
			t.Fatalf("%s is not resident", b.name(h))
		}
		var want uint32
		for _, p := range n.commit.ParentHashes {
			if pn, ok := b.dag.nodes[p]; ok && pn.generation > want {
				want = pn.generation
			}
		}
		want++
		if n.generation != want {
			t.Errorf("%s: generation %d, want %d", b.name(h), n.generation, want)
		}
		for _, p := range n.commit.ParentHashes {
			if pn, ok := b.dag.nodes[p]; ok && pn.generation >= n.generation {
				t.Errorf("%s (generation %d) does not exceed its parent %s (%d)",
					b.name(h), n.generation, b.name(p), pn.generation)
			}
		}
	}
}

// TestPruningOffLeavesGenerationsUnset guards the claim in GraphSettings
// that the zero value costs nothing: with pruning off, no generation is
// derived at all, so the flag is not paying for work it never uses.
func TestPruningOffLeavesGenerationsUnset(t *testing.T) {
	b := newDagBuilder(t)
	hashes := richGraph(b)

	b.dag.mu.RLock()
	defer b.dag.mu.RUnlock()
	for _, h := range hashes {
		if n, ok := b.dag.nodes[h]; ok && n.generation != 0 {
			t.Fatalf("%s has generation %d with pruning off", b.name(h), n.generation)
		}
	}
}

func TestGraphSettingsNormalization(t *testing.T) {
	cases := []struct {
		name string
		in   GraphSettings
		want GraphSettings
	}{
		{
			name: "zero value turns nothing on",
			in:   GraphSettings{},
			want: GraphSettings{},
		},
		{
			// The file stores generation numbers in its records; mapping it
			// and then not pruning with them would be pure cost.
			name: "the file implies pruning and a rebuild threshold",
			in:   GraphSettings{FileEnabled: true},
			want: GraphSettings{
				FileEnabled:     true,
				AncestryPruning: true,
				RebuildCommits:  DefaultGraphRebuildCommits,
			},
		},
		{
			name: "an explicit rebuild threshold is kept",
			in:   GraphSettings{FileEnabled: true, RebuildCommits: 7},
			want: GraphSettings{FileEnabled: true, AncestryPruning: true, RebuildCommits: 7},
		},
		{
			// Nothing to rebuild, so reporting a threshold back would only
			// mislead whoever read it.
			name: "a rebuild threshold without the file is dropped",
			in:   GraphSettings{RebuildCommits: 7},
			want: GraphSettings{},
		},
		{
			name: "pruning alone stays alone",
			in:   GraphSettings{AncestryPruning: true},
			want: GraphSettings{AncestryPruning: true},
		},
		{
			name: "a negative anchor interval means no anchors",
			in:   GraphSettings{AnchorInterval: -1},
			want: GraphSettings{},
		},
		{
			name: "an anchor interval is kept without the file",
			in:   GraphSettings{AnchorInterval: 500},
			want: GraphSettings{AnchorInterval: 500},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.normalized(); got != tc.want {
				t.Errorf("normalized() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestGraphFileIsNotActiveYet states the current truth plainly: the flag
// can be set, and nothing maps a file, because the format is not written.
// It should fail - and be replaced - the moment Phase 2 lands.
func TestGraphFileIsNotActiveYet(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true})
	if !b.dag.GraphSettings().FileEnabled {
		t.Fatal("the setting did not stick")
	}
	if b.dag.GraphFileActive() {
		t.Fatal("a graph file is reported active, but none is implemented")
	}
	if !b.dag.GraphSettings().AncestryPruning {
		t.Fatal("the file did not imply ancestry pruning")
	}
}

// benchmarkIsAncestor measures the shape the plan is aimed at: one commit
// tested against a descendant far down a long history, which is what
// peer-sync and retention checks do repeatedly.
//
// The unpruned path builds the descendant's entire ancestor closure - a map
// proportional to the whole history - and then tests one membership, so its
// cost is a function of history length no matter how near the answer is.
func benchmarkIsAncestor(b *testing.B, depth int, pruning, deep bool) {
	d, err := NewInMemoryCommitDag("bench")
	if err != nil {
		b.Fatal(err)
	}
	d.SetGraphSettings(GraphSettings{AncestryPruning: pruning})
	head, err := d.Head()
	if err != nil {
		b.Fatal(err)
	}
	var near codec.Hash
	root := head
	for i := 0; i < depth; i++ {
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		tx := documentTransaction(txID, head, int64(i), author)
		c, err := d.AppendCommitDetached(tx, head, emptyTree(), nil, "c")
		if err != nil {
			b.Fatal(err)
		}
		head = c.Hash
		if i == depth-2 {
			near = c.Hash
		}
	}
	target := near
	if deep {
		// The worst case, and the one a benchmark of only the near case
		// would flatter: nothing between the root and the head can be
		// pruned, because every commit on the way down is above the root's
		// generation. Pruning cannot beat the walk here - it can only
		// avoid being worse than it.
		target = root
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !d.IsAncestor(target, head) {
			b.Fatal("expected an ancestor")
		}
	}
}

// Near: the ancestor is two commits back, which is the common shape.
func BenchmarkIsAncestorNearUnpruned1k(b *testing.B)  { benchmarkIsAncestor(b, 1000, false, false) }
func BenchmarkIsAncestorNearPruned1k(b *testing.B)    { benchmarkIsAncestor(b, 1000, true, false) }
func BenchmarkIsAncestorNearUnpruned10k(b *testing.B) { benchmarkIsAncestor(b, 10_000, false, false) }
func BenchmarkIsAncestorNearPruned10k(b *testing.B)   { benchmarkIsAncestor(b, 10_000, true, false) }

// Deep: the ancestor is the root, so there is nothing to prune and both
// paths walk the whole history. Here to keep the near numbers honest.
func BenchmarkIsAncestorDeepUnpruned10k(b *testing.B) { benchmarkIsAncestor(b, 10_000, false, true) }
func BenchmarkIsAncestorDeepPruned10k(b *testing.B)   { benchmarkIsAncestor(b, 10_000, true, true) }
