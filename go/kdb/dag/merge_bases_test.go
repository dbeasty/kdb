package dag

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// mergeBasesByDefinition is MergeBases straight from its definition: the common ancestors that
// are not an ancestor of another common ancestor, by full closures.
func mergeBasesByDefinition(d *InMemoryCommitDag, as, bs []codec.Hash) []codec.Hash {
	setA := d.AncestorSetOf(as)
	setB := d.AncestorSetOf(bs)
	var common []codec.Hash
	for h := range setA {
		if _, ok := setB[h]; ok && d.HasCommit(h) {
			common = append(common, h)
		}
	}
	var out []codec.Hash
	for _, h := range common {
		below := false
		for _, o := range common {
			if o != h {
				if _, ok := d.AncestorSet(o)[h]; ok {
					below = true
					break
				}
			}
		}
		if !below {
			out = append(out, h)
		}
	}
	SortHashes(out)
	return out
}

func sameHashes(a, b []codec.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestMergeBasesOfACrissCrossAreBothNearestAncestors: X and Y each merged the other's previous
// tip, so both are nearest common ancestors of the two new tips. CommonAncestor returns one of
// them, and which depends on the argument order (#130, F1).
func TestMergeBasesOfACrissCrossAreBothNearestAncestors(t *testing.T) {
	for _, pruning := range []bool{true, false} {
		t.Run(fmt.Sprintf("pruning=%v", pruning), func(t *testing.T) {
			b := newDagBuilder(t)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: pruning})
			a := b.commit("a", b.root)
			x := b.commit("x", a)
			y := b.commit("y", a)
			m1 := b.merge("m1", x, y)
			m2 := b.merge("m2", y, x)
			t1 := b.commit("t1", m1)
			t2 := b.commit("t2", m2)
			want := []codec.Hash{x, y}
			SortHashes(want)
			for _, args := range [][2]codec.Hash{{t1, t2}, {t2, t1}} {
				got := b.dag.MergeBases([]codec.Hash{args[0]}, []codec.Hash{args[1]})
				if !sameHashes(got, want) {
					t.Fatalf("MergeBases(%s, %s) = %v, want %v", b.name(args[0]), b.name(args[1]), b.names_(got), b.names_(want))
				}
			}
			ca1, ca2 := b.dag.CommonAncestor(t1, t2), b.dag.CommonAncestor(t2, t1)
			if ca1 == nil || ca2 == nil || !containsHash(want, *ca1) || !containsHash(want, *ca2) {
				t.Fatalf("fixture: CommonAncestor should find one of the two bases")
			}
		})
	}
}

// TestMergeBasesPrefersTheNearestOverAnOlderCloserOne: the real merge base A is several hops from
// one head, while an older ancestor (the root's child R) is one hop away through a side merge.
// Breadth-first search finds R first; R is an ancestor of A, so it is not a nearest one.
func TestMergeBasesPrefersTheNearestOverAnOlderCloserOne(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	r := b.commit("r", b.root)
	a := b.commit("a", r)
	b1 := b.commit("b1", a)
	b2 := b.commit("b2", b1)
	b3 := b.commit("b3", b2)
	c1 := b.commit("c1", a)
	c2 := b.commit("c2", c1)
	c3 := b.commit("c3", c2)
	d := b.commit("d", r)
	h2 := b.merge("h2", c3, d)
	got := b.dag.MergeBases([]codec.Hash{b3}, []codec.Hash{h2})
	if !sameHashes(got, []codec.Hash{a}) {
		t.Fatalf("MergeBases = %v, want [a]", b.names_(got))
	}
	if ca := b.dag.CommonAncestor(b3, h2); ca == nil || *ca != r {
		t.Fatalf("fixture: CommonAncestor was expected to return the older r, got %v", ca)
	}
}

func TestMergeBasesEdgeCases(t *testing.T) {
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
	a := b.commit("a", b.root)
	c := b.commit("c", a)
	if got := b.dag.MergeBases([]codec.Hash{c}, []codec.Hash{c}); !sameHashes(got, []codec.Hash{c}) {
		t.Fatalf("a commit with itself: %v", b.names_(got))
	}
	if got := b.dag.MergeBases([]codec.Hash{a}, []codec.Hash{c}); !sameHashes(got, []codec.Hash{a}) {
		t.Fatalf("ancestor and descendant: %v", b.names_(got))
	}
	if got := b.dag.MergeBases([]codec.Hash{c}, []codec.Hash{unknownHash(t, 0x42)}); len(got) != 0 {
		t.Fatalf("with an unknown commit: %v", b.names_(got))
	}
}

// TestMergeBasesAgreesWithTheDefinition: random graphs of commits and merges, every pair (and
// some sets) of commits, the generation walk, the closure fallback and MergeBaseFinder (with and
// without the DAG's own generations) against the definition.
func TestMergeBasesAgreesWithTheDefinition(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		b := newDagBuilder(t)
		b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
		hs := []codec.Hash{b.root}
		for i := 0; i < 40; i++ {
			p := hs[rng.Intn(len(hs))]
			if rng.Intn(3) == 0 && len(hs) > 2 {
				q := hs[rng.Intn(len(hs))]
				if q != p {
					hs = append(hs, b.merge(fmt.Sprintf("m%d", i), p, q))
					continue
				}
			}
			hs = append(hs, b.commit(fmt.Sprintf("c%d", i), p))
		}
		// One finder of each kind across every trial, as a virtual base uses one across its levels.
		finder, finderPruned := b.dag.NewMergeBaseFinder(), b.dag.NewMergeBaseFinder()
		for trial := 0; trial < 60; trial++ {
			as := []codec.Hash{hs[rng.Intn(len(hs))]}
			bs := []codec.Hash{hs[rng.Intn(len(hs))]}
			if trial%4 == 0 {
				as = append(as, hs[rng.Intn(len(hs))])
			}
			want := mergeBasesByDefinition(b.dag, as, bs)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
			gotPruned := b.dag.MergeBases(as, bs)
			gotFinderPruned := finderPruned.MergeBases(as, bs)
			b.dag.SetGraphSettings(GraphSettings{})
			gotClosure := b.dag.MergeBases(as, bs)
			gotFinder := finder.MergeBases(as, bs)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
			if !sameHashes(gotPruned, want) || !sameHashes(gotClosure, want) || !sameHashes(gotFinder, want) || !sameHashes(gotFinderPruned, want) {
				t.Fatalf("seed %d: MergeBases(%v, %v): generation walk %v, closures %v, finder %v (pruning on: %v), want %v", seed,
					b.names_(as), b.names_(bs), b.names_(gotPruned), b.names_(gotClosure), b.names_(gotFinder), b.names_(gotFinderPruned), b.names_(want))
			}
		}
	}
}

// braid builds, on b's root and below it a linear history of prefix commits, the worst history a
// virtual merge base meets: three strands, each level's strands merging the three commits of the
// level below in their own order, so any two of a level have the three below as nearest common
// ancestors, whose own merge bases are the three below them, and so on down. Returns the top
// level.
func braid(b *dagBuilder, prefix, levels int) [3]codec.Hash {
	at := b.root
	for i := 0; i < prefix; i++ {
		at = b.commit(fmt.Sprintf("p%d", i), at)
	}
	lvl := [3]codec.Hash{b.commit("a0", at), b.commit("b0", at), b.commit("c0", at)}
	for l := 1; l <= levels; l++ {
		x, y, z := lvl[0], lvl[1], lvl[2]
		lvl = [3]codec.Hash{
			b.commit(fmt.Sprintf("a%d", l), b.merge(fmt.Sprintf("ma%d", l), b.merge(fmt.Sprintf("mab%d", l), x, y), z)),
			b.commit(fmt.Sprintf("b%d", l), b.merge(fmt.Sprintf("mb%d", l), b.merge(fmt.Sprintf("mac%d", l), x, z), y)),
			b.commit(fmt.Sprintf("c%d", l), b.merge(fmt.Sprintf("mc%d", l), b.merge(fmt.Sprintf("mbc%d", l), y, z), x)),
		}
	}
	return lvl
}

// foldBases asks find for merge bases the way peersync's virtualBase does - the bases of two
// heads, then, folding them in order, the merge bases of those folded so far with the next, and
// recursively of those - and returns how many questions it asked.
func foldBases(find func(as, bs []codec.Hash) []codec.Hash, bases []codec.Hash, seen map[string]bool) int {
	key := fmt.Sprint(bases)
	if len(bases) < 2 || seen[key] {
		return 0
	}
	seen[key] = true
	calls := 0
	for i := 1; i < len(bases); i++ {
		calls += 1 + foldBases(find, find(bases[:i], bases[i:i+1]), seen)
	}
	return calls
}

// TestMergeBaseFinderCostIsBoundedOnACrissCross is the cost the 5-node procedure-counter load
// test wedged a node on: without AncestryPruning, every MergeBases of a virtual base's recursion
// compared full ancestor closures - all of history, once per pair of candidates, at every level -
// inside the write gate. A finder numbers the history once and walks only from the commits asked
// about down to their bases, so the whole recursion costs about one pass over history however
// long the history below the criss-crosses is; plain MergeBases pays for that history on every
// call.
func TestMergeBaseFinderCostIsBoundedOnACrissCross(t *testing.T) {
	const levels = 12
	for _, prefix := range []int{500, 4000} {
		t.Run(fmt.Sprintf("prefix=%d", prefix), func(t *testing.T) {
			b := newDagBuilder(t)
			top := braid(b, prefix, levels)
			commits := uint64(prefix + 1 + 3 + levels*9)
			heads := b.dag.MergeBases([]codec.Hash{top[0]}, []codec.Hash{top[1]})
			if len(heads) != 3 {
				t.Fatalf("fixture: the top two strands have %d nearest common ancestors, want 3", len(heads))
			}

			before := b.dag.MergeBaseSteps()
			calls := foldBases(b.dag.MergeBases, heads, map[string]bool{})
			plain := b.dag.MergeBaseSteps() - before

			f := b.dag.NewMergeBaseFinder()
			before = b.dag.MergeBaseSteps()
			if got := foldBases(f.MergeBases, heads, map[string]bool{}); got != calls {
				t.Fatalf("the finder's recursion asked %d questions, plain MergeBases' %d: they answered differently", got, calls)
			}
			bounded := b.dag.MergeBaseSteps() - before

			t.Logf("%d merge-base questions over %d commits: %d steps plain, %d with a finder", calls, commits, plain, bounded)
			// Numbering visits each commit twice (down, then settling); each question then walks
			// from its commits to their bases - a few levels of the braid, never the prefix.
			if limit := 2*commits + uint64(calls)*50; bounded > limit {
				t.Fatalf("the finder took %d steps for %d questions over %d commits, want at most %d", bounded, calls, commits, limit)
			}
			if plain < uint64(calls)*uint64(prefix) {
				t.Fatalf("fixture: plain MergeBases took only %d steps for %d questions over a %d-commit prefix", plain, calls, prefix)
			}
		})
	}
}
