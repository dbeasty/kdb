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
// some sets) of commits, the generation walk and the closure fallback against the definition.
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
		for trial := 0; trial < 60; trial++ {
			as := []codec.Hash{hs[rng.Intn(len(hs))]}
			bs := []codec.Hash{hs[rng.Intn(len(hs))]}
			if trial%4 == 0 {
				as = append(as, hs[rng.Intn(len(hs))])
			}
			want := mergeBasesByDefinition(b.dag, as, bs)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
			gotPruned := b.dag.MergeBases(as, bs)
			b.dag.SetGraphSettings(GraphSettings{})
			gotClosure := b.dag.MergeBases(as, bs)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true})
			if !sameHashes(gotPruned, want) || !sameHashes(gotClosure, want) {
				t.Fatalf("seed %d: MergeBases(%v, %v): generation walk %v, closures %v, want %v", seed,
					b.names_(as), b.names_(bs), b.names_(gotPruned), b.names_(gotClosure), b.names_(want))
			}
		}
	}
}
