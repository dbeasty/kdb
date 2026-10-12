package peersync

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// perNodeCounterProcedure is the documented per-node counter (test_cluster_resolution.py's
// PER_NODE_COUNTER_PROC) in Go: each node raises only its own key of counts, a merge takes the
// per-key max. calls counts how often it ran.
func perNodeCounterProcedure(calls *int) func(string, string, ProcedureConflict) (Settlement, bool, error) {
	return func(_, _ string, c ProcedureConflict) (Settlement, bool, error) {
		*calls++
		if c.Side0 == nil || c.Side1 == nil {
			return Settlement{}, false, nil
		}
		counts := func(s *string) map[string]float64 {
			var m struct {
				Counts map[string]float64 `json:"counts"`
			}
			_ = json.Unmarshal([]byte(*s), &m)
			return m.Counts
		}
		out := counts(c.Side0)
		if out == nil {
			out = map[string]float64{}
		}
		for k, v := range counts(c.Side1) {
			if v > out[k] {
				out[k] = v
			}
		}
		b, _ := json.Marshal(map[string]any{"counts": out})
		s := string(b)
		return Settlement{Body: &s}, true, nil
	}
}

// TestV2PerNodeCounterMeshConverges runs the documented per-node counter (#133) under a version 2
// chain on five nodes that increment their own key and sync in rounds, every node with a random
// peer, all planned from the same moment, so the merges cross. No merge may run the procedure
// more than a bounded number of times or be held, and the mesh must converge on every key holding
// its node's increments. TestV2VirtualBaseOfABraidIsLinear below is the worst case built exactly.
func TestV2PerNodeCounterMeshConverges(t *testing.T) {
	const size, budget = 5, 500
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ns := fmt.Sprintf("app/v2-counter-mesh-%d", seed)
			rng := rand.New(rand.NewSource(seed))
			calls, worst := 0, 0
			opts := chainV(ChainVersionMergeBases, ResolutionRule{Kind: RuleProcedure, Name: "perNodeCounter"}, ResolutionRule{Kind: RuleLastWrite})
			opts.Procedure = perNodeCounterProcedure(&calls)
			nodes := make([]side, size)
			for i := range nodes {
				nodes[i] = newSide(t, ns)
			}
			doc := newUUID(t)
			made := make([]int, size)
			key := func(i int) string { return fmt.Sprintf("n%d", i) }

			// pullAt is dst taking src's history as of srcHead: a sync planned at an earlier moment.
			pullAt := func(dst, src int, srcHead codec.Hash) {
				cs, stubs, err := MissingCommits(nodes[src].dag, srcHead, spreadAncestorsAll(nodes[dst]), 1<<30)
				if err != nil {
					t.Fatal(err)
				}
				env := IngestEnv{DAG: nodes[dst].dag, Storage: nodes[dst].storage, NamespaceID: ns, ApplyToStorage: true, Resolution: opts}
				if _, err := StoreCommits(env, cs, stubs); err != nil {
					t.Fatal(err)
				}
				calls = 0
				res, err := Adopt(env, srcHead)
				if err != nil {
					t.Fatalf("n%d pulling n%d: %v", dst, src, err)
				}
				if calls > worst {
					worst = calls
				}
				if calls > budget {
					t.Fatalf("n%d pulling n%d ran the procedure %d times for one document (budget %d): the virtual base is rebuilt once per path to it", dst, src, calls, budget)
				}
				if res.Outcome.Kind == OutcomeConflict {
					t.Fatalf("n%d pulling n%d held the merge: %+v", dst, src, res.Outcome.Details)
				}
			}
			pull := func(dst, src int) {
				h, _ := nodes[src].dag.Head()
				pullAt(dst, src, h)
			}
			read := func(i int) map[string]float64 {
				head, _ := nodes[i].dag.Head()
				c, _ := nodes[i].dag.GetCommitOrThrow(head)
				d, _ := nodes[i].storage.GetDocument(ns, doc, c.DocumentTreeHash)
				if d == nil {
					return map[string]float64{}
				}
				var m struct {
					Counts map[string]float64 `json:"counts"`
				}
				_ = json.Unmarshal([]byte(d.JSON), &m)
				if m.Counts == nil {
					m.Counts = map[string]float64{}
				}
				return m.Counts
			}
			increment := func(i int) {
				counts := read(i)
				counts[key(i)]++
				made[i]++
				b, _ := json.Marshal(map[string]any{"counts": counts})
				writeAtHead(t, nodes[i], ns, doc, string(b))
			}

			// One shared starting point, then the load: increments and random pulls interleaved.
			increment(0)
			for i := 1; i < size; i++ {
				pull(i, 0)
			}
			// Rounds as a live mesh has them: some nodes increment, then every node syncs with a
			// random peer, all planned from the same moment - so the merges cross.
			for round := 0; round < 40; round++ {
				for i := 0; i < size; i++ {
					for k := rng.Intn(3); k > 0; k-- {
						increment(i)
					}
				}
				snap := make([]codec.Hash, size)
				for i, n := range nodes {
					snap[i], _ = n.dag.Head()
				}
				for dst := 0; dst < size; dst++ {
					src := (dst + 1 + rng.Intn(size-1)) % size
					pullAt(dst, src, snap[src])
				}
			}
			// Quiesce: all-pairs rounds until every node holds one head.
			for round := 0; ; round++ {
				if round > 10 {
					t.Fatalf("no convergence after %d all-pairs rounds", round)
				}
				for i := 0; i < size; i++ {
					for j := 0; j < size; j++ {
						if i != j {
							pull(i, j)
						}
					}
				}
				hs := map[codec.Hash]bool{}
				for _, n := range nodes {
					h, _ := n.dag.Head()
					hs[h] = true
				}
				if len(hs) == 1 {
					break
				}
			}
			want := map[string]float64{}
			for i, m := range made {
				if m > 0 {
					want[key(i)] = float64(m)
				}
			}
			if got := read(0); !reflect.DeepEqual(got, want) {
				t.Fatalf("converged on %v, want %v", got, want)
			}
			t.Logf("worst merge ran the procedure %d times", worst)
		})
	}
}

// TestVirtualBaseMemoMatchesTheUnmemoizedFold checks the memo changes nothing but the cost: on a
// criss-cross small enough to fold without it, the virtual base built with and without the memo is
// the same value from the same origin, and holds the same documents.
func TestVirtualBaseMemoMatchesTheUnmemoizedFold(t *testing.T) {
	g := newGraph(t, "app/v2-memo")
	calls := 0
	opts := chainV(ChainVersionMergeBases, ResolutionRule{Kind: RuleProcedure, Name: "perNodeCounter"}, ResolutionRule{Kind: RuleLastWrite})
	opts.Procedure = perNodeCounterProcedure(&calls)
	rng := rand.New(rand.NewSource(7))
	inc := func(at codec.Hash, k string) codec.Hash {
		counts, _ := g.body(at)["counts"].(map[string]any)
		next := map[string]any{}
		for kk, v := range counts {
			next[kk] = v
		}
		n, _ := next[k].(float64)
		next[k] = n + 1
		return g.write(at, map[string]any{"counts": next})
	}
	pool := []codec.Hash{g.write(g.root(), map[string]any{"counts": map[string]any{}})}
	for step := 0; step < 60; step++ {
		a := pool[rng.Intn(len(pool))]
		if rng.Intn(2) == 0 || len(pool) < 2 {
			pool = append(pool, inc(a, fmt.Sprintf("k%d", rng.Intn(4))))
			continue
		}
		b := pool[rng.Intn(len(pool))]
		if a == b {
			continue
		}
		h, out := g.merge(a, b, opts)
		if out.Kind != OutcomeConflict {
			pool = append(pool, h)
		}
	}
	vi := valueIndex{d: g.s.dag, store: g.s.storage, ns: g.ns}
	ids := []codec.UUID{g.doc}
	compared := 0
	for i := 0; i < len(pool); i++ {
		for j := i + 1; j < len(pool); j++ {
			bases := g.s.dag.MergeBases([]codec.Hash{pool[i]}, []codec.Hash{pool[j]})
			if len(bases) < 2 {
				continue
			}
			memo := &virtualBase{d: g.s.dag, vi: vi, opts: opts, held: map[codec.UUID]string{}}
			plain := &virtualBase{d: g.s.dag, vi: vi, opts: opts, held: map[codec.UUID]string{}, noMemo: true}
			got, err := memo.values(bases, ids, 0)
			if err != nil {
				t.Fatal(err)
			}
			want, err := plain.values(bases, ids, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !sameBody(got[g.doc].body, want[g.doc].body) || got[g.doc].origin != want[g.doc].origin || !reflect.DeepEqual(memo.held, plain.held) {
				t.Fatalf("memoized virtual base of %s, %s differs: %v@%s vs %v@%s",
					pool[i].Hex()[:8], pool[j].Hex()[:8], deref(got[g.doc].body), got[g.doc].origin.Hex()[:8], deref(want[g.doc].body), want[g.doc].origin.Hex()[:8])
			}
			compared++
		}
	}
	if compared == 0 {
		t.Fatal("fixture too weak: no pair had several nearest common ancestors")
	}
	t.Logf("compared %d virtual bases", compared)
}

// TestV2VirtualBaseOfABraidIsLinear is the nightly failure of
// test_load_procedure_per_node_counter[5-*] (v0.7.0, run 38197566122) at the merge layer, with its
// history shape built exactly: three strands, and at every level each strand merges the level
// below's three commits in its own pairing order before its own increment. Any two commits of a
// level then have the three below as nearest common ancestors, whose own merge bases are the three
// below them, and so on down. virtualBase folded them recursively without remembering any level,
// so it rebuilt the same inner bases - and reran the procedure over them - once per path to them:
// 2^(levels+1)-1 times for one document (32,767 here). On the mesh that passed the server's
// per-sync budget of 10,000 procedure calls, so the merge was held; the history being fixed, every
// retry on every node held it again, forever, and each retry ran the script for seconds inside the
// write gate, timing out the node's other syncs.
func TestV2VirtualBaseOfABraidIsLinear(t *testing.T) {
	const levels, budget = 14, 500
	g := newGraph(t, "app/v2-braid")
	calls := 0
	v2 := chainV(ChainVersionMergeBases, ResolutionRule{Kind: RuleProcedure, Name: "perNodeCounter"}, ResolutionRule{Kind: RuleLastWrite})
	v2.Procedure = perNodeCounterProcedure(&calls)
	// The braid itself is built under version 1, which hands a rule one base and so costs nothing
	// to build; only the merge under test runs version 2.
	v1 := chainV(0, ResolutionRule{Kind: RuleProcedure, Name: "perNodeCounter"}, ResolutionRule{Kind: RuleLastWrite})
	v1.Procedure = perNodeCounterProcedure(new(int))
	inc := func(at codec.Hash, k string) codec.Hash {
		counts, _ := g.body(at)["counts"].(map[string]any)
		next := map[string]any{}
		for kk, v := range counts {
			next[kk] = v
		}
		n, _ := next[k].(float64)
		next[k] = n + 1
		return g.write(at, map[string]any{"counts": next})
	}
	root := g.write(g.root(), map[string]any{"counts": map[string]any{}})
	lvl := [3]codec.Hash{inc(root, "a"), inc(root, "b"), inc(root, "c")}
	for l := 0; l < levels; l++ {
		a, b, c := lvl[0], lvl[1], lvl[2]
		lvl = [3]codec.Hash{
			inc(g.mustMerge(g.mustMerge(a, b, v1), c, v1), "a"),
			inc(g.mustMerge(g.mustMerge(a, c, v1), b, v1), "b"),
			inc(g.mustMerge(g.mustMerge(b, c, v1), a, v1), "c"),
		}
	}
	if bases := g.s.dag.MergeBases([]codec.Hash{lvl[0]}, []codec.Hash{lvl[1]}); len(bases) != 3 {
		t.Fatalf("fixture: the top two strands have %d nearest common ancestors, want 3", len(bases))
	}
	_, out := g.merge(lvl[0], lvl[1], v2)
	if calls > budget {
		t.Fatalf("one merge of one document ran the procedure %d times (budget %d): the virtual base is rebuilt once per path to it", calls, budget)
	}
	if out.Kind == OutcomeConflict {
		t.Fatalf("the merge was held: %+v", out.Details)
	}
	t.Logf("the merge ran the procedure %d times", calls)
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
