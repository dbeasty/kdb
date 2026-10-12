package peersync

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Version 2 chains (ChainVersionMergeBases): #130's F1, F2 and F4 at the merge layer. Every test
// here builds one commit graph in one DAG and merges pairs of its commits directly, so the merge
// order - the thing under test - is chosen exactly.

type graphT struct {
	t   *testing.T
	s   side
	ns  string
	doc codec.UUID
	// sets records, per write commit, the fields it set and to what: the ground truth a
	// last-writer-wins map is checked against.
	sets map[codec.Hash]map[string]any
}

func newGraph(t *testing.T, ns string) *graphT {
	return &graphT{t: t, s: newSide(t, ns), ns: ns, doc: newUUID(t), sets: map[codec.Hash]map[string]any{}}
}

func (g *graphT) root() codec.Hash {
	h, _ := g.s.dag.Head()
	return h
}

func (g *graphT) body(at codec.Hash) map[string]any {
	g.t.Helper()
	c, err := g.s.dag.GetCommitOrThrow(at)
	if err != nil {
		g.t.Fatal(err)
	}
	d, err := g.s.storage.GetDocument(g.ns, g.doc, c.DocumentTreeHash)
	if err != nil {
		g.t.Fatal(err)
	}
	if d == nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(d.JSON), &m); err != nil {
		g.t.Fatal(err)
	}
	return m
}

// write patches fields onto the document as it is at parent, in a new commit on parent.
func (g *graphT) write(parent codec.Hash, fields map[string]any) codec.Hash {
	g.t.Helper()
	cur := g.body(parent)
	if cur == nil {
		cur = map[string]any{}
	}
	for k, v := range fields {
		cur[k] = v
	}
	b, _ := json.Marshal(cur)
	c := writeDocBy(g.t, g.s, g.ns, parent, g.doc, string(b), newUUID(g.t))
	g.sets[c.Hash] = fields
	return c.Hash
}

// merge merges b into a (a is the local head), returning the resulting head.
func (g *graphT) merge(a, b codec.Hash, opts ResolutionOptions) (codec.Hash, CommitPushOutcome) {
	g.t.Helper()
	setHead(g.t, g.s, a)
	out, err := ResolveDivergence(g.s.dag, g.s.storage, g.ns, a, b, opts)
	if err != nil {
		g.t.Fatalf("merge: %v", err)
	}
	h, _ := g.s.dag.Head()
	return h, out
}

func (g *graphT) mustMerge(a, b codec.Hash, opts ResolutionOptions) codec.Hash {
	g.t.Helper()
	h, out := g.merge(a, b, opts)
	if out.Kind == OutcomeConflict {
		g.t.Fatalf("merge left a conflict: %+v", out.Details)
	}
	return h
}

// lww is what a last-writer-wins map makes of every write g recorded: each field at the value of
// its latest write, by commit timestamp then hash - the order last-write uses.
func (g *graphT) lww() map[string]any {
	latest := map[string]codec.Hash{}
	for c, fields := range g.sets {
		for k := range fields {
			if o, ok := latest[k]; !ok || originLater(g.s.dag, c, o) {
				latest[k] = c
			}
		}
	}
	out := map[string]any{}
	for k, c := range latest {
		// Through JSON, so numbers compare as the merged document's do.
		b, _ := json.Marshal(g.sets[c][k])
		var v any
		_ = json.Unmarshal(b, &v)
		out[k] = v
	}
	return out
}

func chainV(version int, rules ...ResolutionRule) ResolutionOptions {
	return ResolutionOptions{Chain: &ResolutionChain{Version: version, Rules: rules}}
}

var fieldMergeLastWrite = []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleLastWrite}}

// The F1 shape of test_field_merge_against_the_true_merge_base (3 nodes): since A (a=1) one side
// changed a and b, the other c and e, both carry d - nothing collides. The heads have two nearest
// common ancestors (A's side and D's side), and the base version 1 picks makes field-merge see a
// collision and fall to whole-document last-write.
func TestV2FieldMergeKeepsEveryChangeSinceTheTrueBase(t *testing.T) {
	run := func(version int) map[string]any {
		g := newGraph(t, fmt.Sprintf("app/v2-f1-%d", version))
		opts := chainV(version, fieldMergeLastWrite...)
		base := g.write(g.root(), map[string]any{"a": 0, "b": 0, "c": 0, "d": 0, "e": 0})
		a := g.write(base, map[string]any{"a": 1})
		d := g.write(base, map[string]any{"d": 1})
		b1 := g.write(a, map[string]any{"b": 1})
		b2 := g.write(b1, map[string]any{"a": 2})
		c1 := g.write(a, map[string]any{"c": 1})
		c2 := g.write(c1, map[string]any{"c": 2})
		h1 := g.mustMerge(b2, d, opts)
		e := g.write(d, map[string]any{"e": 1})
		h2 := g.mustMerge(c2, e, opts)
		return g.body(g.mustMerge(h1, h2, opts))
	}
	want := map[string]any{"a": 2.0, "b": 1.0, "c": 2.0, "d": 1.0, "e": 1.0}
	if got := run(ChainVersionMergeBases); !reflect.DeepEqual(got, want) {
		t.Fatalf("version 2 lost a field change: got %v, want %v", got, want)
	}
	// Version 1 keeps its behaviour, defect included: the fixture is the defect.
	if got := run(0); reflect.DeepEqual(got, want) {
		t.Fatalf("fixture too weak: version 1 got it right too (%v)", got)
	}
}

// F2: n0 then n1 write x, n2 writes c. merge(merge(C,A),B) and merge(merge(A,C),B) and
// merge(merge(B,C),A) must agree, on x from n1 - the later write.
func TestV2FieldMergeThenLastWriteIsOrderIndependent(t *testing.T) {
	g := newGraph(t, "app/v2-f2")
	opts := chainV(ChainVersionMergeBases, fieldMergeLastWrite...)
	base := g.write(g.root(), map[string]any{"x": "base", "c": 0})
	a := g.write(base, map[string]any{"x": "n0"})
	b := g.write(base, map[string]any{"x": "n1"})
	c := g.write(base, map[string]any{"c": 2})
	want := map[string]any{"x": "n1", "c": 2.0}
	for name, got := range map[string]map[string]any{
		"merge(merge(C,A),B)": g.body(g.mustMerge(b, g.mustMerge(c, a, opts), opts)),
		"merge(merge(A,C),B)": g.body(g.mustMerge(g.mustMerge(a, c, opts), b, opts)),
		"merge(merge(B,C),A)": g.body(g.mustMerge(g.mustMerge(b, c, opts), a, opts)),
	} {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestV2FieldMergeLastWriteIsAssociativeAndCommutative is the property test: 3-5 heads with
// random overlapping field writes, some of them having merged each other already (criss-crosses),
// reduced to one head in many random orders. Every order must give the same document, and it must
// be the last-writer-wins map of all the writes.
func TestV2FieldMergeLastWriteIsAssociativeAndCommutative(t *testing.T) {
	fields := []string{"w", "x", "y", "z"}
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		g := newGraph(t, fmt.Sprintf("app/v2-prop-%d", seed))
		opts := chainV(ChainVersionMergeBases, fieldMergeLastWrite...)
		init := map[string]any{}
		for _, f := range fields {
			init[f] = "base"
		}
		base := g.write(g.root(), init)
		k := 3 + rng.Intn(3)
		heads := make([]codec.Hash, k)
		for i := range heads {
			heads[i] = base
		}
		step := 0
		for round := 0; round < 6; round++ {
			// Some writes...
			for i := range heads {
				if rng.Intn(3) == 0 {
					continue
				}
				set := map[string]any{}
				for _, f := range fields {
					if rng.Intn(2) == 0 {
						set[f] = fmt.Sprintf("n%d-%d", i, step)
					}
				}
				step++
				if len(set) > 0 {
					heads[i] = g.write(heads[i], set)
				}
			}
			// ...and some pairwise syncs, both nodes ending at the merge: criss-crosses.
			if rng.Intn(2) == 0 {
				i, j := rng.Intn(k), rng.Intn(k)
				if i != j {
					m := g.mustMerge(heads[i], heads[j], opts)
					heads[i], heads[j] = m, m
				}
			}
		}
		want := g.lww()
		var first map[string]any
		for order := 0; order < 8; order++ {
			pool := append([]codec.Hash(nil), heads...)
			var trace []string
			for len(pool) > 1 {
				i := rng.Intn(len(pool))
				j := rng.Intn(len(pool) - 1)
				if j >= i {
					j++
				}
				m := g.mustMerge(pool[i], pool[j], opts)
				trace = append(trace, fmt.Sprintf("(%d,%d)", i, j))
				if i > j {
					i, j = j, i
				}
				pool = append(pool[:j], pool[j+1:]...)
				pool[i] = m
			}
			got := g.body(pool[0])
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d, order %s: got %v, want the last-writer-wins map %v", seed, strings.Join(trace, ""), got, want)
			}
			if first == nil {
				first = got
			}
		}
	}
}

// sumProcedure is the textbook three-way counter merge, local + remote - base, which is right
// exactly when the base is the true merge base.
func sumProcedure(_, _ string, c ProcedureConflict) (Settlement, bool, error) {
	n := func(s *string) float64 {
		if s == nil {
			return 0
		}
		var m map[string]float64
		_ = json.Unmarshal([]byte(*s), &m)
		return m["n"]
	}
	b := fmt.Sprintf(`{"n":%d}`, int64(n(c.Side0)+n(c.Side1)-n(c.Base)))
	return Settlement{Body: &b}, true, nil
}

// F1 with a criss-cross: the counter test of test_cluster_resolution.py. Each increment owns a
// decimal digit; the converged total must count each exactly once. With several nearest common
// ancestors, version 1 hands the procedure one of them and counts some increments twice; version
// 2 hands it the virtual base.
func TestV2ProcedureSeesTheVirtualBaseOfACrissCross(t *testing.T) {
	run := func(version int) float64 {
		g := newGraph(t, fmt.Sprintf("app/v2-sum-%d", version))
		opts := chainV(version, ResolutionRule{Kind: RuleProcedure, Name: "sum"}, ResolutionRule{Kind: RuleLastWrite})
		opts.Procedure = sumProcedure
		base := g.write(g.root(), map[string]any{"n": 0})
		inc := func(at codec.Hash, by int) codec.Hash {
			cur := g.body(at)["n"].(float64)
			return g.write(at, map[string]any{"n": int(cur) + by})
		}
		a, b, c := inc(base, 1), inc(base, 10), inc(base, 100)
		n0 := g.mustMerge(a, b, opts)  // A+B
		n1 := g.mustMerge(b, c, opts)  // B+C
		n2 := g.mustMerge(c, n0, opts) // C+(A+B)
		n0 = inc(n0, 1000)
		n1 = inc(n1, 10000)
		m := g.mustMerge(n0, n1, opts)
		m = g.mustMerge(m, n2, opts)
		return g.body(m)["n"].(float64)
	}
	if got := run(ChainVersionMergeBases); got != 11111 {
		t.Fatalf("version 2 counted the increments as %v, want 11111 (each once)", got)
	}
	if got := run(0); got == 11111 {
		t.Fatalf("fixture too weak: version 1 counted right too")
	}
}

// F4: a version 2 chain with validity holds rather than settling by the next rule on a node with
// no schema, and binds the schema into the hash peers compare. Version 1 is unchanged.
func TestV2ValidityNeedsTheSchema(t *testing.T) {
	rules := []ResolutionRule{{Kind: RuleValidity}, {Kind: RuleLastWrite}}
	valid := func(body string) bool { return !strings.Contains(body, "bad") }

	f := newChainFixture(t, "app/v2-validity", `{"v":0}`, `{"v":2}`, `{"v":"bad"}`)
	noSchema := chainV(ChainVersionMergeBases, rules...)
	noSchema.Valid = valid
	out, _ := f.resolveBothWays(t, noSchema)
	if out.Kind != OutcomeConflict || len(out.Details) != 1 || !strings.Contains(out.Details[0].Reason, "no schema") {
		t.Fatalf("expected a held conflict naming the missing schema, got %v %+v", out.Kind, out.Details)
	}

	withSchema := noSchema
	withSchema.SchemaHash = func() string { return "s1" }
	f2 := newChainFixture(t, "app/v2-validity-2", `{"v":0}`, `{"v":2}`, `{"v":"bad"}`)
	if _, got := f2.resolveBothWays(t, withSchema); got != `{"v":2}` {
		t.Fatalf("with the schema the valid value must win, got %s", got)
	}

	// The schema is part of what peers must agree on - under version 2 only.
	other := withSchema
	other.SchemaHash = func() string { return "s2" }
	if withSchema.AdvertisedHash() == other.AdvertisedHash() {
		t.Fatal("two nodes judging by different schemas advertise the same hash")
	}
	if noSchema.AdvertisedHash() == withSchema.AdvertisedHash() {
		t.Fatal("a node without the schema advertises what a node with it does")
	}
	v1a, v1b := chainV(0, rules...), chainV(0, rules...)
	v1a.SchemaHash, v1b.SchemaHash = func() string { return "s1" }, func() string { return "s2" }
	if v1a.AdvertisedHash() != v1b.AdvertisedHash() || v1a.AdvertisedHash() != v1a.Chain.Hash() {
		t.Fatal("a version 1 chain's advertised hash changed")
	}
}

// The gate itself: version 1 hashes as before the field existed (and "version": 1 is the same
// chain), version 2 is a different chain, and a version this release does not run is refused
// at set time, advertised as unsupported, and holds every conflict with the reason.
func TestChainVersionGate(t *testing.T) {
	rules := []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleLastWrite}}
	legacy := &ResolutionChain{Rules: rules}
	b, _ := json.Marshal(legacy)
	if strings.Contains(string(b), "version") {
		t.Fatalf("a version 1 chain serializes a version field: %s", b)
	}
	one := &ResolutionChain{Rules: rules, Version: 1}
	one.Normalize()
	if one.Hash() != legacy.Hash() {
		t.Fatal("version 1 and no version hash differently")
	}
	two := &ResolutionChain{Rules: rules, Version: ChainVersionMergeBases}
	if two.Hash() == legacy.Hash() {
		t.Fatal("version 2 hashes like version 1, so mixed nodes would merge differently and agree")
	}
	var parsed ResolutionChain
	if err := json.Unmarshal([]byte(`{"rules":[{"kind":"last-write"}],"version":2}`), &parsed); err != nil || parsed.Version != 2 {
		t.Fatalf("parse: %v %+v", err, parsed)
	}

	future := ResolutionChain{Rules: rules, Version: MaxChainVersion + 1}
	if err := future.Validate(); err == nil {
		t.Fatal("a chain version this release does not run was accepted")
	}
	if err := future.ValidateRules(); err != nil {
		t.Fatalf("its rules are fine: %v", err)
	}
	opts := ResolutionOptions{Chain: &future}
	if h := opts.AdvertisedHash(); !strings.HasPrefix(h, "unsupported:") {
		t.Fatalf("advertised %q for an unsupported chain", h)
	}
	f := newChainFixture(t, "app/chain-future", `{"a":0}`, `{"a":1}`, `{"a":2}`)
	out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: &future, Policy: transaction.ConflictPolicyLastWrite})
	if out.Kind != OutcomeConflict || len(out.Details) != 1 || !strings.Contains(out.Details[0].Reason, "newer than this node runs") {
		t.Fatalf("an unsupported chain must hold with its reason, not fall to the policy: %v %+v", out.Kind, out.Details)
	}
}

// fieldOrigin follows a field through the writes that left it alone and through merges.
func TestFieldOriginFollowsTheFieldNotTheDocument(t *testing.T) {
	g := newGraph(t, "app/v2-field-origin")
	base := g.write(g.root(), map[string]any{"x": 0, "y": 0})
	setX := g.write(base, map[string]any{"x": 1})
	setY := g.write(setX, map[string]any{"y": 1}) // the document's writer, not x's
	other := g.write(base, map[string]any{"y": 2})
	m := g.mustMerge(setY, other, chainV(ChainVersionMergeBases, fieldMergeLastWrite...))
	vi := valueIndex{d: g.s.dag, store: g.s.storage, ns: g.ns}
	memo := map[fieldKey]codec.Hash{}
	for _, tc := range []struct {
		at    codec.Hash
		field string
		want  codec.Hash
	}{
		{setY, "x", setX}, {setY, "y", setY}, {m, "x", setX}, {other, "x", base},
	} {
		got, err := vi.fieldOrigin(tc.at, g.doc, tc.field, memo)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("origin of %s: got %s, want %s", tc.field, got.Hex()[:10], tc.want.Hex()[:10])
		}
	}
	_ = document.Commit{}
}

// A mesh that merges constantly leaves a history full of criss-crosses, where the nearest common
// ancestors of two heads are themselves several and are merged into a virtual base one level at a
// time. Built without sharing, that recursion re-derived the same base sets once per path that
// reached them - exponential in the depth - and ran the conflict rule at every step, all while the
// merge held the node's write gate: a 5-node load test on Linux left one node unable to take
// writes or peer syncs for minutes. The work must stay proportional to the history, and the
// answer must still count every increment once.
func TestV2VirtualBaseWorkStaysBoundedOnADeepCrissCrossLattice(t *testing.T) {
	const nodes, rounds = 5, 5
	g := newGraph(t, "app/v2-lattice")
	calls := 0
	opts := chainV(ChainVersionMergeBases, ResolutionRule{Kind: RuleProcedure, Name: "sum"}, ResolutionRule{Kind: RuleLastWrite})
	opts.Procedure = func(a, b string, c ProcedureConflict) (Settlement, bool, error) {
		calls++
		return sumProcedure(a, b, c)
	}
	base := g.write(g.root(), map[string]any{"n": 0})
	heads := make([]codec.Hash, nodes)
	for i := range heads {
		heads[i] = base
	}
	want := 0
	for r := 0; r < rounds; r++ {
		// Every node counts one increment of its own, each in its own decimal digit.
		for i := range heads {
			cur := g.body(heads[i])["n"].(float64)
			by := 1
			for k := 0; k < i; k++ {
				by *= 10
			}
			heads[i] = g.write(heads[i], map[string]any{"n": int(cur) + by})
			want += by
		}
		// ...then every node syncs with all the others, from the heads they had before this
		// round's merges: any two heads end up with several nearest common ancestors.
		prev := append([]codec.Hash(nil), heads...)
		for i := range heads {
			for k := 1; k < nodes; k++ {
				heads[i] = g.mustMerge(heads[i], prev[(i+k)%nodes], opts)
			}
		}
	}
	t.Logf("heads 0,1 have %d nearest common ancestors", len(g.s.dag.MergeBases([]codec.Hash{heads[0]}, []codec.Hash{heads[1]})))
	final := heads[0]
	for i := 1; i < nodes; i++ {
		final = g.mustMerge(final, heads[i], opts)
	}
	got := g.body(final)["n"].(float64)
	if int(got) != want {
		t.Fatalf("converged counter %v, want %d (every increment once)", got, want)
	}
	// Generous: proportional to the history, nowhere near the exponential count.
	if limit := nodes * nodes * rounds * 40; calls > limit {
		t.Fatalf("building the lattice ran the conflict rule %d times (limit %d): virtual base work is not bounded", calls, limit)
	}
	t.Logf("the whole lattice ran the conflict rule %d times over %d nodes x %d rounds", calls, nodes, rounds)
}
