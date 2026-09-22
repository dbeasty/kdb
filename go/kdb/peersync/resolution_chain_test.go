package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// writeDocBy is writeDoc with a chosen author node, for rules that look at who wrote a value.
func writeDocBy(t *testing.T, s side, ns string, parent codec.Hash, docID codec.UUID, json string, author codec.UUID) document.Commit {
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
	tx := document.Transaction{
		ID: newUUID(t), BaseVersion: parent,
		Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: json}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
	commit, err := s.dag.AppendCommitDetached(tx, parent, tree, nil, "test write")
	if err != nil {
		t.Fatalf("appendCommit: %v", err)
	}
	return commit
}

// chainFixture is one document written at a shared base, then changed on each side.
type chainFixture struct {
	ns              string
	local, remote   side
	doc             codec.UUID
	localC, remoteC document.Commit
	localNode       codec.UUID
	remoteNode      codec.UUID
}

func newChainFixture(t *testing.T, ns, base, localBody, remoteBody string) chainFixture {
	t.Helper()
	f := chainFixture{ns: ns, doc: newUUID(t), localNode: newUUID(t), remoteNode: newUUID(t)}
	f.local, f.remote = forkTwoSides(t, ns)
	parent, _ := f.local.dag.Head()
	if base != "" {
		baseC := writeDocBy(t, f.local, ns, parent, f.doc, base, f.localNode)
		mergeInto(t, f.remote, ns, baseC)
		parent = baseC.Hash
	}
	f.localC = writeDocBy(t, f.local, ns, parent, f.doc, localBody, f.localNode)
	f.remoteC = writeDocBy(t, f.remote, ns, parent, f.doc, remoteBody, f.remoteNode)
	mergeInto(t, f.local, ns, f.remoteC)
	mergeInto(t, f.remote, ns, f.localC)
	return f
}

// resolveBothWays resolves the divergence on each side, as each node would on receiving the
// other's head, and requires them to build the same merge.
func (f chainFixture) resolveBothWays(t *testing.T, opts ResolutionOptions) (CommitPushOutcome, string) {
	t.Helper()
	outL, err := ResolveDivergence(f.local.dag, f.local.storage, f.ns, f.localC.Hash, f.remoteC.Hash, opts)
	if err != nil {
		t.Fatalf("resolve on local: %v", err)
	}
	outR, err := ResolveDivergence(f.remote.dag, f.remote.storage, f.ns, f.remoteC.Hash, f.localC.Hash, opts)
	if err != nil {
		t.Fatalf("resolve on remote: %v", err)
	}
	if outL.Kind != outR.Kind {
		t.Fatalf("the two nodes disagree on the outcome: local %v, remote %v", outL.Kind, outR.Kind)
	}
	if outL.Kind != OutcomeMerged {
		return outL, ""
	}
	if outL.MergeCommit.Hash != outR.MergeCommit.Hash {
		t.Fatalf("the two nodes built different merges: %s vs %s", outL.MergeCommit.Hash.Hex(), outR.MergeCommit.Hash.Hex())
	}
	doc, err := f.local.storage.GetDocument(f.ns, f.doc, outL.MergeCommit.DocumentTreeHash)
	if err != nil || doc == nil {
		t.Fatalf("merged document: %v %v", doc, err)
	}
	return outL, doc.JSON
}

func TestChainSourcePriorityPrefersTheRankedNodeFromEitherSide(t *testing.T) {
	f := newChainFixture(t, "app/chain-source", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority, Nodes: []string{f.remoteNode.String()}}}}
	_, got := f.resolveBothWays(t, ResolutionOptions{Chain: chain})
	if got != `{"v":"remote"}` {
		t.Fatalf("expected the ranked node's value, got %s", got)
	}
}

func TestChainSourcePriorityWithNeitherNodeRankedFallsThrough(t *testing.T) {
	f := newChainFixture(t, "app/chain-source-unranked", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority, Nodes: []string{newUUID(t).String()}}}}
	out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: chain})
	if out.Kind != OutcomeConflict {
		t.Fatalf("expected an undecided conflict under the strict policy, got %v", out.Kind)
	}
}

func TestChainFieldMergeCombinesDifferentFields(t *testing.T) {
	f := newChainFixture(t, "app/chain-fields", `{"a":1,"b":1,"c":1}`, `{"a":2,"b":1,"c":1}`, `{"a":1,"b":3}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}}}
	_, got := f.resolveBothWays(t, ResolutionOptions{Chain: chain})
	if got != `{"a":2,"b":3}` {
		t.Fatalf("expected a from local, b from remote and c removed, got %s", got)
	}
}

func TestChainFieldMergeOnTheSameFieldFallsToTheNextRule(t *testing.T) {
	f := newChainFixture(t, "app/chain-fields-clash", `{"a":1}`, `{"a":2}`, `{"a":3}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleSourcePriority, Nodes: []string{f.localNode.String()}}}}
	_, got := f.resolveBothWays(t, ResolutionOptions{Chain: chain})
	if got != `{"a":2}` {
		t.Fatalf("expected source priority to settle what field merge could not, got %s", got)
	}
}

func TestChainFieldMergeWithoutABaseMergesDisjointFields(t *testing.T) {
	f := newChainFixture(t, "app/chain-fields-nobase", "", `{"a":1}`, `{"b":2}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}}}
	_, got := f.resolveBothWays(t, ResolutionOptions{Chain: chain})
	if got != `{"a":1,"b":2}` && got != `{"b":2,"a":1}` {
		t.Fatalf("expected both fields, got %s", got)
	}
}

func TestChainValidityPrefersTheValidSide(t *testing.T) {
	f := newChainFixture(t, "app/chain-valid", `{"name":"x"}`, `{"name":"y"}`, `{"nick":"z"}`)
	valid := func(body string) bool { return strings.Contains(body, `"name"`) }
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleValidity}}}
	_, got := f.resolveBothWays(t, ResolutionOptions{Chain: chain, Valid: valid})
	if got != `{"name":"y"}` {
		t.Fatalf("expected the side that passes validation, got %s", got)
	}
}

func TestChainQueueReportsEvenUnderLastWrite(t *testing.T) {
	f := newChainFixture(t, "app/chain-queue", `{"a":1}`, `{"a":2}`, `{"a":3}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleQueue}}}
	out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: chain, Policy: transaction.ConflictPolicyLastWrite})
	if out.Kind != OutcomeConflict || len(out.Report.Conflicts) != 1 {
		t.Fatalf("expected the queue rule to report the conflict, got %v %+v", out.Kind, out.Report)
	}
}

func TestChainLeavingADocumentUndecidedFallsToThePolicy(t *testing.T) {
	f := newChainFixture(t, "app/chain-policy", `{"a":1}`, `{"a":2}`, `{"a":3}`)
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}}}
	out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: chain, Policy: transaction.ConflictPolicyLastWrite})
	if out.Kind != OutcomeMerged {
		t.Fatalf("expected last-write to settle what the chain left, got %v", out.Kind)
	}
}

// An operator's choice for one document stands while the chain settles the others, and the
// report names only what is still undecided.
func TestChooseDecidesPerDocumentAlongsideTheChain(t *testing.T) {
	ns := "app/chain-choose"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	a, b, c := newUUID(t), newUUID(t), newUUID(t)
	ln, rn := newUUID(t), newUUID(t)
	l1 := writeDocBy(t, local, ns, genesis, a, `{"x":"la"}`, ln)
	l2 := writeDocBy(t, local, ns, l1.Hash, b, `{"x":1,"y":1}`, ln)
	l3 := writeDocBy(t, local, ns, l2.Hash, c, `{"x":"lc"}`, ln)
	r1 := writeDocBy(t, remote, ns, genesis, a, `{"x":"ra"}`, rn)
	r2 := writeDocBy(t, remote, ns, r1.Hash, b, `{"y":1,"z":1}`, rn)
	r3 := writeDocBy(t, remote, ns, r2.Hash, c, `{"x":"rc"}`, rn)
	for _, cm := range []document.Commit{r1, r2, r3} {
		mergeInto(t, local, ns, cm)
	}
	chain := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleQueue}}}
	choose := func(id codec.UUID, l, r document.Op) (document.Op, bool) {
		if id == a {
			return r, true
		}
		return nil, false
	}
	out, err := ResolveDivergence(local.dag, local.storage, ns, l3.Hash, r3.Hash, ResolutionOptions{Chain: chain, Choose: choose})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != OutcomeConflict || len(out.Report.Conflicts) != 1 || out.Report.Conflicts[0].DocumentID != c.String() {
		t.Fatalf("expected only document c to be left, got %v %+v", out.Kind, out.Report)
	}
	choose2 := func(id codec.UUID, l, r document.Op) (document.Op, bool) {
		switch id {
		case a:
			return r, true
		case c:
			return l, true
		}
		return nil, false
	}
	out, err = ResolveDivergence(local.dag, local.storage, ns, l3.Hash, r3.Hash, ResolutionOptions{Chain: chain, Choose: choose2})
	if err != nil || out.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v %v", out.Kind, err)
	}
	// b has no common base - each side added it - so each side's own fields count as added and
	// all three survive. Key order follows the merge's first parent, which is chosen by hash.
	want := map[codec.UUID][]string{
		a: {`{"x":"ra"}`},
		b: {`{"x":1,"y":1,"z":1}`, `{"y":1,"z":1,"x":1}`},
		c: {`{"x":"lc"}`},
	}
	for id, bodies := range want {
		doc, err := local.storage.GetDocument(ns, id, out.MergeCommit.DocumentTreeHash)
		if err != nil || doc == nil || !contains(bodies, doc.JSON) {
			t.Fatalf("document %s: want one of %v, got %+v (%v)", id, bodies, doc, err)
		}
	}
}

type recordingResolver struct {
	got []transaction.DocumentConflict
}

func (r *recordingResolver) Resolve(c transaction.DocumentConflict) (*document.Document, error) {
	r.got = append(r.got, c)
	return c.IncomingDoc, nil
}

func TestCustomResolverSeesBaseAndOrigins(t *testing.T) {
	f := newChainFixture(t, "app/chain-custom-origins", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	res := &recordingResolver{}
	if _, err := ResolveDivergence(f.local.dag, f.local.storage, f.ns, f.localC.Hash, f.remoteC.Hash, ResolutionOptions{
		Policy: transaction.ConflictPolicyCustom, Resolver: res,
	}); err != nil {
		t.Fatal(err)
	}
	if len(res.got) != 1 {
		t.Fatalf("expected one conflict, got %d", len(res.got))
	}
	c := res.got[0]
	if c.BaseDoc == nil || c.BaseDoc.JSON != `{"v":0}` {
		t.Fatalf("expected the base value, got %+v", c.BaseDoc)
	}
	byBody := map[string]transaction.ConflictOrigin{c.ExistingDoc.JSON: c.ExistingOrigin, c.IncomingDoc.JSON: c.IncomingOrigin}
	if byBody[`{"v":"local"}`].NodeID != f.localNode || byBody[`{"v":"remote"}`].NodeID != f.remoteNode {
		t.Fatalf("origins do not name the writing nodes: %+v", byBody)
	}
	if byBody[`{"v":"local"}`].Commit != f.localC.Hash || byBody[`{"v":"local"}`].TimestampMicros == 0 {
		t.Fatalf("origin commit/timestamp missing: %+v", byBody[`{"v":"local"}`])
	}
}

func TestResolutionChainValidate(t *testing.T) {
	node := codec.DerivedUUID("n").String()
	for name, tc := range map[string]struct {
		chain ResolutionChain
		ok    bool
	}{
		"empty":               {ResolutionChain{}, true},
		"full":                {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority, Nodes: []string{node}}, {Kind: RuleValidity}, {Kind: RuleFieldMerge}, {Kind: RuleLastWrite}}}, true},
		"unknown":             {ResolutionChain{Rules: []ResolutionRule{{Kind: "coin-flip"}}}, false},
		"no nodes":            {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority}}}, false},
		"bad node":            {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority, Nodes: []string{"aws"}}}}, false},
		"duplicate node":      {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleSourcePriority, Nodes: []string{node, node}}}}, false},
		"rule after terminal": {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleQueue}, {Kind: RuleFieldMerge}}}, false},
		"nodes on wrong rule": {ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge, Nodes: []string{node}}}}, false},
	} {
		if err := tc.chain.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v", name, err)
		}
	}
	var none *ResolutionChain
	if none.Hash() != "" || (&ResolutionChain{}).Hash() != "" {
		t.Error("no chain and an empty chain must hash alike")
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
