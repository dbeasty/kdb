package peersync

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// procRule is a chain of just the procedure rule, then whatever follows.
func procRule(name string, then ...ResolutionRule) *ResolutionChain {
	return &ResolutionChain{Rules: append([]ResolutionRule{{Kind: RuleProcedure, Name: name, SourceHash: "h1"}}, then...)}
}

func TestChainProcedureDecidesTheConflict(t *testing.T) {
	f := newChainFixture(t, "app/proc-take", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	// Side 0 is the merge's first parent's, the same on both nodes - which is what lets a rule
	// this blunt still produce one merge.
	run := func(name, hash string, c ProcedureConflict) (Settlement, bool, error) {
		return Settlement{Body: c.Side0}, true, nil
	}
	out, got := f.resolveBothWays(t, ResolutionOptions{Chain: procRule("pick"), Procedure: run})
	if out.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", out.Kind)
	}
	if got != `{"v":"local"}` && got != `{"v":"remote"}` {
		t.Fatalf("got %s", got)
	}
}

func TestChainProcedureIsToldWhichRevisionTheChainPinned(t *testing.T) {
	f := newChainFixture(t, "app/proc-hash", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	var sawName, sawHash string
	run := func(name, hash string, c ProcedureConflict) (Settlement, bool, error) {
		sawName, sawHash = name, hash
		return Settlement{Body: c.Side0}, true, nil
	}
	f.resolveBothWays(t, ResolutionOptions{Chain: procRule("pick"), Procedure: run})
	if sawName != "pick" || sawHash != "h1" {
		t.Fatalf("got %q %q", sawName, sawHash)
	}
}

func TestChainProcedureSeesBothSidesAndTheirBase(t *testing.T) {
	f := newChainFixture(t, "app/proc-input", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	var got ProcedureConflict
	run := func(_, _ string, c ProcedureConflict) (Settlement, bool, error) {
		got = c
		return Settlement{Body: c.Side0}, true, nil
	}
	f.resolveBothWays(t, ResolutionOptions{Chain: procRule("look"), Procedure: run})
	if got.DocID != f.doc {
		t.Fatalf("wrong document: %s", got.DocID)
	}
	if got.Base == nil || *got.Base != `{"v":0}` {
		t.Fatalf("base: %v", got.Base)
	}
	if got.Side0 == nil || got.Side1 == nil || *got.Side0 == *got.Side1 {
		t.Fatalf("sides: %v %v", got.Side0, got.Side1)
	}
	if got.Origin0.NodeID == got.Origin1.NodeID {
		t.Fatalf("each side should name the node that wrote it, both say %s", got.Origin0.NodeID)
	}
}

func TestChainProcedureDeferFallsThroughToTheNextRule(t *testing.T) {
	f := newChainFixture(t, "app/proc-defer", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	run := func(string, string, ProcedureConflict) (Settlement, bool, error) { return Settlement{}, false, nil }
	_, got := f.resolveBothWays(t, ResolutionOptions{
		Chain:     procRule("abstain", ResolutionRule{Kind: RuleSourcePriority, Nodes: []string{f.localNode.String()}}),
		Procedure: run,
	})
	if got != `{"v":"local"}` {
		t.Fatalf("expected the next rule to settle it, got %s", got)
	}
}

// TestChainProcedureFailureHoldsTheMerge: a procedure that cannot run never falls through. It may
// well run on the other node - it timed out here, or this node has an older copy - and two nodes
// carrying on under different rules build different merges. Nothing merges until it is fixed.
func TestChainProcedureFailureHoldsTheMerge(t *testing.T) {
	f := newChainFixture(t, "app/proc-error", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	run := func(string, string, ProcedureConflict) (Settlement, bool, error) {
		return Settlement{}, false, errors.New("it threw")
	}
	out, _ := f.resolveBothWays(t, ResolutionOptions{
		// A last-write rule follows, and must not be reached.
		Chain:     procRule("boom", ResolutionRule{Kind: RuleLastWrite}),
		Procedure: run,
	})
	if out.Kind != OutcomeConflict {
		t.Fatalf("expected the merge to be held, got %v", out.Kind)
	}
	if len(out.Details) != 1 || !strings.Contains(out.Details[0].Reason, "it threw") {
		t.Fatalf("the reason should say what went wrong: %+v", out.Details)
	}
}

func TestChainProcedureWithoutARunnerHolds(t *testing.T) {
	f := newChainFixture(t, "app/proc-norunner", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: procRule("pick", ResolutionRule{Kind: RuleLastWrite})})
	if out.Kind != OutcomeConflict {
		t.Fatalf("a node that cannot run procedures must not fall through to the next rule, got %v", out.Kind)
	}
}

func TestChainProcedureCanForkAndDelete(t *testing.T) {
	t.Run("fork", func(t *testing.T) {
		f := newChainFixture(t, "app/proc-fork", `{"t":"draft"}`, `{"t":"mine"}`, `{"t":"theirs"}`)
		run := func(_, _ string, c ProcedureConflict) (Settlement, bool, error) {
			fork, ok := ForkOf(c.DocID, c.Side1, c.Origin1.Commit)
			if !ok {
				t.Fatal("side 1 should fork")
			}
			return Settlement{Body: c.Side0, Fork: fork}, true, nil
		}
		out, _ := f.resolveBothWays(t, ResolutionOptions{Chain: procRule("keepBoth"), Procedure: run})
		if out.Kind != OutcomeMerged {
			t.Fatalf("got %v", out.Kind)
		}
		var forked int
		for _, op := range out.MergeCommit.Operations {
			if opDocID(op) != f.doc {
				forked++
			}
		}
		if forked != 1 {
			t.Fatalf("expected exactly one forked document in the merge, got %d", forked)
		}
	})
	t.Run("delete", func(t *testing.T) {
		f := newChainFixture(t, "app/proc-delete", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
		run := func(string, string, ProcedureConflict) (Settlement, bool, error) {
			return Settlement{}, true, nil
		}
		out, err := ResolveDivergence(f.local.dag, f.local.storage, f.ns, f.localC.Hash, f.remoteC.Hash,
			ResolutionOptions{Chain: procRule("drop"), Procedure: run})
		if err != nil || out.Kind != OutcomeMerged {
			t.Fatalf("got %v %v", out.Kind, err)
		}
		doc, err := f.local.storage.GetDocument(f.ns, f.doc, out.MergeCommit.DocumentTreeHash)
		if err != nil || doc != nil {
			t.Fatalf("the document should be gone: %v %v", doc, err)
		}
	})
}

// TestAdvertisedHashHidesAChainThisNodeCannotRun: peers compare this hash before merging, so a
// node missing the procedure - or holding another revision of it - must not look like a node that
// agrees.
func TestAdvertisedHashHidesAChainThisNodeCannotRun(t *testing.T) {
	chain := procRule("pick")
	agrees := ResolutionOptions{Chain: chain, ProcedureHash: func(string) string { return "h1" }}
	if got := agrees.AdvertisedHash(); got != chain.Hash() {
		t.Fatalf("a node holding the pinned revision advertises the chain's own hash, got %q", got)
	}
	for name, have := range map[string]string{"missing": "", "another revision": "h2"} {
		o := ResolutionOptions{Chain: chain, ProcedureHash: func(string) string { return have }}
		got := o.AdvertisedHash()
		if got == chain.Hash() || !strings.HasPrefix(got, "unavailable:") {
			t.Fatalf("%s: got %q", name, got)
		}
	}
	if o := (ResolutionOptions{Chain: chain}); o.AdvertisedHash() == chain.Hash() {
		t.Fatal("a node that cannot say what it holds must not claim to agree")
	}
	// No chain at all: nothing to advertise, and a node with no chain must still match a node
	// with an empty one.
	if got := (ResolutionOptions{}).AdvertisedHash(); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestProcedureRuleValidation(t *testing.T) {
	if err := (ResolutionChain{Rules: []ResolutionRule{{Kind: RuleProcedure}}}).Validate(); err == nil {
		t.Fatal("a procedure rule without a name names no procedure")
	}
	if err := (ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge, Name: "x"}}}).Validate(); err == nil {
		t.Fatal("only the procedure rule takes a name")
	}
	if err := (ResolutionChain{Rules: []ResolutionRule{
		{Kind: RuleProcedure, Name: "pick"}, {Kind: RuleLastWrite},
	}}).Validate(); err != nil {
		t.Fatalf("a procedure rule may be followed by others: %v", err)
	}
}

func TestProcedureRuleChangesTheChainHash(t *testing.T) {
	// The pinned revision is part of the chain, so it is part of the hash: two nodes running
	// different code do not look alike.
	a := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleProcedure, Name: "pick", SourceHash: "h1"}}}
	b := &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleProcedure, Name: "pick", SourceHash: "h2"}}}
	if a.Hash() == b.Hash() {
		t.Fatal("the pinned revision must change the chain hash")
	}
	// A chain without procedures hashes exactly as it did before this rule existed: the two new
	// fields are omitted, so no already-deployed chain's hash moved under it and no pair of nodes
	// stopped merging on an upgrade.
	old, err := json.Marshal(ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleQueue}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(old) != `{"rules":[{"kind":"field-merge"},{"kind":"queue"}]}` {
		t.Fatalf("got %s", old)
	}
}
