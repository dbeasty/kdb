package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/script"
)

const keepLocalProc = `function main(c) { return { take: "local" }; }`

// TestProcedureReplicates: a procedure defined on A reaches B through the metadata namespace,
// byte for byte - so both nodes compute the same source hash, which is what a chain pins.
func TestProcedureReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "keepLocal", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if got := a.data.ProcedureHash("keepLocal"); got != script.SourceHash(keepLocalProc) {
		t.Fatalf("A did not apply its own procedure: %q", got)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the procedure", func() bool {
		return b.data.ProcedureHash("keepLocal") == script.SourceHash(keepLocalProc)
	})
	if src, _ := b.data.ProcedureSource("keepLocal"); src != keepLocalProc {
		t.Fatalf("B holds different source: %q", src)
	}
}

// TestProcedureRedefinitionReplicates: a changed procedure replaces the old one everywhere, and
// the hash changes with it - a node still on the old revision must be able to tell.
func TestProcedureRedefinitionReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	const v2 = `function main(c) { return { take: "remote" }; }`
	if err := a.store.SetProcedure("app/data", "pick", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the first revision", func() bool { return b.data.ProcedureHash("pick") != "" })
	first := b.data.ProcedureHash("pick")

	if err := a.store.SetProcedure("app/data", "pick", v2); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the second revision", func() bool { return b.data.ProcedureHash("pick") != first })
	if src, _ := b.data.ProcedureSource("pick"); src != v2 {
		t.Fatalf("B holds %q", src)
	}
}

// TestProcedureDropReplicates: dropping is a tombstone, not a deleted document - B has to learn
// that the procedure is gone, which a missing document could not tell it apart from one it never
// received.
func TestProcedureDropReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "keepLocal", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the procedure", func() bool { return b.data.ProcedureHash("keepLocal") != "" })

	if err := a.store.DropProcedure("app/data", "keepLocal"); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.data.ProcedureSource("keepLocal"); ok {
		t.Fatal("A still holds the dropped procedure")
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to drop the procedure", func() bool { return b.data.ProcedureHash("keepLocal") == "" })
}

// TestBrokenProcedureIsRefusedAtDefinition: source that will not compile never becomes a
// definition. Refusing it here means no node ever has to decide what to do with a procedure that
// cannot run in the middle of a merge.
func TestBrokenProcedureIsRefusedAtDefinition(t *testing.T) {
	a := newMetaNode(t)
	err := a.store.SetProcedure("app/data", "broken", `function main( {`)
	if !errors.Is(err, script.ErrCompile) {
		t.Fatalf("got %v", err)
	}
	if _, ok := a.data.ProcedureSource("broken"); ok {
		t.Fatal("the broken procedure was stored anyway")
	}
}

// TestProcedureNeedsAName: the document id is derived from the name, so an unnamed procedure
// would collide with every other unnamed one in the namespace.
func TestProcedureNeedsAName(t *testing.T) {
	a := newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "", keepLocalProc); err == nil || !strings.Contains(err.Error(), "needs a name") {
		t.Fatalf("got %v", err)
	}
}

// TestProcedureNamesAreListed: an operator can see what a namespace defines.
func TestProcedureNamesAreListed(t *testing.T) {
	a := newMetaNode(t)
	for _, n := range []string{"zeta", "alpha"} {
		if err := a.store.SetProcedure("app/data", n, keepLocalProc); err != nil {
			t.Fatal(err)
		}
	}
	got := a.data.ProcedureNames()
	if len(got) != 2 || got[0] != "alpha" || got[1] != "zeta" {
		t.Fatalf("got %v", got)
	}
}

// higherQtyProc keeps whichever side has the larger qty. It reads only the two sides, so both
// nodes merging the same heads reach the same answer - which is what an in-merge rule must do.
const higherQtyProc = `function main(c) { return { take: c.side0.qty >= c.side1.qty ? "side0" : "side1" }; }`

// procedureChain is a chain of one procedure rule, then queue.
func procedureChain(name string) peersync.ResolutionChain {
	return peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleProcedure, Name: name},
		{Kind: peersync.RuleQueue},
	}}
}

// qtyConflict gives both nodes a document and has each change its qty differently.
func qtyConflict(t *testing.T, a, b metaNode, localQty, remoteQty int) codec.UUID {
	t.Helper()
	doc := mustRandomUUID(t)
	if _, err := a.data.Upsert("app/data", doc, `{"qty":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.data.Upsert("app/data", doc, fmt.Sprintf(`{"qty":%d}`, localQty), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.data.Upsert("app/data", doc, fmt.Sprintf(`{"qty":%d}`, remoteQty), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	return doc
}

// shareDefinition replicates the metadata namespace and waits for cond on B.
func shareDefinition(t *testing.T, a, b metaNode, what string, cond func() bool) {
	t.Helper()
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, what, cond)
}

// TestProcedureChainSettlesAMergeOnBothNodes: the whole path end to end - a procedure and a chain
// that calls it replicate, a conflict happens, and both nodes converge on one merge whose content
// the procedure decided.
func TestProcedureChainSettlesAMergeOnBothNodes(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "higherQty", higherQtyProc); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/data", procedureChain("higherQty")); err != nil {
		t.Fatal(err)
	}
	if pinned := a.data.ResolutionChainOf().Procedures()[0].SourceHash; pinned != script.SourceHash(higherQtyProc) {
		t.Fatalf("the chain did not pin the procedure it calls: %q", pinned)
	}
	shareDefinition(t, a, b, "B to apply the chain and the procedure", func() bool {
		return b.data.ProcedureHash("higherQty") != "" && b.data.ResolutionChainOf() != nil
	})

	doc := qtyConflict(t, a, b, 5, 9)
	res, err := syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range res.Namespaces {
		if ns.Err != nil || ns.ResolutionMismatch || len(ns.Conflicts) > 0 {
			t.Fatalf("%s: err=%v mismatch=%v conflicts=%+v", ns.Namespace, ns.Err, ns.ResolutionMismatch, ns.Conflicts)
		}
	}
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the nodes did not converge on one head")
	}
	for name, rt := range map[string]*KdbServerRuntime{"A": a.data, "B": b.data} {
		if got := docBody(t, rt, doc); got != `{"qty":9}` {
			t.Fatalf("%s holds %s, want the larger qty", name, got)
		}
	}
}

// TestProcedureForkKeepsBothSidesOnBothNodes: a procedure can refuse to throw either side away.
// Both nodes then hold the same two documents, the loser under a derived id of its own.
func TestProcedureForkKeepsBothSidesOnBothNodes(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	const keepBoth = `function main(c) { return { fork: { keep: "side0" } }; }`
	if err := a.store.SetProcedure("app/data", "keepBoth", keepBoth); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/data", procedureChain("keepBoth")); err != nil {
		t.Fatal(err)
	}
	shareDefinition(t, a, b, "B to apply the definitions", func() bool {
		return b.data.ProcedureHash("keepBoth") != "" && b.data.ResolutionChainOf() != nil
	})

	doc := qtyConflict(t, a, b, 5, 9)
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the nodes did not converge on one head")
	}
	kept := docBody(t, a.data, doc)
	losing := `{"qty":9}`
	if kept == losing {
		losing = `{"qty":5}`
	}
	forkID := peersync.ForkID(doc, losing)
	for name, rt := range map[string]*KdbServerRuntime{"A": a.data, "B": b.data} {
		got := docBody(t, rt, forkID)
		if !strings.Contains(got, `"conflictOf"`) || !strings.Contains(got, doc.String()) {
			t.Fatalf("%s's fork has no back-reference: %s", name, got)
		}
		if docBody(t, rt, doc) != kept {
			t.Fatalf("%s kept a different side", name)
		}
	}
}

// TestChangedProcedureHoldsOffMergingUntilItHasReplicated: this is the whole point of pinning the
// source hash. While one node runs a newer revision than the other, nothing merges - rather than
// each node merging under its own version of the rules and diverging for good.
func TestChangedProcedureHoldsOffMergingUntilItHasReplicated(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "higherQty", higherQtyProc); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/data", procedureChain("higherQty")); err != nil {
		t.Fatal(err)
	}
	shareDefinition(t, a, b, "B to apply the definitions", func() bool {
		return b.data.ProcedureHash("higherQty") != "" && b.data.ResolutionChainOf() != nil
	})

	// A changes the procedure. Its chain is re-pinned to the new revision, so A and B no longer
	// agree about how a conflict is settled - and both know it.
	const lowerQty = `function main(c) { return { take: c.side0.qty <= c.side1.qty ? "side0" : "side1" }; }`
	if err := a.store.SetProcedure("app/data", "higherQty", lowerQty); err != nil {
		t.Fatal(err)
	}
	if a.data.ResolutionChainOf().Hash() == b.data.ResolutionChainOf().Hash() {
		t.Fatal("changing the procedure must change the chain A advertises")
	}

	doc := qtyConflict(t, a, b, 5, 9)
	headB := mustHeadOf(t, b.data)
	res, err := syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	if ns := res.Namespaces[0]; !ns.ResolutionMismatch {
		t.Fatalf("expected a reported mismatch, got %+v", ns)
	}
	if mustHeadOf(t, b.data) != headB {
		t.Fatal("B merged while the two nodes ran different code")
	}

	// Once the new revision has replicated they agree again, and the next sync merges - under the
	// new rule, which keeps the smaller qty.
	shareDefinition(t, a, b, "B to apply the new revision", func() bool {
		return b.data.ProcedureHash("higherQty") == script.SourceHash(lowerQty)
	})
	res, err = syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	if ns := res.Namespaces[0]; ns.Err != nil || ns.ResolutionMismatch {
		t.Fatalf("after replication: err=%v mismatch=%v", ns.Err, ns.ResolutionMismatch)
	}
	if got := docBody(t, b.data, doc); got != `{"qty":5}` {
		t.Fatalf("B holds %s, want the smaller qty the new revision keeps", got)
	}
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the nodes did not converge")
	}
}

// TestProcedureThatThrowsHoldsTheMergeAndSaysWhy: a broken procedure stops the merge and the
// conflict record says what happened, rather than the chain quietly falling through to a rule
// the other node might not reach.
func TestProcedureThatThrowsHoldsTheMergeAndSaysWhy(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	const throws = `function main(c) { throw new Error("no rule for this"); }`
	if err := a.store.SetProcedure("app/data", "throws", throws); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleProcedure, Name: "throws"},
		// Never reached: a failure holds, it does not fall through.
		{Kind: peersync.RuleLastWrite},
	}}); err != nil {
		t.Fatal(err)
	}
	shareDefinition(t, a, b, "B to apply the definitions", func() bool {
		return b.data.ProcedureHash("throws") != "" && b.data.ResolutionChainOf() != nil
	})

	doc := qtyConflict(t, a, b, 5, 9)
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	if got := docBody(t, a.data, doc); got != `{"qty":5}` {
		t.Fatalf("A merged under a procedure that cannot run: %s", got)
	}
	var reasons []string
	for _, e := range a.data.Conflicts.List() {
		for _, d := range e.Details {
			reasons = append(reasons, d.Reason)
		}
	}
	if len(reasons) == 0 || !strings.Contains(strings.Join(reasons, " "), "no rule for this") {
		t.Fatalf("the queued conflict should say why: %v", reasons)
	}
}

// TestChainRefusesAnUndefinedProcedure: a chain may only call a procedure that exists, because
// this is the last moment at which anyone can be told. Every later one is a merge that silently
// does not happen.
func TestChainRefusesAnUndefinedProcedure(t *testing.T) {
	a := newMetaNode(t)
	err := a.store.SetResolution("app/data", procedureChain("neverDefined"))
	if err == nil || !strings.Contains(err.Error(), "define procedure") {
		t.Fatalf("got %v", err)
	}
	if a.data.ResolutionChainOf() != nil {
		t.Fatal("the chain was applied anyway")
	}
}
