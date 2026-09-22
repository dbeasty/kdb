package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// conflictingWrites gives both nodes a shared document, then has each change it differently.
func conflictingWrites(t *testing.T, a, b metaNode) codec.UUID {
	t.Helper()
	doc := mustRandomUUID(t)
	if _, err := a.data.Upsert("app/data", doc, `{"v":"base"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.data.Upsert("app/data", doc, `{"v":"from A"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.data.Upsert("app/data", doc, `{"v":"from B"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	return doc
}

func docBody(t *testing.T, rt *KdbServerRuntime, id codec.UUID) string {
	t.Helper()
	body, _, found, err := rt.GetDocument("app/data", id)
	if err != nil || !found {
		t.Fatalf("document %s: found=%v err=%v", id, found, err)
	}
	return body
}

// TestResolutionChainReplicatesAndSettlesTheMerge: a chain set on A reaches B through the metadata
// namespace, and both nodes then settle a conflict the way it says - here, A's writes win.
func TestResolutionChainReplicatesAndSettlesTheMerge(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleSourcePriority, Nodes: []string{a.data.NodeID.String()}},
		{Kind: peersync.RuleQueue},
	}}
	if err := a.store.SetResolution("app/data", chain); err != nil {
		t.Fatal(err)
	}
	if got := a.data.ResolutionChainOf().Hash(); got != chain.Hash() || got == "" {
		t.Fatalf("A did not apply its own chain: %q", got)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the chain", func() bool { return b.data.ResolutionChainOf().Hash() == chain.Hash() })

	doc := conflictingWrites(t, a, b)
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
		if got := docBody(t, rt, doc); got != `{"v":"from A"}` {
			t.Fatalf("%s holds %s, want A's write", name, got)
		}
	}
}

// TestMismatchedChainsHoldOffMerging: while only A has a chain, a sync merges on neither node - the
// divergence is queued on A and main is not pushed to B. Once the chain replicates, the next sync
// merges.
func TestMismatchedChainsHoldOffMerging(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	doc := conflictingWrites(t, a, b)
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleSourcePriority, Nodes: []string{b.data.NodeID.String()}},
	}}); err != nil {
		t.Fatal(err)
	}
	headB := mustHeadOf(t, b.data)
	res, err := syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	ns := res.Namespaces[0]
	if ns.Err != nil || !ns.ResolutionMismatch {
		t.Fatalf("expected a reported mismatch: err=%v mismatch=%v", ns.Err, ns.ResolutionMismatch)
	}
	if ns.RefErrors["branch:main"] == "" {
		t.Fatalf("expected main not to be pushed, got %+v", ns.RefErrors)
	}
	if mustHeadOf(t, b.data) != headB {
		t.Fatal("B merged under a chain it does not have")
	}
	if len(a.data.Conflicts.List()) == 0 {
		t.Fatal("expected the divergence queued on A")
	}
	if got := docBody(t, a.data, doc); got != `{"v":"from A"}` {
		t.Fatalf("A merged under a chain B does not have: %s", got)
	}

	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the chain", func() bool { return b.data.ResolutionChainOf() != nil })
	res, err = syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	if ns := res.Namespaces[0]; ns.Err != nil || ns.ResolutionMismatch {
		t.Fatalf("after replication: err=%v mismatch=%v", ns.Err, ns.ResolutionMismatch)
	}
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the nodes did not converge once their chains agreed")
	}
	if got := docBody(t, b.data, doc); got != `{"v":"from B"}` {
		t.Fatalf("B holds %s, want B's write per the chain", got)
	}
}

func TestSetResolutionRefusesAnInvalidChain(t *testing.T) {
	a := newMetaNode(t)
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: "coin-flip"}}}); err == nil {
		t.Fatal("expected an invalid chain to be refused")
	}
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{}); err != nil || a.data.ResolutionChainOf() != nil {
		t.Fatalf("an empty chain should clear it: %v", err)
	}
}
