package server

import (
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// TestConcurrentPassCallsSettleOneMultiDocumentEntryExactlyOnce: a single sync batches every
// document it finds diverged into one ConflictEntry - "all or nothing" (settle.go's doc comment)
// means one entry, however many documents it holds, is one commit. ConflictProcedure.Pass is
// meant to be called from a timer loop, but Kick() can also wake it from an OnRecord callback -
// nothing stops two passes from being in flight if a caller drives Pass directly, as a
// control-plane "settle now" action might. passMu should make concurrent passes serialize rather
// than race each other onto the same entry, which would either double-commit its settlement or
// have two passes each think they were the one that did. This builds one entry out of eight
// conflicting documents and fires many concurrent Pass calls at it: exactly one must report
// having settled it, every document must carry the procedure's answer, and nothing is left
// queued.
func TestConcurrentPassCallsSettleOneMultiDocumentEntryExactlyOnce(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthorityProcedure(t, a, b, "settle", `function main(c) { return { take: "remote" }; }`,
		peersync.PendingHold, a.data.NodeID.String())

	const docCount = 8
	docs := make([]codec.UUID, docCount)
	for i := 0; i < docCount; i++ {
		docs[i] = conflictingWrites(t, a, b)
	}
	mustSync(t, a, b)
	entries := authorityEntries(a.data.Conflicts)
	if len(entries) != 1 || len(entries[0].Items) != docCount {
		t.Fatalf("expected one entry holding %d documents, got %d entries: %+v", docCount, len(entries), entries)
	}

	runner := procedureRunnerFor(a, auth.Principal{})
	const passes = 20
	settledCounts := make([]int, passes)
	var wg sync.WaitGroup
	for i := 0; i < passes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			settledCounts[i] = runner.Pass()
		}(i)
	}
	wg.Wait()

	total := 0
	for _, c := range settledCounts {
		total += c
	}
	if total != 1 {
		t.Fatalf("settled %d total across %d concurrent passes, want exactly 1 (one entry, settled once)", total, passes)
	}
	if left := len(authorityEntries(a.data.Conflicts)); left != 0 {
		t.Fatalf("%d entries still queued after settlement", left)
	}
	for _, id := range docs {
		if got := docBody(t, a.data, id); got != `{"v":"from B"}` {
			t.Fatalf("document %s: got %s, want the procedure's answer applied to every document in the entry", id, got)
		}
	}
}

// TestConcurrentPassAcrossNamespacesBothProgress: Pass walks every namespace's runtime in one
// call (sorted by name), so two namespaces on the same node are never settled by two independent
// locks - passMu covers the whole node. This is the flip side of the isolation tests in
// procedure_namespace_test.go: those prove a namespace's procedure cannot see or answer for
// another's conflict; this proves that sharing one Pass loop still lets both namespaces' queued
// conflicts get settled, not just the first one found.
func TestConcurrentPassAcrossNamespacesBothProgress(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	aOther := addDataNamespace(t, a, "app2", "app/other")
	bOther := addDataNamespace(t, b, "app2", "app/other")

	const takeRemote = `function main(c) { return { take: "remote" }; }`
	if err := a.store.SetProcedure("app/data", "settle", takeRemote); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetProcedure("app/other", "settle", takeRemote); err != nil {
		t.Fatal(err)
	}
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleFieldMerge},
		{Kind: peersync.RuleAuthority, Pending: peersync.PendingHold, Node: a.data.NodeID.String(), Procedure: "settle"},
	}}
	if err := a.store.SetResolution("app/data", chain); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/other", chain); err != nil {
		t.Fatal(err)
	}
	shareDefinition(t, a, b, "B to apply both namespaces' definitions and chains", func() bool {
		return b.data.ProcedureHash("settle") != "" && bOther.ProcedureHash("settle") != "" &&
			b.data.ResolutionChainOf() != nil && bOther.ResolutionChainOf() != nil
	})

	// Sync each namespace's divergence separately - a single sync call batches everything it
	// finds into one ConflictEntry, even across namespaces, which would leave one entry to settle
	// instead of the two independent ones this test needs.
	docData := conflictingWrites(t, a, b)
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	// Both sides must actually move the field away from its base value (1) - a one-sided change
	// is exactly what RuleFieldMerge (the rule ahead of the authority in this chain) resolves on
	// its own, which would never reach the procedure at all.
	docOther := conflictOn(t, a, b, aOther, bOther, "app/other", 3, 5)
	if _, err := syncPatterns(t, a, b, "app/other"); err != nil {
		t.Fatal(err)
	}
	if got := len(authorityEntries(a.data.Conflicts)) + len(authorityEntries(aOther.Conflicts)); got != 2 {
		t.Fatalf("expected one queued entry per namespace, got %d total", got)
	}

	runner := procedureRunnerFor(a, auth.Principal{})
	var wg sync.WaitGroup
	settledCounts := make([]int, 10)
	for i := range settledCounts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			settledCounts[i] = runner.Pass()
		}(i)
	}
	wg.Wait()

	total := 0
	for _, c := range settledCounts {
		total += c
	}
	if total != 2 {
		t.Fatalf("settled %d total across both namespaces, want 2", total)
	}
	if got := docBody(t, a.data, docData); got != `{"v":"from B"}` {
		t.Fatalf("app/data: got %s", got)
	}
	if got := docBodyOn(t, aOther, "app/other", docOther); got != `{"qty":5}` {
		t.Fatalf("app/other: got %s", got)
	}
}
