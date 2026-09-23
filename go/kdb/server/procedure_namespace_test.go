package server

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/script"
)

// A procedure invocation, and a conflict it resolves, is scoped to exactly one namespace (spec
// docs/kdb-spec-layer11-component32-stored-procedures.md - the kdb host API namespace is "fixed
// at invocation"). newMetaNode's fixture gives every other test in this package a single data
// namespace, "app/data", which cannot exercise that scoping: nothing has ever had a second
// namespace to leak into. These tests add one.

// addDataNamespace gives n a second data namespace beside "app/data", in the same NamespaceSet -
// same node, same process, same procRuntime program cache - the shape that would actually let a
// scoping bug show up.
func addDataNamespace(t *testing.T, n metaNode, catalog, ns string) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime(catalog, ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	krt := NewKdbServerRuntime(rt)
	krt.NodeID = n.data.NodeID
	if _, err := krt.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	krt.Namespaces = n.data.Namespaces
	if err := n.data.Namespaces.Add(krt); err != nil {
		t.Fatal(err)
	}
	return krt
}

// conflictOn is qtyConflict generalized to an arbitrary namespace and runtime pair.
func conflictOn(t *testing.T, a, b metaNode, aRT, bRT *KdbServerRuntime, ns string, localQty, remoteQty int) codec.UUID {
	t.Helper()
	doc := mustRandomUUID(t)
	if _, err := aRT.Upsert(ns, doc, `{"qty":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, ns); err != nil {
		t.Fatal(err)
	}
	if _, err := aRT.Upsert(ns, doc, jsonQty(localQty), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := bRT.Upsert(ns, doc, jsonQty(remoteQty), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	return doc
}

func jsonQty(qty int) string { return fmt.Sprintf(`{"qty":%d}`, qty) }

func docBodyOn(t *testing.T, rt *KdbServerRuntime, ns string, id codec.UUID) string {
	t.Helper()
	body, _, found, err := rt.GetDocument(ns, id)
	if err != nil || !found {
		t.Fatalf("document %s in %s: found=%v err=%v", id, ns, found, err)
	}
	return body
}

// TestOverlappingProcedureNamesStayIsolatedPerNamespace: two namespaces on one node each define a
// procedure named "settle" - the same name, deliberately different source. A conflict in each
// namespace must be settled by that namespace's own copy, never the other's.
func TestOverlappingProcedureNamesStayIsolatedPerNamespace(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	aOther := addDataNamespace(t, a, "app2", "app/other")
	bOther := addDataNamespace(t, b, "app2", "app/other")

	const keepLarger = `function main(c) { return { take: c.side0.qty >= c.side1.qty ? "side0" : "side1" }; }`
	const keepSmaller = `function main(c) { return { take: c.side0.qty <= c.side1.qty ? "side0" : "side1" }; }`
	if err := a.store.SetProcedure("app/data", "settle", keepLarger); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetProcedure("app/other", "settle", keepSmaller); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/data", procedureChain("settle")); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetResolution("app/other", procedureChain("settle")); err != nil {
		t.Fatal(err)
	}
	shareDefinition(t, a, b, "B to apply both namespaces' definitions and chains", func() bool {
		return b.data.ProcedureHash("settle") != "" && bOther.ProcedureHash("settle") != "" &&
			b.data.ResolutionChainOf() != nil && bOther.ResolutionChainOf() != nil
	})
	if b.data.ProcedureHash("settle") == bOther.ProcedureHash("settle") {
		t.Fatal("the two procedures should hash differently - fix the fixture, not the assertion below")
	}

	docData := qtyConflict(t, a, b, 5, 9)
	docOther := conflictOn(t, a, b, aOther, bOther, "app/other", 5, 9)
	if _, err := syncPatterns(t, a, b, "app/data", "app/other"); err != nil {
		t.Fatal(err)
	}

	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("app/data did not converge")
	}
	if mustHeadOf(t, aOther) != mustHeadOf(t, bOther) {
		t.Fatal("app/other did not converge")
	}
	if got := docBody(t, a.data, docData); got != `{"qty":9}` {
		t.Fatalf("app/data should keep the larger qty under its own procedure, got %s", got)
	}
	if got := docBodyOn(t, aOther, "app/other", docOther); got != `{"qty":5}` {
		t.Fatalf("app/other should keep the smaller qty under its own procedure, got %s - it used app/data's rule", got)
	}
}

// TestRedefiningAProcedureInOneNamespaceLeavesAnotherNamespacesCopyAlone: KdbServerRuntime.procs
// is per-runtime, so nothing should couple two namespaces' same-named procedures even when they
// share a node, a store and the process-wide compiled-program cache.
func TestRedefiningAProcedureInOneNamespaceLeavesAnotherNamespacesCopyAlone(t *testing.T) {
	a := newMetaNode(t)
	aOther := addDataNamespace(t, a, "app2", "app/other")
	if err := a.store.SetProcedure("app/data", "settle", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if err := a.store.SetProcedure("app/other", "settle", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	before := aOther.ProcedureHash("settle")

	const takesRemote = `function main(c) { return { take: "remote" }; }`
	if err := a.store.SetProcedure("app/data", "settle", takesRemote); err != nil {
		t.Fatal(err)
	}

	if got := aOther.ProcedureHash("settle"); got != before {
		t.Fatalf("redefining app/data's procedure changed app/other's: got %q want %q", got, before)
	}
	if got := a.data.ProcedureHash("settle"); got != script.SourceHash(takesRemote) {
		t.Fatalf("app/data itself did not pick up its own redefinition: %q", got)
	}
}

// TestAuthorityProcedureQueryIsScopedToItsOwnNamespace: the authority position lets a procedure
// read the database (spec §12.2), which is exactly the capability that could leak across a
// scoping bug. A marker document lives only in app/other; a procedure settling an app/data
// conflict counts app/data's own rows and refuses to settle unless the count is exactly what
// app/data alone should hold - if kdb.query ever saw app/other's marker too, the count would be
// wrong and the settlement would not happen.
func TestAuthorityProcedureQueryIsScopedToItsOwnNamespace(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	aOther := addDataNamespace(t, a, "app2", "app/other")
	addDataNamespace(t, b, "app2", "app/other")

	if _, err := aOther.Upsert("app/other", mustRandomUUID(t), `{"marker":"only-in-other"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	const countExactlyOne = `function main(c) {
	  var rows = kdb.query("SELECT count(*) AS n FROM docs", []);
	  if (rows[0].n !== 1) { throw new Error("saw " + rows[0].n + " row(s), expected exactly app/data's own"); }
	  return { take: "remote" };
	}`
	withAuthorityProcedure(t, a, b, "scoped", countExactlyOne, peersync.PendingHold, a.data.NodeID.String())

	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)

	runner := procedureRunnerFor(a, auth.Principal{})
	if n := runner.Pass(); n != 1 {
		t.Fatalf("settled %d - the query likely saw the wrong row count (leaked across namespaces?): %s",
			n, runner.LastFailure(authorityEntries(a.data.Conflicts)[0].ID))
	}
	if got := docBody(t, a.data, doc); got != `{"v":"from B"}` {
		t.Fatalf("got %s", got)
	}
}
