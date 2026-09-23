package server

import (
	"context"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// The resolver authority as a stored procedure: the same end-to-end shape as the webhook tests,
// with the procedure in the application service's place.

// procedureRunnerFor builds a runner for n without starting its loop, so a test drives Pass
// itself. Principal defaults to the runtime's own, which AllowAll accepts.
func procedureRunnerFor(n metaNode, principal auth.Principal) *ConflictProcedure {
	if principal.ID == "" {
		principal = systemPrincipal()
	}
	return &ConflictProcedure{
		Principal: principal,
		set:       n.data.namespaceSet(), node: n.data.NodeID.String(),
		hooked: map[*peersync.ConflictQueue]bool{}, attempts: map[string]int{},
		failures: map[string]string{}, stop: make(chan struct{}),
	}
}

// withAuthorityProcedure defines a procedure on a and gives both nodes a chain that hands what it
// cannot settle to that procedure, on node only.
func withAuthorityProcedure(t *testing.T, a, b metaNode, name, source, pending, node string) {
	t.Helper()
	if err := a.store.SetProcedure("app/data", name, source); err != nil {
		t.Fatal(err)
	}
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleFieldMerge},
		{Kind: peersync.RuleAuthority, Pending: pending, Node: node, Procedure: name},
	}}
	if err := a.store.SetResolution("app/data", chain); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the chain", func() bool { return b.data.ResolutionChainOf().Hash() == chain.Hash() })
}

// TestAuthorityProcedureSettlesAHeldConflict: the conflict is held for the authority, the
// procedure decides it on the notifying node, and the settlement replicates and closes it on the
// other node - exactly the path an operator or a webhook receiver takes.
func TestAuthorityProcedureSettlesAHeldConflict(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	const takeRemote = `function main(c) { return { take: "remote" }; }`
	withAuthorityProcedure(t, a, b, "settle", takeRemote, peersync.PendingHold, a.data.NodeID.String())
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)
	if len(authorityEntries(a.data.Conflicts)) != 1 {
		t.Fatal("expected one held entry on A")
	}

	if n := procedureRunnerFor(a, auth.Principal{}).Pass(); n != 1 {
		t.Fatalf("the procedure should have settled one entry, settled %d", n)
	}
	mustSync(t, a, b)
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the settlement did not converge the nodes")
	}
	for name, n := range map[string]metaNode{"A": a, "B": b} {
		if got := docBody(t, n.data, doc); got != `{"v":"from B"}` {
			t.Fatalf("%s holds %s, want the procedure's choice", name, got)
		}
		if es := authorityEntries(n.data.Conflicts); len(es) != 0 {
			t.Fatalf("%s still holds %+v", name, es)
		}
	}
}

// TestAuthorityProcedureReadsTheDatabase: this is what the authority position buys. The same
// procedure inside a merge could not do this - two nodes reading their own copies could disagree
// - but here only the notifying node runs it, and its answer is an ordinary commit.
func TestAuthorityProcedureReadsTheDatabase(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	policy := mustRandomUUID(t)
	if _, err := a.data.Upsert("app/data", policy, `{"prefer":"local"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	src := `function main(c) { return { take: kdb.get("` + policy.String() + `").prefer }; }`
	withAuthorityProcedure(t, a, b, "byPolicy", src, peersync.PendingHold, a.data.NodeID.String())
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)

	if n := procedureRunnerFor(a, auth.Principal{}).Pass(); n != 1 {
		t.Fatalf("settled %d", n)
	}
	if got := docBody(t, a.data, doc); got != `{"v":"from A"}` {
		t.Fatalf("the policy document says local, A holds %s", got)
	}
}

// TestAuthorityProcedureCanQueryTheDatabase: SQL as well as document reads, with parameters.
func TestAuthorityProcedureCanQueryTheDatabase(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if _, err := a.data.Upsert("app/data", mustRandomUUID(t), `{"prefer":"remote"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	src := `function main(c) {
	  var rows = kdb.query("SELECT prefer FROM docs WHERE prefer = ?", ["remote"]);
	  return { take: rows.length === 1 ? rows[0].prefer : "local" };
	}`
	withAuthorityProcedure(t, a, b, "byQuery", src, peersync.PendingHold, a.data.NodeID.String())
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)

	runner := procedureRunnerFor(a, auth.Principal{})
	if n := runner.Pass(); n != 1 {
		t.Fatalf("settled %d: %s", n, runner.LastFailure(authorityEntries(a.data.Conflicts)[0].ID))
	}
	if got := docBody(t, a.data, doc); got != `{"v":"from B"}` {
		t.Fatalf("A holds %s", got)
	}
}

// TestAuthorityProcedureDeferLeavesItForAPerson: a procedure that declines settles nothing and
// the conflict stays queued, which is what an authority rule promised in the first place.
func TestAuthorityProcedureDeferLeavesItForAPerson(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthorityProcedure(t, a, b, "abstain", `function main(c) { return { defer: true }; }`, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	entry := authorityEntries(a.data.Conflicts)[0]

	runner := procedureRunnerFor(a, auth.Principal{})
	if n := runner.Pass(); n != 0 {
		t.Fatalf("settled %d", n)
	}
	if len(authorityEntries(a.data.Conflicts)) != 1 {
		t.Fatal("the conflict should still be waiting")
	}
	if got := runner.LastFailure(entry.ID); !strings.Contains(got, "to a person") {
		t.Fatalf("the reason should say the procedure declined: %q", got)
	}
}

// TestAuthorityProcedureStopsRetryingAFailure: a procedure that fails the same way every pass is
// a hot loop, and a conflict nothing can settle should reach a person rather than be tried
// forever.
func TestAuthorityProcedureStopsRetryingAFailure(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthorityProcedure(t, a, b, "boom", `function main(c) { throw new Error("cannot decide"); }`, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	entry := authorityEntries(a.data.Conflicts)[0]

	runner := procedureRunnerFor(a, auth.Principal{})
	for i := 0; i < maxProcedureAttempts+3; i++ {
		if n := runner.Pass(); n != 0 {
			t.Fatalf("pass %d settled %d", i, n)
		}
	}
	if got := runner.attempts[entry.ID]; got != maxProcedureAttempts {
		t.Fatalf("expected it to stop after %d attempts, made %d", maxProcedureAttempts, got)
	}
	if got := runner.LastFailure(entry.ID); !strings.Contains(got, "cannot decide") {
		t.Fatalf("the reason should carry what the procedure threw: %q", got)
	}
	if len(authorityEntries(a.data.Conflicts)) != 1 {
		t.Fatal("the conflict should still be waiting for a person")
	}
}

// TestAuthorityProcedureRunsOnlyOnTheNotifyingNode: both nodes hold the entry, but only the node
// the rule names acts on it - otherwise both would settle the same conflict.
func TestAuthorityProcedureRunsOnlyOnTheNotifyingNode(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthorityProcedure(t, a, b, "settle", `function main(c) { return { take: "local" }; }`, peersync.PendingProvisional, a.data.NodeID.String())
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	if len(authorityEntries(b.data.Conflicts)) == 0 {
		t.Fatal("B should also record the provisional entry")
	}
	if n := procedureRunnerFor(b, auth.Principal{}).Pass(); n != 0 {
		t.Fatalf("B is not the notifying node and must not settle, settled %d", n)
	}
	if n := procedureRunnerFor(a, auth.Principal{}).Pass(); n != 1 {
		t.Fatalf("A should settle, settled %d", n)
	}
}

// TestAuthorityProcedureWithoutAProcedureNameDoesNothing: a chain whose authority is a webhook
// or a person is untouched by this.
func TestAuthorityProcedureWithoutAProcedureNameDoesNothing(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	if n := procedureRunnerFor(a, auth.Principal{}).Pass(); n != 0 {
		t.Fatalf("settled %d", n)
	}
	if len(authorityEntries(a.data.Conflicts)) != 1 {
		t.Fatal("the entry should be untouched")
	}
}

// TestAuthorityProcedureNeedsTheResolvePermission: a procedure settling for the authority commits
// as a principal, and needs the same "resolve" an operator does. It is someone acting, not the
// database quietly changing its own mind.
func TestAuthorityProcedureNeedsTheResolvePermission(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthorityProcedure(t, a, b, "settle", `function main(c) { return { take: "remote" }; }`, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	entry := authorityEntries(a.data.Conflicts)[0]

	engine, store := newTestRegistryAuthEngine(t)
	a.data.AuthEngine = engine
	if err := store.CreateRole("writer", []string{"write:app/data", "read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("resolver", []string{"write:app/data", "read:app/data", "resolve:app/data"}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []struct{ id, role string }{{"w", "writer"}, {"r", "resolver"}} {
		if err := store.CreateUser(u.id, "pw", []string{u.role}); err != nil {
			t.Fatal(err)
		}
	}
	login := func(id string) auth.Principal {
		p, err := engine.Authenticator().Authenticate(context.Background(), auth.Credentials{User: ptr(id), Password: ptr("pw")})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	runner := procedureRunnerFor(a, login("w"))
	if n := runner.Pass(); n != 0 {
		t.Fatalf("a principal without resolve must be refused, settled %d", n)
	}
	if got := runner.LastFailure(entry.ID); got == "" {
		t.Fatal("the refusal should be recorded")
	}
	if n := procedureRunnerFor(a, login("r")).Pass(); n != 1 {
		t.Fatalf("a principal with resolve should settle, settled %d", n)
	}
}
