package server

import (
	"strings"
	"testing"
)

// TestCatchUpPolicyReplicatesToEveryNode: a namespace's refusal to serve squash catch-ups is a
// replicated definition - set on A (here by pattern), it reaches B through the metadata
// namespace, and both describe the namespace to peer sync as refusing. Clearing it does too.
func TestCatchUpPolicyReplicatesToEveryNode(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if got := a.data.PeerIngestEnv().SquashRefused; got != "" {
		t.Fatalf("a namespace with no policy refuses squash: %q", got)
	}
	if err := a.store.SetCatchUp("app/**", CatchUpPolicy{RefuseSquash: true, Reason: "audit trail"}); err != nil {
		t.Fatal(err)
	}
	if got := a.data.PeerIngestEnv().SquashRefused; got != "audit trail" {
		t.Fatalf("on A after setting the pattern: SquashRefused %q", got)
	}
	later := openInto(t, a, "app/later")
	if !later.CatchUpPolicyOf().RefuseSquash {
		t.Fatal("a namespace opened after the pattern was set does not refuse squash")
	}

	putDoc(t, a.data, `{"v":1}`)
	syncNodes(t, a, b)
	waitFor(t, "B to apply the policy", func() bool { return b.data.PeerIngestEnv().SquashRefused == "audit trail" })

	// A namespace's own definition overrides the pattern; with no reason, a generic one is given.
	if err := a.store.SetCatchUp("app/data", CatchUpPolicy{RefuseSquash: true}); err != nil {
		t.Fatal(err)
	}
	if got := a.data.PeerIngestEnv().SquashRefused; !strings.Contains(got, "app/data") {
		t.Fatalf("generic refusal should name the namespace: %q", got)
	}
	if err := a.store.SetCatchUp("app/data", CatchUpPolicy{}); err != nil {
		t.Fatal(err)
	}
	if got := a.data.PeerIngestEnv().SquashRefused; got != "" {
		t.Fatalf("after allowing squash again: SquashRefused %q", got)
	}
	syncNodes(t, a, b)
	waitFor(t, "B to clear the policy", func() bool { return b.data.PeerIngestEnv().SquashRefused == "" })
	if !later.CatchUpPolicyOf().RefuseSquash {
		t.Fatal("app/data's own definition changed the pattern's other namespaces")
	}
}
