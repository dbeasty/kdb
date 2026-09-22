package auth

import (
	"context"
	"testing"
)

type fixedGrants map[string][]string

// grantsAuthorizer answers like the registry engine over fixed role grants.
type grantsAuthorizer struct{ roles fixedGrants }

func (g grantsAuthorizer) Authorize(_ context.Context, p Principal, action Action) error {
	kind, res := actionToResource(action)
	ok := PrincipalHasPermission(p, g.roles, kind, res)
	if !ok && (kind == "sync_pull" || kind == "sync_push") {
		ok = PrincipalHasPermission(p, g.roles, "sync", res)
	}
	if !ok {
		return errDenied
	}
	return nil
}

type deniedErr struct{}

func (deniedErr) Error() string { return "denied" }

var errDenied = deniedErr{}

// TestPeerDirectionsFromGrants: "sync" is both directions (as before directions existed);
// "sync_pull" and "sync_push" grant one each, per namespace.
func TestPeerDirectionsFromGrants(t *testing.T) {
	a := grantsAuthorizer{roles: fixedGrants{
		"phone": {"sync:zolik/u/1", "sync_pull:zolik/u/1/ro", "sync_push:zolik/inbox"},
	}}
	p := Principal{ID: "phone-1", Roles: map[string]struct{}{"phone": {}}}
	ctx := context.Background()
	cases := []struct {
		ns         string
		pull, push bool
	}{
		{"zolik/u/1", true, true},
		{"zolik/u/1/ro", true, false},
		{"zolik/inbox", false, true},
		{"zolik/u/2", false, false},
	}
	for _, c := range cases {
		pull, push := PeerDirections(ctx, a, p, c.ns)
		if pull != c.pull || push != c.push {
			t.Errorf("%s: pull=%v push=%v, want %v %v", c.ns, pull, push, c.pull, c.push)
		}
		if (AuthorizePeer(ctx, a, p, c.ns, true) == nil) != c.push || (AuthorizePeer(ctx, a, p, c.ns, false) == nil) != c.pull {
			t.Errorf("%s: AuthorizePeer disagrees with PeerDirections", c.ns)
		}
	}
	if kind, _ := actionToResource(PeerPullAction{Namespace: "x"}); kind != "sync_pull" {
		t.Fatalf("pull kind %q", kind)
	}
}
