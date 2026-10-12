package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// hostCall serves one message on host, as the frame a peer would send.
func hostCall(t *testing.T, host *V2Host, m wire.Message) wire.Message {
	t.Helper()
	w := wire.NewCodec(wire.EncodingJSON)
	frame, err := w.Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	out, err := host.HandleFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := w.Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// TestPushIntoAShallowHostSendsTheForkPoint: local, bootstrapped from remote's snapshot, is only
// dialled. remote pushes it a merge of a branch forked behind local's root; the push carries the
// fork point's state on the same session, and local reaches remote's head.
func TestPushIntoAShallowHostSendsTheForkPoint(t *testing.T) {
	ns := "app/boundary-push"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	f, _, head := boundaryScenario(t, "hub-boundary-push-src", local, remote, ns)

	v2Hub(t, "hub-boundary-push", newHost(local), nil)
	res := syncV2(t, "hub-boundary-push", remote, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPush})
	if got := res.Namespaces[0].BoundarySent; len(got) != 1 || got[0] != f.Hash.Hex() {
		t.Fatalf("the push should have sent the fork point's state: %v", got)
	}
	if mustHead(t, local.side(ns)) != head.Hash {
		t.Fatal("local did not take the pushed head")
	}
	if !local.side(ns).dag.IsShallow(f.Hash) {
		t.Fatal("the fork point should be a shallow root on local")
	}
}

// TestOldPusherGetsATypedBoundaryRefusal: a pusher that does not know SHALLOW_BOUNDARY (v0.7.0)
// sends its page and nothing else. It is refused with that code and a message naming the commit
// and the parent behind the root - not a bare "missing parent" - and main does not move. The
// refusal is also all that lets a GRAFT_PUSH skip allowUnrelated: a commit the session was not
// refused for is still a graft, and refused here; the refused one is taken, and the page then
// stores.
func TestOldPusherGetsATypedBoundaryRefusal(t *testing.T) {
	ns := "app/boundary-old-pusher"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	f, side, head := boundaryScenario(t, "hub-boundary-old-src", local, remote, ns)
	before := mustHead(t, local.side(ns))
	host := newHost(local)
	if ack, ok := hostCall(t, host, wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, 1), NodeID: "old", Protocol: wire.SyncProtocolVersion, Namespaces: []string{ns},
	}).(wire.SyncHelloAckMessage); !ok || !ack.Accepted {
		t.Fatalf("hello: %+v", ack)
	}
	commits, stubs, done, err := MissingCommitsFrom(remote.side(ns).dag, []codec.Hash{head.Hash}, []codec.Hash{before}, 0)
	if err != nil || !done {
		t.Fatalf("pack: %v done=%v", err, done)
	}
	update := wire.RefUpdateMessage{
		H: header(wire.MsgRefUpdate, 2), Namespace: ns, Kind: wire.RefBranch, Ref: mainBranch,
		OldHex: before.Hex(), NewHex: head.Hash.Hex(), Commits: commits, Stubs: stubs,
	}
	pe, ok := hostCall(t, host, update).(wire.PeerErrorMessage)
	if !ok {
		t.Fatal("the push should have been refused")
	}
	if pe.Code != wire.ErrorCodeShallowBoundary || !strings.Contains(pe.Message, side.Hash.Hex()) || !strings.Contains(pe.Message, f.Hash.Hex()) {
		t.Fatalf("want SHALLOW_BOUNDARY naming %s and %s, got %s: %s", side.Hash.Hex(), f.Hash.Hex(), pe.Code, pe.Message)
	}
	if mustHead(t, local.side(ns)) != before {
		t.Fatal("main moved although the push was refused")
	}

	remoteEnv, err := remote.Env(ns, false)
	if err != nil {
		t.Fatal(err)
	}
	graft := func(at codec.Hash, cid int) wire.Message {
		page, err := snapshotPage(remoteEnv, at, "", 0)
		if err != nil || !page.Done {
			t.Fatalf("snapshot page at %s: %v", at.Hex(), err)
		}
		return hostCall(t, host, wire.GraftPushMessage{H: header(wire.MsgGraftPush, cid), Page: page})
	}
	// A commit behind the root this session was not refused for - the root's own parent: an
	// ordinary graft, which this namespace does not allow.
	rootCommit, _ := remote.side(ns).dag.GetCommit(before)
	behind := rootCommit.ParentHashes[0]
	if behind == f.Hash || local.side(ns).dag.HasCommit(behind) {
		t.Fatal("scenario: want a commit behind the root other than the fork point")
	}
	if pe, ok := graft(behind, 3).(wire.PeerErrorMessage); !ok || !strings.Contains(pe.Message, ErrUnrelatedNotAllowed.Error()) {
		t.Fatalf("a GRAFT_PUSH the session was not refused for must still need allowUnrelated, got %+v", pe)
	}
	if r, ok := graft(f.Hash, 4).(wire.GraftPushResultMessage); !ok || !r.Grafted {
		t.Fatalf("the refused parent's state should be taken: %+v", r)
	}
	update.H = header(wire.MsgRefUpdate, 5)
	ack, ok := hostCall(t, host, update).(wire.RefUpdateAckMessage)
	if !ok || ack.HeadHex != head.Hash.Hex() {
		t.Fatalf("after the fork point's state, the page should store and main move: %+v", ack)
	}
}
