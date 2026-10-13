package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestGraftPushStagesPagesOnDisk: a graft pushed in many pages is staged on disk as it arrives -
// the session holds a staging file, not the pages - and grafts exactly as a one-page push does.
func TestGraftPushStagesPagesOnDisk(t *testing.T) {
	f := newGraftFixture(t, "graft-push-pages", unrelatedChain())
	host := newHost(f.c)
	w := wire.NewCodec(wire.EncodingJSON)
	pushes, staged := 0, 0
	v2HubRaw(t, "graft-push-pages-c", func(frame []byte) []byte {
		msg, _ := w.Decode(frame)
		gp, isPush := msg.(wire.GraftPushMessage)
		out, _ := host.HandleFrame(frame)
		if isPush {
			pushes++
			host.mu.Lock()
			if st := host.grafts[gp.Page.Namespace]; st != nil && !gp.Page.Done {
				if st.spool.docs > 0 && st.spool.f != nil {
					staged++
				}
			}
			host.mu.Unlock()
		}
		return out
	})
	res, err := SyncV2(w, stream.NewInMemoryTransport(), V2ClientConfig{
		PeerURI: "memory://graft-push-pages-c", Local: f.a, NodeID: "a", Namespaces: []string{f.ns}, Mode: SyncPush, PageBytes: 400,
	})
	if err != nil || len(res.Namespaces[0].RefErrors) != 0 {
		t.Fatalf("push: %v %+v", err, res.Namespaces[0])
	}
	if pushes < 4 || staged != pushes-1 {
		t.Fatalf("%d GRAFT_PUSH pages, %d of them staged on disk: want many, all but the last staged", pushes, staged)
	}
	c := f.c.side(f.ns)
	if !c.dag.IsShallow(f.root) || !c.dag.HasCommit(f.aPre) {
		t.Fatal("C should hold A's root as a graft and A's history above it")
	}
	host.mu.Lock()
	left := len(host.grafts)
	host.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d graft stages left in the session after the graft", left)
	}
}

// TestGraftPushIsCappedAndReleasedOnClose: a session pushing more than MaxGraftPushBytes is
// refused and its stage dropped; a push cut off mid-way is released when the session closes.
func TestGraftPushIsCappedAndReleasedOnClose(t *testing.T) {
	f := newGraftFixture(t, "graft-push-cap", unrelatedChain())
	host := NewV2Host(wire.NewCodec(wire.EncodingJSON), V2HostConfig{NodeID: "host", Namespaces: f.c, MaxGraftPushBytes: 1_500}, auth.AllowAll, auth.EmptyContext)
	if ack, ok := hostCall(t, host, wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, 1), NodeID: "pusher", Protocol: wire.SyncProtocolVersion, Namespaces: []string{f.ns},
	}).(wire.SyncHelloAckMessage); !ok || !ack.Accepted {
		t.Fatalf("hello: %+v", ack)
	}
	aEnv, err := f.a.Env(f.ns, false)
	if err != nil {
		t.Fatal(err)
	}
	cid := 2
	push := func(after string) (wire.Message, wire.SnapshotPageMessage) {
		page, err := snapshotPage(aEnv, f.root, after, 300)
		if err != nil {
			t.Fatal(err)
		}
		cid++
		return hostCall(t, host, wire.GraftPushMessage{H: header(wire.MsgGraftPush, cid), Page: page}), page
	}
	stages := func() int {
		host.mu.Lock()
		defer host.mu.Unlock()
		return len(host.grafts)
	}
	after := ""
	var refused *wire.PeerErrorMessage
	for i := 0; i < 50 && refused == nil; i++ {
		reply, page := push(after)
		if pe, ok := reply.(wire.PeerErrorMessage); ok {
			refused = &pe
			break
		}
		if page.Done {
			t.Fatal("the whole state fit under the cap: the fixture is too small")
		}
		after = page.Next
	}
	if refused == nil || !strings.Contains(refused.Message, "exceeds 1500 bytes") {
		t.Fatalf("a push over the cap should be refused, got %+v", refused)
	}
	if stages() != 0 {
		t.Fatal("a refused push left its stage in the session")
	}
	if f.c.side(f.ns).dag.HasCommit(f.root) {
		t.Fatal("a refused push grafted the root")
	}

	// A push cut off part-way: its stage lives until the session closes.
	if _, page := push(""); page.Done {
		t.Fatal("expected a partial push")
	}
	host.mu.Lock()
	var stage *graftStage
	for _, s := range host.grafts {
		stage = s
	}
	host.mu.Unlock()
	if stage == nil {
		t.Fatal("the partial push was not staged")
	}
	host.Close()
	if stages() != 0 || stage.spool.f != nil {
		t.Fatal("Close did not release the partial push's staging file")
	}
}
