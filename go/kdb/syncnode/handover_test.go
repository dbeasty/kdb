package syncnode

import (
	"errors"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/server"
)

// write is put that reports the error instead of failing the test.
func (n testNode) write(ns string, body string) error {
	rt, err := n.set.Resolve(ns, true)
	if err != nil {
		return err
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	_, err = rt.Upsert(ns, id, body, auth.Principal{})
	return err
}

func phoneOf(t *testing.T, addr string) testNode {
	t.Helper()
	return newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: addr, Namespaces: []string{"zolik/m/9"},
		Mode: peersync.SyncBoth, Interval: time.Hour, CreateLocal: true,
	}}})
}

// TestApplicationDrivenHandover (G8): resuming a match on another device. The cloud is the match's
// home; a phone asks to take it over, the cloud's application decides, and on yes the phone
// writes and the cloud does not. When the phone holding it goes dark, the namespace's authority
// hands it to another device without it, and the dark phone's late write is refused by the fence.
func TestApplicationDrivenHandover(t *testing.T) {
	var asked []server.HandoverRequest
	cloud := newTestNode(t, "zolik/cloud", Config{HandoverPolicy: func(r server.HandoverRequest) error {
		asked = append(asked, r)
		if r.Reason != "resume" {
			return errors.New("the match is in progress")
		}
		return nil
	}})
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	addr := "tcp://" + ln.Addr().String()
	cloudID := cloud.primary.NodeID.String()
	cloud.put(t, "zolik/m/9", mustID(t), `{"move":"e4"}`)
	// The cloud is the authority of every match: it may force a handover.
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleAuthority, Node: cloudID}}}
	if err := cloud.node.Meta().SetResolution("zolik/m/*", chain); err != nil {
		t.Fatal(err)
	}
	if _, err := cloud.node.Handover("zolik/m/9", cloudID, "tcp://cloud"); err != nil {
		t.Fatal(err)
	}

	phoneA := phoneOf(t, addr)
	if err := phoneA.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phoneA.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	var notHome *server.NotHomeError
	if err := phoneA.write("zolik/m/9", `{"move":"e5"}`); !errors.As(err, &notHome) || notHome.Home.Node != cloudID {
		t.Fatalf("while the cloud is home the phone must not write, got %v", err)
	}

	// The application refuses, then approves.
	var refused *server.HandoverRefusedError
	if _, err := phoneA.node.RequestHome("cloud", "zolik/m/9", "tcp://phone-a", "grab", false); !errors.As(err, &refused) || refused.Reason != "the match is in progress" {
		t.Fatalf("the policy's refusal should reach the phone, got %v", err)
	}
	h, err := phoneA.node.RequestHome("cloud", "zolik/m/9", "tcp://phone-a", "resume", false)
	if err != nil {
		t.Fatal(err)
	}
	if h.Node != phoneA.primary.NodeID.String() || h.Fence < 2 {
		t.Fatalf("new home: %+v", h)
	}
	if len(asked) != 2 || asked[1].Node != h.Node || !asked[1].HasHome || asked[1].Current.Node != cloudID {
		t.Fatalf("the policy should see who asks and what it replaces: %+v", asked)
	}
	if err := phoneA.write("zolik/m/9", `{"move":"e5"}`); err != nil {
		t.Fatalf("the new home should write: %v", err)
	}
	if err := cloud.write("zolik/m/9", `{"move":"d4"}`); !errors.As(err, &notHome) {
		t.Fatalf("the old home must stop writing, got %v", err)
	}
	if _, err := phoneA.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}

	// Phone A goes dark holding the match (its replication stops); it keeps playing offline.
	phoneA.node.StopSync()
	lateBody := `{"move":"late"}`
	if err := phoneA.write("zolik/m/9", lateBody); err != nil {
		t.Fatal(err)
	}
	// Phone B asks the authority to force the move.
	phoneB := phoneOf(t, addr)
	if err := phoneB.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phoneB.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if _, err := phoneB.node.RequestHome("cloud", "zolik/m/9", "tcp://phone-b", "resume", false); !errors.As(err, &refused) || refused.Home.Node != h.Node {
		t.Fatalf("without force the cloud is not the home and should say who is, got %v", err)
	}
	hb, err := phoneB.node.RequestHome("cloud", "zolik/m/9", "tcp://phone-b", "resume", true)
	if err != nil {
		t.Fatal(err)
	}
	if hb.Node != phoneB.primary.NodeID.String() || hb.Fence <= h.Fence {
		t.Fatalf("forced home: %+v (was %+v)", hb, h)
	}
	if err := phoneB.write("zolik/m/9", `{"move":"f4"}`); err != nil {
		t.Fatalf("the forced home should write: %v", err)
	}
	// Phone A comes back: its late write is refused wherever it arrives.
	// The replicator is stopped; one sync still runs. The cloud refuses the push: its commit was
	// made by a home at a fence the namespace has since moved past.
	if _, err := phoneA.node.SyncNow("cloud"); err == nil || !strings.Contains(err.Error(), "fence") {
		t.Fatalf("the cloud should refuse the stale home's write by its fence, got %v", err)
	}
	if hasBody(t, cloud, "zolik/m/9", lateBody) {
		t.Fatal("the dark phone's late write was adopted on the cloud after the forced handover")
	}
	if !hasBody(t, cloud, "zolik/m/9", `{"move":"e5"}`) {
		t.Fatal("phone A's write made while it was home must stay")
	}
}

// hasBody reports whether a document of ns at head is exactly body.
func hasBody(t *testing.T, n testNode, ns, body string) bool {
	t.Helper()
	rt, ok := n.set.Get(ns)
	if !ok {
		return false
	}
	_, commit, ok, err := rt.Runtime.DAG.HeadCommit()
	if err != nil || !ok {
		t.Fatal(err)
	}
	found := false
	if err := rt.Runtime.Storage.ScanDocuments(ns, commit.DocumentTreeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			if d.JSON == body {
				found = true
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return found
}

// TestHomeRequestOnlyForItself: a node cannot ask for a namespace on another node's behalf, and a
// node without a handover policy hands nothing over.
func TestHomeRequestOnlyForItself(t *testing.T) {
	cloud := newTestNode(t, "zolik/cloud", Config{})
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	cloud.put(t, "zolik/m/9", mustID(t), `{"move":"e4"}`)
	phone := phoneOf(t, "tcp://"+ln.Addr().String())
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	s, err := phone.node.Replicator().OpenRepairSessionTo("cloud", "zolik/m/9")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	res, err := s.RequestHome("zolik/m/9", "someone-else", "tcp://x", "", false)
	if err != nil || res.Granted || !strings.Contains(res.Reason, "only for itself") {
		t.Fatalf("asking for another node: %+v %v", res, err)
	}
	res, err = s.RequestHome("zolik/m/9", phone.primary.NodeID.String(), "tcp://x", "", false)
	if err != nil || res.Granted || !strings.Contains(res.Reason, "no handover policy") {
		t.Fatalf("without a policy: %+v %v", res, err)
	}
}
