package peersync

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// A host caps the page it builds whatever MaxBytes the requester names: a FETCH_REQUEST asking for
// a page of a gigabyte used to be served the whole history in one page, built in memory.
func TestHostCapsTheRequestedPageBytes(t *testing.T) {
	old := maxServedPageBytes
	maxServedPageBytes = 2_000
	defer func() { maxServedPageBytes = old }()

	ns := "app/served-page"
	remote := newTestNamespaces(t, ns)
	g, _ := remote.side(ns).dag.Head()
	tip := chain(t, remote.side(ns), ns, g, 40, "r")
	host := newHost(remote)
	ack, ok := hostCall(t, host, wire.SyncHelloMessage{H: header(wire.MsgSyncHello, 1), NodeID: "peer",
		Protocol: wire.SyncProtocolVersion, Capabilities: ClientCapabilities, Namespaces: []string{ns}}).(wire.SyncHelloAckMessage)
	if !ok || !ack.Accepted {
		t.Fatalf("hello: %+v", ack)
	}
	p, ok := hostCall(t, host, wire.FetchRequestMessage{H: header(wire.MsgFetchRequest, 2), Namespace: ns,
		Wants: []codec.Hash{tip.Hash}, Haves: []codec.Hash{g}, MaxBytes: 1 << 30}).(wire.PackPageMessage)
	if !ok {
		t.Fatal("no PACK_PAGE")
	}
	if p.Done || len(p.Commits) == 0 || len(p.Commits) >= 40 {
		t.Fatalf("asked for 1 GiB, served %d of 40 commits (done=%v); want a page cut at the host's cap", len(p.Commits), p.Done)
	}
}
