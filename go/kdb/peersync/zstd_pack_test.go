package peersync

import (
	"slices"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// zstdHub serves host on an in-memory hub, recording whether each PACK_PAGE and REF_UPDATE went
// compressed. With oldHost set it plays a host from before SyncCapZstdPack: the capability is
// stripped from its hello ack, as a build that never knew it would answer.
func zstdHub(t *testing.T, hub string, host *V2Host, oldHost bool) (pages, pushes func() []bool) {
	t.Helper()
	w := wire.NewCodec(wire.EncodingJSON)
	var mu sync.Mutex
	var pageZ, pushZ []bool
	h := stream.HubFor(hub)
	h.ServerHandler = func(frame []byte) {
		if in, err := w.Decode(frame); err == nil {
			if u, ok := in.(wire.RefUpdateMessage); ok {
				mu.Lock()
				pushZ = append(pushZ, u.Zstd)
				mu.Unlock()
			}
		}
		out, err := host.HandleFrame(frame)
		if err != nil || out == nil {
			return
		}
		msg, err := w.Decode(out)
		if err == nil {
			switch m := msg.(type) {
			case wire.PackPageMessage:
				mu.Lock()
				pageZ = append(pageZ, m.Zstd)
				mu.Unlock()
			case wire.SyncHelloAckMessage:
				if oldHost {
					m.Capabilities = slices.DeleteFunc(slices.Clone(m.Capabilities), func(c string) bool { return c == wire.SyncCapZstdPack })
					if out, err = w.Encode(m); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}
		h.ServerSend(out)
	}
	t.Cleanup(func() { h.ServerHandler = nil })
	get := func(xs *[]bool) func() []bool {
		return func() []bool { mu.Lock(); defer mu.Unlock(); return slices.Clone(*xs) }
	}
	return get(&pageZ), get(&pushZ)
}

// TestV2PacksGoCompressedBetweenNodesThatBothSupportIt: two current nodes negotiate
// SyncCapZstdPack, and every page and push between them is compressed.
func TestV2PacksGoCompressedBetweenNodesThatBothSupportIt(t *testing.T) {
	ns := "app/zstd-both"
	remote, local := newTestNamespaces(t, ns), newTestNamespaces(t, ns)
	g, _ := remote.side(ns).dag.Head()
	tip := chain(t, remote.side(ns), ns, g, 120, "r")
	pages, _ := zstdHub(t, "hub-zstd-both", newHost(remote), false)
	syncV2(t, "hub-zstd-both", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PageBytes: 4_000})
	if mustHead(t, local.side(ns)) != tip.Hash {
		t.Fatal("pull did not reach the remote head")
	}
	got := pages()
	if len(got) < 2 || slices.Contains(got, false) {
		t.Fatalf("expected every page compressed, got %v", got)
	}

	// And the other way: local writes on top and pushes.
	ltip := chain(t, local.side(ns), ns, tip.Hash, 60, "l")
	_, pushes := zstdHub(t, "hub-zstd-push", newHost(remote), false)
	syncV2(t, "hub-zstd-push", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPush, PageBytes: 4_000})
	if mustHead(t, remote.side(ns)) != ltip.Hash {
		t.Fatal("push did not reach the remote")
	}
	if p := pushes(); len(p) < 2 || slices.Contains(p, false) {
		t.Fatalf("expected every push compressed, got %v", p)
	}
}

// TestV2OldHostGetsUncompressedPushes: a host that did not advertise SyncCapZstdPack - any build
// before it - is pushed plain payloads it can read, and the sync completes.
func TestV2OldHostGetsUncompressedPushes(t *testing.T) {
	ns := "app/zstd-oldhost"
	remote, local := newTestNamespaces(t, ns), newTestNamespaces(t, ns)
	g, _ := local.side(ns).dag.Head()
	ltip := chain(t, local.side(ns), ns, g, 80, "l")
	pages, pushes := zstdHub(t, "hub-zstd-oldhost", newHost(remote), true)
	syncV2(t, "hub-zstd-oldhost", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncBoth, PageBytes: 4_000})
	if mustHead(t, remote.side(ns)) != ltip.Hash {
		t.Fatal("push did not reach the old host")
	}
	if p := pushes(); len(p) == 0 || slices.Contains(p, true) {
		t.Fatalf("an old host was pushed compressed payloads: %v", p)
	}
	_ = pages
}

// TestV2OldClientGetsUncompressedPages: a client that did not offer SyncCapZstdPack in its hello is
// served plain pages.
func TestV2OldClientGetsUncompressedPages(t *testing.T) {
	ns := "app/zstd-oldclient"
	remote := newTestNamespaces(t, ns)
	g, _ := remote.side(ns).dag.Head()
	tip := chain(t, remote.side(ns), ns, g, 40, "r")
	host := newHost(remote)
	oldCaps := slices.DeleteFunc(slices.Clone(ClientCapabilities), func(c string) bool { return c == wire.SyncCapZstdPack })
	ack, ok := hostCall(t, host, wire.SyncHelloMessage{H: header(wire.MsgSyncHello, 1), NodeID: "old",
		Protocol: wire.SyncProtocolVersion, Capabilities: oldCaps, Namespaces: []string{ns}}).(wire.SyncHelloAckMessage)
	if !ok || !ack.Accepted || slices.Contains(ack.Capabilities, wire.SyncCapZstdPack) {
		t.Fatalf("hello: %+v", ack)
	}
	p, ok := hostCall(t, host, wire.FetchRequestMessage{H: header(wire.MsgFetchRequest, 2), Namespace: ns,
		Wants: []codec.Hash{tip.Hash}, Haves: []codec.Hash{g}}).(wire.PackPageMessage)
	if !ok || p.Zstd || len(p.Commits) != 40 {
		t.Fatalf("an old client got zstd=%v with %d commits", p.Zstd, len(p.Commits))
	}
}
