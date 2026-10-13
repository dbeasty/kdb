package peersync

import (
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pushHub serves host on a hub and, when the last REF_UPDATE page of a push arrives, records how
// many commits the host had logged by then and where its main was.
func pushHub(t *testing.T, hub string, host *V2Host, remote *testNamespaces, ns string, logged func() int) (beforeLast func() (int, codec.Hash)) {
	t.Helper()
	w := wire.NewCodec(wire.EncodingJSON)
	var mu sync.Mutex
	n, head := -1, codec.Hash{}
	h := stream.HubFor(hub)
	h.ServerHandler = func(frame []byte) {
		if in, err := w.Decode(frame); err == nil {
			if u, ok := in.(wire.RefUpdateMessage); ok && !u.More {
				mu.Lock()
				n = logged()
				head, _ = remote.side(ns).dag.Head()
				mu.Unlock()
			}
		}
		out, err := host.HandleFrame(frame)
		if err != nil || out == nil {
			return
		}
		h.ServerSend(out)
	}
	t.Cleanup(func() { h.ServerHandler = nil })
	return func() (int, codec.Hash) { mu.Lock(); defer mu.Unlock(); return n, head }
}

func loggingNamespaces(t *testing.T, ns string) (*testNamespaces, func() []codec.Hash) {
	p := newTestNamespaces(t, ns)
	var mu sync.Mutex
	var order []codec.Hash
	p.persistAsync = func(c document.Commit) (func() error, error) {
		mu.Lock()
		order = append(order, c.Hash)
		mu.Unlock()
		return func() error { return nil }, nil
	}
	return p, func() []codec.Hash { mu.Lock(); defer mu.Unlock(); return append([]codec.Hash(nil), order...) }
}

// TestV2PushAdoptsALongRangeInChunks: a host receiving a long push of main adopts and logs it a
// chunk at a time, instead of holding every page until the last one - and ends exactly where a
// single adoption would: main at the pushed head, every commit logged once, parents first.
func TestV2PushAdoptsALongRangeInChunks(t *testing.T) {
	ns := "app/v2-push-chunks"
	remote, logged := loggingNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	g, _ := local.side(ns).dag.Head()
	tip := chain(t, local.side(ns), ns, g, 300, "l")
	host := newHost(remote)
	host.cfg.AdoptEveryBytes = 10_000
	beforeLast := pushHub(t, "hub-v2-push-chunks", host, remote, ns, func() int { return len(logged()) })
	syncV2(t, "hub-v2-push-chunks", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPush, PageBytes: 4_000})
	if mustHead(t, remote.side(ns)) != tip.Hash {
		t.Fatal("the host's main did not reach the pushed head")
	}
	n, head := beforeLast()
	if n <= 0 || n >= 300 {
		t.Fatalf("expected part of the push logged before its last page, got %d of 300", n)
	}
	if head == g {
		t.Fatal("the host's main had not moved before the last page")
	}
	order := logged()
	seen := map[codec.Hash]int{}
	for i, h := range order {
		if _, dup := seen[h]; dup {
			t.Fatalf("commit %s logged twice", h.Hex()[:8])
		}
		seen[h] = i
	}
	if len(seen) != 300 {
		t.Fatalf("%d commits logged, want 300", len(seen))
	}
	for _, h := range order {
		c, err := remote.side(ns).dag.GetCommitOrThrow(h)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range c.ParentHashes {
			if i, ok := seen[p]; ok && i > seen[h] {
				t.Fatalf("commit %s logged before its parent", h.Hex()[:8])
			}
		}
	}
}

// TestV2PushAdoptsOnlyAtTheEndWhenDisabled: a negative AdoptEveryBytes keeps the old behaviour.
func TestV2PushAdoptsOnlyAtTheEndWhenDisabled(t *testing.T) {
	ns := "app/v2-push-end"
	remote, logged := loggingNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	g, _ := local.side(ns).dag.Head()
	tip := chain(t, local.side(ns), ns, g, 100, "l")
	host := newHost(remote)
	host.cfg.AdoptEveryBytes = -1
	beforeLast := pushHub(t, "hub-v2-push-end", host, remote, ns, func() int { return len(logged()) })
	syncV2(t, "hub-v2-push-end", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPush, PageBytes: 4_000})
	if n, _ := beforeLast(); n != 0 {
		t.Fatalf("expected nothing logged before the last page, got %d", n)
	}
	if mustHead(t, remote.side(ns)) != tip.Hash || len(logged()) != 100 {
		t.Fatalf("expected main at the pushed head with 100 commits logged, got %d", len(logged()))
	}
}

// TestV2PushIntoADivergedHostMergesOnce: when the host's main has a commit of its own, early
// adoption must not merge with a partial range; the push's last page merges once, as before.
func TestV2PushIntoADivergedHostMergesOnce(t *testing.T) {
	ns := "app/v2-push-diverged"
	remote, _ := loggingNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	g, _ := local.side(ns).dag.Head()
	chain(t, local.side(ns), ns, g, 200, "l")
	own := chain(t, remote.side(ns), ns, g, 1, "r")
	host := newHost(remote)
	host.cfg.AdoptEveryBytes = 5_000
	beforeLast := pushHub(t, "hub-v2-push-diverged", host, remote, ns, func() int { return 0 })
	syncV2(t, "hub-v2-push-diverged", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPush, PageBytes: 4_000})
	if _, head := beforeLast(); head != own.Hash {
		t.Fatal("early adoption moved a diverged host's main before the last page")
	}
	h := mustHead(t, remote.side(ns))
	c, err := remote.side(ns).dag.GetCommitOrThrow(h)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ParentHashes) != 2 {
		t.Fatalf("expected the host to end on one merge, got a commit with %d parents", len(c.ParentHashes))
	}
}
