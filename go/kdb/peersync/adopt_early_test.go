package peersync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestV2PullAdoptsALongRangeInChunks: a pull fast-forwarding main over many pages moves main and
// logs the commits a chunk at a time while the transfer runs, rather than holding the whole range
// until its last page. The outcome is the one a single adoption gives: main at the remote head,
// every commit logged exactly once, parents before children.
func TestV2PullAdoptsALongRangeInChunks(t *testing.T) {
	ns := "app/v2-adopt-chunks"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	tip := chain(t, r, ns, g, 300, "r")

	var mu sync.Mutex
	logged := map[codec.Hash]int{}
	var order []codec.Hash
	local.persistAsync = func(c document.Commit) (func() error, error) {
		mu.Lock()
		defer mu.Unlock()
		logged[c.Hash]++
		order = append(order, c.Hash)
		return func() error { return nil }, nil
	}
	// How much was logged, and where main was, when the host served its last page.
	var loggedBeforeLast atomic.Int32
	var headBeforeLast atomic.Value
	v2Hub(t, "hub-v2-adopt-chunks", newHost(remote), func(m wire.Message) bool {
		if p, ok := m.(wire.PackPageMessage); ok && p.Done {
			mu.Lock()
			loggedBeforeLast.Store(int32(len(order)))
			mu.Unlock()
			h, _ := local.side(ns).dag.Head()
			headBeforeLast.Store(h)
		}
		return true
	})
	res := syncV2(t, "hub-v2-adopt-chunks", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull,
		PageBytes: 4_000, AdoptEveryBytes: 10_000})
	got := res.Namespaces[0]
	if got.Pulled != 300 {
		t.Fatalf("pulled %d, want 300", got.Pulled)
	}
	if got.EarlyAdoptions < 2 {
		t.Fatalf("expected the range adopted in several chunks, got %d early adoption(s)", got.EarlyAdoptions)
	}
	if mustHead(t, local.side(ns)) != tip.Hash {
		t.Fatal("local main did not reach the remote head")
	}
	if n := loggedBeforeLast.Load(); n == 0 || n >= 300 {
		t.Fatalf("expected part of the range logged before the last page, got %d of 300", n)
	}
	if h := headBeforeLast.Load().(codec.Hash); h == g {
		t.Fatal("main had not moved before the last page")
	}
	if len(logged) != 300 || len(order) != 300 {
		t.Fatalf("expected each of 300 commits logged once, got %d distinct in %d calls", len(logged), len(order))
	}
	pos := map[codec.Hash]int{}
	for i, h := range order {
		pos[h] = i
	}
	for _, h := range order {
		c, err := local.side(ns).dag.GetCommitOrThrow(h)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range c.ParentHashes {
			if i, ok := pos[p]; ok && i > pos[h] {
				t.Fatalf("commit %s logged before its parent %s", h.Hex()[:8], p.Hex()[:8])
			}
		}
	}
}

// TestV2PullAdoptsOnlyAtTheEndWhenDisabled: a negative AdoptEveryBytes keeps the old behaviour -
// nothing is logged until the whole range has arrived.
func TestV2PullAdoptsOnlyAtTheEndWhenDisabled(t *testing.T) {
	ns := "app/v2-adopt-end"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	tip := chain(t, r, ns, g, 100, "r")
	var logged atomic.Int32
	local.persistAsync = func(document.Commit) (func() error, error) {
		logged.Add(1)
		return func() error { return nil }, nil
	}
	var loggedBeforeLast atomic.Int32
	v2Hub(t, "hub-v2-adopt-end", newHost(remote), func(m wire.Message) bool {
		if p, ok := m.(wire.PackPageMessage); ok && p.Done {
			loggedBeforeLast.Store(logged.Load())
		}
		return true
	})
	res := syncV2(t, "hub-v2-adopt-end", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull,
		PageBytes: 4_000, AdoptEveryBytes: -1})
	if res.Namespaces[0].EarlyAdoptions != 0 || loggedBeforeLast.Load() != 0 {
		t.Fatalf("expected no early adoption, got %d (%d logged before the last page)", res.Namespaces[0].EarlyAdoptions, loggedBeforeLast.Load())
	}
	if mustHead(t, local.side(ns)) != tip.Hash || logged.Load() != 100 {
		t.Fatalf("expected main at the remote head with 100 commits logged, got %d", logged.Load())
	}
}

// TestAdoptFastForwardLeavesADivergenceAlone: when main has a commit of its own the fetched range
// does not contain, an early adoption must do nothing - no merge with a partial range, no head
// move - and leave the decision to the adoption at the end, which merges once.
func TestAdoptFastForwardLeavesADivergenceAlone(t *testing.T) {
	ns := "app/adopt-ff-diverged"
	local, remote := forkTwoSides(t, ns)
	g, _ := remote.dag.Head()
	remoteTip := chain(t, remote, ns, g, 5, "r")
	localTip := chain(t, local, ns, g, 1, "l")

	missing := mustMissing(t, remote, remoteTip.Hash, []codec.Hash{g})
	env := IngestEnv{DAG: local.dag, Storage: local.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution: ResolutionOptions{Policy: transaction.ConflictPolicyLastWrite}}
	if _, err := StoreCommits(env, missing, nil); err != nil {
		t.Fatal(err)
	}
	res, err := AdoptFastForward(env, remoteTip.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome.Kind != OutcomeNoOp || mustHead(t, local) != localTip.Hash {
		t.Fatalf("expected a no-op leaving main at the local commit, got %v at %s", res.Outcome.Kind, mustHead(t, local).Hex()[:8])
	}
	res, err = Adopt(env, remoteTip.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome.Kind != OutcomeMerged {
		t.Fatalf("expected the adoption at the end to merge, got %v", res.Outcome.Kind)
	}
}

// TestV2EarlyAdoptionSurvivesAnInterruptedPull: a pull cut off part-way has already moved main to
// - and logged - a prefix of the range, and the resumed pull fetches only the rest.
func TestV2EarlyAdoptionSurvivesAnInterruptedPull(t *testing.T) {
	ns := "app/v2-adopt-resume"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	tip := chain(t, r, ns, g, 300, "r")
	var pages atomic.Int32
	cut := true
	v2Hub(t, "hub-v2-adopt-resume", newHost(remote), func(m wire.Message) bool {
		if _, ok := m.(wire.PackPageMessage); ok && cut && pages.Add(1) > 20 {
			return false
		}
		return true
	})
	cfg := V2ClientConfig{PeerURI: "memory://hub-v2-adopt-resume", Local: local, NodeID: "client", Namespaces: []string{ns},
		Mode: SyncPull, PageBytes: 2_000, AdoptEveryBytes: 6_000, Timeout: 300 * time.Millisecond}
	first, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	partial := first.Namespaces[0]
	if partial.Err == nil || partial.EarlyAdoptions == 0 {
		t.Fatalf("expected an interrupted pull that adopted early, got early=%d err=%v", partial.EarlyAdoptions, partial.Err)
	}
	mid := mustHead(t, local.side(ns))
	if mid == g || mid == tip.Hash {
		t.Fatal("expected main part-way along the range after the interruption")
	}
	cut = false
	cfg.ExtraHaves = map[string][]codec.Hash{ns: partial.ReceivedTips}
	second, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err != nil || second.Namespaces[0].Err != nil {
		t.Fatalf("resume: %v %v", err, second.Namespaces[0].Err)
	}
	if got := partial.Pulled + second.Namespaces[0].Pulled; got != 300 {
		t.Fatalf("resume refetched: %d + %d commits for a 300-commit history", partial.Pulled, second.Namespaces[0].Pulled)
	}
	if mustHead(t, local.side(ns)) != tip.Hash {
		t.Fatal("resumed pull did not reach the remote head")
	}
}
