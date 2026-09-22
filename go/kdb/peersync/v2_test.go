package peersync

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/wire"
)

// testNamespaces is a NamespaceProvider over in-memory sides.
type testNamespaces struct {
	t     *testing.T
	mu    sync.Mutex
	sides map[string]side
	// installed, when set, is every env's SnapshotInstalled.
	installed func(document.Commit) error
}

func newTestNamespaces(t *testing.T, names ...string) *testNamespaces {
	p := &testNamespaces{t: t, sides: map[string]side{}}
	for _, ns := range names {
		p.sides[ns] = newSide(t, ns)
	}
	return p
}

func (p *testNamespaces) List() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for ns := range p.sides {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

func (p *testNamespaces) Env(ns string, create bool) (IngestEnv, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.sides[ns]
	if !ok {
		if !create {
			return IngestEnv{}, fmt.Errorf("no namespace %s", ns)
		}
		s = newSide(p.t, ns)
		p.sides[ns] = s
	}
	return IngestEnv{DAG: s.dag, Storage: s.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution:        ResolutionOptions{Policy: transaction.ConflictPolicyLastWrite},
		SnapshotInstalled: p.installed}, nil
}

func (p *testNamespaces) side(ns string) side {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sides[ns]
}

// v2Hub serves a v2 host on an in-memory hub, with an optional frame filter for tests that need
// to drop or count replies.
func v2Hub(t *testing.T, hub string, host *V2Host, filter func(reply wire.Message) bool) {
	t.Helper()
	w := wire.NewCodec(wire.EncodingJSON)
	h := stream.HubFor(hub)
	h.ServerHandler = func(frame []byte) {
		out, err := host.HandleFrame(frame)
		if err != nil || out == nil {
			return
		}
		if filter != nil {
			msg, _ := w.Decode(out)
			if !filter(msg) {
				return
			}
		}
		h.ServerSend(out)
	}
	t.Cleanup(func() { h.ServerHandler = nil })
}

func syncV2(t *testing.T, hub string, local *testNamespaces, cfg V2ClientConfig) V2Result {
	t.Helper()
	cfg.PeerURI = "memory://" + hub
	cfg.Local = local
	if cfg.NodeID == "" {
		cfg.NodeID = "client"
	}
	res, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatalf("SyncV2: %v", err)
	}
	for _, ns := range res.Namespaces {
		if ns.Err != nil {
			t.Fatalf("namespace %s: %v", ns.Namespace, ns.Err)
		}
	}
	return res
}

func newHost(remote *testNamespaces) *V2Host {
	return NewV2Host(wire.NewCodec(wire.EncodingJSON), V2HostConfig{NodeID: "host", Namespaces: remote}, auth.AllowAll, auth.EmptyContext)
}

func TestV2SyncsAllBranchesAndTags(t *testing.T) {
	ns := "app/v2-refs"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	genesis, _ := r.dag.Head()
	mainTip := chain(t, r, ns, genesis, 3, "main")
	featureBase := mainTip.Hash
	feature := writeDoc(t, r, ns, featureBase, newUUID(t), `{"on":"feature"}`)
	if _, err := r.dag.CreateBranch("feature", feature.Hash); err != nil {
		t.Fatal(err)
	}
	setHead(t, r, mainTip.Hash) // writeDoc advances main; the feature commit belongs to feature only
	if _, err := r.dag.CreateTag("v1", mainTip.Hash, "release"); err != nil {
		t.Fatal(err)
	}
	v2Hub(t, "hub-v2-refs", newHost(remote), nil)

	res := syncV2(t, "hub-v2-refs", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull})
	l := local.side(ns)
	if h, _ := l.dag.Head(); h != mainTip.Hash {
		rh, _ := r.dag.Head()
		t.Fatalf("main not fast-forwarded: local %s remote %s want %s; result %+v", h.Hex(), rh.Hex(), mainTip.Hash.Hex(), res)
	}
	if b, ok := l.dag.GetBranch("feature"); !ok || b.HeadHash != feature.Hash {
		t.Fatalf("branch feature not replicated: %+v", b)
	}
	if tag, ok := l.dag.GetTag("v1"); !ok || tag.CommitHash != mainTip.Hash {
		t.Fatalf("tag v1 not replicated: %+v", tag)
	}
	if got := res.Namespaces[0].Local["branch:feature"]; got != wire.RefCreated {
		t.Fatalf("expected branch created, got %s", got)
	}
}

func TestV2MultipleNamespacesOneSession(t *testing.T) {
	remote := newTestNamespaces(t, "site/berlin/orders", "site/berlin/stock", "site/paris/orders")
	for _, ns := range remote.List() {
		s := remote.side(ns)
		g, _ := s.dag.Head()
		chain(t, s, ns, g, 2, ns)
	}
	local := newTestNamespaces(t)
	v2Hub(t, "hub-v2-multi", newHost(remote), nil)

	res := syncV2(t, "hub-v2-multi", local, V2ClientConfig{Namespaces: []string{"site/berlin/*"}, Mode: SyncPull, CreateLocal: true})
	if len(res.Namespaces) != 2 {
		t.Fatalf("expected the two berlin namespaces, got %d", len(res.Namespaces))
	}
	if got := local.List(); strings.Join(got, ",") != "site/berlin/orders,site/berlin/stock" {
		t.Fatalf("local namespaces after sync: %v", got)
	}
	for _, ns := range local.List() {
		rh, _ := remote.side(ns).dag.Head()
		lh, _ := local.side(ns).dag.Head()
		if rh != lh {
			t.Fatalf("%s not in sync", ns)
		}
	}
}

func TestV2PagesByBytes(t *testing.T) {
	ns := "app/v2-pages"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	parent := g
	for i := 0; i < 200; i++ {
		c := writeDoc(t, r, ns, parent, newUUID(t), fmt.Sprintf(`{"i":%d,"pad":%q}`, i, strings.Repeat("x", 2000)))
		parent = c.Hash
	}
	setHead(t, r, parent)
	var pages atomic.Int32
	v2Hub(t, "hub-v2-pages", newHost(remote), func(m wire.Message) bool {
		if _, ok := m.(wire.PackPageMessage); ok {
			pages.Add(1)
		}
		return true
	})
	res := syncV2(t, "hub-v2-pages", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PageBytes: 50_000})
	if res.Namespaces[0].Pulled != 200 {
		t.Fatalf("pulled %d, want 200", res.Namespaces[0].Pulled)
	}
	if n := pages.Load(); n < 8 {
		t.Fatalf("expected the 400KB transfer in ~50KB pages, got %d page(s)", n)
	}
	if h, _ := local.side(ns).dag.Head(); h != parent {
		t.Fatal("local main did not reach the remote head")
	}
}

func TestV2TagConflictReported(t *testing.T) {
	ns := "app/v2-tag-conflict"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	g, _ := remote.side(ns).dag.Head()
	rc := chain(t, remote.side(ns), ns, g, 1, "r")
	lc := chain(t, local.side(ns), ns, g, 1, "l")
	if _, err := remote.side(ns).dag.CreateTag("v1", rc.Hash, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := local.side(ns).dag.CreateTag("v1", lc.Hash, ""); err != nil {
		t.Fatal(err)
	}
	v2Hub(t, "hub-v2-tagc", newHost(remote), nil)
	res := syncV2(t, "hub-v2-tagc", local, V2ClientConfig{Namespaces: []string{ns}})
	sides := map[string]bool{}
	for _, c := range res.Namespaces[0].Conflicts {
		if c.Ref == "tag:v1" {
			sides[c.Side] = true
		}
	}
	if !sides["local"] || !sides["remote"] {
		t.Fatalf("expected the tag conflict reported on both sides, got %+v", res.Namespaces[0].Conflicts)
	}
	if tag, _ := local.side(ns).dag.GetTag("v1"); tag.CommitHash != lc.Hash {
		t.Fatal("a conflicting tag was moved")
	}
}

// TestV2BidirectionalConvergesAfterDivergence: both sides wrote; one sync leaves both on the
// same (deterministic) merge.
func TestV2BidirectionalConvergesAfterDivergence(t *testing.T) {
	ns := "app/v2-diverged"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	g, _ := remote.side(ns).dag.Head()
	shared := newUUID(t)
	writeAtHead(t, remote.side(ns), ns, shared, `{"v":"remote"}`)
	chain(t, remote.side(ns), ns, mustHead(t, remote.side(ns)), 3, "r")
	_ = g
	writeAtHead(t, local.side(ns), ns, shared, `{"v":"local"}`)
	chain(t, local.side(ns), ns, mustHead(t, local.side(ns)), 2, "l")
	v2Hub(t, "hub-v2-div", newHost(remote), nil)
	res := syncV2(t, "hub-v2-div", local, V2ClientConfig{Namespaces: []string{ns}})
	if res.Namespaces[0].Local["branch:main"] != wire.RefMerged || res.Namespaces[0].Remote["branch:main"] != wire.RefFastForwarded {
		t.Fatalf("expected a local merge then a remote fast-forward, got local=%v remote=%v",
			res.Namespaces[0].Local, res.Namespaces[0].Remote)
	}
	if mustHead(t, local.side(ns)) != mustHead(t, remote.side(ns)) {
		t.Fatal("heads differ after a bidirectional sync")
	}
}

func mustHead(t *testing.T, s side) codec.Hash {
	t.Helper()
	h, err := s.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestV2ClientFallsBackToV1: a peer that only speaks v1 answers SYNC_HELLO with UNSUPPORTED, and
// the client syncs main over v1 instead.
func TestV2ClientFallsBackToV1(t *testing.T) {
	ns := "app/v2-fallback"
	local := newTestNamespaces(t, ns)
	_, remote := forkTwoSides(t, ns)
	g, _ := remote.dag.Head()
	tip := chain(t, remote, ns, g, 4, "v1")
	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remote.dag, remote.storage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{NamespaceID: ns, NodeID: "old", TransportHub: "hub-v2-fallback", ApplyToStorage: true}); err != nil {
		t.Fatal(err)
	}
	defer host.Stop()
	res := syncV2(t, "hub-v2-fallback", local, V2ClientConfig{Namespaces: []string{ns}})
	if res.Protocol != 1 {
		t.Fatalf("expected a v1 fallback, got protocol %d", res.Protocol)
	}
	if mustHead(t, local.side(ns)) != tip.Hash {
		t.Fatal("fallback sync did not bring main up to date")
	}
}

// TestV2ResumeAfterInterruption: a pull cut off mid-transfer keeps what it stored, and a second
// sync naming the first one's received tips fetches only the rest.
func TestV2ResumeAfterInterruption(t *testing.T) {
	ns := "app/v2-resume"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	tip := chain(t, r, ns, g, 300, "r")
	var pages atomic.Int32
	cut := true
	v2Hub(t, "hub-v2-resume", newHost(remote), func(m wire.Message) bool {
		if _, ok := m.(wire.PackPageMessage); ok {
			if pages.Add(1) > 2 && cut {
				return false // the connection "drops": no reply
			}
		}
		return true
	})
	cfg := V2ClientConfig{PeerURI: "memory://hub-v2-resume", Local: local, NodeID: "client", Namespaces: []string{ns},
		Mode: SyncPull, PageBytes: 10_000, Timeout: 300 * time.Millisecond}
	first, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var partial NamespaceSyncResult
	if len(first.Namespaces) == 1 {
		partial = first.Namespaces[0]
	}
	if partial.Err == nil || partial.Pulled == 0 {
		t.Fatalf("expected an interrupted pull that stored something, got pulled=%d err=%v", partial.Pulled, partial.Err)
	}
	cut = false
	pagesBefore := pages.Load()
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
	_ = pagesBefore
}

// TestV2UngrantedNamespaceRefused: a frame naming a namespace the session was not granted is
// refused, not served.
func TestV2UngrantedNamespaceRefused(t *testing.T) {
	remote := newTestNamespaces(t, "a/one", "b/two")
	host := newHost(remote)
	w := wire.NewCodec(wire.EncodingJSON)
	hello, _ := w.Encode(wire.SyncHelloMessage{H: header(wire.MsgSyncHello, 1), NodeID: "c", Protocol: 2, Namespaces: []string{"a/*"}})
	if _, err := host.HandleFrame(hello); err != nil {
		t.Fatal(err)
	}
	fetch, _ := w.Encode(wire.FetchRequestMessage{H: header(wire.MsgFetchRequest, 2), Namespace: "b/two"})
	out, err := host.HandleFrame(fetch)
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := w.Decode(out)
	pe, ok := msg.(wire.PeerErrorMessage)
	if !ok {
		t.Fatalf("expected PEER_ERROR, got %T", msg)
	}
	var re *RemoteError
	_ = errors.As(nil, &re)
	if !strings.Contains(pe.Message, "not granted") {
		t.Fatalf("unexpected refusal: %s", pe.Message)
	}
}

func TestMatchNamespace(t *testing.T) {
	cases := []struct {
		pattern, ns string
		want        bool
	}{
		{"a/b", "a/b", true},
		{"a/*", "a/b", true},
		{"a/*", "a/b/c", false},
		{"a/**", "a/b/c", true},
		{"a/**", "a", true},
		{"**", "x/y", true},
		{"*/orders", "berlin/orders", true},
		{"*/orders", "berlin/stock", false},
	}
	for _, c := range cases {
		if got := MatchNamespace(c.pattern, c.ns); got != c.want {
			t.Errorf("MatchNamespace(%q, %q) = %v, want %v", c.pattern, c.ns, got, c.want)
		}
	}
	got := SelectNamespaces([]string{"site/**", "!site/*/audit"}, []string{"site/a/orders", "site/a/audit", "other"})
	if strings.Join(got, ",") != "site/a/orders" {
		t.Errorf("SelectNamespaces with exclusion: %v", got)
	}
}
