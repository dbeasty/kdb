package server

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// End-to-end resolver authority scenarios: two nodes, real peer sync, the authority acting
// through the server runtime - and, where it matters, a webhook receiver standing in for the
// application service.

// withAuthority gives both nodes a chain whose authority rule has the given pending mode and
// notifying node, replicated from a to b.
func withAuthority(t *testing.T, a, b metaNode, pending string, node string) peersync.ResolutionChain {
	t.Helper()
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleFieldMerge},
		{Kind: peersync.RuleAuthority, Pending: pending, Node: node},
	}}
	if err := a.store.SetResolution("app/data", chain); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the chain", func() bool { return b.data.ResolutionChainOf().Hash() == chain.Hash() })
	return chain
}

func mustSync(t *testing.T, from, to metaNode) peersync.NamespaceSyncResult {
	t.Helper()
	res, err := syncPatterns(t, from, to, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	ns := res.Namespaces[0]
	if ns.Err != nil || ns.ResolutionMismatch {
		t.Fatalf("sync: err=%v mismatch=%v", ns.Err, ns.ResolutionMismatch)
	}
	return ns
}

func authorityEntries(q *peersync.ConflictQueue) []peersync.ConflictEntry {
	var out []peersync.ConflictEntry
	for _, e := range q.List() {
		if e.Authority {
			out = append(out, e)
		}
	}
	return out
}

// receiver is a stand-in resolver authority's webhook endpoint.
type receiver struct {
	srv      *httptest.Server
	secret   string
	failures atomic.Int32 // answer 503 this many more times
	mu       sync.Mutex
	got      []ConflictNotification
	badSig   int
}

func newReceiver(t *testing.T, secret string) *receiver {
	r := &receiver{secret: secret}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if r.failures.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if !hmac.Equal([]byte(req.Header.Get("X-KDB-Signature")), []byte(SignConflictNotification(r.secret, body))) {
			r.badSig++
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var n ConflictNotification
		if err := json.Unmarshal(body, &n); err != nil || req.Header.Get("X-KDB-Delivery") != n.Conflict.ID {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.got = append(r.got, n)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) received() []ConflictNotification {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ConflictNotification(nil), r.got...)
}

func webhookFor(n metaNode, r *receiver) *ConflictWebhook {
	return &ConflictWebhook{URL: r.srv.URL, Secret: r.secret, set: n.data.namespaceSet(), node: n.data.NodeID.String(),
		Client: r.srv.Client(), hooked: map[*peersync.ConflictQueue]bool{}, stop: make(chan struct{})}
}

// Hold: the conflict stays unmerged on both nodes, only the named node tells the authority, the
// authority settles it on that node, and the settlement closes the conflict on the other.
func TestAuthorityHoldEndToEnd(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, a.data.NodeID.String())
	doc := conflictingWrites(t, a, b)
	headB := mustHeadOf(t, b.data)

	ns := mustSync(t, a, b)
	if len(ns.Conflicts) == 0 {
		t.Fatal("expected the divergence reported")
	}
	if mustHeadOf(t, b.data) != headB || docBody(t, a.data, doc) != `{"v":"from A"}` {
		t.Fatal("hold must merge on neither node")
	}
	held := authorityEntries(a.data.Conflicts)
	if len(held) != 1 || held[0].AuthorityNode != a.data.NodeID.String() || len(held[0].Details) != 1 {
		t.Fatalf("A should hold one authority entry with details: %+v", held)
	}
	if d := held[0].Details[0]; d.Base == nil || *d.Base != `{"v":"base"}` || d.IncomingOrigin.NodeID != b.data.NodeID {
		t.Fatalf("detail should carry the base and B as the incoming writer: %+v", d)
	}

	// Only A - the named node - notifies.
	rA, rB := newReceiver(t, "s3cret"), newReceiver(t, "s3cret")
	if n := webhookFor(a, rA).Pass(); n != 1 {
		t.Fatalf("A should deliver its entry, delivered %d", n)
	}
	if n := webhookFor(b, rB).Pass(); n != 0 || len(rB.received()) != 0 {
		t.Fatalf("B is not the notifying node and must stay quiet, delivered %d", n)
	}
	got := rA.received()[0]
	if got.Type != "kdb.conflict" || got.Namespace != "app/data" || got.Node != a.data.NodeID.String() || got.Conflict.ID != held[0].ID {
		t.Fatalf("unexpected notification %+v", got)
	}
	if e, _ := a.data.Conflicts.Get(held[0].ID); !e.Delivered {
		t.Fatal("an acknowledged delivery must be recorded")
	}
	if n := webhookFor(a, rA).Pass(); n != 0 {
		t.Fatal("a delivered entry must not be sent again")
	}

	// The authority takes B's value.
	if _, err := a.data.ResolveConflict(held[0].ID, map[codec.UUID]peersync.Choice{doc: {Take: "remote"}}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	mustSync(t, a, b)
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("the settlement did not converge the nodes")
	}
	for name, n := range map[string]metaNode{"A": a, "B": b} {
		if got := docBody(t, n.data, doc); got != `{"v":"from B"}` {
			t.Fatalf("%s holds %s, want the authority's choice", name, got)
		}
		if es := authorityEntries(n.data.Conflicts); len(es) != 0 {
			t.Fatalf("%s still holds %+v", name, es)
		}
	}
}

// Provisional: the merge happens at once by last write; both nodes record the same entry from
// the merge; the authority overrules it with an ordinary write that closes the entry everywhere.
func TestAuthorityProvisionalEndToEnd(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingProvisional, "")
	doc := conflictingWrites(t, a, b)

	mustSync(t, a, b)
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("a provisional merge should converge the nodes at once")
	}
	if got := docBody(t, a.data, doc); got != `{"v":"from B"}` {
		t.Fatalf("provisional value should be the later write, got %s", got)
	}
	ea, eb := authorityEntries(a.data.Conflicts), authorityEntries(b.data.Conflicts)
	if len(ea) != 1 || len(eb) != 1 || ea[0].ID != eb[0].ID || ea[0].Kind != peersync.ConflictProvisional {
		t.Fatalf("both nodes should record the same provisional entry: A %+v B %+v", ea, eb)
	}

	// With no node named, every node notifies - the receiver deduplicates on the delivery id.
	r := newReceiver(t, "k")
	webhookFor(a, r).Pass()
	webhookFor(b, r).Pass()
	if got := r.received(); len(got) != 2 || got[0].Conflict.ID != got[1].Conflict.ID {
		t.Fatalf("expected the same entry from both nodes, got %+v", got)
	}

	// The authority overrules: A's value after all.
	c, err := a.data.ResolveConflict(ea[0].ID, map[codec.UUID]peersync.Choice{doc: {Take: "remote"}}, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := peersync.ResolvedEntry(c); !ok || id != ea[0].ID {
		t.Fatalf("the resolution commit should name its entry, message %q", c.Message)
	}
	mustSync(t, a, b)
	for name, n := range map[string]metaNode{"A": a, "B": b} {
		if got := docBody(t, n.data, doc); got != `{"v":"from A"}` {
			t.Fatalf("%s holds %s, want the overruled value", name, got)
		}
		if es := authorityEntries(n.data.Conflicts); len(es) != 0 {
			t.Fatalf("%s still holds %+v", name, es)
		}
	}
}

// Confirming a provisional decision also closes it everywhere, and changes nothing.
func TestAuthorityConfirmsProvisionalDecision(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingProvisional, "")
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)
	e := authorityEntries(b.data.Conflicts)[0]
	if _, err := b.data.ResolveConflict(e.ID, map[codec.UUID]peersync.Choice{doc: {Take: "local"}}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	mustSync(t, a, b)
	if len(authorityEntries(a.data.Conflicts)) != 0 || docBody(t, a.data, doc) != `{"v":"from B"}` {
		t.Fatal("confirmation should close A's entry and keep the provisional value")
	}
}

// A provisional decision is refused once the document has moved on: the authority decided
// against a value that is gone.
func TestAuthorityResolutionRefusedWhenTheDocumentChanged(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingProvisional, "")
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)
	e := authorityEntries(a.data.Conflicts)[0]
	if _, err := a.data.Upsert("app/data", doc, `{"v":"later"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	_, err := a.data.ResolveConflict(e.ID, map[codec.UUID]peersync.Choice{doc: {Take: "remote"}}, auth.Principal{})
	var stale *ErrResolutionStale
	if !errors.As(err, &stale) || len(stale.Documents) != 1 {
		t.Fatalf("expected a stale refusal, got %v", err)
	}
	if got := docBody(t, a.data, doc); got != `{"v":"later"}` {
		t.Fatalf("the later write must survive, got %s", got)
	}
	if _, ok := a.data.Conflicts.Get(e.ID); !ok {
		t.Fatal("a refused resolution must leave the entry open")
	}
	// Missing choices are refused too.
	if _, err := a.data.ResolveConflict(e.ID, nil, auth.Principal{}); err == nil {
		t.Fatal("expected a refusal naming the undecided document")
	}
}

// In a namespace with an authority, settling needs "resolve": write alone is not enough, and a
// principal with both succeeds.
func TestAuthorityResolutionNeedsTheResolvePermission(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	doc := conflictingWrites(t, a, b)
	mustSync(t, a, b)
	e := authorityEntries(a.data.Conflicts)[0]

	engine, store := newTestRegistryAuthEngine(t)
	a.data.AuthEngine = engine
	if err := store.CreateRole("writer", []string{"write:app/data", "read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("resolver", []string{"write:app/data", "read:app/data", "resolve:app/data"}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []struct{ id, role string }{{"w", "writer"}, {"r", "resolver"}} {
		if err := store.CreateUser(u.id, "pw", []string{u.role}); err != nil {
			t.Fatal(err)
		}
	}
	login := func(id string) auth.Principal {
		p, err := engine.Authenticator().Authenticate(context.Background(), auth.Credentials{User: ptr(id), Password: ptr("pw")})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	choice := map[codec.UUID]peersync.Choice{doc: {Take: "local"}}
	var authz *AuthorizationError
	if _, err := a.data.ResolveConflict(e.ID, choice, login("w")); !errors.As(err, &authz) {
		t.Fatalf("a writer without resolve must be refused, got %v", err)
	}
	if err := a.data.DismissConflict(e.ID, login("w")); !errors.As(err, &authz) {
		t.Fatalf("dismissing needs resolve too, got %v", err)
	}
	if err := a.data.AckConflict(e.ID, login("w")); !errors.As(err, &authz) {
		t.Fatalf("acknowledging needs resolve too, got %v", err)
	}
	if _, err := a.data.ResolveConflict(e.ID, choice, login("r")); err != nil {
		t.Fatalf("the resolver should settle it: %v", err)
	}

	// Without an authority in the chain, commit rights are enough, as before.
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{}); err != nil {
		t.Fatal(err)
	}
	if err := a.data.authorizeResolve(login("w")); err != nil {
		t.Fatalf("without an authority a writer may resolve: %v", err)
	}
}

// Bulk: "the node that sent these is right about all of them", previewed first.
func TestResolveAllTakesOneSideForEveryMatchingConflict(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	d1 := conflictingWrites(t, a, b)
	d2 := conflictingWrites(t, a, b)
	mustSync(t, a, b)
	held := authorityEntries(a.data.Conflicts)
	if len(held) != 1 || len(held[0].Items) != 2 {
		t.Fatalf("expected one entry covering both documents: %+v", held)
	}

	preview, err := a.data.ResolveAll(ConflictFilter{Origin: b.data.NodeID.String()}, "remote", true, auth.Principal{})
	if err != nil || len(preview) != 1 || len(preview[0].Documents) != 2 || preview[0].CommitHex != "" {
		t.Fatalf("dry run should list the entry without settling it: %+v %v", preview, err)
	}
	if _, ok := a.data.Conflicts.Get(held[0].ID); !ok {
		t.Fatal("a dry run must not settle anything")
	}
	if none, _ := a.data.ResolveAll(ConflictFilter{Origin: a.data.NodeID.String()}, "remote", true, auth.Principal{}); len(none) != 0 {
		t.Fatalf("a filter naming a node that sent nothing must match nothing: %+v", none)
	}
	if _, err := a.data.ResolveAll(ConflictFilter{}, "theirs", true, auth.Principal{}); err == nil {
		t.Fatal("take must be local or remote")
	}

	done, err := a.data.ResolveAll(ConflictFilter{Origin: b.data.NodeID.String()}, "remote", false, auth.Principal{})
	if err != nil || len(done) != 1 || done[0].Error != "" || done[0].CommitHex == "" {
		t.Fatalf("bulk resolution failed: %+v %v", done, err)
	}
	mustSync(t, a, b)
	for _, d := range []codec.UUID{d1, d2} {
		if got := docBody(t, b.data, d); got != `{"v":"from B"}` {
			t.Fatalf("document %s: %s", d, got)
		}
	}
	if mustHeadOf(t, a.data) != mustHeadOf(t, b.data) {
		t.Fatal("nodes did not converge after the bulk resolution")
	}
}

// Polling instead of a webhook: the authority lists what it has not seen and acknowledges it.
func TestPollingAuthorityAcknowledgesEntries(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	e := authorityEntries(a.data.Conflicts)[0]
	if e.Delivered {
		t.Fatal("new entries start undelivered")
	}
	if err := a.data.AckConflict(e.ID, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.data.Conflicts.Get(e.ID); !got.Delivered {
		t.Fatal("ack should mark the entry delivered")
	}
	// Seen again unchanged on the next sync, it stays acknowledged.
	mustSync(t, a, b)
	if got, _ := a.data.Conflicts.Get(e.ID); !got.Delivered || got.Seen < 2 {
		t.Fatalf("an unchanged re-sighting must stay acknowledged: %+v", got)
	}
	if err := a.data.AckConflict("nope", auth.Principal{}); !errors.Is(err, peersync.ErrConflictNotFound) {
		t.Fatalf("unknown entry: %v", err)
	}
}

// The webhook retries a receiver that is down, and refuses nothing it signs.
func TestConflictWebhookRetriesUntilAcknowledged(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	r := newReceiver(t, "k")
	r.failures.Store(2)
	w := webhookFor(a, r)
	if n := w.Pass(); n != 0 {
		t.Fatalf("a failing receiver acknowledges nothing, got %d", n)
	}
	if n := w.Pass(); n != 0 {
		t.Fatalf("still failing, got %d", n)
	}
	if n := w.Pass(); n != 1 || len(r.received()) != 1 || r.badSig != 0 {
		t.Fatalf("expected delivery once the receiver is back: %d %+v bad=%d", n, r.received(), r.badSig)
	}

	// A receiver with a different secret refuses the signature, so nothing is marked.
	a2, b2 := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a2, b2, peersync.PendingHold, "")
	conflictingWrites(t, a2, b2)
	mustSync(t, a2, b2)
	wrong := newReceiver(t, "other")
	w2 := webhookFor(a2, wrong)
	w2.Secret = "k"
	if n := w2.Pass(); n != 0 || wrong.badSig != 1 {
		t.Fatalf("a mis-signed delivery must not count: %d bad=%d", n, wrong.badSig)
	}
}

// The started webhook delivers a new entry promptly, without waiting for its interval.
func TestStartedConflictWebhookDeliversOnRecord(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingHold, "")
	r := newReceiver(t, "k")
	w := StartConflictWebhook(a.data.namespaceSet(), a.data.NodeID.String(), &ConflictWebhook{URL: r.srv.URL, Secret: "k", Interval: time.Hour})
	defer w.Close()
	waitFor(t, "the webhook to hook the queue", func() bool {
		w.passMu.Lock()
		defer w.passMu.Unlock()
		return w.hooked[a.data.Conflicts]
	})
	conflictingWrites(t, a, b)
	mustSync(t, a, b)
	waitFor(t, "the conflict to be delivered", func() bool { return len(r.received()) == 1 })
}

// A v1 peer cannot confirm it has the same chain, so a v1 sync of a namespace with one neither
// merges here nor pushes a head the peer would have to merge.
func TestV1SyncOfANamespaceWithAChainNeverMerges(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	withAuthority(t, a, b, peersync.PendingProvisional, "")
	conflictingWrites(t, a, b)
	headA, headB := mustHeadOf(t, a.data), mustHeadOf(t, b.data)

	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b.data, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), a.data.dag, a.data.Runtime.Storage)
	session, err := client.Connect(peersync.ClientConfig{
		NamespaceID: "app/data", NodeID: a.data.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(),
		ApplyToStorage: true, ConflictPolicy: transaction.ConflictPolicyStrict, FastForwardPushOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	r, err := session.SyncBidirectional()
	if err != nil {
		t.Fatal(err)
	}
	if r.Conflict == nil || r.PushedCommits != 0 {
		t.Fatalf("expected a held conflict and nothing pushed: %+v", r)
	}
	if mustHeadOf(t, a.data) != headA || mustHeadOf(t, b.data) != headB {
		t.Fatal("neither node may move main over v1")
	}
}
