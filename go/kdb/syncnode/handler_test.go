package syncnode

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
)

// tokenEngine admits one bearer token, as the principal "phone", to everything.
type tokenEngine struct{ token string }

func (e tokenEngine) Authenticator() auth.Authenticator { return e }
func (e tokenEngine) Authorizer() auth.Authorizer       { return e }
func (e tokenEngine) Authenticate(_ context.Context, c auth.Credentials) (auth.Principal, error) {
	if c.Token == nil || *c.Token != e.token {
		return auth.Principal{}, errors.New("unauthenticated: bad or missing token")
	}
	return auth.Principal{ID: "phone"}, nil
}
func (e tokenEngine) Authorize(context.Context, auth.Principal, auth.Action) error { return nil }

// cloudOverHTTP is a cloud node whose peer sync is served by Handler on an application's own HTTP
// server, at /kdb/sync beside its API, with bearer-token auth.
func cloudOverHTTP(t *testing.T) (testNode, *httptest.Server) {
	t.Helper()
	cloud := newTestNode(t, "zolik/cloud", Config{})
	cloud.primary.AuthEngine = tokenEngine{token: "phone-token"}
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("pong")) })
	mux.Handle("/kdb/sync", cloud.node.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return cloud, srv
}

func phoneDialing(t *testing.T, url string, token *string) testNode {
	t.Helper()
	return newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: strings.Replace(url, "http://", "ws://", 1) + "/kdb/sync", Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncBoth, Interval: 50 * time.Millisecond, CreateLocal: true, Token: token,
	}}})
}

// TestPeerSyncOverTheApplicationsHTTPServer (G2): a phone syncs with the cloud through the
// cloud's own HTTP server - same origin and port as its API - authenticated by a bearer token.
func TestPeerSyncOverTheApplicationsHTTPServer(t *testing.T) {
	cloud, srv := cloudOverHTTP(t)
	profile := mustID(t)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	token := "phone-token"
	phone := phoneDialing(t, srv.URL, &token)
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the phone to pull over WebSocket", func() bool { return phone.get("zolik/u/1", profile) != "" })
	mine := mustID(t)
	phone.put(t, "zolik/u/1", mine, `{"from":"phone"}`)
	eventually(t, "the phone's write to reach the cloud", func() bool { return cloud.get("zolik/u/1", mine) != "" })

	// The API on the same server is untouched.
	res, err := http.Get(srv.URL + "/api/ping")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("api: %v %v", res, err)
	}
}

// TestPeerSyncOverHTTPRefusesAMissingToken: without the token the cloud refuses the session and
// the phone records the failure; nothing syncs.
func TestPeerSyncOverHTTPRefusesAMissingToken(t *testing.T) {
	cloud, srv := cloudOverHTTP(t)
	cloud.put(t, "zolik/u/1", mustID(t), `{"name":"ada"}`)
	phone := phoneDialing(t, srv.URL, nil)
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	_, err := phone.node.SyncNow("cloud")
	if err == nil || !strings.Contains(err.Error(), "unauthenticated") {
		t.Fatalf("want an authentication refusal, got %v", err)
	}
}

// TestTokenOnlyOnTheUpgradeRequest: the phone's credentials callback supplies the token only as an
// Authorization header - as a proxy or an app's HTTP client would - and the hello carries none;
// the cloud authenticates from the upgrade request.
func TestTokenOnlyOnTheUpgradeRequest(t *testing.T) {
	cloud, srv := cloudOverHTTP(t)
	profile := mustID(t)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	var calls atomic.Int32 // the callback runs on the replicator's goroutines
	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: strings.Replace(srv.URL, "http://", "ws://", 1) + "/kdb/sync", Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncPull, Interval: time.Hour, CreateLocal: true,
		Credentials: func() (auth.ConnectionContext, error) {
			calls.Add(1) // refreshed per connection
			return auth.ConnectionContext{Headers: map[string]string{"Authorization": "Bearer phone-token"}}, nil
		},
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if phone.get("zolik/u/1", profile) == "" || calls.Load() == 0 {
		t.Fatalf("header-only token: synced=%v credential calls=%d", phone.get("zolik/u/1", profile) != "", calls.Load())
	}
}

// TestHandlerRefusesAPlainRequest: a non-upgrade request to the sync route is an HTTP error, not
// a hang.
func TestHandlerRefusesAPlainRequest(t *testing.T) {
	_, srv := cloudOverHTTP(t)
	res, err := http.Get(srv.URL + "/kdb/sync")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for a plain GET, got %d", res.StatusCode)
	}
}

// TestConnectionContextFromReadsBearerAndBasic.
func TestConnectionContextFromReadsBearerAndBasic(t *testing.T) {
	r := httptest.NewRequest("GET", "/kdb/sync", nil)
	r.Header.Set("Authorization", "Bearer abc")
	if cc := ConnectionContextFrom(r); cc.Token == nil || *cc.Token != "abc" {
		t.Fatalf("bearer: %+v", cc)
	}
	r.SetBasicAuth("ada", "s3cret")
	if cc := ConnectionContextFrom(r); cc.User == nil || *cc.User != "ada" || *cc.Password != "s3cret" || cc.Token != nil {
		t.Fatalf("basic: %+v", cc)
	}
}

// directionEngine admits every caller as "phone" and grants peer sync per namespace: both
// directions on zolik/u/1, pull only on zolik/u/1/ro (cloud-authored), nothing else.
type directionEngine struct{}

func (directionEngine) Authenticator() auth.Authenticator { return directionEngine{} }
func (directionEngine) Authorizer() auth.Authorizer       { return directionEngine{} }
func (directionEngine) Authenticate(context.Context, auth.Credentials) (auth.Principal, error) {
	return auth.Principal{ID: "phone"}, nil
}
func (directionEngine) Authorize(_ context.Context, _ auth.Principal, a auth.Action) error {
	switch a := a.(type) {
	case auth.PeerSyncAction:
		if a.Namespace == "zolik/u/1" || a.Namespace == "_kdb/meta" {
			return nil
		}
	case auth.PeerPullAction:
		if a.Namespace == "zolik/u/1/ro" {
			return nil
		}
	case auth.PeerPushAction:
	default:
		return nil // local writes and reads: not what this test is about
	}
	return errors.New("forbidden")
}

// TestPhoneCannotPushToItsReadOnlyNamespace (G3): the phone pulls its cloud-authored namespace but
// the cloud never takes a push to it - a modified client that writes its own stats locally gets
// nothing onto the cloud, and its sync of the namespace is a skipped direction, not a failure.
func TestPhoneCannotPushToItsReadOnlyNamespace(t *testing.T) {
	cloud := newTestNode(t, "zolik/cloud", Config{})
	cloud.primary.AuthEngine = directionEngine{}
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	stats, profile := mustID(t), mustID(t)
	cloud.put(t, "zolik/u/1/ro", stats, `{"wins":3}`)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{"zolik/u/1", "zolik/u/1/ro"},
		Mode: peersync.SyncBoth, Interval: time.Hour, CreateLocal: true,
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if phone.get("zolik/u/1/ro", stats) != `{"wins":3}` {
		t.Fatal("the phone should pull its read-only namespace")
	}

	// A tampered client rewrites its stats locally and syncs.
	phone.put(t, "zolik/u/1/ro", stats, `{"wins":999}`)
	mine := mustID(t)
	phone.put(t, "zolik/u/1", mine, `{"theme":"dark"}`)
	res, err := phone.node.SyncNow("cloud")
	if err != nil {
		t.Fatalf("a direction the cloud does not grant must be skipped, not fail the sync: %v", err)
	}
	for _, ns := range res.Namespaces {
		if ns.Namespace == "zolik/u/1/ro" && ns.Access != "pull" {
			t.Fatalf("the cloud should report pull-only access, got %q", ns.Access)
		}
	}
	if got := cloud.get("zolik/u/1/ro", stats); got != `{"wins":3}` {
		t.Fatalf("the cloud took a push to a read-only namespace: %s", got)
	}
	if cloud.get("zolik/u/1", mine) == "" {
		t.Fatal("the phone's own namespace should still push")
	}
}

// docEngine lets peers sync everything but refuses writes to one document.
type docEngine struct{ locked string }

func (e docEngine) Authenticator() auth.Authenticator { return e }
func (e docEngine) Authorizer() auth.Authorizer       { return e }
func (docEngine) Authenticate(context.Context, auth.Credentials) (auth.Principal, error) {
	return auth.Principal{ID: "peer"}, nil
}
func (e docEngine) Authorize(_ context.Context, p auth.Principal, a auth.Action) error {
	if w, ok := a.(auth.DocumentWriteAction); ok && w.DocID == e.locked && p.ID == "peer" {
		return errors.New("forbidden: document is locked")
	}
	return nil
}

// TestPushedDocumentsAreAuthorizedWhenAsked: with AuthorizePushedDocuments, a push that writes a
// document the engine locks is refused whole; other pushes go through.
func TestPushedDocumentsAreAuthorizedWhenAsked(t *testing.T) {
	locked := mustID(t)
	cloud := newTestNode(t, "zolik/cloud", Config{AuthorizePushedDocuments: true})
	cloud.primary.AuthEngine = docEngine{locked: locked.String()}
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	cloud.put(t, "zolik/u/1", mustID(t), `{"seed":true}`)
	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncBoth, Interval: time.Hour, CreateLocal: true,
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	ok := mustID(t)
	phone.put(t, "zolik/u/1", ok, `{"fine":true}`)
	if _, err := phone.node.SyncNow("cloud"); err != nil || cloud.get("zolik/u/1", ok) == "" {
		t.Fatalf("an allowed push: %v", err)
	}
	phone.put(t, "zolik/u/1", locked, `{"sneaky":true}`)
	if _, err := phone.node.SyncNow("cloud"); err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("a push writing a locked document must be refused, got %v", err)
	}
	if cloud.get("zolik/u/1", locked) != "" {
		t.Fatal("the locked document reached the cloud")
	}
}
