package syncnode

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	calls := 0
	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: strings.Replace(srv.URL, "http://", "ws://", 1) + "/kdb/sync", Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncPull, Interval: time.Hour, CreateLocal: true,
		Credentials: func() (auth.ConnectionContext, error) {
			calls++ // refreshed per connection
			return auth.ConnectionContext{Headers: map[string]string{"Authorization": "Bearer phone-token"}}, nil
		},
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if phone.get("zolik/u/1", profile) == "" || calls == 0 {
		t.Fatalf("header-only token: synced=%v credential calls=%d", phone.get("zolik/u/1", profile) != "", calls)
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
