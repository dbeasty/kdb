package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// newFixture starts a control plane over a fresh in-memory namespace holding n documents, and
// returns its base URL.
func newFixture(t *testing.T, opts ...func(*Options)) (*Server, string) {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	srv := server.NewKdbServerRuntime(rt)
	o := Options{
		Addr:      "127.0.0.1:0",
		Runtime:   srv,
		Namespace: "demo/users",
		Version:   "test",
		Settings: config.Describe(nil, func(string) (string, bool) { return "", false },
			func(string) bool { return false }, config.DefaultServiceSettings()),
	}
	for _, fn := range opts {
		fn(&o)
	}
	cs, err := New(o)
	if err != nil {
		t.Fatalf("start control plane: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, "http://" + cs.Addr().String()
}

func seed(t *testing.T, cs *Server, bodies ...string) []embed.PutResult {
	t.Helper()
	rt := cs.opts.Runtime.Runtime
	out := make([]embed.PutResult, 0, len(bodies))
	for _, body := range bodies {
		res, err := embed.PutJSONDocument(rt, "demo/users", body)
		if err != nil {
			t.Fatalf("seed %s: %v", body, err)
		}
		out = append(out, res)
	}
	return out
}

// get issues an authenticated request. The runtime's default engine is auth.AllowAll, so any
// credential authenticates - what is under test here is that one is *required* and carried.
func get(t *testing.T, base, path string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer alice:secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	raw, _ := io.ReadAll(res.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	return res, body
}

// TestEveryEndpointRequiresCredentials is the property that matters most about this package: the
// admin endpoint is deliberately unauthenticated, this one must never be.
func TestEveryEndpointRequiresCredentials(t *testing.T) {
	_, base := newFixture(t)
	for _, path := range []string{
		"/v1/health", "/v1/namespaces", "/v1/settings", "/v1/ops/runtime",
		"/v1/ns/demo%2Fusers/status", "/v1/ns/demo%2Fusers/log",
		"/v1/ns/demo%2Fusers/refs", "/v1/ns/demo%2Fusers/schema",
	} {
		res, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without credentials: got %d, want 401", path, res.StatusCode)
		}
		if res.Header.Get("WWW-Authenticate") == "" {
			t.Errorf("GET %s: a 401 should say how to authenticate", path)
		}
	}
}

func TestMalformedCredentialsAreRejected(t *testing.T) {
	_, base := newFixture(t)
	for _, header := range []string{"Bearer", "Basic !!!not-base64", "Weird abc", "nospace"} {
		req, _ := http.NewRequest(http.MethodGet, base+"/v1/health", nil)
		req.Header.Set("Authorization", header)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: got %d, want 401", header, res.StatusCode)
		}
	}
}

func TestHealthAndNamespaces(t *testing.T) {
	_, base := newFixture(t)

	res, body := get(t, base, "/v1/health")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("health: %d", res.StatusCode)
	}
	if body["version"] != "test" || body["namespace"] != "demo/users" {
		t.Errorf("unexpected health body: %v", body)
	}
	if body["controlWritesEnabled"] != false {
		t.Error("a control plane must default to read-only")
	}

	res, body = get(t, base, "/v1/namespaces")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("namespaces: %d", res.StatusCode)
	}
	list, _ := body["namespaces"].([]any)
	if len(list) != 1 {
		t.Fatalf("want one namespace, got %v", body["namespaces"])
	}
}

func TestLogListsCommitsNewestFirst(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`, `{"name":"grace"}`, `{"name":"alan"}`)

	res, body := get(t, base, "/v1/ns/demo%2Fusers/log?limit=2")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("log: %d (%v)", res.StatusCode, body)
	}
	commits, _ := body["commits"].([]any)
	if len(commits) != 2 {
		t.Fatalf("limit=2 must return two commits, got %d", len(commits))
	}
	if body["hasMore"] != true {
		t.Error("three commits and a limit of two means more remain")
	}
	first := commits[0].(map[string]any)
	if len(first["shortHash"].(string)) != 8 {
		t.Errorf("shortHash should be 8 hex digits, got %q", first["shortHash"])
	}

	// Paging with skip must not repeat what the first page already showed.
	_, page2 := get(t, base, "/v1/ns/demo%2Fusers/log?limit=2&skip=2")
	rest, _ := page2["commits"].([]any)
	seen := map[string]bool{}
	for _, c := range commits {
		seen[c.(map[string]any)["hash"].(string)] = true
	}
	for _, c := range rest {
		if seen[c.(map[string]any)["hash"].(string)] {
			t.Error("a skipped page repeated a commit from the first page")
		}
	}
}

func TestLogRejectsNonsenseParameters(t *testing.T) {
	_, base := newFixture(t)
	res, body := get(t, base, "/v1/ns/demo%2Fusers/log?limit=abc")
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=abc should be a 400, got %d", res.StatusCode)
	}
	if body["error"] == nil {
		t.Error("an error response must carry the standard error body")
	}
}

func TestCommitDetailAndDiff(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`)
	put := seed(t, cs, `{"name":"grace"}`)[0]

	// A commit is addressable by its short hash, which is what a UI has in a log listing.
	short := put.Commit.Hex()[:8]
	res, body := get(t, base, "/v1/ns/demo%2Fusers/commits/"+short)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("commit by short hash: %d (%v)", res.StatusCode, body)
	}
	commit := body["commit"].(map[string]any)
	if commit["hash"] != put.Commit.Hex() {
		t.Errorf("resolved the wrong commit: %v", commit["hash"])
	}
	if body["operationsAvailable"] != true {
		t.Error("a freshly written commit's operations should still be resident")
	}
	ops, _ := body["operations"].([]any)
	if len(ops) == 0 {
		t.Error("the commit that wrote a document should list an operation")
	}

	// The diff against the first parent is what "what did this commit change" means.
	res, body = get(t, base, "/v1/ns/demo%2Fusers/commits/"+short+"/diff")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("diff: %d (%v)", res.StatusCode, body)
	}
	entries, _ := body["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("this commit added exactly one document, got %d entries", len(entries))
	}
	e := entries[0].(map[string]any)
	if e["change"] != "added" || e["docId"] != put.DocID.String() {
		t.Errorf("unexpected diff entry: %v", e)
	}
	counts := body["counts"].(map[string]any)
	if counts["added"].(float64) != 1 {
		t.Errorf("counts should agree with entries: %v", counts)
	}
}

// TestRootCommitDiffIsRefusedNotFaked matters because the alternative - reporting "nothing
// changed" for a root commit - is wrong in a way a user cannot detect.
func TestRootCommitDiffIsRefusedNotFaked(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`)

	_, logBody := get(t, base, "/v1/ns/demo%2Fusers/log?limit=100")
	commits, _ := logBody["commits"].([]any)
	root := commits[len(commits)-1].(map[string]any)
	if parents, _ := root["parents"].([]any); len(parents) != 0 {
		t.Skip("the oldest commit walked is not a root commit in this fixture")
	}
	res, body := get(t, base, "/v1/ns/demo%2Fusers/commits/"+root["hash"].(string)+"/diff")
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("a root commit's diff must be refused, got %d (%v)", res.StatusCode, body)
	}
}

func TestUnknownRevisionIsNotFound(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`)
	for _, spec := range []string{"deadbeef", strings.Repeat("a", 64), "no-such-branch"} {
		res, _ := get(t, base, "/v1/ns/demo%2Fusers/commits/"+spec)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("revision %q: got %d, want 404", spec, res.StatusCode)
		}
	}
}

func TestShortPrefixIsRefusedRatherThanGuessed(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`)
	// Under 8 digits is below LookupHashPrefix's floor. Refusing is the point: resolving it
	// arbitrarily would show an operator a commit they did not ask for.
	res, body := get(t, base, "/v1/ns/demo%2Fusers/commits/abc")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("got %d, want 404", res.StatusCode)
	}
	msg := fmt.Sprint(body["error"])
	if !strings.Contains(msg, "8 hex digits") {
		t.Errorf("the error should explain the prefix floor: %s", msg)
	}
}

func TestCommitIsCacheableByETag(t *testing.T) {
	cs, base := newFixture(t)
	put := seed(t, cs, `{"name":"ada"}`)[0]
	path := "/v1/ns/demo%2Fusers/commits/" + put.Commit.Hex()

	res, _ := get(t, base, path)
	etag := res.Header.Get("ETag")
	if etag == "" {
		t.Fatal("a content-addressed commit should carry a validator")
	}
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	req.Header.Set("Authorization", "Bearer alice:secret")
	req.Header.Set("If-None-Match", etag)
	res2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusNotModified {
		t.Errorf("a matching ETag should short-circuit, got %d", res2.StatusCode)
	}
}

func TestRefsReportsBranchesAndHonestlyEmptyTags(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"name":"ada"}`)
	res, body := get(t, base, "/v1/ns/demo%2Fusers/refs")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("refs: %d", res.StatusCode)
	}
	branches, _ := body["branches"].([]any)
	if len(branches) == 0 {
		t.Error("a namespace with commits has at least one branch")
	}
	if body["tags"] == nil {
		t.Error("tags must be present and empty rather than absent - the engine's tag support is a stub")
	}
}

func TestDocumentReadAtPastCommitIsRefusedNotSilentlyHead(t *testing.T) {
	cs, base := newFixture(t)
	put := seed(t, cs, `{"name":"ada"}`)[0]
	res, body := get(t, base,
		"/v1/ns/demo%2Fusers/docs/"+put.DocID.String()+"?at="+put.Commit.Hex())
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("time travel is not wired into the server read path; answering from head would "+
			"show the wrong data under a URL that says otherwise. Got %d (%v)", res.StatusCode, body)
	}
}

func TestDocumentReadReturnsBodyByteExact(t *testing.T) {
	cs, base := newFixture(t)
	// Deliberately unsorted keys. The engine stores a document body byte-exact - nothing injected,
	// no key reordered - and the control plane must not quietly undo that by decoding and
	// re-encoding on the way out.
	body := `{"zeta":1,"alpha":2,"nested":{"b":1,"a":2}}`
	put := seed(t, cs, body)[0]

	// Read the raw response rather than a parsed map: unmarshalling into map[string]any would
	// itself reorder the keys, destroying the very evidence under test.
	req, _ := http.NewRequest(http.MethodGet, base+"/v1/ns/demo%2Fusers/docs/"+put.DocID.String(), nil)
	req.Header.Set("Authorization", "Bearer alice:secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("document read: %d (%s)", res.StatusCode, raw)
	}
	if !strings.Contains(string(raw), body) {
		t.Errorf("the stored body did not survive the response verbatim.\n want substring: %s\n got: %s",
			body, raw)
	}
}

func TestSettingsReportProvenance(t *testing.T) {
	_, base := newFixture(t, func(o *Options) {
		o.Settings = append(
			config.Describe(nil, func(name string) (string, bool) {
				if name == "KDB_WS_ADDR" {
					return "ws://127.0.0.1:9999", true
				}
				return "", false
			}, func(string) bool { return false }, config.ServiceSettings{WSAddr: "ws://127.0.0.1:9999"}),
			config.EnvOnlyDescriptors(func(name string) (string, bool) {
				if name == "KDB_DOCUMENT_CACHE_BYTES" {
					return "not-a-number", true
				}
				return "", false
			})...)
	})
	res, body := get(t, base, "/v1/settings")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("settings: %d", res.StatusCode)
	}
	if body["mutable"] != false {
		t.Error("this build cannot change settings and must say so rather than implying it can")
	}
	settings, _ := body["settings"].([]any)
	var sawEnv, sawWarning bool
	for _, raw := range settings {
		d := raw.(map[string]any)
		if d["key"] == "listener.wsAddr" && d["source"] == "env" {
			sawEnv = true
		}
		if d["key"] == "cache.documentBytes" {
			if w, _ := d["warnings"].([]any); len(w) > 0 {
				sawWarning = true
			}
		}
	}
	if !sawEnv {
		t.Error("a setting that came from the environment must be attributed to it")
	}
	if !sawWarning {
		t.Error("a KDB_* value the engine silently discarded must surface as a warning - that is " +
			"the entire point of the env-only settings surface")
	}
}

func TestSettingsNeverLeakSecrets(t *testing.T) {
	_, base := newFixture(t, func(o *Options) {
		o.Settings = config.EnvOnlyDescriptors(func(name string) (string, bool) {
			if name == "KDB_S3_SECRET_ACCESS_KEY" {
				return "hunter2", true
			}
			return "", false
		})
	})
	req, _ := http.NewRequest(http.MethodGet, base+"/v1/settings", nil)
	req.Header.Set("Authorization", "Bearer alice:secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if strings.Contains(string(raw), "hunter2") {
		t.Fatal("a sensitive setting value was returned in the clear")
	}
}

func TestMutatingEndpointsAreRefusedWhileReadOnly(t *testing.T) {
	_, base := newFixture(t)
	req, _ := http.NewRequest(http.MethodPut,
		base+"/v1/ns/demo%2Fusers/docs/6f9619ff-8b86-d011-b42d-00cf4fc964ff", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer alice:secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("a read-only control plane must refuse a write with 403, got %d", res.StatusCode)
	}
}

// TestWriteEnabledStillReportsUnbuiltEndpoints keeps "the server said no" and "the server cannot"
// distinguishable, which are different operator problems.
func TestWriteEnabledStillReportsUnbuiltEndpoints(t *testing.T) {
	_, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/ns/demo%2Fusers/revert/plan", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer alice:secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501 for an endpoint that is specified but not built, got %d", res.StatusCode)
	}
}

func TestUIIsServedOnlyWhenEnabled(t *testing.T) {
	t.Run("off by default in these options", func(t *testing.T) {
		_, base := newFixture(t)
		res, err := http.Get(base + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("with ServeUI off, / should not serve the app, got %d", res.StatusCode)
		}
	})

	t.Run("served when enabled", func(t *testing.T) {
		_, base := newFixture(t, func(o *Options) { o.ServeUI = true })
		// Deliberately unauthenticated: the page itself is a static shell that carries no data and
		// must load so it can prompt for credentials.
		res, err := http.Get(base + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("want the UI at /, got %d", res.StatusCode)
		}
		if !strings.Contains(string(raw), "KDB Control") {
			t.Error("the served page is not the control UI")
		}
		if csp := res.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
			t.Errorf("the UI loads nothing externally and should say so: %q", csp)
		}
	})
}

func TestCommitEventsReachSubscribers(t *testing.T) {
	cs, _ := newFixture(t)
	id, ch, ok := cs.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer cs.events.unsubscribe(id)

	put := seed(t, cs, `{"name":"ada"}`)[0]
	rt := cs.opts.Runtime.Runtime
	commit, found := rt.DAG.GetCommit(put.Commit)
	if !found {
		t.Fatal("seeded commit is not in the DAG")
	}
	cs.PublishCommit("demo/users", commit)

	select {
	case ev := <-ch:
		if ev.Hash != put.Commit.Hex() {
			t.Errorf("wrong commit published: %s", ev.Hash)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered")
	}
}

// TestSlowSubscriberNeverBlocksAPublish is the property that makes it safe to call PublishCommit
// from CommitListener, which runs synchronously inside a commit.
func TestSlowSubscriberNeverBlocksAPublish(t *testing.T) {
	hub := newEventHub()
	id, _, ok := hub.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer hub.unsubscribe(id)

	done := make(chan struct{})
	go func() {
		// Far more than the per-subscriber buffer, with nothing draining it.
		for i := 0; i < 1000; i++ {
			hub.publish(commitEvent{Hash: fmt.Sprint(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishing blocked on a subscriber that was not reading; a slow browser must " +
			"never be able to slow down a commit")
	}
}

func TestCloseReleasesSubscribers(t *testing.T) {
	cs, _ := newFixture(t)
	_, ch, ok := cs.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case _, open := <-ch:
		if open {
			t.Error("closing the server should close subscriber channels")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber channel was not closed")
	}
	if n := cs.events.subscriberCount(); n != 0 {
		t.Errorf("want no subscribers after close, got %d", n)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	cs, _ := newFixture(t)
	if err := cs.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("second close should be a no-op, got %v", err)
	}
}

func TestNewRequiresARuntime(t *testing.T) {
	if _, err := New(Options{Addr: "127.0.0.1:0"}); err == nil {
		t.Fatal("a control plane with no runtime has nothing to serve and must refuse to start")
	}
}

func TestDiffOrderingIsStable(t *testing.T) {
	// dag.Diff ranges over maps, so its order is Go's map order. A view that reshuffled on every
	// refresh would be unusable, which is why diffCommits sorts.
	entries := []diffEntry{
		{Change: "removed", DocID: "b"},
		{Change: "added", DocID: "z"},
		{Change: "modified", DocID: "a"},
		{Change: "added", DocID: "a"},
	}
	sortDiff(entries)
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Change+":"+e.DocID)
	}
	want := []string{"added:a", "added:z", "modified:a", "removed:b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

func TestCredentialsFromParsesBothSchemes(t *testing.T) {
	t.Run("bearer user:pass splits into fields", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://x/", nil)
		req.Header.Set("Authorization", "Bearer alice:secret")
		creds, err := credentialsFrom(req)
		if err != nil {
			t.Fatal(err)
		}
		if creds.User == nil || *creds.User != "alice" || creds.Password == nil || *creds.Password != "secret" {
			t.Errorf("the static engines read a bearer token as user:pass; got %v/%v", creds.User, creds.Password)
		}
		if creds.Token == nil || *creds.Token != "alice:secret" {
			t.Error("the raw token must also be preserved for engines that take opaque tokens")
		}
	})

	t.Run("opaque bearer stays a token", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://x/", nil)
		req.Header.Set("Authorization", "Bearer opaque-token")
		creds, err := credentialsFrom(req)
		if err != nil {
			t.Fatal(err)
		}
		if creds.User != nil {
			t.Error("a token with no colon must not be split")
		}
		if creds.Token == nil || *creds.Token != "opaque-token" {
			t.Error("token not carried")
		}
	})

	t.Run("basic decodes", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://x/", nil)
		req.SetBasicAuth("bob", "pw")
		creds, err := credentialsFrom(req)
		if err != nil {
			t.Fatal(err)
		}
		if creds.User == nil || *creds.User != "bob" {
			t.Errorf("basic auth did not decode: %v", creds.User)
		}
	})
}

// TestForbiddenWhenAuthorizerDenies proves the authorize step is real and not decorative.
func TestForbiddenWhenAuthorizerDenies(t *testing.T) {
	cs, base := newFixture(t)
	cs.opts.Runtime.AuthEngine = denyAllEngine{}
	res, _ := get(t, base, "/v1/ns/demo%2Fusers/log")
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 when the authorizer denies, got %d", res.StatusCode)
	}
}

type denyAllEngine struct{}

func (denyAllEngine) Authenticator() auth.Authenticator { return denyAllEngine{} }
func (denyAllEngine) Authorizer() auth.Authorizer       { return denyAllEngine{} }
func (denyAllEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{ID: "nobody"}, nil
}
func (denyAllEngine) Authorize(_ context.Context, _ auth.Principal, _ auth.Action) error {
	return fmt.Errorf("denied by test")
}
