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
	// Under 8 digits is below LookupHashPrefix's floor, so it is not treated as an abbreviated
	// hash at all. Refusing is the point: resolving it arbitrarily would show an operator a commit
	// they did not ask for.
	res, body := get(t, base, "/v1/ns/demo%2Fusers/commits/abc")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("got %d, want 404", res.StatusCode)
	}
	if body["error"] == nil {
		t.Errorf("a refused revision must carry the standard error body: %v", body)
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
	// The SQL console is specified in the plan and not built; revert, which used to stand in here,
	// is built now.
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/ns/demo%2Fusers/sql", strings.NewReader("{}"))
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

// TestDiffReportsModificationAsModification guards the semantic a commit view lives or dies by: a
// document written twice is modified the second time, not added again.
//
// This used to be a cross-check between two local diff implementations, which is how a key-space
// mix-up in one of them was caught. There is one implementation now - the engine's
// embed.DiffCommits - so the check is against the meaning rather than against a second opinion.
func TestDiffReportsModificationAsModification(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"doc-a","v":1}`)
	seed(t, cs, `{"id":"doc-b","v":1}`)
	second := seed(t, cs, `{"id":"doc-a","v":2}`)[0]

	d, err := cs.commitDAG()
	if err != nil {
		t.Fatal(err)
	}
	commit, ok := d.GetCommit(second.Commit)
	if !ok {
		t.Fatal("commit missing")
	}
	_, _, entries, err := cs.diffRevisions(commit.ParentHashes[0].Hex(), second.Commit.Hex())
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(entries) != 1 || entries[0].Change != "modified" {
		t.Fatalf("rewriting doc-a must read as one modification, got %+v", entries)
	}
	if entries[0].FromContentHash == "" || entries[0].ToContentHash == "" {
		t.Errorf("a modification should carry both content hashes; got %+v", entries[0])
	}
	if entries[0].FromContentHash == entries[0].ToContentHash {
		t.Error("the content actually changed, so the hashes must differ")
	}

	_, body := get(t, base, "/v1/ns/demo%2Fusers/commits/"+second.Commit.Hex()+"/diff")
	counts := body["counts"].(map[string]any)
	if counts["modified"].(float64) != 1 || counts["added"].(float64) != 0 {
		t.Errorf("the endpoint should agree with the diff: %v", counts)
	}
}

// TestRewritingIdenticalContentIsNotAChange: the engine accepts a write whose content matches what
// is already stored. Content addressing means the tree entry does not move, so the diff must show
// nothing - reporting it would send a reader looking for a difference that does not exist.
func TestRewritingIdenticalContentIsNotAChange(t *testing.T) {
	cs, _ := newFixture(t)
	seed(t, cs, `{"id":"doc-a","v":1}`)
	again := seed(t, cs, `{"id":"doc-a","v":1}`)[0]

	d, _ := cs.commitDAG()
	commit, ok := d.GetCommit(again.Commit)
	if !ok {
		t.Fatal("commit missing")
	}
	_, _, entries, err := cs.diffRevisions(commit.ParentHashes[0].Hex(), again.Commit.Hex())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Change == "modified" {
			t.Errorf("re-writing identical content is not a modification: %+v", e)
		}
	}
}

// TestRevisionSpecsAreTheEnginesGrammar: the control plane does not parse revisions itself, so
// whatever dag.ParseRevision accepts must work over HTTP too.
func TestRevisionSpecsAreTheEnginesGrammar(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a"}`, `{"id":"b"}`, `{"id":"c"}`)

	// head^ is escaped: "^" is an unsafe character in a URL path segment, so a caller putting a
	// revision in a path has to encode it. The grammar itself accepts it (dag.ParseRevision).
	for _, spec := range []string{"head", "head~1", "head%5E", "head~2"} {
		res, body := get(t, base, "/v1/ns/demo%2Fusers/commits/"+spec)
		if res.StatusCode != http.StatusOK {
			t.Errorf("revision %q: got %d (%v)", spec, res.StatusCode, body)
		}
	}
	// And one that walks past the root must be refused rather than clamped.
	res, _ := get(t, base, "/v1/ns/demo%2Fusers/commits/head~999")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("head~999 should be 404, got %d", res.StatusCode)
	}
}

func TestTagsAppearInRefsAndAsBadges(t *testing.T) {
	cs, base := newFixture(t)
	put := seed(t, cs, `{"id":"a"}`)[0]
	d, err := cs.commitDAG()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateTag("v1", put.Commit, "first release"); err != nil {
		t.Fatalf("create tag: %v", err)
	}

	res, body := get(t, base, "/v1/ns/demo%2Fusers/refs")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("refs: %d", res.StatusCode)
	}
	tags, _ := body["tags"].([]any)
	if len(tags) != 1 {
		t.Fatalf("want the tag that was just created, got %v", body["tags"])
	}
	tag := tags[0].(map[string]any)
	if tag["name"] != "v1" || tag["message"] != "first release" {
		t.Errorf("unexpected tag: %v", tag)
	}

	// A tag is also a revision, and should badge its commit in the log.
	_, logBody := get(t, base, "/v1/ns/demo%2Fusers/log")
	commits, _ := logBody["commits"].([]any)
	var badged bool
	for _, c := range commits {
		// refs is omitted for a commit nothing points at, so this must not assume the key.
		refs, _ := c.(map[string]any)["refs"].([]any)
		for _, r := range refs {
			if r == "tag:v1" {
				badged = true
			}
		}
	}
	if !badged {
		t.Error("a tagged commit should carry its tag as a ref badge")
	}

	res, _ = get(t, base, "/v1/ns/demo%2Fusers/commits/tag:v1")
	if res.StatusCode != http.StatusOK {
		t.Errorf("a tag must resolve as a revision, got %d", res.StatusCode)
	}
}

func TestDocumentReadAtPastRevision(t *testing.T) {
	cs, base := newFixture(t)
	first := seed(t, cs, `{"id":"doc-a","v":1}`)[0]
	seed(t, cs, `{"id":"doc-a","v":2}`)

	// At head: the new value.
	_, head := get(t, base, "/v1/ns/demo%2Fusers/docs/"+first.DocID.String())
	if head["atHead"] != true {
		t.Errorf("a read with no ?at= is a head read: %v", head)
	}

	// At the first commit: the old value, and marked read-only.
	res, past := get(t, base,
		"/v1/ns/demo%2Fusers/docs/"+first.DocID.String()+"?at="+first.Commit.Hex())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("time-travel read: %d (%v)", res.StatusCode, past)
	}
	if past["atHead"] != false || past["readOnly"] != true {
		t.Errorf("a historical read must announce itself as such: %v", past)
	}
	body := past["body"].(map[string]any)
	if body["v"].(float64) != 1 {
		t.Errorf("reading at the first commit should see v=1, got %v", body)
	}
	if head["body"].(map[string]any)["v"].(float64) != 2 {
		t.Error("the head read should still see v=2")
	}
}

func TestDocumentAbsentAtThatRevisionIsNotFound(t *testing.T) {
	cs, base := newFixture(t)
	first := seed(t, cs, `{"id":"doc-a"}`)[0]
	later := seed(t, cs, `{"id":"doc-b"}`)[0]

	// doc-b did not exist at the first commit.
	res, _ := get(t, base,
		"/v1/ns/demo%2Fusers/docs/"+later.DocID.String()+"?at="+first.Commit.Hex())
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("a document that did not exist yet must be 404 at that revision, got %d",
			res.StatusCode)
	}
}

func postJSON(t *testing.T, base, path, body string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer alice:secret")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	raw, _ := io.ReadAll(res.Body)
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	return res, parsed
}

// TestRevertPlanIsReadableWithoutWritePermission: an operator should always be able to see what a
// revert *would* do. Planning writes nothing, so gating it would only mean deciding blind.
func TestRevertPlanIsReadableWithoutWritePermission(t *testing.T) {
	cs, base := newFixture(t)
	first := seed(t, cs, `{"id":"doc-a","v":1}`)[0]
	seed(t, cs, `{"id":"doc-b","v":1}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/plan",
		`{"to":"`+first.Commit.Hex()+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("plan on a read-only control plane: %d (%v)", res.StatusCode, body)
	}
	// Reverting to the first commit removes doc-b, which did not exist then.
	if body["willRemove"].(float64) != 1 {
		t.Errorf("reverting past doc-b's creation should remove it: %v", body)
	}
	if body["expectHead"] == nil || body["expectHead"] == "" {
		t.Error("a plan must name the head it was computed against, so apply can check it")
	}
}

func TestRevertApplyIsRefusedWhileReadOnly(t *testing.T) {
	cs, base := newFixture(t)
	first := seed(t, cs, `{"id":"doc-a","v":1}`)[0]
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/apply",
		`{"to":"`+first.Commit.Hex()+`","expectHead":"head"}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 on a read-only control plane, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "read_only" {
		t.Errorf("the refusal should name the deployment setting, not look like an auth failure: %v",
			body["error"])
	}
}

func TestRevertRestoresStateAsAForwardCommit(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"doc-a","v":1}`)[0]
	seed(t, cs, `{"id":"doc-a","v":2}`)
	seed(t, cs, `{"id":"doc-b","v":1}`)

	_, plan := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/plan", `{"to":"`+first.Commit.Hex()+`"}`)
	expectHead := plan["expectHead"].(string)

	res, applied := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/apply",
		`{"to":"`+first.Commit.Hex()+`","expectHead":"`+expectHead+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("apply: %d (%v)", res.StatusCode, applied)
	}

	// History moves forward: the revert is a new commit, not a rewind.
	newCommit := applied["commit"].(string)
	if newCommit == expectHead || newCommit == first.Commit.Hex() {
		t.Fatalf("a revert must write a new commit, not move head backwards: got %s", newCommit)
	}
	if applied["removed"].(float64) != 1 {
		t.Errorf("doc-b did not exist at the target and should have been removed: %v", applied)
	}

	// And the state is genuinely restored: doc-a reads v=1 again at head.
	_, doc := get(t, base, "/v1/ns/demo%2Fusers/docs/"+first.DocID.String())
	if doc["body"].(map[string]any)["v"].(float64) != 1 {
		t.Errorf("the reverted state should be current: %v", doc["body"])
	}

	// The commit before the revert is still reachable - nothing was destroyed.
	res, _ = get(t, base, "/v1/ns/demo%2Fusers/commits/"+expectHead)
	if res.StatusCode != http.StatusOK {
		t.Error("the pre-revert commit must still be in history; a revert is not a rewrite")
	}
}

// TestRevertApplyRefusesWhenHeadMoved: applying a plan computed against a different head would
// undo a write that never appeared in the preview.
func TestRevertApplyRefusesWhenHeadMoved(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"doc-a","v":1}`)[0]
	seed(t, cs, `{"id":"doc-a","v":2}`)

	_, plan := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/plan", `{"to":"`+first.Commit.Hex()+`"}`)
	staleHead := plan["expectHead"].(string)

	// Someone else commits between the preview and the apply.
	seed(t, cs, `{"id":"doc-c","v":1}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/apply",
		`{"to":"`+first.Commit.Hex()+`","expectHead":"`+staleHead+`"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 when head moved under the plan, got %d (%v)", res.StatusCode, body)
	}
}

func TestRevertApplyRequiresExpectHead(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"doc-a"}`)[0]
	res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/apply", `{"to":"`+first.Commit.Hex()+`"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("apply without expectHead must be refused, got %d", res.StatusCode)
	}
}

func TestRevertToUnknownRevisionIsNotFound(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"doc-a"}`)
	res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/revert/plan", `{"to":"head~999"}`)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404 for a revision that names nothing, got %d", res.StatusCode)
	}
}
