package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
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
	if body["revision"] == nil {
		t.Error("the settings listing must carry the revision a patch compares against")
	}
	if body["canChange"] != false {
		t.Error("a read-only control plane cannot change settings and must say so")
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

// Every endpoint this control plane declares is now implemented, so the "declared but answers 501"
// pattern has no remaining instance to test. An endpoint from the plan that is not built - the
// recovery surface, §8 - is simply not registered and answers 404, which is what an unimplemented
// route should look like once there is no half-built one left to distinguish it from.

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

	d, err := cs.commitDAGFor(cs.opts.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	commit, ok := d.GetCommit(second.Commit)
	if !ok {
		t.Fatal("commit missing")
	}
	_, _, entries, err := cs.diffRevisions(cs.opts.Runtime, commit.ParentHashes[0].Hex(), second.Commit.Hex())
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

	d, _ := cs.commitDAGFor(cs.opts.Runtime)
	commit, ok := d.GetCommit(again.Commit)
	if !ok {
		t.Fatal("commit missing")
	}
	_, _, entries, err := cs.diffRevisions(cs.opts.Runtime, commit.ParentHashes[0].Hex(), again.Commit.Hex())
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
	d, err := cs.commitDAGFor(cs.opts.Runtime)
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

func TestDocumentListPagesAndPreviews(t *testing.T) {
	cs, base := newFixture(t)
	for i := 0; i < 7; i++ {
		seed(t, cs, fmt.Sprintf(`{"id":"doc-%d","n":%d}`, i, i))
	}
	// One body long enough that a listing must not carry it whole.
	seed(t, cs, fmt.Sprintf(`{"id":"big","blob":%q}`, strings.Repeat("x", 500)))

	res, body := get(t, base, "/v1/ns/demo%2Fusers/docs?limit=3")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("docs: %d (%v)", res.StatusCode, body)
	}
	docs, _ := body["documents"].([]any)
	if len(docs) != 3 {
		t.Fatalf("limit=3 must return three documents, got %d", len(docs))
	}
	if body["hasMore"] != true {
		t.Error("eight documents and a limit of three means more remain")
	}
	if body["atHead"] != true {
		t.Error("a listing with no ?at= is a head read")
	}

	// The cursor must not repeat what the first page already showed.
	cursor := body["nextCursor"].(string)
	_, page2 := get(t, base, "/v1/ns/demo%2Fusers/docs?limit=3&cursor="+cursor)
	seen := map[string]bool{}
	for _, d := range docs {
		seen[d.(map[string]any)["docId"].(string)] = true
	}
	rest, _ := page2["documents"].([]any)
	if len(rest) == 0 {
		t.Fatal("the second page is empty")
	}
	for _, d := range rest {
		if seen[d.(map[string]any)["docId"].(string)] {
			t.Error("a cursor page repeated a document from the first page")
		}
	}
}

func TestDocumentListTruncatesLongBodies(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, fmt.Sprintf(`{"id":"big","blob":%q}`, strings.Repeat("x", 5000)))

	_, body := get(t, base, "/v1/ns/demo%2Fusers/docs")
	docs, _ := body["documents"].([]any)
	if len(docs) != 1 {
		t.Fatalf("want one document, got %d", len(docs))
	}
	d := docs[0].(map[string]any)
	if d["truncated"] != true {
		t.Error("a 5KB body must be reported as truncated, not returned whole in a listing")
	}
	if len(d["preview"].(string)) > previewBytes {
		t.Errorf("preview is %d bytes, over the cap", len(d["preview"].(string)))
	}
	if d["sizeBytes"].(float64) < 5000 {
		t.Error("sizeBytes should describe the whole document, not the preview")
	}
}

// TestDocumentListAtPastRevision: the browser is as useful looking backwards as forwards, which is
// most of the argument for having it in this database rather than a generic one.
func TestDocumentListAtPastRevision(t *testing.T) {
	cs, base := newFixture(t)
	first := seed(t, cs, `{"id":"a"}`)[0]
	seed(t, cs, `{"id":"b"}`)

	_, now := get(t, base, "/v1/ns/demo%2Fusers/docs")
	if len(now["documents"].([]any)) != 2 {
		t.Fatalf("head should hold both documents: %v", now["documents"])
	}

	_, past := get(t, base, "/v1/ns/demo%2Fusers/docs?at="+first.Commit.Hex())
	if n := len(past["documents"].([]any)); n != 1 {
		t.Fatalf("only one document existed at the first commit, got %d", n)
	}
	if past["atHead"] != false || past["readOnly"] != true {
		t.Errorf("a historical listing must announce itself as such: %v", past)
	}
}

func TestSQLConsoleRunsSelects(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a","name":"ada"}`, `{"id":"b","name":"grace"}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELECT * FROM users"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("select: %d (%v)", res.StatusCode, body)
	}
	if body["rowCount"].(float64) != 2 {
		t.Errorf("want two rows, got %v", body["rowCount"])
	}
	if body["plan"] == nil {
		t.Error("the chosen access path should be reported - it is how you learn a query is a full scan")
	}
	if body["resolvedCommit"] == "" {
		t.Error("a query result should name the commit it read at")
	}
}

// TestSQLConsoleRefusesWritesWhenReadOnly: a control plane started read-only runs SELECT and
// nothing else, and the refusal names the deployment setting rather than looking like an auth
// failure - they send an operator to look in different places.
func TestSQLConsoleRefusesWritesWhenReadOnly(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a","name":"ada"}`)

	for _, stmt := range []string{
		`DELETE FROM users`,
		`UPDATE users SET name = 'x'`,
		`INSERT INTO users (name) VALUES ('x')`,
	} {
		res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", fmt.Sprintf(`{"sql":%q}`, stmt))
		if res.StatusCode != http.StatusForbidden {
			t.Errorf("%q: want 403 on a read-only control plane, got %d (%v)", stmt, res.StatusCode, body)
			continue
		}
		if body["error"].(map[string]any)["code"] != "read_only" {
			t.Errorf("%q: the refusal should name the deployment setting: %v", stmt, body["error"])
		}
	}

	_, after := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELECT * FROM users"}`)
	if after["rowCount"].(float64) != 1 {
		t.Errorf("a refused statement must not have run: %v", after["rowCount"])
	}
}

func TestSQLConsoleWritesWhenAllowed(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a","name":"ada","score":1}`, `{"id":"b","name":"grace","score":2}`)

	t.Run("update commits and reports the commit", func(t *testing.T) {
		res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql",
			`{"sql":"UPDATE users SET score = 99"}`)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("update: %d (%v)", res.StatusCode, body)
		}
		if body["committed"] != true {
			t.Errorf("a statement that changed rows must commit: %v", body)
		}
		if body["rowsAffected"].(float64) != 2 {
			t.Errorf("want two rows affected, got %v", body["rowsAffected"])
		}
		if body["commit"] == nil || body["commit"] == "" {
			t.Error("a write should name the commit it produced - that is the audit trail")
		}
	})

	t.Run("the change is visible and is real history", func(t *testing.T) {
		_, sel := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELECT * FROM users"}`)
		if sel["rowCount"].(float64) != 2 {
			t.Fatalf("want two rows, got %v", sel["rowCount"])
		}
		// The commit the write produced must be in the log, not merely reported.
		_, logBody := get(t, base, "/v1/ns/demo%2Fusers/log")
		commits, _ := logBody["commits"].([]any)
		if len(commits) < 3 {
			t.Errorf("the update should have added a commit to history: %d commits", len(commits))
		}
	})

	t.Run("delete commits", func(t *testing.T) {
		res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"DELETE FROM users"}`)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("delete: %d (%v)", res.StatusCode, body)
		}
		_, sel := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELECT * FROM users"}`)
		if sel["rowCount"].(float64) != 0 {
			t.Errorf("everything should be deleted, got %v rows", sel["rowCount"])
		}
	})
}

// TestSQLWriteMatchingNothingDoesNotCommit: an empty commit for a statement that changed nothing is
// noise in the one place noise is expensive.
func TestSQLWriteMatchingNothingDoesNotCommit(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a","name":"ada"}`)
	_, before := get(t, base, "/v1/ns/demo%2Fusers/log")
	countBefore := len(before["commits"].([]any))

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql",
		`{"sql":"DELETE FROM users WHERE name = 'nobody'"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d (%v)", res.StatusCode, body)
	}
	if body["committed"] != false {
		t.Errorf("nothing matched, so nothing should have been committed: %v", body)
	}
	_, after := get(t, base, "/v1/ns/demo%2Fusers/log")
	if len(after["commits"].([]any)) != countBefore {
		t.Error("history grew for a statement that changed nothing")
	}
}

// TestSQLWriteAtPastRevisionIsRefused: history moves forward. A statement asking to change the past
// is a mistake worth naming rather than quietly running against head.
func TestSQLWriteAtPastRevisionIsRefused(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","name":"ada"}`)[0]

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql",
		fmt.Sprintf(`{"sql":"DELETE FROM users","at":%q}`, first.Commit.Hex()))
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "not_writable_at_revision" {
		t.Errorf("the refusal should say why: %v", body["error"])
	}
}

// TestSQLWriteNeedsAWriteGrant: --control-write says what the deployment permits; the grant says
// what this principal may do. Both are asked, and neither substitutes for the other.
func TestSQLWriteNeedsAWriteGrant(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a","name":"ada"}`)
	cs.opts.Runtime.AuthEngine = readOnlyEngine{}

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"DELETE FROM users"}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 when the principal has no write grant, got %d (%v)", res.StatusCode, body)
	}
	// And a SELECT from the same principal still works: the grant is per-action, not per-endpoint.
	res, _ = postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELECT * FROM users"}`)
	if res.StatusCode != http.StatusOK {
		t.Errorf("a read grant should still allow SELECT, got %d", res.StatusCode)
	}
}

// readOnlyEngine authenticates anyone and authorizes reads only.
type readOnlyEngine struct{}

func (readOnlyEngine) Authenticator() auth.Authenticator { return readOnlyEngine{} }
func (readOnlyEngine) Authorizer() auth.Authorizer       { return readOnlyEngine{} }
func (readOnlyEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{ID: "reader"}, nil
}
func (readOnlyEngine) Authorize(_ context.Context, _ auth.Principal, action auth.Action) error {
	if a, ok := action.(auth.SqlExecAction); ok && !a.ReadOnly {
		return fmt.Errorf("principal reader lacks write on %s", a.Namespace)
	}
	return nil
}

func TestSQLConsoleReportsParseErrors(t *testing.T) {
	_, base := newFixture(t)
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql", `{"sql":"SELEKT nonsense"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for unparseable SQL, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "parse_error" {
		t.Errorf("a parse failure should be distinguishable from a query failure: %v", body["error"])
	}
}

func TestUnknownNamespaceIsNotFound(t *testing.T) {
	_, base := newFixture(t)
	// 404 rather than 403: answering "forbidden" for a namespace that does not exist would tell an
	// unauthorized caller which namespaces do.
	res, _ := get(t, base, "/v1/ns/no%2Fsuch/log")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404 for a namespace this plane does not serve, got %d", res.StatusCode)
	}
}

func TestMultipleNamespacesAreServedIndependently(t *testing.T) {
	rtA, err := embed.OpenMemoryRuntime("demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rtB, err := embed.OpenMemoryRuntime("demo", "demo/orders", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srvA, srvB := server.NewKdbServerRuntime(rtA), server.NewKdbServerRuntime(rtB)
	cs, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: srvA, Namespace: "demo/users", Version: "test",
		Namespaces: StaticNamespaces(map[string]*server.KdbServerRuntime{
			"demo/users": srvA, "demo/orders": srvB,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	base := "http://" + cs.Addr().String()

	if _, err := embed.PutJSONDocument(rtA, "demo/users", `{"id":"u1"}`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := embed.PutJSONDocument(rtB, "demo/orders", fmt.Sprintf(`{"id":"o%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}

	_, list := get(t, base, "/v1/namespaces")
	if n := len(list["namespaces"].([]any)); n != 2 {
		t.Fatalf("want both namespaces listed, got %d", n)
	}
	if list["default"] != "demo/users" {
		t.Errorf("the default namespace should be reported: %v", list["default"])
	}

	// Each namespace must answer about itself, not about whichever runtime was registered first.
	_, a := get(t, base, "/v1/ns/demo%2Fusers/docs")
	_, b := get(t, base, "/v1/ns/demo%2Forders/docs")
	if len(a["documents"].([]any)) != 1 {
		t.Errorf("demo/users has one document: %v", a["documents"])
	}
	if len(b["documents"].([]any)) != 3 {
		t.Errorf("demo/orders has three documents: %v", b["documents"])
	}
}

func sendJSON(t *testing.T, method, base, path, body string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer alice:secret")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	raw, _ := io.ReadAll(res.Body)
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	return res, parsed
}

func TestDocumentEditRoundTrip(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","name":"ada","score":1}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	// A read hands back the hash the next write should assert on.
	_, read := get(t, base, path)
	hash, _ := read["contentHash"].(string)
	if hash == "" {
		t.Fatal("a document read must carry its content hash, or an editor cannot write conditionally")
	}

	res, saved := sendJSON(t, http.MethodPut, base, path,
		`{"body":{"id":"a","name":"ada","score":2},"ifContentHash":"`+hash+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("conditional save: %d (%v)", res.StatusCode, saved)
	}
	if saved["commit"] == nil || saved["contentHash"] == nil {
		t.Errorf("a save should report its commit and the new hash: %v", saved)
	}

	_, after := get(t, base, path)
	if after["body"].(map[string]any)["score"].(float64) != 2 {
		t.Errorf("the edit did not land: %v", after["body"])
	}
	if after["contentHash"] == hash {
		t.Error("the content changed, so its hash must have changed too")
	}
}

// TestConditionalSaveRefusesAStaleEdit is the whole point of the content hash: a browser holds a
// document on screen while other clients keep writing.
func TestConditionalSaveRefusesAStaleEdit(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","name":"ada","score":1}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	_, read := get(t, base, path)
	stale := read["contentHash"].(string)

	// Someone else writes first.
	if _, err := embed.PutJSONDocument(cs.opts.Runtime.Runtime, "demo/users",
		`{"id":"a","name":"ada","score":99}`); err != nil {
		t.Fatal(err)
	}

	res, body := sendJSON(t, http.MethodPut, base, path,
		`{"body":{"id":"a","name":"ada","score":2},"ifContentHash":"`+stale+`"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("a stale conditional save must be refused, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "precondition_failed" {
		t.Errorf("a failed compare-and-set is not a plain conflict: %v", body["error"])
	}
	// The hash that beat it, so the client can re-read and merge rather than retry blind.
	conflicts, _ := body["conflicts"].([]any)
	if len(conflicts) == 0 || conflicts[0].(map[string]any)["actualContentHash"] == nil {
		t.Errorf("the refusal must carry the content hash that won: %v", body["conflicts"])
	}

	// And the other writer's value is intact - the refused save changed nothing.
	_, after := get(t, base, path)
	if after["body"].(map[string]any)["score"].(float64) != 99 {
		t.Errorf("a refused save must not have partially applied: %v", after["body"])
	}
}

func TestUnconditionalSaveOverwrites(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","score":1}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	res, _ := sendJSON(t, http.MethodPut, base, path, `{"body":{"id":"a","score":7}}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("an unconditional save should apply, got %d", res.StatusCode)
	}
	_, after := get(t, base, path)
	if after["body"].(map[string]any)["score"].(float64) != 7 {
		t.Errorf("unexpected body: %v", after["body"])
	}
}

// TestSaveMergesAndSaysWhatItKept records the semantics an editor has to live with, and the reason
// the response is not silent about them.
//
// Every write path in this engine is a shallow root-level merge, so a key the body omits keeps its
// stored value. An operator deleting a line in a text box and pressing save would otherwise have no
// way to know the key is still there. There is no replace primitive to reach for - a WriteOp is
// merged on the way in, and a DeleteOp in the same transaction does not help, because staging reads
// every operation against the baseline tree rather than against each other.
func TestSaveMergesAndSaysWhatItKept(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","keep":1,"remove":2}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	res, saved := sendJSON(t, http.MethodPut, base, path, `{"body":{"id":"a","keep":9}}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("save: %d (%v)", res.StatusCode, saved)
	}
	retained, _ := saved["retainedKeys"].([]any)
	if len(retained) != 1 || retained[0] != "remove" {
		t.Fatalf("the save must name the key it kept, or the operator learns about it later: %v", saved)
	}
	if saved["note"] == nil {
		t.Error("a kept key needs an explanation, not just a list")
	}

	_, after := get(t, base, path)
	body := after["body"].(map[string]any)
	if body["keep"].(float64) != 9 {
		t.Errorf("the edited value should be current: %v", body)
	}
	if _, still := body["remove"]; !still {
		t.Error("this engine merges; the omitted key is expected to survive")
	}

	// The returned hash must describe what is stored, not what was sent - a client using it for
	// its next conditional write would otherwise be refused every time.
	if saved["contentHash"] != after["contentHash"] {
		t.Errorf("the save reported hash %v but the document reads %v",
			saved["contentHash"], after["contentHash"])
	}
}

// TestSavedHashIsUsableForTheNextSave is the practical consequence: edit twice in a row without
// re-reading in between.
func TestSavedHashIsUsableForTheNextSave(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","n":1}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	_, read := get(t, base, path)
	_, first := sendJSON(t, http.MethodPut, base, path,
		`{"body":{"id":"a","n":2},"ifContentHash":"`+read["contentHash"].(string)+`"}`)
	hash, ok := first["contentHash"].(string)
	if !ok || hash == "" {
		t.Fatalf("a save must report the resulting hash: %v", first)
	}
	res, second := sendJSON(t, http.MethodPut, base, path,
		`{"body":{"id":"a","n":3},"ifContentHash":"`+hash+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("the hash from the previous save should still be current: %d (%v)",
			res.StatusCode, second)
	}
}

func TestIfAbsentCreatesOnlyOnce(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a"}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	res, body := sendJSON(t, http.MethodPut, base, path, `{"body":{"id":"a","v":2},"ifAbsent":true}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("ifAbsent against an existing document must be refused, got %d (%v)", res.StatusCode, body)
	}
}

func TestContradictoryPreconditionsAreRefused(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a"}`)[0]
	res, body := sendJSON(t, http.MethodPut, base,
		"/v1/ns/demo%2Fusers/docs/"+put.DocID.String(),
		`{"body":{"id":"a"},"ifAbsent":true,"ifContentHash":"`+strings.Repeat("a", 64)+`"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("asserting both absence and a content hash is incoherent: got %d (%v)",
			res.StatusCode, body)
	}
}

func TestDocumentDelete(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a"}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	res, body := sendJSON(t, http.MethodDelete, base, path, `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d (%v)", res.StatusCode, body)
	}
	res, _ = get(t, base, path)
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("the document should be gone from head, got %d", res.StatusCode)
	}
	// But still readable in history: a delete is a commit, not an erasure.
	_, logBody := get(t, base, "/v1/ns/demo%2Fusers/log")
	commits, _ := logBody["commits"].([]any)
	prior := commits[1].(map[string]any)["hash"].(string)
	res, _ = get(t, base, path+"?at="+prior)
	if res.StatusCode != http.StatusOK {
		t.Errorf("a deleted document must still be readable at a commit before the delete, got %d",
			res.StatusCode)
	}
}

func TestConditionalDeleteRefusesAStaleHash(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","v":1}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()
	_, read := get(t, base, path)
	stale := read["contentHash"].(string)

	if _, err := embed.PutJSONDocument(cs.opts.Runtime.Runtime, "demo/users", `{"id":"a","v":2}`); err != nil {
		t.Fatal(err)
	}
	res, _ := sendJSON(t, http.MethodDelete, base, path, `{"ifContentHash":"`+stale+`"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("a conditional delete against a stale hash must be refused, got %d", res.StatusCode)
	}
	res, _ = get(t, base, path)
	if res.StatusCode != http.StatusOK {
		t.Error("the refused delete must not have removed anything")
	}
}

func TestDocumentWritesRefusedWhileReadOnly(t *testing.T) {
	cs, base := newFixture(t)
	put := seed(t, cs, `{"id":"a"}`)[0]
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	for _, m := range []string{http.MethodPut, http.MethodDelete} {
		res, body := sendJSON(t, m, base, path, `{"body":{"id":"a"}}`)
		if res.StatusCode != http.StatusForbidden {
			t.Errorf("%s on a read-only control plane: got %d (%v)", m, res.StatusCode, body)
		}
	}
}

func TestInvalidDocumentBodyIsRefused(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a"}`)[0]
	res, _ := sendJSON(t, http.MethodPut, base,
		"/v1/ns/demo%2Fusers/docs/"+put.DocID.String(), `{"body":"not an object"}`)
	// A string is valid JSON but not a document; the engine is the authority on that, and the
	// point of the test is that it is refused rather than stored.
	if res.StatusCode == http.StatusOK {
		t.Error("a non-object body should not be stored as a document")
	}
}

// TestEngineWriteAndReplayAgree checks the write path this control plane actually uses against the
// one that reconstructs it after a restart.
//
// They read the same WriteOp differently. The transaction engine merges the patch over whatever is
// stored (transaction/default_engine.go: baseDoc.Merge), while replay treats the patch as the whole
// document (embed/delta_replay.go: FromJSONWithID then PutDocument). A commit records the operation
// it was given, not the merged result - so a write naming fewer keys than the document already has
// is a case where the two readings could disagree, and a document could come back smaller after a
// restart than it was when the write was acknowledged.
//
// This drives a real HTTP save over a file-backed namespace, closes it, reopens, and compares.
func TestEngineWriteAndReplayAgree(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := server.NewKdbServerRuntime(rt)
	cs, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: srv, Namespace: "demo/users",
		Version: "test", AllowWrites: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + cs.Addr().String()

	put, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a","x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	// A save naming only y - fewer keys than the stored document has.
	if res, body := sendJSON(t, http.MethodPut, base, path, `{"body":{"id":"a","y":2}}`); res.StatusCode != http.StatusOK {
		t.Fatalf("save: %d (%v)", res.StatusCode, body)
	}
	_, live := get(t, base, path)
	liveKeys := keysOf(live["body"])
	_, liveStatus := get(t, base, "/v1/ns/demo%2Fusers/status")
	t.Logf("live head tree: %v", liveStatus["headCommit"].(map[string]any)["treeHash"])

	_ = cs.Close()
	rt.Close()

	reopened, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	srv2 := server.NewKdbServerRuntime(reopened)
	cs2, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: srv2, Namespace: "demo/users", Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs2.Close() })

	base2 := "http://" + cs2.Addr().String()
	res2, after := get(t, base2, path)
	t.Logf("after restart: HTTP %d body=%v", res2.StatusCode, after)
	_, list := get(t, base2, "/v1/ns/demo%2Fusers/docs")
	t.Logf("documents after restart: %v", list["documents"])
	if hc, ok := func() (map[string]any, bool) {
		_, st := get(t, base2, "/v1/ns/demo%2Fusers/status")
		m, ok := st["headCommit"].(map[string]any)
		return m, ok
	}(); ok {
		t.Logf("restarted head tree: %v", hc["treeHash"])
	}
	_, lg := get(t, base2, "/v1/ns/demo%2Fusers/log")
	if cs, ok := lg["commits"].([]any); ok {
		t.Logf("commits after restart: %d", len(cs))
	}
	replayedKeys := keysOf(after["body"])

	if liveKeys != replayedKeys {
		t.Fatalf(
			"the document reads differently before and after a restart.\n"+
				"  live (transaction engine merged the patch): %s\n"+
				"  after restart (replay took it whole):       %s\n"+
				"A commit records the operation it was given, so a partial write is reconstructed "+
				"as a smaller document than the one that was acknowledged.", liveKeys, replayedKeys)
	}
}

func keysOf(body any) string {
	m, ok := body.(map[string]any)
	if !ok {
		return "<absent>"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return "{" + strings.Join(keys, ",") + "}"
}

// withSettings builds a fixture whose descriptors are the real service defaults, plus a live log
// level holder, so the settings endpoints have something true to report and change.
func withSettings(level *slog.LevelVar, persist bool) func(*Options) {
	return func(o *Options) {
		o.AllowWrites = true
		o.AllowSettingsPersist = persist
		o.LogLevel = level
		o.Settings = append(
			config.Describe(nil, noEnvLookup, noFlagSet, config.DefaultServiceSettings()),
			config.EnvOnlyDescriptors(noEnvLookup)...)
	}
}

func noEnvLookup(string) (string, bool) { return "", false }
func noFlagSet(string) bool             { return false }

func TestLiveSettingChangeApplies(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	_, base := newFixture(t, withSettings(level, false))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	outcomes := body["changes"].([]any)
	if outcomes[0].(map[string]any)["applied"] != true {
		t.Fatalf("log.level is live-mutable and should have applied: %v", outcomes[0])
	}
	// The process's actual logger, not just the reported value.
	if level.Level() != slog.LevelDebug {
		t.Errorf("the running log level is still %v", level.Level())
	}

	// And the reported settings now say what is in force, and who changed it.
	_, listed := get(t, base, "/v1/settings")
	for _, raw := range listed["settings"].([]any) {
		d := raw.(map[string]any)
		if d["key"] != "log.level" {
			continue
		}
		if d["value"] != "debug" {
			t.Errorf("the listing should report the running value: %v", d["value"])
		}
		if d["source"] != "runtime" {
			t.Errorf("a changed setting is sourced from the runtime, not its original layer: %v", d["source"])
		}
		if !strings.Contains(fmt.Sprint(d["sourceDetail"]), "changed at") {
			t.Errorf("the change should be attributed: %v", d["sourceDetail"])
		}
	}
}

// TestRestartOnlySettingIsRefusedWithItsClass: "cannot change that" sends an operator looking. The
// mutability class tells them what would work.
func TestRestartOnlySettingIsRefusedWithItsClass(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))

	for _, tc := range []struct{ key, value, wants string }{
		{"listener.sqlAddr", `"tcp://127.0.0.1:1"`, "startup"},
		{"governance.maxConnections", `99`, "listener or connection is created"},
		{"cache.documentBytes", `1024`, "namespace"},
		{"history.strategy", `"objects"`, "migration"},
	} {
		res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
			fmt.Sprintf(`{"changes":[{"key":%q,"value":%s}]}`, tc.key, tc.value))
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: want 400 when nothing applied, got %d (%v)", tc.key, res.StatusCode, body)
			continue
		}
		outcome := body["changes"].([]any)[0].(map[string]any)
		if outcome["applied"] == true {
			t.Errorf("%s must not be changeable at runtime", tc.key)
		}
		if !strings.Contains(fmt.Sprint(outcome["refused"]), tc.wants) {
			t.Errorf("%s: the refusal should explain what would work; got %q", tc.key, outcome["refused"])
		}
	}
}

func TestUnknownSettingIsRefused(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"not.a.setting","value":1}]}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d (%v)", res.StatusCode, body)
	}
	if body["changes"].([]any)[0].(map[string]any)["refused"] != "no such setting" {
		t.Errorf("unexpected refusal: %v", body["changes"])
	}
}

func TestInvalidValueIsRefusedBeforeAnythingChanges(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	_, base := newFixture(t, withSettings(level, false))

	res, _ := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"log.level","value":"chatty"}]}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for an unknown level, got %d", res.StatusCode)
	}
	if level.Level() != slog.LevelInfo {
		t.Error("a rejected value must not have been installed")
	}
}

// TestDryRunChangesNothing: the review step before applying has to be exact, which means it runs
// the same validation and then stops.
func TestDryRunChangesNothing(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelWarn)
	_, base := newFixture(t, withSettings(level, false))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"dryRun":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dry run: %d (%v)", res.StatusCode, body)
	}
	if body["dryRun"] != true {
		t.Error("the response should say it was a dry run")
	}
	if level.Level() != slog.LevelWarn {
		t.Error("a dry run must not change the running level")
	}
	// And an invalid value is still caught by a dry run - that is the point of it.
	res, _ = sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"dryRun":true,"changes":[{"key":"log.level","value":"nonsense"}]}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("a dry run should surface an invalid value, got %d", res.StatusCode)
	}
}

func TestSettingsRevisionGuardsAgainstOverwriting(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))

	_, first := get(t, base, "/v1/settings")
	rev := int64(first["revision"].(float64))

	if res, _ := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		fmt.Sprintf(`{"expectRevision":%d,"changes":[{"key":"log.level","value":"debug"}]}`, rev)); res.StatusCode != http.StatusOK {
		t.Fatalf("a patch at the current revision should apply, got %d", res.StatusCode)
	}
	// The same revision is now stale - someone else (this test) moved it.
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		fmt.Sprintf(`{"expectRevision":%d,"changes":[{"key":"log.level","value":"warn"}]}`, rev))
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("a stale revision must be refused, got %d (%v)", res.StatusCode, body)
	}
}

// TestDriftReportsWhatARestartWouldUndo: a live change that silently vanishes on restart is a trap.
func TestDriftReportsWhatARestartWouldUndo(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))

	_, clean := get(t, base, "/v1/settings/drift")
	if len(clean["drift"].([]any)) != 0 {
		t.Fatalf("a freshly started process has no drift: %v", clean["drift"])
	}

	sendJSON(t, http.MethodPatch, base, "/v1/settings", `{"changes":[{"key":"log.level","value":"error"}]}`)

	_, drifted := get(t, base, "/v1/settings/drift")
	items := drifted["drift"].([]any)
	if len(items) != 1 {
		t.Fatalf("the applied change should show as drift: %v", items)
	}
	item := items[0].(map[string]any)
	if item["key"] != "log.level" || item["running"] != "error" || item["atStartup"] != "info" {
		t.Errorf("drift should say running vs startup: %v", item)
	}
}

func TestPersistIsRefusedWhenTheDeploymentForbidsIt(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 when persistence is off, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "persist_disabled" {
		t.Errorf("the refusal should name the reason: %v", body["error"])
	}
	// And it must point at the alternative rather than just refusing.
	if !strings.Contains(fmt.Sprint(body["error"].(map[string]any)["message"]), "drift") {
		t.Error("the refusal should say the change can still be applied and will show as drift")
	}
}

func TestSettingsCannotChangeOnAReadOnlyControlPlane(t *testing.T) {
	_, base := newFixture(t, func(o *Options) {
		o.LogLevel = new(slog.LevelVar)
		o.Settings = append(
			config.Describe(nil, noEnvLookup, noFlagSet, config.DefaultServiceSettings()),
			config.EnvOnlyDescriptors(noEnvLookup)...)
	})
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d (%v)", res.StatusCode, body)
	}
}

func TestSingleSettingLookup(t *testing.T) {
	_, base := newFixture(t, withSettings(new(slog.LevelVar), false))

	res, body := get(t, base, "/v1/settings/log.level")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("lookup: %d (%v)", res.StatusCode, body)
	}
	if body["liveMutable"] != true {
		t.Error("log.level is live-mutable and should say so")
	}
	d := body["setting"].(map[string]any)
	if d["help"] == nil || d["help"] == "" {
		t.Error("a single-setting lookup is where the full help text belongs")
	}

	res, body = get(t, base, "/v1/settings/listener.sqlAddr")
	if body["liveMutable"] != false || body["refusal"] == nil {
		t.Errorf("a restart-only setting should say so and explain: %v", body)
	}

	res, _ = get(t, base, "/v1/settings/nope")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown key is 404, got %d", res.StatusCode)
	}
}

// TestMemoryBudgetChangeReachesAdmission: the memory trio share one setter, so a change to one has
// to carry the other two forward rather than resetting them.
func TestMemoryBudgetChangeReachesAdmission(t *testing.T) {
	cs, base := newFixture(t, withSettings(new(slog.LevelVar), false))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"memory.budgetMB","value":512}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	if cs.opts.Runtime.Admission() == nil {
		t.Fatal("setting a budget should have installed admission control")
	}
	// The reserve came from the defaults, not from zero: changing the budget must not silently
	// discard the rest of the tuple the setter takes.
	if got := cs.opts.Runtime.Admission().RescueReserveBytes(); got == 0 {
		t.Error("the rescue reserve was reset to zero by a budget change")
	}
}

// fileFixture starts a control plane over a file-backed namespace, which recovery needs: an
// in-memory namespace has no delta log to verify or back up.
func fileFixture(t *testing.T, opts ...func(*Options)) (*Server, string, *embed.EmbeddedKdbRuntime) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "data")
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(rt.Close)
	srv := server.NewKdbServerRuntime(rt)
	o := Options{
		Addr: "127.0.0.1:0", Runtime: srv, Namespace: "demo/users", Version: "test",
		AllowWrites: true, BackupDir: filepath.Join(t.TempDir(), "backups"),
	}
	for _, fn := range opts {
		fn(&o)
	}
	cs, err := New(o)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, "http://" + cs.Addr().String(), rt
}

// TestVerifyRunsAgainstALiveNamespace is the property the whole online tier rests on: the server
// holds the data directory's exclusive lock, so a verification that tried to acquire it would
// deadlock against itself.
func TestVerifyRunsAgainstALiveNamespace(t *testing.T) {
	_, base, rt := fileFixture(t)
	for i := 0; i < 5; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d","n":%d}`, i, i)); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing has run yet, and that is reported rather than looking like a clean result.
	_, before := get(t, base, "/v1/ns/demo%2Fusers/integrity")
	if before["everRun"] != false {
		t.Errorf("a namespace with no verification must not look verified: %v", before)
	}

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/integrity/verify", `{"level":"L2"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("verify: %d (%v)", res.StatusCode, body)
	}
	report := body["report"].(map[string]any)
	if report["level"] != "L2" {
		t.Errorf("the report should say which level ran: %v", report["level"])
	}
	if len(report["segments"].([]any)) == 0 {
		t.Error("five commits should have produced at least one segment")
	}
	// A live namespace can legitimately report a finding on the segment being appended to; what it
	// must not do is fail to run.
	for _, raw := range report["findings"].([]any) {
		f := raw.(map[string]any)
		if f["onActiveSegment"] != true {
			t.Errorf("unexpected finding away from the active segment: %v", f)
		}
	}

	// And it is cached, so the UI does not have to re-scan to show the last result.
	_, after := get(t, base, "/v1/ns/demo%2Fusers/integrity")
	if after["everRun"] != true || after["report"] == nil {
		t.Errorf("the report should be cached: %v", after)
	}
}

func TestVerifyRejectsAnUnknownLevel(t *testing.T) {
	_, base, _ := fileFixture(t)
	res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/integrity/verify", `{"level":"L9"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for an unknown level, got %d", res.StatusCode)
	}
}

func TestVerifyIsRefusedForAMemoryNamespace(t *testing.T) {
	_, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/integrity/verify", `{}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("an in-memory namespace has no log to verify: got %d (%v)", res.StatusCode, body)
	}
}

// TestVerifyNeedsNoWritePermission: an operator must always be able to find out whether their data
// is intact, whatever the deployment's write setting.
func TestVerifyNeedsNoWritePermission(t *testing.T) {
	_, base, rt := fileFixture(t, func(o *Options) { o.AllowWrites = false })
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/integrity/verify", `{"level":"L1"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("verify on a read-only control plane: %d (%v)", res.StatusCode, body)
	}
}

func TestBackupCreateListAndVerify(t *testing.T) {
	_, base, rt := fileFixture(t)
	for i := 0; i < 3; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}

	res, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("backup: %d (%v)", res.StatusCode, made)
	}
	id, _ := made["backupId"].(string)
	if id == "" {
		t.Fatalf("a backup must report its id: %v", made)
	}
	if made["commitCount"].(float64) == 0 {
		t.Error("a backup of three commits should count them")
	}

	_, listed := get(t, base, "/v1/ns/demo%2Fusers/backups")
	backups := listed["backups"].([]any)
	if len(backups) != 1 || backups[0].(map[string]any)["backupId"] != id {
		t.Fatalf("the backup should be listed: %v", backups)
	}

	// Verifying re-reads every object and re-hashes it. An unverified backup is a guess.
	res, verified := postJSON(t, base, "/v1/ns/demo%2Fusers/backups/"+id+"/verify", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("backup verify: %d (%v)", res.StatusCode, verified)
	}
	if verified["clean"] != true {
		t.Errorf("a backup taken moments ago should verify clean: %v", verified)
	}
}

// TestBackupOfALiveNamespaceRecordsTheActivePrefix: backing up while writes are landing works
// because Create stores the still-being-written segment's CRC-verified prefix rather than
// requiring a sealed log. The response says so, because "everything up to when I started" is a
// different promise from "everything".
func TestBackupOfALiveNamespaceRecordsTheActivePrefix(t *testing.T) {
	_, base, rt := fileFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	if made["verifiedPrefixSegments"] != nil && made["note"] == nil {
		t.Error("a prefix-only segment needs the caveat spelled out, not just counted")
	}
}

func TestIncrementalBackupReferencesItsBase(t *testing.T) {
	_, base, rt := fileFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	_, first := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	firstID := first["backupId"].(string)

	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"b"}`); err != nil {
		t.Fatal(err)
	}
	res, second := postJSON(t, base, "/v1/ns/demo%2Fusers/backups",
		`{"baseBackupId":"`+firstID+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("incremental backup: %d (%v)", res.StatusCode, second)
	}
	if second["incremental"] != true || second["baseBackupId"] != firstID {
		t.Errorf("an incremental backup should name its base: %v", second)
	}
}

func TestBackupsRefusedWithoutADirectory(t *testing.T) {
	_, base, _ := fileFixture(t, func(o *Options) { o.BackupDir = "" })
	res, body := get(t, base, "/v1/ns/demo%2Fusers/backups")
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501 when no backup directory is configured, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "--control-backup-dir") {
		t.Error("the refusal should name the flag that enables it")
	}
}

func TestBackupCreationIsGatedOnWrites(t *testing.T) {
	_, base, _ := fileFixture(t, func(o *Options) { o.AllowWrites = false })
	res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("creating a backup consumes disk and is gated; got %d", res.StatusCode)
	}
}

// TestMaintenancePlanNamesTheCommandAndItsPrecondition is the offline tier: the control plane
// cannot run these, so the value is in getting the operator to the right one with the right flags.
func TestMaintenancePlanNamesTheCommandAndItsPrecondition(t *testing.T) {
	_, base, rt := fileFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}

	res, plan := get(t, base, "/v1/ns/demo%2Fusers/maintenance/plan")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("plan: %d (%v)", res.StatusCode, plan)
	}
	ops := plan["operations"].([]any)
	if len(ops) == 0 {
		t.Fatal("the plan should list the offline operations")
	}
	byName := map[string]map[string]any{}
	for _, raw := range ops {
		o := raw.(map[string]any)
		byName[o["name"].(string)] = o
	}
	repair, ok := byName["repair-segments"]
	if !ok {
		t.Fatal("repair-segments should be listed")
	}
	cmd := repair["command"].([]any)
	if cmd[0] != "kdb-inspect" {
		t.Errorf("the command should be runnable as printed: %v", cmd)
	}
	if !strings.Contains(fmt.Sprint(cmd), "demo/users") {
		t.Error("the command should be filled in with this namespace, not a placeholder")
	}
	if !strings.Contains(fmt.Sprint(repair["precondition"]), "must not be running") {
		t.Errorf("the precondition has to be explicit: %v", repair["precondition"])
	}
	// With no verification run, applicability is unknown rather than asserted.
	if repair["applicable"] != false || !strings.Contains(fmt.Sprint(repair["reason"]), "no verification") {
		t.Errorf("without a verification the plan should say it does not know: %v", repair)
	}

	// The restore entry is the one that can run while the service is up, and should say so.
	restore := byName["restore"]
	if !strings.Contains(fmt.Sprint(restore["precondition"]), "different directory") {
		t.Errorf("restore locks only its output directory and should say so: %v", restore["precondition"])
	}
}

func TestCheckpointStatusReportsReplayCost(t *testing.T) {
	_, base, rt := fileFixture(t)
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	res, body := get(t, base, "/v1/ns/demo%2Fusers/checkpoints")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("checkpoints: %d (%v)", res.StatusCode, body)
	}
	if body["fileBacked"] != true {
		t.Fatalf("this namespace is file-backed: %v", body)
	}
	if body["segments"].(float64) == 0 || body["deltaLogBytes"].(float64) == 0 {
		t.Errorf("the numbers that predict restart time should be real: %v", body)
	}
}

// stagingFixture adds a staging directory to the file-backed fixture.
func stagingFixture(t *testing.T) (*Server, string, *embed.EmbeddedKdbRuntime) {
	t.Helper()
	staging := filepath.Join(t.TempDir(), "staging")
	return fileFixture(t, func(o *Options) { o.StagingDir = staging })
}

// TestRestoreToStagingAndAttach is the whole point of §8.3: a running server rebuilds a namespace
// into a scratch directory and then opens that copy read-only *alongside* the live one, so the
// restored data can be checked with the same views used on production before anything is promoted.
func TestRestoreToStagingAndAttach(t *testing.T) {
	cs, base, rt := stagingFixture(t)
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d","n":%d}`, i, i)); err != nil {
			t.Fatal(err)
		}
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	backupID, _ := made["backupId"].(string)
	if backupID == "" {
		t.Fatalf("no backup to restore from: %v", made)
	}

	// A write after the backup, so the staged copy and the live namespace are genuinely different
	// and a mix-up between them would be visible.
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"after-backup"}`); err != nil {
		t.Fatal(err)
	}

	res, started := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging",
		`{"backupId":"`+backupID+`"}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("start restore: %d (%v)", res.StatusCode, started)
	}
	jobID := started["job"].(map[string]any)["id"].(string)

	job := awaitRestore(t, base, jobID)
	if job["state"] != "complete" {
		t.Fatalf("restore did not complete: %v", job)
	}
	if job["appliedCommits"].(float64) == 0 {
		t.Errorf("the restore applied nothing: %v", job)
	}
	sources, _ := job["sourcesUsed"].([]any)
	if len(sources) == 0 || !strings.Contains(fmt.Sprint(sources), backupID) {
		t.Errorf("the backup should be named as a contributing source: %v", job["sourcesUsed"])
	}

	// Attach it, and it becomes a browsable namespace under a distinguishable id.
	res, attached := postJSON(t, base, "/v1/restore/staging/"+jobID+"/attach", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("attach: %d (%v)", res.StatusCode, attached)
	}
	alias, _ := attached["attachedAs"].(string)
	if !strings.HasPrefix(alias, "staged/") {
		t.Fatalf("a staged copy must be distinguishable from the live namespace: %q", alias)
	}

	_, list := get(t, base, "/v1/namespaces")
	var found bool
	for _, raw := range list["namespaces"].([]any) {
		if raw.(map[string]any)["id"] == alias {
			found = true
		}
	}
	if !found {
		t.Errorf("the attached copy should appear in the namespace list: %v", list["namespaces"])
	}

	// The staged copy is readable with the ordinary views, and holds the backup's state - not the
	// write that landed after it.
	escaped := strings.ReplaceAll(alias, "/", "%2F")
	res, staged := get(t, base, "/v1/ns/"+escaped+"/docs?limit=100")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("reading the staged copy: %d (%v)", res.StatusCode, staged)
	}
	if strings.Contains(fmt.Sprint(staged["documents"]), "after-backup") {
		t.Error("the staged copy should hold the backup's state, not writes that came after it")
	}
	_, liveDocs := get(t, base, "/v1/ns/demo%2Fusers/docs?limit=100")
	if !strings.Contains(fmt.Sprint(liveDocs["documents"]), "after-backup") {
		t.Error("the live namespace should still have the later write")
	}

	// And the history views work on it too, which is what makes inspection worth anything.
	res, log := get(t, base, "/v1/ns/"+escaped+"/log")
	if res.StatusCode != http.StatusOK || len(log["commits"].([]any)) == 0 {
		t.Errorf("the commit log should work on a staged copy: %d %v", res.StatusCode, log)
	}

	_ = cs
}

// TestStagedCopyRefusesWrites: it is opened read-only, and the refusal has to say *why* rather than
// surfacing a lower-level error about a missing write path.
func TestStagedCopyRefusesWrites(t *testing.T) {
	_, base, rt := stagingFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	_, started := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging",
		`{"backupId":"`+made["backupId"].(string)+`"}`)
	jobID := started["job"].(map[string]any)["id"].(string)
	awaitRestore(t, base, jobID)
	_, attached := postJSON(t, base, "/v1/restore/staging/"+jobID+"/attach", `{}`)
	alias := attached["attachedAs"].(string)
	escaped := strings.ReplaceAll(alias, "/", "%2F")

	res, body := postJSON(t, base, "/v1/ns/"+escaped+"/sql", `{"sql":"DELETE FROM users"}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("a staged copy must refuse writes, got %d (%v)", res.StatusCode, body)
	}
	if body["error"].(map[string]any)["code"] != "staged_copy" {
		t.Errorf("the refusal should say it is a staged copy, not just 'read only': %v", body["error"])
	}
}

func TestDetachReleasesTheStagedCopy(t *testing.T) {
	_, base, rt := stagingFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	_, started := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging",
		`{"backupId":"`+made["backupId"].(string)+`"}`)
	jobID := started["job"].(map[string]any)["id"].(string)
	awaitRestore(t, base, jobID)
	_, attached := postJSON(t, base, "/v1/restore/staging/"+jobID+"/attach", `{}`)
	alias := attached["attachedAs"].(string)
	escaped := strings.ReplaceAll(alias, "/", "%2F")

	res, _ := postJSON(t, base, "/v1/restore/staging/"+jobID+"/detach", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("detach: %d", res.StatusCode)
	}
	// Gone from the namespace list, and a read of it is a 404 rather than a stale view.
	res, _ = get(t, base, "/v1/ns/"+escaped+"/docs")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("a detached copy must no longer resolve, got %d", res.StatusCode)
	}
}

func TestAttachRefusedBeforeTheRestoreCompletes(t *testing.T) {
	_, base, _ := stagingFixture(t)
	res, _ := postJSON(t, base, "/v1/restore/staging/nosuchjob/attach", `{}`)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown job is 404, got %d", res.StatusCode)
	}
}

func TestRestoreRefusedWithoutAStagingDirectory(t *testing.T) {
	_, base, _ := fileFixture(t)
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging", `{"includeLive":true}`)
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501 with no staging directory, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "--control-staging-dir") {
		t.Error("the refusal should name the flag that enables it")
	}
}

func TestRestoreNeedsASource(t *testing.T) {
	_, base, _ := stagingFixture(t)
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging", `{}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("a restore from nothing is not a restore, got %d (%v)", res.StatusCode, body)
	}
}

// TestHybridRestoreUsesBothSources: the reason HybridRestore takes a list is that a damaged log and
// a backup can each hold commits the other does not.
func TestHybridRestoreUsesBothSources(t *testing.T) {
	_, base, rt := stagingFixture(t)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"before"}`); err != nil {
		t.Fatal(err)
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"after"}`); err != nil {
		t.Fatal(err)
	}

	_, started := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging",
		`{"backupId":"`+made["backupId"].(string)+`","includeLive":true}`)
	job := awaitRestore(t, base, started["job"].(map[string]any)["id"].(string))
	if job["state"] != "complete" {
		t.Fatalf("hybrid restore failed: %v", job)
	}
	// The union carries the later write, which only the live log has.
	_, attached := postJSON(t, base, "/v1/restore/staging/"+job["id"].(string)+"/attach", `{}`)
	escaped := strings.ReplaceAll(attached["attachedAs"].(string), "/", "%2F")
	_, docs := get(t, base, "/v1/ns/"+escaped+"/docs?limit=100")
	if !strings.Contains(fmt.Sprint(docs["documents"]), "after") {
		t.Errorf("the union should include the commit only the live log had: %v", docs["documents"])
	}
}

func awaitRestore(t *testing.T, base, jobID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, body := get(t, base, "/v1/restore/staging/"+jobID)
		job := body["job"].(map[string]any)
		if job["state"] != "running" {
			return job
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("restore job %s did not finish", jobID)
	return nil
}

// TestDocumentTimelineListsOnlyRealChanges is the semantic the whole view rests on: a version is a
// point at which the *content* changed. A commit that rewrote the document with identical bytes did
// not change it, and listing it would send a reader hunting for a difference that is not there.
func TestDocumentTimelineListsOnlyRealChanges(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","n":1}`)[0]
	seed(t, cs, `{"id":"a","n":2}`)     // modified
	seed(t, cs, `{"id":"a","n":2}`)     // identical - not a version
	seed(t, cs, `{"id":"other","n":1}`) // unrelated document - not a version of this one
	seed(t, cs, `{"id":"a","n":3}`)     // modified

	path := "/v1/ns/demo%2Fusers/docs/" + first.DocID.String() + "/history"
	res, body := get(t, base, path)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("history: %d (%v)", res.StatusCode, body)
	}
	versions := body["versions"].([]any)
	kinds := make([]string, 0, len(versions))
	for _, raw := range versions {
		kinds = append(kinds, raw.(map[string]any)["change"].(string))
	}
	// Newest first: n=3 modified, n=2 modified, created. The identical rewrite and the unrelated
	// document contribute nothing.
	want := []string{"modified", "modified", "created"}
	if len(kinds) != len(want) {
		t.Fatalf("want %v, got %v (full: %v)", want, kinds, versions)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("want %v, got %v", want, kinds)
		}
	}

	// Every version names the commit that produced it and identifies its content.
	newest := versions[0].(map[string]any)
	if newest["commit"] == nil || newest["contentHash"] == nil {
		t.Errorf("a version must name its commit and content: %v", newest)
	}
	if newest["shortCommit"] == nil || len(newest["shortCommit"].(string)) != 8 {
		t.Errorf("the short commit is what the log shows: %v", newest["shortCommit"])
	}
}

func TestDocumentTimelineRecordsADeletion(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	put := seed(t, cs, `{"id":"a","n":1}`)[0]
	docPath := "/v1/ns/demo%2Fusers/docs/" + put.DocID.String()

	if res, body := sendJSON(t, http.MethodDelete, base, docPath, `{}`); res.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d (%v)", res.StatusCode, body)
	}

	_, body := get(t, base, docPath+"/history")
	versions := body["versions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("a create and a delete are two versions, got %d: %v", len(versions), versions)
	}
	del := versions[0].(map[string]any)
	if del["change"] != "deleted" {
		t.Fatalf("the newest version should be the deletion: %v", del)
	}
	if del["contentHash"] != nil {
		t.Error("a deletion has no content, so it should carry no content hash")
	}
	if versions[1].(map[string]any)["change"] != "created" {
		t.Errorf("the older version should be the creation: %v", versions[1])
	}
}

// TestDocumentTimelineSurvivesEvictedOperations is why this is built on trees rather than on commit
// operations. Operations are evictable; the document's history is not allowed to shrink with them.
func TestDocumentTimelineSurvivesEvictedOperations(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","n":1}`)[0]
	for i := 2; i <= 5; i++ {
		seed(t, cs, fmt.Sprintf(`{"id":"a","n":%d}`, i))
	}

	// Squeeze the operations budget as far as it will go. A history built by filtering operations
	// would come back short here; one built from trees is unaffected.
	d, err := cs.commitDAGFor(cs.opts.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	d.SetOperationsBudget(1)

	_, body := get(t, base, "/v1/ns/demo%2Fusers/docs/"+first.DocID.String()+"/history")
	if n := len(body["versions"].([]any)); n != 5 {
		t.Fatalf("five distinct versions were written and all five must be listed, got %d: %v",
			n, body["versions"])
	}
}

func TestDocumentTimelinePages(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","n":1}`)[0]
	for i := 2; i <= 6; i++ {
		seed(t, cs, fmt.Sprintf(`{"id":"a","n":%d}`, i))
	}
	path := "/v1/ns/demo%2Fusers/docs/" + first.DocID.String() + "/history"

	_, page1 := get(t, base, path+"?limit=2")
	v1 := page1["versions"].([]any)
	if len(v1) != 2 {
		t.Fatalf("limit=2 must return two versions, got %d", len(v1))
	}
	if page1["hasMore"] != true {
		t.Error("six versions and a limit of two means more remain")
	}

	_, page2 := get(t, base, path+"?limit=2&skip=2")
	v2 := page2["versions"].([]any)
	seen := map[string]bool{}
	for _, v := range v1 {
		seen[v.(map[string]any)["commit"].(string)] = true
	}
	if len(v2) == 0 {
		t.Fatal("the second page is empty")
	}
	for _, v := range v2 {
		if seen[v.(map[string]any)["commit"].(string)] {
			t.Error("a paged version was repeated")
		}
	}
}

// TestDocumentTimelineBodiesAreReadableAtEachVersion: the timeline is only useful if the versions
// it names can be opened, which is what makes the diff between two of them possible.
func TestDocumentTimelineBodiesAreReadableAtEachVersion(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","n":1}`)[0]
	seed(t, cs, `{"id":"a","n":2}`)
	seed(t, cs, `{"id":"a","n":3}`)

	docPath := "/v1/ns/demo%2Fusers/docs/" + first.DocID.String()
	_, hist := get(t, base, docPath+"/history")
	versions := hist["versions"].([]any)

	seenValues := map[float64]bool{}
	for _, raw := range versions {
		v := raw.(map[string]any)
		res, body := get(t, base, docPath+"?at="+v["commit"].(string))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("reading version %v: %d (%v)", v["shortCommit"], res.StatusCode, body)
		}
		if body["contentHash"] != v["contentHash"] {
			t.Errorf("the timeline's hash for %v disagrees with the document read there",
				v["shortCommit"])
		}
		seenValues[body["body"].(map[string]any)["n"].(float64)] = true
	}
	for _, want := range []float64{1, 2, 3} {
		if !seenValues[want] {
			t.Errorf("version with n=%v was not reachable through the timeline", want)
		}
	}
}

// TestRestoreAVersionThroughTheDocumentEndpoint: "restore this version" needs no new machinery -
// read the body at that commit and save it, which is an ordinary forward write.
func TestRestoreAVersionThroughTheDocumentEndpoint(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	first := seed(t, cs, `{"id":"a","n":1}`)[0]
	seed(t, cs, `{"id":"a","n":2}`)
	docPath := "/v1/ns/demo%2Fusers/docs/" + first.DocID.String()

	// The oldest version, read back and written forward.
	_, hist := get(t, base, docPath+"/history")
	versions := hist["versions"].([]any)
	oldest := versions[len(versions)-1].(map[string]any)
	_, old := get(t, base, docPath+"?at="+oldest["commit"].(string))
	oldBody, err := json.Marshal(old["body"])
	if err != nil {
		t.Fatal(err)
	}
	_, current := get(t, base, docPath)

	res, saved := sendJSON(t, http.MethodPut, base, docPath,
		fmt.Sprintf(`{"body":%s,"ifContentHash":%q}`, oldBody, current["contentHash"]))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("restoring a version: %d (%v)", res.StatusCode, saved)
	}

	_, after := get(t, base, docPath)
	if after["body"].(map[string]any)["n"].(float64) != 1 {
		t.Errorf("the restored version should be current: %v", after["body"])
	}
	// And it is a new version rather than a rewind: the timeline grew.
	_, hist2 := get(t, base, docPath+"/history")
	if len(hist2["versions"].([]any)) <= len(versions) {
		t.Error("restoring a version is a forward write and should add to the timeline")
	}
}

func TestDocumentTimelineForAnUnknownDocumentIsEmpty(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`)
	res, body := get(t, base,
		"/v1/ns/demo%2Fusers/docs/6f9619ff-8b86-d011-b42d-00cf4fc964ff/history")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("want 200 with no versions, got %d (%v)", res.StatusCode, body)
	}
	if n := len(body["versions"].([]any)); n != 0 {
		t.Errorf("a document that never existed has no versions, got %d", n)
	}
}

func TestLogServesGraphLanes(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a"}`, `{"id":"b"}`, `{"id":"c"}`)

	res, body := get(t, base, "/v1/ns/demo%2Fusers/log?graph=true")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("log with graph: %d (%v)", res.StatusCode, body)
	}
	rows, ok := body["graph"].([]any)
	if !ok {
		t.Fatalf("graph=true must return lanes: %v", body["graph"])
	}
	commits := body["commits"].([]any)
	if len(rows) != len(commits) {
		t.Fatalf("one lane row per commit: %d rows, %d commits", len(rows), len(commits))
	}
	// A linear history is one column, and the gutter says so.
	for i, raw := range rows {
		r := raw.(map[string]any)
		if r["lane"].(float64) != 0 {
			t.Errorf("row %d of a linear history should be lane 0, got %v", i, r["lane"])
		}
		if r["width"].(float64) != 1 {
			t.Errorf("row %d: a linear history needs a gutter of 1, got %v", i, r["width"])
		}
	}

	// Without the flag the lanes are absent rather than empty, so a client cannot mistake "not
	// asked for" for "no graph".
	_, plain := get(t, base, "/v1/ns/demo%2Fusers/log")
	if _, present := plain["graph"]; present {
		t.Error("lanes should only be computed when asked for")
	}
}

// TestGraphIsRefusedForAPartialWalk: a lane number means "this branch, relative to this walk", so
// a page starting partway in would draw columns unrelated to the page above it.
func TestGraphIsRefusedForAPartialWalk(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a"}`, `{"id":"b"}`, `{"id":"c"}`)

	res, body := get(t, base, "/v1/ns/demo%2Fusers/log?graph=true&skip=1")
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for lanes on a partial walk, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "skip=0") {
		t.Errorf("the refusal should say what to do instead: %v", body["error"])
	}
}

// Persisting a setting - docs/kdb-control-ui-plan.md §7.4's other half.
//
// The property under test throughout is not "a file was written" but "a restart would produce the
// value that is running". That is why every case here checks drift as well: drift is the report an
// operator trusts to tell them whether a change is durable, and a persist that writes a file
// without clearing drift - or clears drift without writing a file - is worse than no persist.

// withPersistableConfig builds a fixture whose settings were resolved from a real config file on
// disk, which is the only situation in which persisting means anything.
func withPersistableConfig(t *testing.T, level *slog.LevelVar, body string) (func(*Options), string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kdb.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := config.LoadServiceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := config.ResolveService(file, noEnvLookup, noFlagSet, config.DefaultServiceSettings())
	if err != nil {
		t.Fatal(err)
	}
	return func(o *Options) {
		o.AllowWrites = true
		o.AllowSettingsPersist = true
		o.ConfigPath = path
		o.LogLevel = level
		o.Settings = append(
			config.Describe(file, noEnvLookup, noFlagSet, resolved),
			config.EnvOnlyDescriptors(noEnvLookup)...)
	}, path
}

func loadConfig(t *testing.T, path string) *config.ServiceFile {
	t.Helper()
	f, err := config.LoadServiceFile(path)
	if err != nil {
		t.Fatalf("the config file no longer loads: %v", err)
	}
	return f
}

func outcomeFor(t *testing.T, body map[string]any, key string) map[string]any {
	t.Helper()
	for _, raw := range body["changes"].([]any) {
		out := raw.(map[string]any)
		if out["key"] == key {
			return out
		}
	}
	t.Fatalf("no outcome for %s in %v", key, body["changes"])
	return nil
}

func TestPersistWritesTheChangeToTheConfigFile(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	opt, path := withPersistableConfig(t, level, `{"logLevel":"info","memoryBudgetMb":512}`)
	_, base := newFixture(t, opt)

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"debug"},{"key":"memory.budgetMB","value":1024}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	if body["persistedAll"] != true {
		t.Fatalf("both keys are file-backed and should have been written: %v", body)
	}
	if body["configPath"] != path {
		t.Errorf("the response should name the file it wrote: %v", body["configPath"])
	}
	for _, key := range []string{"log.level", "memory.budgetMB"} {
		out := outcomeFor(t, body, key)
		if out["applied"] != true {
			t.Errorf("%s should have applied: %v", key, out)
		}
		if !strings.Contains(fmt.Sprint(out["persisted"]), path) {
			t.Errorf("%s should say where it was written: %v", key, out["persisted"])
		}
	}

	// The file, not just the report.
	file := loadConfig(t, path)
	if file.LogLevel == nil || *file.LogLevel != "debug" {
		t.Errorf("logLevel in the file: %v", file.LogLevel)
	}
	if file.MemoryBudgetMB == nil || *file.MemoryBudgetMB != 1024 {
		t.Errorf("memoryBudgetMb in the file: %v", file.MemoryBudgetMB)
	}

	// And the point of all of it: a restart would no longer undo the change, so there is no drift
	// left to report.
	_, drift := get(t, base, "/v1/settings/drift")
	if items := drift["drift"].([]any); len(items) != 0 {
		t.Errorf("a persisted change is not drift; still reported: %v", items)
	}

	// The settings table has to say so too. Its source stays "runtime" - that is where the value
	// came from - but on its own that reads as ephemeral, and this one is not.
	_, settings := get(t, base, "/v1/settings")
	for _, raw := range settings["settings"].([]any) {
		d := raw.(map[string]any)
		if d["key"] != "log.level" {
			continue
		}
		if !strings.Contains(fmt.Sprint(d["sourceDetail"]), "written to the config file") {
			t.Errorf("the running descriptor should say the value is durable: %v", d["sourceDetail"])
		}
	}
}

// TestPersistIsRefusedWithoutAConfigFile: a process started on flags alone has nowhere durable to
// put a value. Inventing a file would be worse than refusing - nothing would read it.
func TestPersistIsRefusedWithoutAConfigFile(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	_, base := newFixture(t, func(o *Options) {
		withSettings(level, true)(o)
		o.ConfigPath = ""
	})

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("the live change should still apply: %d (%v)", res.StatusCode, body)
	}
	if body["persistedAll"] != false {
		t.Error("nothing was written, so persistedAll must be false")
	}
	out := outcomeFor(t, body, "log.level")
	if out["applied"] != true {
		t.Errorf("the live change should stand: %v", out)
	}
	note := fmt.Sprint(out["persisted"])
	if !strings.Contains(note, "--config") || !strings.Contains(note, "drift") {
		t.Errorf("the note should name what is missing and where the change now shows: %q", note)
	}
	if level.Level() != slog.LevelDebug {
		t.Errorf("the running log level should have changed regardless: %v", level.Level())
	}
	_, drift := get(t, base, "/v1/settings/drift")
	if len(drift["drift"].([]any)) != 1 {
		t.Errorf("an unpersisted change must remain drift: %v", drift["drift"])
	}
}

// TestPersistIsRefusedWhenAFlagOutranksTheFile is the trap this feature could most easily walk
// into. Precedence is file < environment < explicitly-set flag, so writing a value into the file
// while the flag is on the command line succeeds and changes nothing on the next startup: the
// operator would read "written" and be wrong.
func TestPersistIsRefusedWhenAFlagOutranksTheFile(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelWarn)
	path := filepath.Join(t.TempDir(), "kdb.json")
	if err := os.WriteFile(path, []byte(`{"logLevel":"info"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := config.LoadServiceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	flags := config.DefaultServiceSettings()
	flags.LogLevel = "warn"
	onCommandLine := func(name string) bool { return name == "log-level" }
	resolved, err := config.ResolveService(file, noEnvLookup, onCommandLine, flags)
	if err != nil {
		t.Fatal(err)
	}
	_, base := newFixture(t, func(o *Options) {
		o.AllowWrites = true
		o.AllowSettingsPersist = true
		o.ConfigPath = path
		o.LogLevel = level
		o.Settings = config.Describe(file, noEnvLookup, onCommandLine, resolved)
	})

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("the live change should still apply: %d (%v)", res.StatusCode, body)
	}
	note := fmt.Sprint(outcomeFor(t, body, "log.level")["persisted"])
	if !strings.Contains(note, "--log-level") {
		t.Errorf("the refusal must name the flag that would win: %q", note)
	}
	if !strings.Contains(note, "command line") {
		t.Errorf("the refusal must say why writing the file would not help: %q", note)
	}
	// Nothing was written, so the file still says what it said.
	if got := loadConfig(t, path); got.LogLevel == nil || *got.LogLevel != "info" {
		t.Errorf("the file should be untouched, got %v", got.LogLevel)
	}
	if body["persistedAll"] != false {
		t.Error("persistedAll must be false")
	}
}

// The same trap via the environment, which also outranks the file.
func TestPersistIsRefusedWhenTheEnvironmentOutranksTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kdb.json")
	if err := os.WriteFile(path, []byte(`{"logLevel":"info"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := config.LoadServiceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	env := func(name string) (string, bool) {
		if name == "KDB_LOG_LEVEL" {
			return "warn", true
		}
		return "", false
	}
	resolved, err := config.ResolveService(file, env, noFlagSet, config.DefaultServiceSettings())
	if err != nil {
		t.Fatal(err)
	}
	_, base := newFixture(t, func(o *Options) {
		o.AllowWrites = true
		o.AllowSettingsPersist = true
		o.ConfigPath = path
		o.LogLevel = new(slog.LevelVar)
		o.Settings = config.Describe(file, env, noFlagSet, resolved)
	})

	_, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	note := fmt.Sprint(outcomeFor(t, body, "log.level")["persisted"])
	if !strings.Contains(note, "KDB_LOG_LEVEL") {
		t.Errorf("the refusal must name the variable that would win: %q", note)
	}
}

// TestPersistIsRefusedForAnEnvironmentOnlySetting: cache.commitOpsBytes is live-changeable but has
// no config-file field at all. Its durable home is a variable this process cannot set for its own
// successor, and saying so is more use than a generic failure.
func TestPersistIsRefusedForAnEnvironmentOnlySetting(t *testing.T) {
	opt, path := withPersistableConfig(t, new(slog.LevelVar), `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"cache.commitOpsBytes","value":8388608}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("the live change should apply: %d (%v)", res.StatusCode, body)
	}
	out := outcomeFor(t, body, "cache.commitOpsBytes")
	if out["applied"] != true {
		t.Fatalf("cache.commitOpsBytes is live-changeable: %v", out)
	}
	note := fmt.Sprint(out["persisted"])
	if !strings.Contains(note, "no config-file field") || !strings.Contains(note, "environment") {
		t.Errorf("the note should explain there is nowhere in the file for it: %q", note)
	}
	// The file must not have been rewritten for a key that cannot go in it.
	if got := loadConfig(t, path); got.LogLevel == nil || *got.LogLevel != "info" {
		t.Errorf("the file should be untouched, got %v", got.LogLevel)
	}
}

// A dry run runs the persist checks too. Discovering afterwards that a reviewed change could not
// be written down defeats the point of reviewing it.
func TestPersistDryRunSaysWhatWouldBeWrittenWithoutWritingIt(t *testing.T) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	opt, path := withPersistableConfig(t, level, `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"dryRun":true,"changes":[{"key":"log.level","value":"debug"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	note := fmt.Sprint(outcomeFor(t, body, "log.level")["persisted"])
	if !strings.Contains(note, "would be written") || !strings.Contains(note, path) {
		t.Errorf("a dry run should say what would happen and where: %q", note)
	}
	if got := loadConfig(t, path); got.LogLevel == nil || *got.LogLevel != "info" {
		t.Errorf("a dry run must not touch the file, got %v", got.LogLevel)
	}
	if level.Level() != slog.LevelInfo {
		t.Errorf("a dry run must not change the running process, got %v", level.Level())
	}
}

// TestPersistMergesIntoWhatIsOnDiskNow: something else may have edited the file since startup - a
// deployment tool, or an operator with an editor. Writing this process's startup copy back would
// silently revert their change.
func TestPersistMergesIntoWhatIsOnDiskNow(t *testing.T) {
	opt, path := withPersistableConfig(t, new(slog.LevelVar), `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)

	// An edit this process has never seen.
	if err := os.WriteFile(path, []byte(`{"logLevel":"info","namespace":"other/ns"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"error"}]}`)

	file := loadConfig(t, path)
	if file.LogLevel == nil || *file.LogLevel != "error" {
		t.Errorf("the change should be in the file: %v", file.LogLevel)
	}
	if file.Namespace == nil || *file.Namespace != "other/ns" {
		t.Errorf("the edit made outside this process was reverted: %v", file.Namespace)
	}
}

// A file deleted since startup is still a place a value can be written; an unreadable one is not,
// because merging into it means guessing what to keep.
func TestPersistRecreatesAFileDeletedSinceStartup(t *testing.T) {
	opt, path := withPersistableConfig(t, new(slog.LevelVar), `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	_, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"error"}]}`)
	if body["persistedAll"] != true {
		t.Fatalf("an absent file can hold the value: %v", body)
	}
	if file := loadConfig(t, path); file.LogLevel == nil || *file.LogLevel != "error" {
		t.Errorf("logLevel: %v", file.LogLevel)
	}
}

func TestPersistIsRefusedWhenTheConfigFileCannotBeRead(t *testing.T) {
	opt, path := withPersistableConfig(t, new(slog.LevelVar), `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)
	if err := os.WriteFile(path, []byte(`{ this is not json`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[{"key":"log.level","value":"error"}]}`)
	note := fmt.Sprint(outcomeFor(t, body, "log.level")["persisted"])
	if !strings.Contains(note, "cannot be read") {
		t.Errorf("the note should say the file is the problem: %q", note)
	}
	// And the damaged file is left exactly as it was rather than being overwritten with a guess.
	body2, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body2) != `{ this is not json` {
		t.Errorf("the unreadable file should not have been replaced, got %q", body2)
	}
}

// TestPersistPartitionsAPatchByWhatCanBeWritten: a patch mixing a file-backed setting with an
// environment-only one must write the first and explain the second, not fail as a whole. Refusing
// everything because one key has no home in the file would make the mixed case unusable, and an
// operator adjusting memory at 3am should not have to know which keys live where to get the ones
// that do written down.
func TestPersistPartitionsAPatchByWhatCanBeWritten(t *testing.T) {
	opt, path := withPersistableConfig(t, new(slog.LevelVar), `{"logLevel":"info"}`)
	_, base := newFixture(t, opt)

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"persist":true,"changes":[`+
			`{"key":"memory.reserveMB","value":64},`+
			`{"key":"cache.commitOpsBytes","value":4194304}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	// Both applied live; only one could be written down.
	for _, key := range []string{"memory.reserveMB", "cache.commitOpsBytes"} {
		if out := outcomeFor(t, body, key); out["applied"] != true {
			t.Errorf("%s should have applied live: %v", key, out)
		}
	}
	if note := fmt.Sprint(outcomeFor(t, body, "memory.reserveMB")["persisted"]); !strings.Contains(note, path) {
		t.Errorf("the file-backed key should have been written: %q", note)
	}
	if note := fmt.Sprint(outcomeFor(t, body, "cache.commitOpsBytes")["persisted"]); !strings.Contains(note, "no config-file field") {
		t.Errorf("the environment-only key should have been explained: %q", note)
	}
	if body["persistedAll"] != false {
		t.Error("persistedAll must be false when any key could not be written")
	}

	file := loadConfig(t, path)
	if file.MemoryReserveMB == nil || *file.MemoryReserveMB != 64 {
		t.Errorf("memoryReserveMb should be in the file: %v", file.MemoryReserveMB)
	}
	if file.LogLevel == nil || *file.LogLevel != "info" {
		t.Errorf("the rest of the file should survive: %v", file.LogLevel)
	}

	// Drift now holds exactly the key that could not be persisted - which is the whole point of
	// reporting it per key rather than per request.
	_, drift := get(t, base, "/v1/settings/drift")
	items := drift["drift"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["key"] != "cache.commitOpsBytes" {
		t.Errorf("drift should be just the unpersisted key: %v", items)
	}
}

// Promotion - docs/kdb-control-ui-plan.md §8.4's supervised flow.
//
// The endpoints do three things: stage a copy inside the live data root, record the intent, and
// ask the process to restart. The install itself is recovery.ApplyPromotion's job at the next
// startup and is tested there. What is tested here is the part an operator interacts with: that
// the plan tells the truth about what will happen and what stands in the way, that nothing
// irreversible happens without the confirmation, and that a request can still be taken back.

// promoteFixture is a staging fixture with promotion enabled and a restart callback that records
// the request instead of ending the test process.
func promoteFixture(t *testing.T) (*Server, string, *embed.EmbeddedKdbRuntime, *[]string) {
	t.Helper()
	restarts := &[]string{}
	staging := filepath.Join(t.TempDir(), "staging")
	cs, base, rt := fileFixture(t, func(o *Options) {
		o.StagingDir = staging
		o.AllowPromotion = true
		o.RequestRestart = func(reason string) error {
			*restarts = append(*restarts, reason)
			return nil
		}
	})
	return cs, base, rt, restarts
}

// stagedRestoreJob seeds documents, backs them up, writes once more, and restores the backup into
// staging - so the staged copy and the live namespace differ in a way a promotion would show.
func stagedRestoreJob(t *testing.T, base string, rt *embed.EmbeddedKdbRuntime) string {
	t.Helper()
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	_, made := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	backupID, _ := made["backupId"].(string)
	if backupID == "" {
		t.Fatalf("no backup to restore from: %v", made)
	}
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"after-backup"}`); err != nil {
		t.Fatal(err)
	}
	res, started := postJSON(t, base, "/v1/ns/demo%2Fusers/restore/staging",
		`{"backupId":"`+backupID+`"}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("start restore: %d (%v)", res.StatusCode, started)
	}
	jobID := started["job"].(map[string]any)["id"].(string)
	if job := awaitRestore(t, base, jobID); job["state"] != "complete" {
		t.Fatalf("restore did not complete: %v", job)
	}
	return jobID
}

func TestPromotionPlanSaysWhatItWillDo(t *testing.T) {
	_, base, rt, _ := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)

	res, plan := get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("plan: %d (%v)", res.StatusCode, plan)
	}
	if blockers, _ := plan["blockers"].([]any); len(blockers) != 0 {
		t.Fatalf("nothing should stand in the way here: %v", blockers)
	}
	if plan["confirmWith"] != "demo/users" {
		t.Errorf("the plan should name the confirmation string: %v", plan["confirmWith"])
	}
	steps := fmt.Sprint(plan["steps"])
	// The three facts an operator has to know before agreeing to this.
	if !strings.Contains(steps, "next startup") {
		t.Errorf("the plan must say the install happens on the next startup: %v", steps)
	}
	if !strings.Contains(fmt.Sprint(plan["restart"]), "cannot restart itself") {
		t.Errorf("the plan must say the process cannot restart itself: %v", plan["restart"])
	}
	if !strings.Contains(fmt.Sprint(plan["rollback"]), "not deleted") {
		t.Errorf("the plan must say the old namespace is kept: %v", plan["rollback"])
	}
	// And it is a read: asking has changed nothing.
	_, status := get(t, base, "/v1/promotion")
	if status["pending"] != nil {
		t.Errorf("reading the plan must not stage anything: %v", status["pending"])
	}
}

// The confirmation is server-side because this request ends with the process exiting. A stray POST
// must not be able to take the service down.
func TestPromotionNeedsATypedConfirmation(t *testing.T) {
	_, base, rt, restarts := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)

	res, body := postJSON(t, base, "/v1/restore/staging/"+jobID+"/promote", `{}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 without a confirmation, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "demo/users") {
		t.Errorf("the refusal should say what to send: %v", body["error"])
	}
	res, body = postJSON(t, base, "/v1/restore/staging/"+jobID+"/promote", `{"confirm":"wrong/ns"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for the wrong confirmation, got %d (%v)", res.StatusCode, body)
	}
	if len(*restarts) != 0 {
		t.Errorf("nothing should have been restarted: %v", *restarts)
	}
	_, status := get(t, base, "/v1/promotion")
	if status["pending"] != nil {
		t.Errorf("nothing should have been staged: %v", status["pending"])
	}
}

func TestPromotionStagesTheCopyAndAsksForARestart(t *testing.T) {
	cs, base, rt, restarts := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)
	root := cs.opts.Runtime.Runtime.DataRoot

	// The live namespace before: it has the write that landed after the backup.
	_, before := get(t, base, "/v1/ns/demo%2Fusers/docs?limit=100")
	if !strings.Contains(fmt.Sprint(before["documents"]), "after-backup") {
		t.Fatal("the fixture should have a write the staged copy does not")
	}

	res, body := postJSON(t, base, "/v1/restore/staging/"+jobID+"/promote",
		`{"confirm":"demo/users"}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("promote: %d (%v)", res.StatusCode, body)
	}
	if body["staged"] != true || body["restarting"] != true {
		t.Errorf("the response should say what it did: %v", body)
	}
	if body["commits"].(float64) == 0 {
		t.Errorf("the intent should record the staged commit count: %v", body["commits"])
	}
	if len(*restarts) != 1 || !strings.Contains((*restarts)[0], "demo/users") {
		t.Fatalf("a restart should have been requested once, naming the namespace: %v", *restarts)
	}

	// Staged, recorded, and the live namespace untouched: the install is the next startup's job,
	// which is what makes this cancellable right up to the restart.
	_, status := get(t, base, "/v1/promotion")
	pending, _ := status["pending"].(map[string]any)
	if pending == nil || pending["namespace"] != "demo/users" {
		t.Fatalf("the promotion should be pending: %v", status)
	}
	if _, err := os.Stat(filepath.Join(root, ".kdb.promote")); err != nil {
		t.Errorf("the copy should be staged inside the data root: %v", err)
	}
	_, after := get(t, base, "/v1/ns/demo%2Fusers/docs?limit=100")
	if !strings.Contains(fmt.Sprint(after["documents"]), "after-backup") {
		t.Error("the live namespace must not change until the restart")
	}
}

// Cancelling is the reason nothing irreversible happens before the restart.
func TestPromotionCanBeAbandonedBeforeTheRestart(t *testing.T) {
	cs, base, rt, _ := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)
	root := cs.opts.Runtime.Runtime.DataRoot

	postJSON(t, base, "/v1/restore/staging/"+jobID+"/promote", `{"confirm":"demo/users"}`)

	res, body := sendJSON(t, http.MethodDelete, base, "/v1/promotion", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("abandon: %d (%v)", res.StatusCode, body)
	}
	if body["abandoned"] != true {
		t.Errorf("it should say it cancelled something: %v", body)
	}
	_, status := get(t, base, "/v1/promotion")
	if status["pending"] != nil {
		t.Errorf("nothing should be pending: %v", status["pending"])
	}
	if _, err := os.Stat(filepath.Join(root, ".kdb.promote")); !os.IsNotExist(err) {
		t.Errorf("the staged copy should be gone: %v", err)
	}

	// Cancelling nothing says so rather than failing.
	res, body = sendJSON(t, http.MethodDelete, base, "/v1/promotion", `{}`)
	if res.StatusCode != http.StatusOK || body["abandoned"] != false {
		t.Errorf("abandoning nothing should be quiet: %d (%v)", res.StatusCode, body)
	}
}

// TestPromotionIsRefusedWhenTheDeploymentDidNotAskForIt: the plan is still readable, because an
// operator should be able to see what promoting *would* do on a server that will not do it. That
// is the difference between a documented capability and a mystery.
func TestPromotionIsRefusedWhenTheDeploymentDidNotAskForIt(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "staging")
	_, base, rt := fileFixture(t, func(o *Options) { o.StagingDir = staging })
	jobID := stagedRestoreJob(t, base, rt)

	res, plan := get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("the plan should still be readable: %d (%v)", res.StatusCode, plan)
	}
	blockers := fmt.Sprint(plan["blockers"])
	if !strings.Contains(blockers, "--control-promote") {
		t.Errorf("the blocker should name the flag: %v", blockers)
	}

	res, body := postJSON(t, base, "/v1/restore/staging/"+jobID+"/promote",
		`{"confirm":"demo/users"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("want 409, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["blockers"]), "--control-promote") {
		t.Errorf("the refusal should carry the blockers: %v", body["blockers"])
	}
}

// Without a way to shut down, staging a promotion would leave an intent nobody asked for waiting
// on disk for a restart that may never come.
func TestPromotionIsRefusedWithoutAWayToRestart(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "staging")
	_, base, rt := fileFixture(t, func(o *Options) {
		o.StagingDir = staging
		o.AllowPromotion = true
	})
	jobID := stagedRestoreJob(t, base, rt)

	_, plan := get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if !strings.Contains(fmt.Sprint(plan["blockers"]), "shut itself down") {
		t.Errorf("the blocker should say what is missing: %v", plan["blockers"])
	}
}

// An attached copy is being read from. Moving it while a runtime holds its files open is not
// something to do quietly.
func TestPromotionIsRefusedWhileTheCopyIsAttached(t *testing.T) {
	_, base, rt, _ := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)

	if res, body := postJSON(t, base, "/v1/restore/staging/"+jobID+"/attach", `{}`); res.StatusCode != http.StatusOK {
		t.Fatalf("attach: %d (%v)", res.StatusCode, body)
	}
	_, plan := get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if !strings.Contains(fmt.Sprint(plan["blockers"]), "detach it first") {
		t.Errorf("an attached copy should block promotion: %v", plan["blockers"])
	}

	// Detaching clears it.
	if res, body := postJSON(t, base, "/v1/restore/staging/"+jobID+"/detach", `{}`); res.StatusCode != http.StatusOK {
		t.Fatalf("detach: %d (%v)", res.StatusCode, body)
	}
	_, plan = get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if blockers, _ := plan["blockers"].([]any); len(blockers) != 0 {
		t.Errorf("after detaching there should be nothing in the way: %v", blockers)
	}
}

// A second promotion staged while one waits would leave two intents disagreeing about one payload.
func TestASecondPromotionIsRefusedWhileOneIsPending(t *testing.T) {
	_, base, rt, _ := promoteFixture(t)
	first := stagedRestoreJob(t, base, rt)
	postJSON(t, base, "/v1/restore/staging/"+first+"/promote", `{"confirm":"demo/users"}`)

	second := stagedRestoreJob(t, base, rt)
	res, body := postJSON(t, base, "/v1/restore/staging/"+second+"/promote",
		`{"confirm":"demo/users"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("want 409, got %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["blockers"]), "already staged") {
		t.Errorf("the refusal should say one is already waiting: %v", body["blockers"])
	}
}

// A restore that is still running, or that failed, is not something to promote.
func TestAnIncompleteRestoreCannotBePromoted(t *testing.T) {
	cs, base, _, _ := promoteFixture(t)
	cs.restore.mu.Lock()
	cs.restore.jobs["job-running"] = &restoreJob{
		ID: "job-running", Namespace: "demo/users", State: "running",
		Dir: filepath.Join(cs.opts.StagingDir, "job-running"),
	}
	cs.restore.mu.Unlock()

	_, plan := get(t, base, "/v1/restore/staging/job-running/promote/plan")
	if !strings.Contains(fmt.Sprint(plan["blockers"]), "only a completed restore") {
		t.Errorf("a running restore should block promotion: %v", plan["blockers"])
	}
}

func TestPromotionPlanIsNotFoundForAnUnknownJob(t *testing.T) {
	_, base, _, _ := promoteFixture(t)
	res, body := get(t, base, "/v1/restore/staging/no-such-job/promote/plan")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d (%v)", res.StatusCode, body)
	}
}

// TestPromotionWarnsAboutAShortfall: a restore missing commits can still be promoted - sometimes
// an incomplete history is the best available - but making that the live history is a decision,
// and a decision needs to be presented as one.
func TestPromotionWarnsAboutAShortfall(t *testing.T) {
	cs, base, rt, _ := promoteFixture(t)
	jobID := stagedRestoreJob(t, base, rt)
	cs.restore.mu.Lock()
	cs.restore.jobs[jobID].Missing = []string{"deadbeef"}
	cs.restore.mu.Unlock()

	_, plan := get(t, base, "/v1/restore/staging/"+jobID+"/promote/plan")
	if !strings.Contains(fmt.Sprint(plan["warnings"]), "shortfall") {
		t.Errorf("a missing commit should be warned about: %v", plan["warnings"])
	}
	if blockers, _ := plan["blockers"].([]any); len(blockers) != 0 {
		t.Errorf("a shortfall is a warning, not a blocker: %v", blockers)
	}
}

// TestJobSnapshotsDoNotAliasTheLiveJob: a restore writes its own job struct from its own goroutine
// while handlers read it. Handing a reader the pointer - which every one of them used to do - races
// that writer, and can serialize a torn job ("complete" beside an applied-count not yet written).
// The fix is a copy, and a copy that shared its slices would only move the problem.
func TestJobSnapshotsDoNotAliasTheLiveJob(t *testing.T) {
	cs, _, _ := fileFixture(t, func(o *Options) {
		o.StagingDir = filepath.Join(t.TempDir(), "staging")
	})
	cs.restore.mu.Lock()
	cs.restore.jobs["j1"] = &restoreJob{
		ID: "j1", Namespace: "demo/users", State: "complete",
		Sources: []string{"backup:one"}, Missing: []string{"deadbeef"},
	}
	cs.restore.mu.Unlock()

	snap, ok := cs.snapshotJob("j1")
	if !ok {
		t.Fatal("the job should be found")
	}
	snap.State = "tampered"
	snap.Sources[0] = "tampered"
	snap.Missing[0] = "tampered"

	cs.restore.mu.Lock()
	live := cs.restore.jobs["j1"]
	cs.restore.mu.Unlock()
	if live.State != "complete" {
		t.Errorf("the live job's state was changed through the snapshot: %q", live.State)
	}
	if live.Sources[0] != "backup:one" || live.Missing[0] != "deadbeef" {
		t.Errorf("the snapshot shares its slices with the live job: %v / %v",
			live.Sources, live.Missing)
	}

	if _, ok := cs.snapshotJob("no-such-job"); ok {
		t.Error("an unknown job should report absent rather than an empty one")
	}
}

// Replace, through the document endpoint.
//
// Every write path in this engine merges, which left the editor unable to remove a key and made
// "restore this version" mean "restore this version and keep whatever else is there". A delete and
// a write in one transaction is the spelling; what was missing was staging reading the second
// against the first, which transaction/default_engine.go now does.

func TestPutWithReplaceRemovesOmittedKeys(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"keep":"yes","drop":"me","also":"gone"}`)
	docID := seeded[0].DocID.String()
	path := "/v1/ns/demo%2Fusers/docs/" + docID

	res, body := sendJSON(t, http.MethodPut, base, path,
		`{"replace":true,"body":{"keep":"yes","fresh":"value"}}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("put: %d (%v)", res.StatusCode, body)
	}
	if body["replaced"] != true || body["merged"] != false {
		t.Errorf("the response should say which semantics applied: %v", body)
	}
	removed := fmt.Sprint(body["removedKeys"])
	if !strings.Contains(removed, "drop") || !strings.Contains(removed, "also") {
		t.Errorf("the response should name the keys it dropped: %v", body["removedKeys"])
	}

	_, read := get(t, base, path)
	stored, _ := read["body"].(map[string]any)
	if _, still := stored["drop"]; still {
		t.Errorf("a replace should have removed drop: %v", stored)
	}
	if _, still := stored["also"]; still {
		t.Errorf("a replace should have removed also: %v", stored)
	}
	if stored["fresh"] != "value" || stored["keep"] != "yes" {
		t.Errorf("the replacement's own keys should be there: %v", stored)
	}
}

// The default is unchanged. A control plane whose PUT quietly meant something stronger than the
// wire's would be the more dangerous surprise.
func TestPutWithoutReplaceStillMerges(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"keep":"yes","drop":"me"}`)
	path := "/v1/ns/demo%2Fusers/docs/" + seeded[0].DocID.String()

	res, body := sendJSON(t, http.MethodPut, base, path, `{"body":{"fresh":"value"}}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("put: %d (%v)", res.StatusCode, body)
	}
	if body["merged"] != true || body["replaced"] != false {
		t.Errorf("a plain put still merges: %v", body)
	}
	if !strings.Contains(fmt.Sprint(body["retainedKeys"]), "drop") {
		t.Errorf("the kept keys should still be reported: %v", body["retainedKeys"])
	}
	// And the note should point at the way to remove them, which now exists.
	if !strings.Contains(fmt.Sprint(body["note"]), "replace") {
		t.Errorf("the note should name the replace option: %v", body["note"])
	}

	_, read := get(t, base, path)
	stored, _ := read["body"].(map[string]any)
	if stored["drop"] != "me" {
		t.Errorf("a merge keeps the omitted key: %v", stored)
	}
}

// TestReplaceIsStillReadableAtEveryEarlierCommit: replace removes a key from the *head*, not from
// history. If it did otherwise it would be a rewrite, and this database does not do those.
func TestReplaceIsStillReadableAtEveryEarlierCommit(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"keep":"yes","drop":"me"}`)
	docID := seeded[0].DocID.String()
	path := "/v1/ns/demo%2Fusers/docs/" + docID

	_, before := get(t, base, "/v1/ns/demo%2Fusers/log?limit=1")
	head := before["commits"].([]any)[0].(map[string]any)["hash"].(string)

	res, body := sendJSON(t, http.MethodPut, base, path, `{"replace":true,"body":{"keep":"yes"}}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("put: %d (%v)", res.StatusCode, body)
	}

	_, atOld := get(t, base, path+"?at="+head)
	old, _ := atOld["body"].(map[string]any)
	if old["drop"] != "me" {
		t.Errorf("the dropped key must still be readable at the earlier commit: %v", old)
	}

	// And the document's own timeline shows the replace as a modification, not as a delete.
	_, history := get(t, base, path+"/history")
	versions := history["versions"].([]any)
	if len(versions) < 2 {
		t.Fatalf("want at least a create and a modify: %v", versions)
	}
	if latest := versions[0].(map[string]any); latest["change"] != "modified" {
		t.Errorf("a replace is a modification of the document, got %q", latest["change"])
	}
}

// Refs: creating, deleting, and comparing two of them (M5, §9 screen 5).
//
// The DAG has always had CreateBranch/DeleteBranch/CreateTag/DeleteTag. What is tested here is what
// is not a method call: refusing names that would be read as something else, refusing the deletions
// that would leave the namespace headless or a tag silently moved, and saying out loud that a ref
// is durable only as far as the next checkpoint.

func TestCreateAndDeleteATag(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags",
		`{"name":"v1","message":"first release"}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create tag: %d (%v)", res.StatusCode, body)
	}
	tag := body["tag"].(map[string]any)
	if tag["name"] != "v1" || tag["message"] != "first release" {
		t.Errorf("the tag should come back as created: %v", tag)
	}
	// The durability caveat has to be said: a commit is in the log, a ref is only in the
	// checkpoint, and an operator tagging a release needs to know which one they are relying on.
	note := fmt.Sprint(body["note"])
	if !strings.Contains(note, "checkpoint") || !strings.Contains(note, "kill -9") {
		t.Errorf("the response should state the durability caveat: %q", note)
	}
	if !strings.Contains(note, "retention root") {
		t.Errorf("a tag is also a retention root and should say so: %q", note)
	}

	// It is a real ref: revision resolution finds it.
	if r, b := get(t, base, "/v1/ns/demo%2Fusers/docs?at=tag:v1"); r.StatusCode != http.StatusOK {
		t.Fatalf("reading at the new tag: %d (%v)", r.StatusCode, b)
	}
	_, refs := get(t, base, "/v1/ns/demo%2Fusers/refs")
	if !strings.Contains(fmt.Sprint(refs["tags"]), "v1") {
		t.Errorf("the tag should be listed: %v", refs["tags"])
	}

	res, body = sendJSON(t, http.MethodDelete, base, "/v1/ns/demo%2Fusers/refs/tags/v1", `{}`)
	if res.StatusCode != http.StatusOK || body["deleted"] != "v1" {
		t.Fatalf("delete tag: %d (%v)", res.StatusCode, body)
	}
	// Deleting the tag must not be read as deleting anything else.
	if !strings.Contains(fmt.Sprint(body["note"]), "squashable") {
		t.Errorf("the response should say what deleting a retention root means: %v", body["note"])
	}
	if res, _ := sendJSON(t, http.MethodDelete, base, "/v1/ns/demo%2Fusers/refs/tags/v1", `{}`); res.StatusCode != http.StatusNotFound {
		t.Errorf("deleting it twice should be a 404, got %d", res.StatusCode)
	}
}

// Re-tagging is refused rather than silently moved: a tag that moved would make every reference to
// it - including a retention decision already taken on it - mean something else.
func TestRetaggingADifferentCommitIsRefused(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`)
	if res, b := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", `{"name":"v1"}`); res.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d (%v)", res.StatusCode, b)
	}
	seed(t, cs, `{"id":"b"}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", `{"name":"v1"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 for a moved tag, got %d (%v)", res.StatusCode, body)
	}
	// Re-creating it on the *same* commit is idempotent, not an error: that is a retry.
	_, refs := get(t, base, "/v1/ns/demo%2Fusers/refs")
	head := refs["tags"].([]any)[0].(map[string]any)["head"].(string)
	if res, b := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags",
		`{"name":"v1","at":"`+head+`"}`); res.StatusCode != http.StatusCreated {
		t.Errorf("re-tagging the same commit should be accepted: %d (%v)", res.StatusCode, b)
	}
}

func TestCreateAndDeleteABranch(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`, `{"id":"b"}`)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/branches", `{"name":"feature-x"}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create branch: %d (%v)", res.StatusCode, body)
	}
	if body["branch"].(map[string]any)["name"] != "feature-x" {
		t.Errorf("branch: %v", body["branch"])
	}
	// A branch from an older revision starts there, not at head.
	res, body = postJSON(t, base, "/v1/ns/demo%2Fusers/refs/branches",
		`{"name":"from-earlier","from":"head~1"}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create from head~1: %d (%v)", res.StatusCode, body)
	}
	_, refs := get(t, base, "/v1/ns/demo%2Fusers/refs")
	heads := map[string]string{}
	for _, raw := range refs["branches"].([]any) {
		b := raw.(map[string]any)
		heads[b["name"].(string)] = b["head"].(string)
	}
	if heads["from-earlier"] == heads["main"] {
		t.Errorf("a branch from head~1 should not share main's head: %v", heads)
	}

	if res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/branches", `{"name":"feature-x"}`); res.StatusCode != http.StatusConflict {
		t.Errorf("a duplicate branch name should be a 409, got %d", res.StatusCode)
	}

	res, body = sendJSON(t, http.MethodDelete, base,
		"/v1/ns/demo%2Fusers/refs/branches/feature-x", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("delete branch: %d (%v)", res.StatusCode, body)
	}
	if !strings.Contains(fmt.Sprint(body["note"]), "still in the log") {
		t.Errorf("deleting a branch removes a pointer, not commits, and should say so: %v", body["note"])
	}
}

// The default branch is the one that must always exist; deleting it would leave the namespace with
// no head. Refused as a conflict, not a 404, so it does not read as "no such branch".
func TestTheDefaultBranchCannotBeDeleted(t *testing.T) {
	_, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	res, body := sendJSON(t, http.MethodDelete, base, "/v1/ns/demo%2Fusers/refs/branches/main", `{}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("want 409, got %d (%v)", res.StatusCode, body)
	}
}

// A name that the revision syntax would read as something else is refused at the door. "head~2" as
// a branch name, or a 64-hex name, would resolve to a commit everywhere it was read back.
func TestRefNamesThatWouldBeMisreadAreRefused(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"id":"a"}`)
	_ = seeded

	_, refs := get(t, base, "/v1/ns/demo%2Fusers/refs")
	hashName := refs["head"].(string)

	for _, name := range []string{"", "  ", "head~2", "with space", "a/b", "tag:v1",
		"-leading", ".leading", "..", hashName} {
		payload, _ := json.Marshal(map[string]string{"name": name})
		res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", string(payload))
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("tag name %q should be refused, got %d (%v)", name, res.StatusCode, body)
		}
	}
	// And a name that is merely unusual is fine: this is not a general-purpose name policy.
	for _, name := range []string{"v1.2.3", "release_2026", "RC-4"} {
		payload, _ := json.Marshal(map[string]string{"name": name})
		if res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", string(payload)); res.StatusCode != http.StatusCreated {
			t.Errorf("tag name %q should be accepted, got %d (%v)", name, res.StatusCode, body)
		}
	}
}

func TestRefMutationNeedsWritePermission(t *testing.T) {
	_, base := newFixture(t) // read-only control plane
	for _, path := range []string{"/v1/ns/demo%2Fusers/refs/tags", "/v1/ns/demo%2Fusers/refs/branches"} {
		if res, body := postJSON(t, base, path, `{"name":"nope"}`); res.StatusCode != http.StatusForbidden {
			t.Errorf("%s should need --control-write, got %d (%v)", path, res.StatusCode, body)
		}
	}
}

// Compare is the question the commit log cannot answer.
func TestCompareTwoRevisions(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`)
	postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", `{"name":"v1"}`)
	seed(t, cs, `{"id":"b"}`, `{"id":"c"}`)

	res, body := get(t, base, "/v1/ns/demo%2Fusers/compare?from=tag:v1&to=head")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("compare: %d (%v)", res.StatusCode, body)
	}
	counts := body["counts"].(map[string]any)
	if counts["added"].(float64) != 2 {
		t.Errorf("two documents landed after the tag: %v", counts)
	}
	// The relationship is what makes the diff readable: the same entries mean something different
	// between an ancestor and a diverged branch.
	if body["relationship"] != "ahead" {
		t.Errorf("head should be ahead of the tag, got %v", body["relationship"])
	}

	_, reversed := get(t, base, "/v1/ns/demo%2Fusers/compare?from=head&to=tag:v1")
	if reversed["relationship"] != "behind" {
		t.Errorf("the reverse should be behind, got %v", reversed["relationship"])
	}
	if reversed["counts"].(map[string]any)["removed"].(float64) != 2 {
		t.Errorf("reversed, the same two documents are removals: %v", reversed["counts"])
	}

	_, same := get(t, base, "/v1/ns/demo%2Fusers/compare?from=head&to=head")
	if same["relationship"] != "same" {
		t.Errorf("a point compared with itself is the same: %v", same["relationship"])
	}
	if len(same["entries"].([]any)) != 0 {
		t.Errorf("nothing differs: %v", same["entries"])
	}
}

func TestCompareRequiresBothEnds(t *testing.T) {
	_, base := newFixture(t)
	for _, q := range []string{"", "?from=head", "?to=head"} {
		res, body := get(t, base, "/v1/ns/demo%2Fusers/compare"+q)
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("compare%s should be refused, got %d (%v)", q, res.StatusCode, body)
		}
	}
	if res, _ := get(t, base, "/v1/ns/demo%2Fusers/compare?from=no-such-ref&to=head"); res.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown revision should be a 404, got %d", res.StatusCode)
	}
}

// Compare is a read, so it works on a read-only control plane and on a staged copy - which is the
// point of attaching one: checking a restore against production with the same tools.
func TestCompareWorksOnAReadOnlyControlPlane(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"a"}`)
	if res, body := get(t, base, "/v1/ns/demo%2Fusers/compare?from=head&to=head"); res.StatusCode != http.StatusOK {
		t.Fatalf("compare should not need write permission: %d (%v)", res.StatusCode, body)
	}
}

// The operations dashboard (§9 screen 9).
//
// §5 named sessions, leases, peers and metrics here, plus a drain. Two of those can be built from
// data that exists, one is a real operation, and two cannot be built at all - and the tests care
// about that last part as much as the first: an empty "Sessions" table would be read as "nobody is
// connected", which is a different claim from "this server does not track that".

func TestOpsLocksNamesTheHolder(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"id":"a"}`)
	docID := seeded[0].DocID

	// A lease is the case that matters: a client hold spanning round trips, which is the thing an
	// operator finds in the way of a write that will not go through.
	lease, err := cs.opts.Runtime.DocumentLocks.TryAcquireLease(
		"demo/users", docID, "sess-42", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	res, body := get(t, base, "/v1/ops/locks")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("locks: %d (%v)", res.StatusCode, body)
	}
	if body["count"].(float64) != 1 {
		t.Fatalf("one lock is held: %v", body)
	}
	held := body["locks"].([]any)[0].(map[string]any)
	if held["docId"] != docID.String() {
		t.Errorf("the lock should name the document: %v", held)
	}
	if held["sessionId"] != "sess-42" {
		t.Errorf("and who holds it: %v", held)
	}
	if held["kind"] != "lease" {
		t.Errorf("a lease should be distinguishable from an implicit hold: %v", held)
	}
	if held["expiresIn"] == nil || held["expiresAt"] == nil {
		t.Errorf("a lease has a deadline and should report it: %v", held)
	}
	if held["fence"].(float64) != float64(lease.Fence) {
		t.Errorf("fence should match the lease: %v vs %d", held["fence"], lease.Fence)
	}

	// Released, it stops being reported - the list is what is held now, not what was.
	cs.opts.Runtime.DocumentLocks.Release("demo/users", docID, "sess-42")
	if _, after := get(t, base, "/v1/ops/locks"); after["count"].(float64) != 0 {
		t.Errorf("the released lock should be gone: %v", after)
	}
}

// An expired lease is not held, whether or not the sweeper has run. Expiry is evaluated on every
// lookup, and this list has to agree with that rather than report a lock nothing would honour.
func TestOpsLocksOmitsExpiredLeases(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"id":"a"}`)

	if _, err := cs.opts.Runtime.DocumentLocks.TryAcquireLease(
		"demo/users", seeded[0].DocID, "sess-1", 1*time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)

	_, body := get(t, base, "/v1/ops/locks")
	if body["count"].(float64) != 0 {
		t.Errorf("an expired lease is not held: %v", body)
	}
}

func TestOpsMetricsReportsTheWritePath(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"a"}`, `{"id":"b"}`)

	res, body := get(t, base, "/v1/ops/metrics")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %d (%v)", res.StatusCode, body)
	}
	stages, _ := body["stages"].([]any)
	// The in-memory fixture may not exercise every stage, so this asserts the shape rather than a
	// particular stage: whatever is reported must be complete and explained.
	for _, raw := range stages {
		s := raw.(map[string]any)
		for _, field := range []string{"stage", "count", "meanMs", "p50Ms", "p99Ms", "maxMs"} {
			if _, ok := s[field]; !ok {
				t.Errorf("stage %v is missing %s", s["stage"], field)
			}
		}
		if s["count"].(float64) <= 0 {
			t.Errorf("a reported stage should have samples; a stage with none is absent: %v", s)
		}
		if s["meaning"] == nil || s["meaning"] == "" {
			t.Errorf("stage %v has no explanation, so a number here says nothing", s["stage"])
		}
	}
}

// The two sections that cannot be built have to say so where an operator would look for them.
func TestOpsRuntimeSaysWhatItCannotAnswer(t *testing.T) {
	_, base := newFixture(t)
	_, body := get(t, base, "/v1/ops/runtime")
	items, _ := body["notAvailable"].([]any)
	if len(items) == 0 {
		t.Fatal("the unanswerable sections should be listed rather than omitted")
	}
	found := map[string]string{}
	for _, raw := range items {
		x := raw.(map[string]any)
		found[fmt.Sprint(x["what"])] = fmt.Sprint(x["why"])
	}
	if !strings.Contains(found["sessions"], "per connection") {
		t.Errorf("sessions should explain there is no global registry: %q", found["sessions"])
	}
	if !strings.Contains(found["peers"], "no peer registry") {
		t.Errorf("peers should explain there is nothing to read: %q", found["peers"])
	}
}

// Draining is one-way, so it is gated the way promotion is: --control-write plus the namespace
// typed back.
func TestDrainNeedsWritePermissionAndConfirmation(t *testing.T) {
	_, readOnly := newFixture(t)
	if res, body := postJSON(t, readOnly, "/v1/ops/drain", `{"confirm":"demo/users"}`); res.StatusCode != http.StatusForbidden {
		t.Errorf("a read-only plane must not drain: %d (%v)", res.StatusCode, body)
	}

	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	for _, payload := range []string{`{}`, `{"confirm":"wrong"}`} {
		res, body := postJSON(t, base, "/v1/ops/drain", payload)
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s should be refused: %d (%v)", payload, res.StatusCode, body)
		}
		if !strings.Contains(fmt.Sprint(body["error"]), "cannot be undone") {
			t.Errorf("the refusal should say it is one-way: %v", body["error"])
		}
	}
	if cs.opts.Runtime.IsDraining() {
		t.Fatal("a refused drain must not have drained anything")
	}
}

// TestDrainStopsWritesAndLeavesReadsAlone is the whole contract, and it is destructive - the
// fixture is not usable for writes afterwards, which is exactly the property being asserted.
func TestDrainStopsWritesAndLeavesReadsAlone(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seeded := seed(t, cs, `{"id":"a"}`)

	res, body := postJSON(t, base, "/v1/ops/drain", `{"confirm":"demo/users","waitSeconds":1}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("drain: %d (%v)", res.StatusCode, body)
	}
	if body["draining"] != true || body["writesQuiet"] != true {
		t.Errorf("an idle server should drain immediately: %v", body)
	}
	if body["alreadyWas"] != false {
		t.Errorf("it was not draining before: %v", body)
	}
	if !cs.opts.Runtime.IsDraining() {
		t.Fatal("the runtime should be draining")
	}

	// Writes are refused from now on.
	path := "/v1/ns/demo%2Fusers/docs/" + seeded[0].DocID.String()
	if res, _ := sendJSON(t, http.MethodPut, base, path, `{"body":{"id":"a","n":2}}`); res.StatusCode < 400 {
		t.Errorf("a write after draining should be refused, got %d", res.StatusCode)
	}
	// Reads are not.
	if res, b := get(t, base, "/v1/ns/demo%2Fusers/docs"); res.StatusCode != http.StatusOK {
		t.Errorf("reads must be unaffected by draining: %d (%v)", res.StatusCode, b)
	}
	// And it is idempotent: draining an already-draining server says so rather than failing.
	_, again := postJSON(t, base, "/v1/ops/drain", `{"confirm":"demo/users","waitSeconds":1}`)
	if again["alreadyWas"] != true {
		t.Errorf("a second drain should report it was already draining: %v", again)
	}
}

// Indexes - the other half of §9 screen 8.
//
// The schema view already carries "indexed" and "unique" flags. Those are the schema's intent;
// this reports what the registry actually holds, which is a different thing and can disagree.

func TestIndexesSaysSoWhenThereIsNoRegistry(t *testing.T) {
	// The in-memory fixture never calls OpenIndexes, which is the pre-Layer-16 behaviour: every
	// lookup is a full scan. That is a fact about the runtime, not a failure, and has to read as
	// one - "no indexes" and "indexes unavailable" would otherwise look the same.
	_, base := newFixture(t)
	res, body := get(t, base, "/v1/ns/demo%2Fusers/indexes")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("indexes: %d (%v)", res.StatusCode, body)
	}
	if body["available"] != false {
		t.Errorf("this runtime has no registry: %v", body)
	}
	if !strings.Contains(fmt.Sprint(body["note"]), "full scan") {
		t.Errorf("the note should say what happens instead: %v", body["note"])
	}
	if len(body["indexes"].([]any)) != 0 {
		t.Errorf("and no indexes: %v", body["indexes"])
	}
}

func TestIndexesReportsTheRegistryAndHowCurrentItIs(t *testing.T) {
	cs, base, rt := fileFixture(t)
	if _, err := cs.opts.Runtime.OpenIndexes(stores.Options{}); err != nil {
		t.Fatalf("open indexes: %v", err)
	}
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a","name":"ada"}`); err != nil {
		t.Fatal(err)
	}

	// Nothing defined yet: the answer says so and says what to do about it.
	_, empty := get(t, base, "/v1/ns/demo%2Fusers/indexes")
	if empty["available"] != true {
		t.Fatalf("a file-backed runtime with OpenIndexes has a registry: %v", empty)
	}
	if !strings.Contains(fmt.Sprint(empty["note"]), "CREATE INDEX") {
		t.Errorf("an empty registry should name the way to add one: %v", empty["note"])
	}

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/sql",
		`{"sql":"CREATE INDEX name_idx ON users (name)"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create index: %d (%v)", res.StatusCode, body)
	}

	_, listed := get(t, base, "/v1/ns/demo%2Fusers/indexes")
	indexes := listed["indexes"].([]any)
	if len(indexes) != 1 {
		t.Fatalf("one index should be listed: %v", indexes)
	}
	ix := indexes[0].(map[string]any)
	if ix["name"] != "name_idx" || ix["field"] != "name" {
		t.Errorf("the descriptor should come back: %v", ix)
	}
	if ix["type"] == "" || ix["indexId"] == "" {
		t.Errorf("type and id identify the index: %v", ix)
	}

	// How current the registry is, which is the question the descriptors cannot answer and the one
	// that matters: a query planned against a stale index is invisible until the results are wrong.
	if _, ok := listed["indexedThrough"]; !ok {
		t.Errorf("the answer should say how far the registry has been marked: %v", listed)
	}
	if listed["current"] != true {
		t.Errorf("nothing has been written since, so it should be current: %v", listed)
	}

	// A write through the server runtime keeps the registry current: indexes are updated on the
	// commit path rather than lazily.
	if res, b := postJSON(t, base, "/v1/ns/demo%2Fusers/sql",
		`{"sql":"INSERT INTO users (id, name) VALUES ('b', 'grace')"}`); res.StatusCode != http.StatusOK {
		t.Fatalf("insert: %d (%v)", res.StatusCode, b)
	}
	_, after := get(t, base, "/v1/ns/demo%2Fusers/indexes")
	if after["current"] != true {
		t.Errorf("a write through the server runtime should keep the registry current: %v", after)
	}
	if after["indexedThrough"] == listed["indexedThrough"] {
		t.Errorf("and should have moved it forward: %v", after["indexedThrough"])
	}

	// A commit that did *not* go through the server runtime - here, straight at the embedded
	// runtime, which is what an embedded caller or a repair tool does - leaves the registry behind.
	// That is precisely the situation this field exists to make visible: the index still plans
	// queries, against documents that have moved.
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"c","name":"katherine"}`); err != nil {
		t.Fatal(err)
	}
	_, behind := get(t, base, "/v1/ns/demo%2Fusers/indexes")
	if behind["current"] != false {
		t.Errorf("a commit that bypassed the index path should show as behind: %v", behind)
	}
	if !strings.Contains(fmt.Sprint(behind["note"]), "not at head") {
		t.Errorf("and should say so: %v", behind["note"])
	}
	if behind["indexedThrough"] == behind["head"] {
		t.Errorf("indexedThrough should lag head: %v", behind)
	}
}
