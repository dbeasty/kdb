package control

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// Restore points across namespaces.
//
// The property under test throughout is the one the feature exists for: that a
// point names one instant in *every* namespace, and that restoring it puts
// them all back together. A point that covers some of them is the failure this
// is supposed to make impossible, so most of these tests are about refusing
// rather than about succeeding.

// multiFixture serves several in-memory namespaces from one control plane,
// which is what a restore point needs and what the single-namespace fixtures
// cannot express.
func multiFixture(t *testing.T, names ...string) (map[string]*embed.EmbeddedKdbRuntime, string) {
	t.Helper()
	rts := map[string]*embed.EmbeddedKdbRuntime{}
	srts := map[string]*server.KdbServerRuntime{}
	for _, n := range names {
		rt, err := embed.OpenMemoryRuntime("demo", n, schema.None())
		if err != nil {
			t.Fatalf("opening %s: %v", n, err)
		}
		t.Cleanup(rt.Close)
		rts[n] = rt
		srts[n] = server.NewKdbServerRuntime(rt)
	}
	cs, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: srts[names[0]], Namespace: names[0], Version: "test",
		AllowWrites: true, Namespaces: StaticNamespaces(srts),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return rts, "http://" + cs.Addr().String()
}

func write(t *testing.T, rt *embed.EmbeddedKdbRuntime, ns, id string, v int) {
	t.Helper()
	if _, err := embed.PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":%q,"v":%d}`, id, v)); err != nil {
		t.Fatalf("write %s/%s: %v", ns, id, err)
	}
}

func docsIn(t *testing.T, base, ns string) map[string]any {
	t.Helper()
	res, body := get(t, base, "/v1/ns/"+strings.ReplaceAll(ns, "/", "%2F")+"/docs")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("listing %s: %d %v", ns, res.StatusCode, body)
	}
	return body
}

func onlyPoint(t *testing.T, base, name string) map[string]any {
	t.Helper()
	res, body := get(t, base, "/v1/restore-points")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("listing points: %d %v", res.StatusCode, body)
	}
	for _, raw := range body["points"].([]any) {
		p := raw.(map[string]any)
		if p["name"] == name {
			return p
		}
	}
	t.Fatalf("no restore point named %q in %v", name, body["points"])
	return nil
}

// A point covers every namespace, and says so.
func TestARestorePointSpansEveryNamespace(t *testing.T) {
	rts, base := multiFixture(t, "demo/games", "demo/scores", "demo/people")
	for ns, rt := range rts {
		write(t, rt, ns, "a", 1)
	}

	res, body := postJSON(t, base, "/v1/restore-points", `{"name":"before-import"}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%v)", res.StatusCode, body)
	}
	p := body["point"].(map[string]any)
	if p["complete"] != true || p["restorable"] != true {
		t.Errorf("a freshly captured point is not complete/restorable: %v", p)
	}
	if n := len(p["in"].([]any)); n != 3 {
		t.Errorf("the point covers %d namespaces, want 3", n)
	}
}

// A name that exists in only some namespaces is reported as incomplete, and
// refused for restoring — putting half the database back is the failure this
// feature exists to prevent.
func TestAPointMissingFromANamespaceCannotBeRestored(t *testing.T) {
	rts, base := multiFixture(t, "demo/games", "demo/scores")
	for ns, rt := range rts {
		write(t, rt, ns, "a", 1)
	}

	// Tag one namespace only, by the ordinary per-namespace route.
	if res, body := postJSON(t, base, "/v1/ns/demo%2Fgames/refs/tags", `{"name":"half"}`); res.StatusCode != http.StatusCreated {
		t.Fatalf("tagging one namespace: %d %v", res.StatusCode, body)
	}

	p := onlyPoint(t, base, "half")
	if p["complete"] != false {
		t.Errorf("a point in one of two namespaces reported complete: %v", p)
	}
	if missing := p["missing"].([]any); len(missing) != 1 || missing[0] != "demo/scores" {
		t.Errorf("missing = %v, want [demo/scores]", p["missing"])
	}

	res, body := postJSON(t, base, "/v1/restore-points/half/restore", ``)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — half the database was restored (%v)", res.StatusCode, body)
	}
	if e := body["error"].(map[string]any); e["code"] != "incomplete_point" {
		t.Errorf("code = %v, want incomplete_point", e["code"])
	}
}

// The round trip, which is the whole feature: capture, diverge, restore
// everything together, and be able to walk it back again.
func TestRestoringAPointPutsEveryNamespaceBack(t *testing.T) {
	rts, base := multiFixture(t, "demo/games", "demo/scores")
	for ns, rt := range rts {
		write(t, rt, ns, "a", 1)
	}

	if res, body := postJSON(t, base, "/v1/restore-points", `{"name":"good"}`); res.StatusCode != http.StatusCreated {
		t.Fatalf("capture: %d %v", res.StatusCode, body)
	}

	// Both namespaces move on: one document changes, another appears.
	for ns, rt := range rts {
		write(t, rt, ns, "a", 2)
		write(t, rt, ns, "b", 1)
	}

	res, body := postJSON(t, base, "/v1/restore-points/good/restore", ``)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("restore: %d %v", res.StatusCode, body)
	}
	if n := len(body["restored"].([]any)); n != 2 {
		t.Errorf("restored %d namespaces, want 2", n)
	}
	undoName, _ := body["undoPoint"].(string)
	if undoName == "" {
		t.Fatal("a restore took no undo point, so it cannot be walked back")
	}

	// Every namespace is back: "a" at v1, and "b" gone.
	for ns := range rts {
		got := docsIn(t, base, ns)
		raw, _ := got["documents"].([]any)
		if len(raw) != 1 {
			t.Errorf("%s holds %d documents after the restore, want 1: %v", ns, len(raw), got)
		}
	}

	// And the undo point walks it forward again, because a revert is a commit
	// like any other.
	if res, body := postJSON(t, base, "/v1/restore-points/"+undoName+"/restore", ``); res.StatusCode != http.StatusOK {
		t.Fatalf("undo: %d %v", res.StatusCode, body)
	}
	for ns := range rts {
		got := docsIn(t, base, ns)
		raw, _ := got["documents"].([]any)
		if len(raw) != 2 {
			t.Errorf("%s holds %d documents after undoing the restore, want 2", ns, len(raw))
		}
	}
}

// Points are listed newest first, whatever order the namespaces yield them in.
func TestRestorePointsAreListedNewestFirst(t *testing.T) {
	rts, base := multiFixture(t, "demo/games", "demo/scores")
	for _, name := range []string{"first", "second", "third"} {
		for ns, rt := range rts {
			write(t, rt, ns, "a", len(name))
		}
		if res, body := postJSON(t, base, "/v1/restore-points", `{"name":"`+name+`"}`); res.StatusCode != http.StatusCreated {
			t.Fatalf("capture %s: %d %v", name, res.StatusCode, body)
		}
	}

	_, body := get(t, base, "/v1/restore-points")
	points := body["points"].([]any)
	if len(points) != 3 {
		t.Fatalf("got %d points, want 3", len(points))
	}
	if got := points[0].(map[string]any)["name"]; got != "third" {
		t.Errorf("newest point is %v, want third", got)
	}
	if got := points[2].(map[string]any)["name"]; got != "first" {
		t.Errorf("oldest point is %v, want first", got)
	}
}

// Listing is a read, so a read-only control plane still shows an operator
// where the points are — it just will not make or apply one.
func TestRestorePointsAreReadableWithoutWritePermission(t *testing.T) {
	_, base := newFixture(t) // AllowWrites is off by default
	if res, _ := get(t, base, "/v1/restore-points"); res.StatusCode != http.StatusOK {
		t.Errorf("listing restore points read-only: %d, want 200", res.StatusCode)
	}
	if res, body := postJSON(t, base, "/v1/restore-points", `{"name":"nope"}`); res.StatusCode != http.StatusForbidden {
		t.Errorf("creating one read-only: %d, want 403 (%v)", res.StatusCode, body)
	}
}

func TestRestoringAnUnknownPointIsNotFound(t *testing.T) {
	_, base := multiFixture(t, "demo/games")
	if res, _ := postJSON(t, base, "/v1/restore-points/nothing-here/restore", ``); res.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", res.StatusCode)
	}
}

// The order must follow the instants, not their text. RFC3339Nano drops trailing zeros, so 340ms
// renders as "…49.34Z" and sorts after "…49.341Z" as a string - the flake CI kept hitting when
// three captures landed a few milliseconds apart.
func TestRestorePointOrderFollowsTheInstantNotItsText(t *testing.T) {
	base := int64(1_790_000_049_000)
	mk := func(name string, ms int64) restorePoint {
		return restorePoint{Name: name, CreatedAt: millisToRFC3339(base + ms), createdMillis: base + ms}
	}
	points := []restorePoint{mk("older", 340), mk("newer", 341), mk("oldest", 300)}
	if points[0].CreatedAt < points[1].CreatedAt {
		t.Fatalf("fixture no longer exercises the bug: %q sorts before %q as text", points[0].CreatedAt, points[1].CreatedAt)
	}
	sortRestorePoints(points)
	for i, want := range []string{"newer", "older", "oldest"} {
		if points[i].Name != want {
			t.Errorf("position %d: %s, want %s", i, points[i].Name, want)
		}
	}
}
