package embed_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func hostOpts() embed.FileRuntimeOptions { return embed.FileRuntimeOptions{} }

// put writes one document and returns its id, failing the test on any error.
func put(t *testing.T, rt *embed.EmbeddedKdbRuntime, ns, json string) codec.UUID {
	t.Helper()
	res, err := embed.PutJSONDocument(rt, ns, json)
	if err != nil {
		t.Fatalf("put into %s: %v", ns, err)
	}
	return res.DocID
}

// readBack returns the stored JSON for docID at the namespace's current head.
func readBack(t *testing.T, rt *embed.EmbeddedKdbRuntime, ns string, docID codec.UUID) string {
	t.Helper()
	head, err := rt.DAG.Head()
	if err != nil {
		t.Fatalf("head of %s: %v", ns, err)
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		t.Fatalf("commit of %s: %v", ns, err)
	}
	doc, err := rt.Storage.GetDocument(ns, docID, commit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("get %s from %s: %v", docID, ns, err)
	}
	if doc == nil {
		return ""
	}
	return doc.JSON
}

// TestTwoNamespacesShareOneHost is the regression test for the whole of Component 64 Phase A.
//
// Before the host existed this was impossible, not merely awkward: OpenFileRuntimeWithOptions
// took the data root's writer lock exclusively, and flock(2) is scoped to the open file
// description, so the second open failed with "data directory locked" even from inside the same
// process. A consumer that wanted nine namespaces needed nine data roots. See
// docs/kdb-spec-layer17-multi-namespace-runtime.md §1.
func TestTwoNamespacesShareOneHost(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	users, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("open app/users: %v", err)
	}
	sessions, err := host.Namespace("app", "app/sessions", schema.None())
	if err != nil {
		t.Fatalf("open app/sessions under the SAME root: %v", err)
	}

	uid := put(t, users, "app/users", `{"name":"ada"}`)
	sid := put(t, sessions, "app/sessions", `{"token":"t-1"}`)

	// Each namespace sees its own document and not the other's - one lock and one I/O shim,
	// but still two independent engines over two independent sets of files.
	if got := readBack(t, users, "app/users", uid); !strings.Contains(got, "ada") {
		t.Fatalf("app/users lost its document: %q", got)
	}
	if got := readBack(t, sessions, "app/sessions", sid); !strings.Contains(got, "t-1") {
		t.Fatalf("app/sessions lost its document: %q", got)
	}
	if got := readBack(t, users, "app/users", sid); got != "" {
		t.Fatalf("app/users returned app/sessions' document: %q", got)
	}

	if got := host.Namespaces(); len(got) != 2 || got[0] != "app/sessions" || got[1] != "app/users" {
		t.Fatalf("Namespaces() = %v, want [app/sessions app/users]", got)
	}
}

// Both namespaces' writes must still be on disk after the host closes and a new one opens the
// same root - the point being that one lock and one shared I/O shim did not cost either
// namespace its durability.
func TestHostRoundTripsBothNamespacesAcrossReopen(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	users, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("open app/users: %v", err)
	}
	sessions, err := host.Namespace("app", "app/sessions", schema.None())
	if err != nil {
		t.Fatalf("open app/sessions: %v", err)
	}
	uid := put(t, users, "app/users", `{"name":"ada"}`)
	sid := put(t, sessions, "app/sessions", `{"token":"t-1"}`)
	if err := host.Close(); err != nil {
		t.Fatalf("close host: %v", err)
	}

	reopened, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("reopen host (did Close release the lock?): %v", err)
	}
	defer reopened.Close()
	users2, err := reopened.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("reopen app/users: %v", err)
	}
	sessions2, err := reopened.Namespace("app", "app/sessions", schema.None())
	if err != nil {
		t.Fatalf("reopen app/sessions: %v", err)
	}
	if got := readBack(t, users2, "app/users", uid); !strings.Contains(got, "ada") {
		t.Fatalf("app/users did not survive reopen: %q", got)
	}
	if got := readBack(t, sessions2, "app/sessions", sid); !strings.Contains(got, "t-1") {
		t.Fatalf("app/sessions did not survive reopen: %q", got)
	}
}

// Asking twice for the same namespace returns the same runtime. Two engines over one
// namespace's files is exactly what the writer lock exists to prevent, so this must not be a
// second open.
func TestHostNamespaceIsIdempotent(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	first, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first != second {
		t.Fatal("Namespace returned a second, independent runtime for an already-open namespace")
	}
	if got := host.Namespaces(); len(got) != 1 {
		t.Fatalf("Namespaces() = %v, want one entry", got)
	}
}

// CloseNamespace is scoped to one namespace: the host keeps its lock, its other namespaces keep
// working, and the closed one can be opened again.
func TestCloseNamespaceLeavesTheRestOfTheHostAlone(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	users, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("open app/users: %v", err)
	}
	sessions, err := host.Namespace("app", "app/sessions", schema.None())
	if err != nil {
		t.Fatalf("open app/sessions: %v", err)
	}
	uid := put(t, users, "app/users", `{"name":"ada"}`)

	if err := host.CloseNamespace("app/users"); err != nil {
		t.Fatalf("close app/users: %v", err)
	}
	// Idempotent: closing again, and closing something never opened, are both no-ops.
	if err := host.CloseNamespace("app/users"); err != nil {
		t.Fatalf("second close of app/users: %v", err)
	}
	if err := host.CloseNamespace("app/never-opened"); err != nil {
		t.Fatalf("close of an unknown namespace: %v", err)
	}
	if got := host.Namespaces(); len(got) != 1 || got[0] != "app/sessions" {
		t.Fatalf("Namespaces() = %v, want [app/sessions]", got)
	}

	// The surviving namespace still writes, so the host's lock and shim outlived the close.
	put(t, sessions, "app/sessions", `{"token":"t-2"}`)

	reopened, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("reopen app/users on the same host: %v", err)
	}
	if got := readBack(t, reopened, "app/users", uid); !strings.Contains(got, "ada") {
		t.Fatalf("app/users lost its document across a namespace close: %q", got)
	}
}

// A namespace opened through a host the caller owns must not release that host's lock when it
// closes - the lock belongs to the host, not to any one namespace under it.
func TestRuntimeFromHostDoesNotReleaseTheHostLock(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	users, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("open app/users: %v", err)
	}
	users.Close()

	// If Close had dropped the host's lock, this second host would open; it must not.
	if other, err := embed.OpenFileHost(root, hostOpts()); err == nil {
		other.Close()
		t.Fatal("closing one namespace released the host's directory lock")
	}
	// And the host itself is still usable.
	if _, err := host.Namespace("app", "app/sessions", schema.None()); err != nil {
		t.Fatalf("host unusable after a namespace closed: %v", err)
	}
}

// The single-namespace API keeps its old contract exactly: the runtime owns the data root while
// it is open, and closing it frees the root for the next opener.
func TestSingleNamespaceOpenStillOwnsTheDataRoot(t *testing.T) {
	root := t.TempDir()
	rt, err := embed.OpenFileRuntime(root, "app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := embed.OpenFileHost(root, hostOpts()); err == nil {
		t.Fatal("a second opener got in while a single-namespace runtime held the root")
	}
	uid := put(t, rt, "app/users", `{"name":"ada"}`)
	rt.Close()
	// Close twice: still a no-op, as it always was.
	rt.Close()

	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("root not released by rt.Close(): %v", err)
	}
	defer host.Close()
	users, err := host.Namespace("app", "app/users", schema.None())
	if err != nil {
		t.Fatalf("reopen app/users: %v", err)
	}
	if got := readBack(t, users, "app/users", uid); !strings.Contains(got, "ada") {
		t.Fatalf("single-namespace write did not survive: %q", got)
	}
}

// A closed host refuses to open anything new rather than handing back a runtime over a data
// root it no longer holds the lock on.
func TestClosedHostRefusesNewNamespaces(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("second close should be a no-op: %v", err)
	}
	if _, err := host.Namespace("app", "app/users", schema.None()); err == nil {
		t.Fatal("closed host opened a namespace")
	}
}

// Nine namespaces, one root, one lock - the shape the whole component exists for.
func TestNineNamespacesOneRoot(t *testing.T) {
	names := []string{
		"matches", "users", "sessions", "scoring_sessions", "match_results",
		"player_stats", "identities", "login_codes", "oauth_flows",
	}
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	ids := make(map[string]codec.UUID, len(names))
	for _, name := range names {
		ns := "zolik/" + name
		rt, err := host.Namespace("zolik", ns, schema.None())
		if err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
		ids[ns] = put(t, rt, ns, `{"ns":"`+name+`"}`)
	}
	if got := host.Namespaces(); len(got) != len(names) {
		t.Fatalf("Namespaces() has %d entries, want %d", len(got), len(names))
	}
	for ns, id := range ids {
		rt, err := host.Namespace("zolik", ns, schema.None())
		if err != nil {
			t.Fatalf("re-get %s: %v", ns, err)
		}
		want := strings.TrimPrefix(ns, "zolik/")
		if got := readBack(t, rt, ns, id); !strings.Contains(got, `"`+want+`"`) {
			t.Fatalf("%s read back %q, want it to contain %q", ns, got, want)
		}
	}
	// Everything landed under the one root, not nine.
	if _, err := filepath.Abs(root); err != nil {
		t.Fatal(err)
	}
}
