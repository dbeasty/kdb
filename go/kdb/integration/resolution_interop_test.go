package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestKotlinReadsGoConflictResolution: history made by Go's conflict resolution - a chain's
// field merge, a resolver authority's provisional merge (whose message names the provisional
// documents), and the authority's kdb:resolve/1 commit overruling one of them - written to a
// file-backed namespace and read back by the Kotlin CLI. Kotlin knows none of these conventions;
// it must still replay the merge and resolution commits to exactly the documents Go holds.
//
// Needs a JDK (it runs :kdb-cli:runCli through gradlew); skipped under -short.
func TestKotlinReadsGoConflictResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	root := t.TempDir()
	ns := "app/data"

	fileRT, err := embed.OpenFileRuntime(root, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	a := server.NewKdbServerRuntime(fileRT)
	a.NodeID = randomID(t)
	memRT, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	b := server.NewKdbServerRuntime(memRT)
	b.NodeID = randomID(t)
	chain := &peersync.ResolutionChain{Rules: []peersync.ResolutionRule{
		{Kind: peersync.RuleFieldMerge},
		{Kind: peersync.RuleAuthority, Pending: peersync.PendingProvisional},
	}}
	a.SetResolutionChain(chain)
	b.SetResolutionChain(chain)

	kept, merged, overruled := randomID(t), randomID(t), randomID(t)
	upsert := func(rt *server.KdbServerRuntime, id codec.UUID, body string) {
		t.Helper()
		if _, err := rt.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	upsert(a, kept, `{"v":"base"}`)
	upsert(a, merged, `{"x":1,"y":1}`)
	upsert(a, overruled, `{"v":"base"}`)
	syncInto(t, a, b, ns)
	upsert(a, kept, `{"v":"a"}`)
	upsert(a, merged, `{"x":2,"y":1}`)
	upsert(a, overruled, `{"v":"a"}`)
	upsert(b, kept, `{"v":"b"}`)
	upsert(b, merged, `{"x":1,"y":2}`)
	upsert(b, overruled, `{"v":"b"}`)
	syncInto(t, a, b, ns)

	var entry peersync.ConflictEntry
	for _, e := range a.Conflicts.List() {
		if e.Kind == peersync.ConflictProvisional {
			entry = e
		}
	}
	if entry.ID == "" || len(entry.Items) != 2 {
		t.Fatalf("expected one provisional entry for two documents: %+v", a.Conflicts.List())
	}
	if _, err := a.ResolveConflict(entry.ID, map[codec.UUID]peersync.Choice{
		kept: {Take: "local"}, overruled: {Take: "remote"},
	}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	want := map[codec.UUID]string{kept: `{"v":"b"}`, merged: `{"x":2,"y":2}`, overruled: `{"v":"a"}`}
	for id, body := range want {
		got, _, _, err := a.GetDocument(ns, id)
		if err != nil || !sameJSON(got, body) {
			t.Fatalf("Go holds %s for %s, want %s (%v)", got, id, body, err)
		}
	}
	fileRT.Close()

	for id, body := range want {
		out := kotlinCLI(t, repo, "--data-dir", root, "get", ns, id.String())
		if !sameJSON(out, body) {
			t.Fatalf("Kotlin read %q for %s, Go wrote %s", out, id, body)
		}
	}
	log := kotlinCLI(t, repo, "--data-dir", root, "log", ns)
	for _, marker := range []string{"kdb:merge/1 provisional=", peersync.ResolveMessage(entry.ID)} {
		if !strings.Contains(log, marker) {
			t.Fatalf("Kotlin's log does not show %q:\n%s", marker, log)
		}
	}
}

func syncInto(t *testing.T, from, to *server.KdbServerRuntime, ns string) {
	t.Helper()
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", to, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns}, Local: from.PeerNamespaces(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Namespaces {
		if r.Err != nil || r.ResolutionMismatch {
			t.Fatalf("sync %s: err=%v mismatch=%v", r.Namespace, r.Err, r.ResolutionMismatch)
		}
	}
}

func kotlinCLI(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command(filepath.Join(repo, "gradlew"), ":kdb-cli:runCli", "--args="+strings.Join(args, " "), "--quiet")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "TERM=dumb")
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("kotlin %v: %v\n%s\n%s", args, err, out, stderr)
	}
	return strings.TrimSpace(string(out))
}

func sameJSON(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return string(xa) == string(ya)
}

func randomID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
