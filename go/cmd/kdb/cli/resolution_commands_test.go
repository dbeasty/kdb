package cli

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// captureRun runs the CLI and returns its exit code and standard output.
func captureRun(t *testing.T, args ...string) (int, string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	code := Run(args)
	os.Stdout = stdout
	_ = w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return code, buf.String()
}

// seedDataRoot writes ns's resolution chain into dir's metadata namespace, and body as doc -
// the state a data root has after a service with that chain ran against it.
func seedDataRoot(t *testing.T, dir, ns string, chain peersync.ResolutionChain, doc codec.UUID, body string) {
	t.Helper()
	host, err := embed.OpenFileHost(dir, embed.FileRuntimeOptionsFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	data, err := host.Namespace("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	meta, err := host.Namespace("_kdb", server.MetaNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	set := server.NewNamespaceSet(host.Transactions())
	srv := server.NewKdbServerRuntime(data)
	if err := set.Add(srv); err != nil {
		t.Fatal(err)
	}
	metaSrv := server.NewKdbServerRuntime(meta)
	set.AddSystem(metaSrv)
	store := server.NewMetaStore(metaSrv, set)
	defer store.Close()
	if err := store.SetResolution(ns, chain); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert(ns, doc, body, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

// TestCLIResolvesWithTheReplicatedChain: the CLI reads the namespace's chain from the data root's
// metadata namespace, so `kdb sync` merges as the service would (a CLI without it would present no
// chain, and the peer - which has one - would refuse to merge); `kdb conflicts` shows the entry
// handed to the authority; `kdb resolve --all` previews and then settles it.
func TestCLIResolvesWithTheReplicatedChain(t *testing.T) {
	ns := "app/data"
	chain := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleAuthority, Pending: peersync.PendingProvisional}}}
	doc, _ := codec.RandomUUID()

	rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peer := server.NewKdbServerRuntime(rt)
	peer.NodeID, _ = codec.RandomUUID()
	peer.SetResolutionChain(&chain)
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := "tcp://" + ln.Addr().String()

	dir := t.TempDir()
	seedDataRoot(t, dir, ns, chain, doc, `{"v":"base"}`)
	if code, out := captureRun(t, "--data-dir", dir, "resolution", ns); code != 0 || !strings.Contains(out, "authority pending=provisional") {
		t.Fatalf("kdb resolution: %d %q", code, out)
	}
	if code, _ := captureRun(t, "--data-dir", dir, "sync", ns, addr); code != 0 {
		t.Fatalf("initial sync exited %d", code)
	}

	// Both sides change the document; the peer writes last.
	local, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.NewKdbServerRuntime(local).Upsert(ns, doc, `{"v":"local"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	local.Close()
	if _, err := peer.Upsert(ns, doc, `{"v":"peer"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	if code, out := captureRun(t, "--data-dir", dir, "sync", ns, addr); code != 0 {
		t.Fatalf("a sync under matching chains should merge provisionally, exited %d: %s", code, out)
	}
	code, out := captureRun(t, "--data-dir", dir, "conflicts", ns)
	if code != 0 || !strings.Contains(out, "provisional") || !strings.Contains(out, "authority delivered=false") || !strings.Contains(out, "incoming-by=") {
		t.Fatalf("kdb conflicts should show the provisional entry for the authority: %q", out)
	}
	if code, out := captureRun(t, "--data-dir", dir, "resolve", ns, "--all", "--take", "remote", "--dry-run"); code != 0 || !strings.Contains(out, "would take remote") {
		t.Fatalf("dry run: %d %q", code, out)
	}
	if code, out := captureRun(t, "--data-dir", dir, "resolve", ns, "--all", "--take", "remote"); code != 0 || !strings.Contains(out, "resolved:") {
		t.Fatalf("resolve --all: %d %q", code, out)
	}
	if code, out := captureRun(t, "--data-dir", dir, "conflicts", ns); code != 0 || strings.TrimSpace(out) != "" {
		t.Fatalf("the queue should be empty after resolving: %q", out)
	}
	check, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	body, _, _, err := server.NewKdbServerRuntime(check).GetDocument(ns, doc)
	if err != nil || body != `{"v":"local"}` {
		t.Fatalf("taking the displaced side should restore the local write, got %s %v", body, err)
	}
}

func TestParseResolveAll(t *testing.T) {
	cmd, ok, err := parseReplicationCommand([]string{"resolve", "app/data", "--all", "--take", "remote", "--origin", "n1", "--peer", "p", "--kind", "divergence", "--dry-run"})
	c, isAll := cmd.(ResolveAllCmd)
	if err != nil || !ok || !isAll || c.Take != "remote" || !c.DryRun || c.Filter.Origin != "n1" || c.Filter.Peer != "p" || c.Filter.Kind != peersync.ConflictDivergence {
		t.Fatalf("parse: %+v %v %v", cmd, ok, err)
	}
	if _, _, err := parseReplicationCommand([]string{"resolve", "app/data", "--all"}); err == nil {
		t.Fatal("--all needs --take")
	}
	if _, _, err := parseReplicationCommand([]string{"resolve", "app/data", "--all", "--take", "theirs"}); err == nil {
		t.Fatal("take must be local or remote")
	}
}
