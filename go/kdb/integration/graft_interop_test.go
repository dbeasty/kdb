package integration

import (
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

// TestKotlinReadsAGoNamespaceThatGraftedAnUnrelatedHistory: C, a Go file-backed namespace, grafts
// the snapshot root of a peer whose history nobody else holds and merges it (Phase 11). Its log
// now holds a commit without parents that is not genesis - the grafted root - and the peer's
// commits above it as side history; main reaches them only through the merge. The Kotlin CLI,
// which always replays the whole log, reads the directory: every document must read exactly as Go
// holds it, or the read must be a loud refusal - never a wrong or missing value.
//
// Today it is the refusal: Kotlin's replayer has no shallow roots (it refuses a Go namespace
// bootstrapped from a snapshot the same way) and applies every logged commit to main, where Go
// applies only commits that extend it. Reading grafted namespaces from Kotlin needs both ported.
func TestKotlinReadsAGoNamespaceThatGraftedAnUnrelatedHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	root := t.TempDir()
	ns := "app/data"
	chain := &peersync.ResolutionChain{AllowUnrelated: true, Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}}
	mem := func() *server.KdbServerRuntime {
		rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		s := server.NewKdbServerRuntime(rt)
		s.NodeID = randomID(t)
		return s
	}
	fileRT, err := embed.OpenFileRuntime(root, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	c := server.NewKdbServerRuntime(fileRT)
	c.NodeID = randomID(t)
	a, b := mem(), mem()
	want := map[codec.UUID]string{}
	upsert := func(rt *server.KdbServerRuntime, body string) {
		t.Helper()
		id := randomID(t)
		if _, err := rt.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		want[id] = body
	}
	for _, v := range []string{"b1", "b2", "b3"} {
		upsert(b, `{"from":"`+v+`"}`)
	}
	// A joins B by snapshot; B is then gone.
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: a.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
		Local: a.PeerNamespaces(), PreferSnapshot: true,
	})
	ln.Close()
	if err != nil || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("snapshot bootstrap: %+v %v", res, err)
	}
	upsert(a, `{"from":"a"}`)
	upsert(c, `{"from":"c1"}`)
	upsert(c, `{"from":"c2"}`)
	a.SetResolutionChain(chain)
	c.SetResolutionChain(chain)

	syncInto(t, c, a, ns)
	for id, body := range want {
		got, _, found, err := c.GetDocument(ns, id)
		if err != nil || !found || !sameJSON(got, body) {
			t.Fatalf("Go holds %q for %s, want %s (%v)", got, id, body, err)
		}
	}
	fileRT.Close()

	for id, body := range want {
		out := kotlinCLIResult(t, repo, "--data-dir", root, "get", ns, id.String())
		switch {
		case out.code == 0 && sameJSON(lastLine(out.stdout), body):
		case out.code != 0 && strings.Contains(kotlinError(out.stderr), "reference parent commits never found in the log"):
			t.Logf("Kotlin refuses the grafted log loudly for %s: %s", id, kotlinError(out.stderr))
		default:
			t.Errorf("Kotlin read %q for %s (exit %d, stderr %q), Go holds %s - neither the value nor a refusal", out.stdout, id, out.code, kotlinError(out.stderr), body)
		}
	}
}

// kotlinError is the CLI's own "Error: ..." line from gradle's stderr, which otherwise ends in
// gradle's BUILD FAILED noise.
func kotlinError(stderr string) string {
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "Error:") {
			return strings.TrimSpace(line)
		}
	}
	return ""
}
