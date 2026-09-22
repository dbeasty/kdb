package integration

import (
	"bytes"
	"os"
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

// TestKotlinReadsAGoLogRepairedByScrub: a Go data directory whose delta log has a damaged frame,
// repaired by a scrub from a peer (a kdb:repair/1 commit rewriting the damaged body), read by the
// Kotlin CLI. The damaged frame is still in the log - repair appends, it never rewrites - and
// Kotlin, which has no checkpoints and always replays, cannot replay past it. What it must never
// do is serve something wrong or silently less: each read is either exactly what Go holds or a
// loud refusal naming the damage.
func TestKotlinReadsAGoLogRepairedByScrub(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	root := t.TempDir()
	ns := "app/data"
	open := func() (*embed.EmbeddedKdbRuntime, *server.KdbServerRuntime) {
		rt, err := embed.OpenFileRuntime(root, "app", ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		return rt, server.NewKdbServerRuntime(rt)
	}
	rt, a := open()
	memRT, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peer := server.NewKdbServerRuntime(memRT)
	ids := make([]codec.UUID, 4)
	bodies := []string{`{"n":"needle-a"}`, `{"n":"needle-b"}`, `{"n":"needle-c"}`, `{"n":"needle-d"}`}
	for i := range ids {
		ids[i] = randomID(t)
		if _, err := a.Upsert(ns, ids[i], bodies[i], auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Upsert(ns, ids[i], bodies[i], auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	deltaDir := filepath.Join(root, "ns", "app", "data", "delta")
	entries, _ := os.ReadDir(deltaDir)
	for _, e := range entries {
		p := filepath.Join(deltaDir, e.Name())
		raw, _ := os.ReadFile(p)
		if i := bytes.Index(raw, []byte("needle-b")); i >= 0 {
			raw[i] ^= 1
			_ = os.WriteFile(p, raw, 0o644)
		}
	}

	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	rt, a = open()
	rep, err := a.Scrub(func(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error) {
		s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: "scrubber", PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
		})
		if err != nil {
			return nil, err
		}
		defer s.Close()
		return s.Fetch(ns, wanted, treeHex)
	})
	if err != nil || len(rep.Repaired) != 1 {
		t.Fatalf("Go repair: %+v %v", rep, err)
	}
	rt.Close()

	for i, id := range ids {
		out := kotlinCLIResult(t, repo, "--data-dir", root, "get", ns, id.String())
		switch {
		case out.code == 0 && sameJSON(out.stdout, bodies[i]):
		case out.code != 0 && strings.Contains(out.stderr, "corrupt"):
			// A loud refusal: Kotlin replays the whole log and stops at the damaged frame.
		default:
			t.Errorf("Kotlin read %q for document %d (exit %d, stderr %q), Go holds %s - neither the value nor a refusal", out.stdout, i, out.code, out.stderr, bodies[i])
		}
	}
}

// TestKotlinRefusesRatherThanRollingBackADamagedNewestSegment: damage in the middle of the newest
// delta segment, with intact commits after it, is not a torn tail. Kotlin's replay used to treat
// it as one and silently drop every commit after the damage (as Go's did); it must refuse instead.
func TestKotlinRefusesRatherThanRollingBackADamagedNewestSegment(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	root := t.TempDir()
	rt, err := embed.OpenFileRuntime(root, "app", "app/t", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		if _, err := embed.PutJSONDocument(rt, "app/t", `{"id":"doc-`+n+`","n":"needle-`+n+`"}`); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()
	deltaDir := filepath.Join(root, "ns", "app", "t", "delta")
	entries, _ := os.ReadDir(deltaDir)
	if len(entries) != 1 {
		t.Fatalf("expected the four commits in one segment, got %d segments", len(entries))
	}
	p := filepath.Join(deltaDir, entries[0].Name())
	raw, _ := os.ReadFile(p)
	i := bytes.Index(raw, []byte("needle-b"))
	if i < 0 {
		t.Fatal("body not found to damage")
	}
	raw[i] ^= 1
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	out := kotlinCLIResult(t, repo, "--data-dir", root, "log", "app/t")
	if out.code == 0 {
		t.Fatalf("Kotlin opened a log with damage in the middle of its newest segment and served %q - that is a silent rollback", out.stdout)
	}
	if !strings.Contains(out.stderr, "not a torn tail") {
		t.Fatalf("expected a refusal naming the damage, got %q", out.stderr)
	}
}

type cliResult struct {
	stdout, stderr string
	code           int
}

// kotlinCLIResult runs the Kotlin CLI and returns what it printed and its exit code, failing only
// if it could not be run at all.
func kotlinCLIResult(t *testing.T, repo string, args ...string) cliResult {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := kotlinCommand(repo, args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	res := cliResult{stdout: strings.TrimSpace(out.String()), stderr: strings.TrimSpace(errb.String())}
	if cmd.ProcessState != nil {
		res.code = cmd.ProcessState.ExitCode()
	} else if err != nil {
		t.Fatalf("kotlin %v: %v", args, err)
	}
	return res
}
