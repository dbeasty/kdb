package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// TestCLIScrubRepairsFromAPeerAndPeerDiffCompares: a body damaged in the data directory's log is
// reported by `kdb scrub` (exit 3), repaired by `kdb scrub --peer` from a node holding it, and
// `kdb peer-diff` then finds the two nodes equal.
func TestCLIScrubRepairsFromAPeerAndPeerDiffCompares(t *testing.T) {
	ns := "app/data"
	dir := t.TempDir()
	local, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peerRT, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peer := server.NewKdbServerRuntime(peerRT)
	lsrv := server.NewKdbServerRuntime(local)
	var victim codec.UUID
	for i, body := range []string{`{"n":"needle-a-1"}`, `{"n":"needle-b-2"}`, `{"n":"needle-c-3"}`} {
		id, _ := codec.RandomUUID()
		if i == 1 {
			victim = id
		}
		if _, err := lsrv.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	local.Close()
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := "tcp://" + ln.Addr().String()

	// The two nodes wrote the same documents in different commits, so their heads differ but
	// their trees - what peer-diff compares - are equal.
	if code, out := captureRun(t, "--data-dir", dir, "peer-diff", ns, addr); code != 0 || !strings.Contains(out, "0 difference(s)") {
		t.Fatalf("peer-diff of equal trees: %d %q", code, out)
	}

	deltaDir := filepath.Join(dir, "ns", "app", "data", "delta")
	entries, _ := os.ReadDir(deltaDir)
	flipped := false
	for _, e := range entries {
		p := filepath.Join(deltaDir, e.Name())
		raw, _ := os.ReadFile(p)
		if i := bytes.Index(raw, []byte("needle-b-2")); i >= 0 {
			raw[i] ^= 1
			_ = os.WriteFile(p, raw, 0o644)
			flipped = true
		}
	}
	if !flipped {
		t.Fatal("could not find the body to damage")
	}

	if code, out := captureRun(t, "--data-dir", dir, "scrub", ns); code != 3 || !strings.Contains(out, "damaged 1") || !strings.Contains(out, victim.String()) {
		t.Fatalf("scrub without a peer should report the damage and exit 3: %d %q", code, out)
	}
	if code, out := captureRun(t, "--data-dir", dir, "scrub", ns, "--peer", addr); code != 0 || !strings.Contains(out, "repaired 1") {
		t.Fatalf("scrub with a peer should repair: %d %q", code, out)
	}
	if code, out := captureRun(t, "--data-dir", dir, "scrub", ns); code != 0 || !strings.Contains(out, "damaged 0") {
		t.Fatalf("a scrub after the repair should be clean: %d %q", code, out)
	}

	// A document only the peer has is reported by peer-diff (exit 3).
	extra, _ := codec.RandomUUID()
	if _, err := peer.Upsert(ns, extra, `{"only":"peer"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if code, out := captureRun(t, "--data-dir", dir, "peer-diff", ns, addr); code != 3 || !strings.Contains(out, extra.String()+"\tpeer only") {
		t.Fatalf("peer-diff should report the peer-only document: %d %q", code, out)
	}
}
