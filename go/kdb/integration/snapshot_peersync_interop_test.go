package integration

import (
	"bytes"
	"fmt"
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

// TestKotlinSnapshotBootstrapInterop: snapshot bootstrap across a mixed cluster, over real processes.
//
//   - A Go node B, itself bootstrapped from a snapshot of A (so it holds a shallow root and cannot
//     send history), serves a fresh Kotlin node: the Kotlin CLI installs B's state - documents,
//     attachment table, and a multi-chunk attachment's bytes - and every later CLI run reopens it
//     from the Go-format checkpoint Kotlin wrote. It then catches up on what A commits after.
//   - Go opens the Kotlin data directory: the checkpoint, SSTables, marker and log Kotlin wrote.
//   - A fresh Go node bootstraps from Kotlin's kdb-service, which advertises its shallow root.
//
// Needs a JDK (it runs the Kotlin CLI and kdb-service through gradlew); skipped under -short.
func TestKotlinSnapshotBootstrapInterop(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI and kdb-service through gradle")
	}
	repo := findRepoRoot(t)
	ns := "app/data"

	newGo := func() *server.KdbServerRuntime {
		rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		s := server.NewKdbServerRuntime(rt)
		s.NodeID = randomID(t)
		return s
	}
	listen := func(rt *server.KdbServerRuntime) string {
		ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, ns)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		return ln.Addr().String()
	}
	goPull := func(into *server.KdbServerRuntime, addr string, prefer bool) peersync.NamespaceSyncResult {
		t.Helper()
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: into.NodeID.String(), PeerURI: "tcp://" + addr, Namespaces: []string{ns}, Local: into.PeerNamespaces(),
			Mode: peersync.SyncPull, PreferSnapshot: prefer,
		})
		if err != nil || len(res.Namespaces) != 1 || res.Namespaces[0].Err != nil {
			t.Fatalf("Go pull from %s: %v %+v", addr, err, res.Namespaces)
		}
		return res.Namespaces[0]
	}
	goDoc := func(rt *server.KdbServerRuntime, id codec.UUID) string {
		t.Helper()
		body, _, found, err := rt.GetDocument(ns, id)
		if err != nil || !found {
			t.Fatalf("Go has no document %s: %v", id, err)
		}
		return body
	}

	// A holds the history; B bootstraps from A's snapshot, so B is shallow.
	a := newGo()
	ids := make([]codec.UUID, 25)
	for i := range ids {
		ids[i] = randomID(t)
		if _, err := a.Upsert(ns, ids[i], fmt.Sprintf(`{"n":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	blob := interopBlob(7, 5<<19)
	goPutAttached(t, a, ns, ids[0], "big.bin", blob)
	aAddr := listen(a)
	b := newGo()
	if r := goPull(b, aAddr, true); r.Snapshot == "" {
		t.Fatalf("B did not bootstrap from a snapshot: %+v", r)
	}
	bAddr := listen(b)
	bHead, _ := b.Runtime.DAG.Head()

	// A fresh Kotlin node pulls from shallow B: it must take a snapshot, durably.
	ktRoot := t.TempDir()
	kotlinCLI(t, repo, "--data-dir", ktRoot, "init", ns)
	kotlinCLI(t, repo, "--data-dir", ktRoot, "--peer-sync", "sync", ns, "kdb-tcp://"+bAddr)
	if got := headLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "status", ns)); got != "HEAD "+bHead.Hex() {
		t.Fatalf("Kotlin is at %q after the bootstrap, B at %s", got, bHead.Hex())
	}
	marker, err := os.ReadFile(filepath.Join(ktRoot, "ns", "app", "data", "meta.json"))
	if err != nil || !strings.Contains(string(marker), bHead.Hex()) {
		t.Fatalf("Kotlin's marker does not name the snapshot root: %s %v", marker, err)
	}
	if got := lastLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "get", ns, ids[3].String())); !sameJSON(got, goDoc(b, ids[3])) {
		t.Fatalf("Kotlin read %q for a snapshot document", got)
	}
	if got := kotlinAttachment(t, repo, ktRoot, ns, ids[0], "big.bin"); !bytes.Equal(got, blob) {
		t.Fatalf("Kotlin holds %d bytes of the %d-byte attachment", len(got), len(blob))
	}

	// History above the root: A commits, B catches up, Kotlin catches up from B.
	later := randomID(t)
	if _, err := a.Upsert(ns, later, `{"later":true}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	goPull(b, aAddr, false)
	kotlinCLI(t, repo, "--data-dir", ktRoot, "--peer-sync", "sync", ns, "kdb-tcp://"+bAddr)
	if got := lastLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "get", ns, later.String())); !sameJSON(got, `{"later":true}`) {
		t.Fatalf("Kotlin read %q for a document committed after the snapshot", got)
	}
	ktHead := strings.TrimPrefix(headLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "status", ns)), "HEAD ")

	// Go opens what Kotlin wrote.
	fileRT, err := embed.OpenFileRuntime(ktRoot, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("Go cannot open Kotlin's data directory: %v", err)
	}
	g := server.NewKdbServerRuntime(fileRT)
	if h, _ := fileRT.DAG.Head(); h.Hex() != ktHead {
		t.Fatalf("Go opened Kotlin's directory at %s, Kotlin is at %s", h.Hex(), ktHead)
	}
	if got := goDoc(g, ids[3]); !sameJSON(got, goDoc(a, ids[3])) {
		t.Fatalf("Go read %q from Kotlin's directory", got)
	}
	if got, err := goAttachment(g, ns, ids[0], "big.bin"); err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("Go read the attachment from Kotlin's directory: %d bytes, %v", len(got), err)
	}
	fileRT.Close()

	// A fresh Go node bootstraps from Kotlin, which is shallow and says so.
	svc := startKdbService(t, repo, ktRoot, ns)
	c := newGo()
	r, err := syncWithKotlin(c, svc.peer, ns)
	svc.stop()
	if err != nil || r.Snapshot == "" {
		t.Fatalf("a fresh Go node did not bootstrap from Kotlin's snapshot: %v %+v", err, r)
	}
	if got := goDoc(c, later); !sameJSON(got, `{"later":true}`) {
		t.Fatalf("Go read %q from Kotlin's snapshot", got)
	}
	if got, err := goAttachment(c, ns, ids[0], "big.bin"); err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("Go read the attachment of Kotlin's snapshot: %d bytes, %v", len(got), err)
	}
}
