package integration

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Attachment blobs across a mixed Go/Kotlin cluster, over real processes (docs/kdb-attachments-plan.md,
// decision 10): Kotlin's peer sync v2 fetches a Go node's blobs with BLOB_FETCH before storing the
// commits that attach them, uploads its own with ATTACHMENT_UPLOAD before its REF_UPDATE, and serves
// its blobs to a Go peer with chunk proofs Go checks. A blob whose stored bytes are damaged is
// refused whole, and the commit attaching it is not adopted.
//
// Needs a JDK (it runs the Kotlin CLI and kdb-service through gradlew); skipped under -short.

// interopBlob is n random bytes - random, so a chunk's bytes are found in exactly one file on disk.
func interopBlob(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

// corruptOnDisk flips one byte of blob's second chunk wherever it is stored under root, failing the
// test when it is stored nowhere (in the clear).
func corruptOnDisk(t *testing.T, root string, blob []byte) {
	t.Helper()
	at := document.AttachmentChunkBytes + 4096
	needle := blob[at : at+64]
	found := false
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Size() < int64(len(needle)) {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if i := bytes.Index(data, needle); i >= 0 {
			data[i+10] ^= 0xff
			found = true
			return os.WriteFile(path, data, info.Mode())
		}
		return nil
	})
	if err != nil || !found {
		t.Fatalf("corrupting the blob under %s: found=%v err=%v", root, found, err)
	}
}

func kotlinCLIErr(repo string, args ...string) (string, error) {
	cmd := kotlinCommand(repo, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// kotlinPutAttached writes a document with id and attaches data to it as name, in the Kotlin data dir.
func kotlinPutAttached(t *testing.T, repo, dir, ns string, id codec.UUID, name string, data []byte) {
	t.Helper()
	doc := writeTemp(t, "doc.json", []byte(fmt.Sprintf(`{"id":"%s","from":"kotlin"}`, id)))
	kotlinCLI(t, repo, "--data-dir", dir, "put", ns, doc)
	kotlinCLI(t, repo, "--data-dir", dir, "attach", "put", ns, id.String(), name, writeTemp(t, name, data))
}

func kotlinAttachment(t *testing.T, repo, dir, ns string, id codec.UUID, name string) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.bin")
	kotlinCLI(t, repo, "--data-dir", dir, "attach", "get", ns, id.String(), name, out)
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func goPutAttached(t *testing.T, rt *server.KdbServerRuntime, ns string, id codec.UUID, name string, data []byte) {
	t.Helper()
	if _, err := rt.Upsert(ns, id, `{"from":"go"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Runtime.PutAttachment(ns, id, bytes.NewReader(data), embed.PutAttachmentOptions{Name: name}); err != nil {
		t.Fatal(err)
	}
}

func goAttachment(rt *server.KdbServerRuntime, ns string, id codec.UUID, name string) ([]byte, error) {
	r, _, err := rt.Runtime.GetAttachment(ns, id, embed.AttachmentSelector{Name: name}, codec.Hash{})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// TestKotlinPeerSyncAttachmentsAgainstGo: the Kotlin CLI syncs with a Go node over TCP - it pulls
// a multi-chunk Go blob, pushes a multi-chunk blob of its own, and refuses a Go blob whose second
// chunk is damaged on the Go node's disk.
func TestKotlinPeerSyncAttachmentsAgainstGo(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	ns := "app/data"
	goRoot, ktRoot := t.TempDir(), t.TempDir()
	fileRT, err := embed.OpenFileRuntime(goRoot, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer fileRT.Close()
	g := server.NewKdbServerRuntime(fileRT)
	g.NodeID = randomID(t)
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", g, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peer := "kdb-tcp://" + ln.Addr().String()
	kotlinCLI(t, repo, "--data-dir", ktRoot, "init", ns)

	// Go -> Kotlin: a pull fetches two and a half chunks, each checked against its proof.
	goDoc, goBlob := randomID(t), interopBlob(1, 5<<19)
	goPutAttached(t, g, ns, goDoc, "go.bin", goBlob)
	kotlinCLI(t, repo, "--data-dir", ktRoot, "--peer-sync", "sync", ns, peer)
	if got := kotlinAttachment(t, repo, ktRoot, ns, goDoc, "go.bin"); !bytes.Equal(got, goBlob) {
		t.Fatalf("Kotlin read %d bytes of Go's %d-byte blob, not the same", len(got), len(goBlob))
	}

	// Kotlin -> Go: a push uploads the blob before proposing the ref.
	ktDoc, ktBlob := randomID(t), interopBlob(2, 3<<20+17)
	kotlinPutAttached(t, repo, ktRoot, ns, ktDoc, "kt.bin", ktBlob)
	kotlinCLI(t, repo, "--data-dir", ktRoot, "--peer-sync", "sync", ns, peer)
	if got, err := goAttachment(g, ns, ktDoc, "kt.bin"); err != nil || !bytes.Equal(got, ktBlob) {
		t.Fatalf("Go read Kotlin's blob: %d bytes, %v", len(got), err)
	}

	// A blob damaged on Go's disk: Kotlin is refused it and adopts nothing that names it.
	before := headLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "status", ns))
	badDoc, badBlob := randomID(t), interopBlob(3, 5<<19)
	goPutAttached(t, g, ns, badDoc, "bad.bin", badBlob)
	corruptOnDisk(t, goRoot, badBlob)
	out, err := kotlinCLIErr(repo, "--data-dir", ktRoot, "--peer-sync", "sync", ns, peer)
	if err == nil || !strings.Contains(out, "INTEGRITY") {
		t.Fatalf("a sync of a damaged blob: err=%v\n%s", err, out)
	}
	if list := kotlinCLI(t, repo, "--data-dir", ktRoot, "attach", "list", ns, badDoc.String()); strings.Contains(list, "bad.bin") {
		t.Fatalf("Kotlin lists the damaged attachment: %s", list)
	}
	if after := headLine(kotlinCLI(t, repo, "--data-dir", ktRoot, "status", ns)); after != before {
		t.Fatalf("Kotlin moved main (%s -> %s) onto a commit whose blob it does not hold", before, after)
	}
}

// headLine is the "HEAD <hex>" line of the CLI's status.
func headLine(status string) string {
	for _, l := range strings.Split(status, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "HEAD ") {
			return strings.TrimSpace(l)
		}
	}
	return ""
}

// kdbService is a running Kotlin kdb-service with WebSocket peer sync.
type kdbService struct {
	cmd  *exec.Cmd
	peer string
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startKdbService runs kdb-service over dir until it says it listens, in its own process group so
// stop ends the JVM gradle started, not just gradle.
func startKdbService(t *testing.T, repo, dir, ns string) *kdbService {
	t.Helper()
	port := freePort(t)
	listen := fmt.Sprintf("ws://127.0.0.1:%d/kdb?bind=true", port)
	// The SQL listener too gets a free port: its default (7444) may be another node's.
	sql := fmt.Sprintf("ws://127.0.0.1:%d/kdb?bind=true", freePort(t))
	cmd := exec.Command(filepath.Join(repo, "gradlew"), ":kdb-service:runService", "--quiet",
		"--args=--data-dir "+dir+" --namespace "+ns+" --peer-sync --listen-ws "+listen+" --listen-sql-ws "+sql)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "TERM=dumb")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &kdbService{cmd: cmd, peer: fmt.Sprintf("ws://127.0.0.1:%d/kdb", port)}
	ready := make(chan error, 1)
	var mu sync.Mutex
	var seen []string
	t.Cleanup(func() {
		if t.Failed() {
			mu.Lock()
			t.Logf("kdb-service output:\n%s", strings.Join(seen, "\n"))
			mu.Unlock()
		}
	})
	go func() {
		sc := bufio.NewScanner(stdout)
		sent := false
		for sc.Scan() {
			mu.Lock()
			seen = append(seen, sc.Text())
			mu.Unlock()
			if !sent && strings.Contains(sc.Text(), "KDB service peer=") {
				ready <- nil
				sent = true
			}
		}
		if !sent {
			ready <- fmt.Errorf("kdb-service exited before listening")
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			s.stop()
			t.Fatal(err)
		}
	case <-time.After(5 * time.Minute):
		s.stop()
		t.Fatal("kdb-service did not start")
	}
	// The listener binds right after the banner.
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return s
}

func (s *kdbService) stop() {
	if s.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func syncWithKotlin(rt *server.KdbServerRuntime, peer, ns string) (peersync.NamespaceSyncResult, error) {
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), ws.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: rt.NodeID.String(), PeerURI: peer, Namespaces: []string{ns}, Local: rt.PeerNamespaces(), Timeout: time.Minute,
	})
	if err != nil {
		return peersync.NamespaceSyncResult{}, err
	}
	if res.Protocol != 2 || len(res.Namespaces) != 1 {
		return peersync.NamespaceSyncResult{}, fmt.Errorf("sync with Kotlin: protocol %d, %d namespaces", res.Protocol, len(res.Namespaces))
	}
	return res.Namespaces[0], res.Namespaces[0].Err
}

// TestGoPeerSyncAttachmentsAgainstKotlin: a Go node syncs with Kotlin's kdb-service over WebSocket -
// it pulls a multi-chunk Kotlin blob (checking Kotlin's proofs), pushes a multi-chunk blob of its
// own (uploaded to Kotlin's host), and is refused a Kotlin blob damaged on Kotlin's disk.
func TestGoPeerSyncAttachmentsAgainstKotlin(t *testing.T) {
	if testing.Short() {
		t.Skip("runs kdb-service and the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	ns := "app/data"
	ktRoot := t.TempDir()
	ktDoc, ktBlob := randomID(t), interopBlob(4, 5<<19)
	kotlinCLI(t, repo, "--data-dir", ktRoot, "init", ns)
	kotlinPutAttached(t, repo, ktRoot, ns, ktDoc, "kt.bin", ktBlob)

	memRT, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	g := server.NewKdbServerRuntime(memRT)
	g.NodeID = randomID(t)
	goDoc, goBlob := randomID(t), interopBlob(5, 2<<20+1)

	svc := startKdbService(t, repo, ktRoot, ns)
	// Kotlin -> Go: Go fetches Kotlin's blob and checks every chunk against Kotlin's proofs.
	if _, err := syncWithKotlin(g, svc.peer, ns); err != nil {
		svc.stop()
		t.Fatalf("first sync: %v", err)
	}
	if got, err := goAttachment(g, ns, ktDoc, "kt.bin"); err != nil || !bytes.Equal(got, ktBlob) {
		svc.stop()
		t.Fatalf("Go read Kotlin's blob: %d bytes, %v", len(got), err)
	}
	// Go -> Kotlin: Go uploads its blob to Kotlin's host before proposing main.
	goPutAttached(t, g, ns, goDoc, "go.bin", goBlob)
	r, err := syncWithKotlin(g, svc.peer, ns)
	svc.stop()
	if err != nil || r.Pushed == 0 {
		t.Fatalf("push to Kotlin: pushed %d, %v", r.Pushed, err)
	}
	if got := kotlinAttachment(t, repo, ktRoot, ns, goDoc, "go.bin"); !bytes.Equal(got, goBlob) {
		t.Fatalf("Kotlin holds %d bytes of Go's %d-byte blob, not the same", len(got), len(goBlob))
	}

	// A blob damaged on Kotlin's disk: its host answers INTEGRITY and Go adopts nothing naming it.
	badDoc, badBlob := randomID(t), interopBlob(6, 5<<19)
	kotlinPutAttached(t, repo, ktRoot, ns, badDoc, "bad.bin", badBlob)
	corruptOnDisk(t, ktRoot, badBlob)
	head, _ := g.Runtime.DAG.Head()
	svc = startKdbService(t, repo, ktRoot, ns)
	_, err = syncWithKotlin(g, svc.peer, ns)
	svc.stop()
	if err == nil || !strings.Contains(err.Error(), "INTEGRITY") {
		t.Fatalf("a pull of a damaged blob: %v", err)
	}
	if now, _ := g.Runtime.DAG.Head(); now != head {
		t.Fatal("Go moved main onto a commit whose blob it does not hold")
	}
	if _, err := goAttachment(g, ns, badDoc, "bad.bin"); err == nil {
		t.Fatal("Go lists the damaged attachment")
	}
}
