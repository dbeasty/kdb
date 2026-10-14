package integration

// Issue #43, U6b in docs/kdb-open-issues-plan.md: an acknowledged write must survive SIGKILL of the
// real server process. The existing deterministic crash tests (embed/crash_truncation_test.go)
// write through embed.PutJSONDocument in-process and single-threaded, so they never cover the path
// the issue was seen on: a write acknowledged over the wire by kdb-service, through the write
// gate, the transaction engine and group commit, with other commits in flight when the kill lands.
//
// This test drives exactly that path. Each iteration starts kdb-service on the SAME data
// directory, has several connections write their own documents concurrently, SIGKILLs the
// process at a random moment, restarts it, and checks every write acknowledged in any iteration
// so far - so recovery is also exercised on top of recovered data, kill after kill. The child runs
// with GOMAXPROCS=2, the shape of a CI runner, where the e2e failure was seen.
//
// KDB_KILL9_ITERATIONS sets the number of kills (default 3, and 1 under -short);
// soak it with e.g. KDB_KILL9_ITERATIONS=200 go test -run TestServerKill9 ./kdb/integration/.
// KDB_KILL9_SERVICE_ARGS adds space-separated kdb-service flags, e.g. "--sync-mode fast" to soak
// another durability setting, or "--durability async" - which may lose acknowledged writes by
// design, and is how to check that this test notices a loss at all.
// On a failure the data directory and every launch's log are kept and their path is reported.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

const kill9Namespace = "e2e/data"

var (
	serviceBinOnce sync.Once
	serviceBin     string
	serviceBinErr  error
)

// buildService builds kdb-service once per test binary. It is built without -race even when the
// test runs under it: the point is the production binary's crash behaviour.
func buildService(t *testing.T) string {
	t.Helper()
	serviceBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "kdb-kill9-bin-")
		if err != nil {
			serviceBinErr = err
			return
		}
		serviceBin = filepath.Join(dir, exeName("kdb-service"))
		build := exec.Command("go", "build", "-o", serviceBin, "./cmd/kdb-service")
		build.Dir = filepath.Join("..", "..")
		if out, err := build.CombinedOutput(); err != nil {
			serviceBinErr = fmt.Errorf("build kdb-service: %v\n%s", err, out)
		}
	})
	if serviceBinErr != nil {
		t.Fatal(serviceBinErr)
	}
	return serviceBin
}

func freeTCPPorts(t *testing.T, n int) []int {
	t.Helper()
	var ls []net.Listener
	defer func() {
		for _, l := range ls {
			_ = l.Close()
		}
	}()
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports
}

type killServer struct {
	t        *testing.T
	bin      string
	dataDir  string
	launches int
	cmd      *exec.Cmd
	// exited is closed once cmd has been reaped. Waiting on it, rather than probing with signal 0,
	// is what works on Windows too, where only Kill can be sent to a process.
	exited  chan struct{}
	sqlAddr string
	admin   string
	// slowestStart is the longest a launch took to become ready. Restart time after a crash
	// grows with what has to be replayed, so it is reported rather than asserted: a slow start
	// is its own problem, and failing a durability test over it would hide which one it was.
	slowestStart time.Duration
}

func (s *killServer) start() {
	s.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if lastErr = s.launch(); lastErr == nil {
			return
		}
		if !strings.Contains(lastErr.Error(), "address already in use") {
			break
		}
	}
	s.t.Fatal(lastErr)
}

func (s *killServer) launch() error {
	p := freeTCPPorts(s.t, 4)
	s.launches++
	s.sqlAddr = fmt.Sprintf("tcp://127.0.0.1:%d", p[0])
	s.admin = fmt.Sprintf("127.0.0.1:%d", p[3])
	logPath := filepath.Join(s.dataDir, fmt.Sprintf("service.%d.log", s.launches))
	logFile, err := os.Create(logPath)
	if err != nil {
		return err
	}
	cmd := exec.Command(s.bin,
		"--data-dir", s.dataDir,
		"--namespace", kill9Namespace,
		"--sql-addr", s.sqlAddr+"?bind=true",
		"--peer-addr", fmt.Sprintf("tcp://127.0.0.1:%d?bind=true", p[1]),
		"--stream-addr", fmt.Sprintf("tcp://127.0.0.1:%d?bind=true", p[2]),
		"--admin-addr", s.admin,
		"--log-format", "json",
	)
	if extra := strings.Fields(os.Getenv("KDB_KILL9_SERVICE_ARGS")); len(extra) > 0 {
		cmd.Args = append(cmd.Args, extra...)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	s.cmd = cmd
	exited := make(chan error, 1)
	reaped := make(chan struct{})
	s.exited = reaped
	go func() { err := cmd.Wait(); _ = logFile.Close(); close(reaped); exited <- err }()

	began := time.Now()
	deadline := began.Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case err := <-exited:
			out, _ := os.ReadFile(logPath)
			s.cmd = nil
			return fmt.Errorf("kdb-service exited early (%v):\n%s", err, out)
		default:
		}
		resp, err := http.Get("http://" + s.admin + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if took := time.Since(began); took > s.slowestStart {
					s.slowestStart = took
				}
				go func() { <-exited }()
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return fmt.Errorf("kdb-service not ready after 2m (launch %d, log %s)", s.launches, logPath)
}

func (s *killServer) kill9() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Kill() // SIGKILL on unix, TerminateProcess on Windows
	// Wait for the process to be gone, so the restart cannot race the dying one for the lock.
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
	}
	s.cmd = nil
}

func (s *killServer) stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		// Windows has no SIGTERM to send; there the only stop is a kill.
		if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			_ = s.cmd.Process.Kill()
		}
		select {
		case <-s.exited:
		case <-time.After(20 * time.Second):
			_ = s.cmd.Process.Kill()
			<-s.exited
		}
		s.cmd = nil
	}
}

// ackedWrites holds, per document, the highest value the server acknowledged. Each document has
// one writer, so "at least this value" is the exact durability contract.
type ackedWrites struct {
	mu   sync.Mutex
	last map[string]int
	n    int
}

func (a *ackedWrites) record(doc string, v int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v > a.last[doc] || a.last[doc] == 0 {
		a.last[doc] = v
	}
	a.n++
}

func TestServerKill9LosesNoAcknowledgedWrite(t *testing.T) {
	iterations := 3
	if testing.Short() {
		iterations = 1 // the PR smoke; the nightly runs 3 and soaks 150
	}
	if v := os.Getenv("KDB_KILL9_ITERATIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("KDB_KILL9_ITERATIONS=%q", v)
		}
		iterations = n
	}
	seed := time.Now().UnixNano()
	if v := os.Getenv("KDB_KILL9_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed %d (set KDB_KILL9_SEED to replay the kill timing)", seed)

	dataDir, err := os.MkdirTemp("", "kdb-kill9-data-")
	if err != nil {
		t.Fatal(err)
	}
	failed := true
	defer func() {
		if failed {
			t.Logf("data directory and every launch's log kept at %s", dataDir)
		} else {
			_ = os.RemoveAll(dataDir)
		}
	}()

	srv := &killServer{t: t, bin: buildService(t), dataDir: dataDir}
	acked := &ackedWrites{last: map[string]int{}}
	const writers = 6
	const docsPerWriter = 4
	value := 0 // strictly increasing across iterations, so a rollback across a restart shows

	for it := 0; it < iterations; it++ {
		srv.start()
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		var valueMu sync.Mutex
		next := func() int { valueMu.Lock(); defer valueMu.Unlock(); value++; return value }
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// Writer 0 opens a new connection for every write - session begin, then commit -
				// which is the shape of the e2e test the issue was seen in (one helper process per
				// write). The others keep one connection, the shape of a real application.
				perWrite := w == 0
				var cl *client.Client
				var err error
				connect := func() bool {
					if cl != nil {
						_ = cl.Close()
					}
					cl, err = client.Connect(ctx, srv.sqlAddr, "")
					return err == nil
				}
				if !connect() {
					return
				}
				// A reconnect that fails as the kill lands leaves cl nil.
				defer func() {
					if cl != nil {
						_ = cl.Close()
					}
				}()
				for i := 0; ctx.Err() == nil; i++ {
					if perWrite && i > 0 && !connect() {
						return
					}
					doc := fmt.Sprintf("%04x%028x", w+1, i%docsPerWriter)
					v := next()
					body := fmt.Sprintf(`{"id":%q,"v":%d,"w":%d}`, doc, v, w)
					var hash string
					// Mix the two write shapes: PutJSON replaces (delete + write in one
					// transaction), Upsert merges into the stored document.
					if i%3 == 2 {
						hash, err = cl.Upsert(ctx, kill9Namespace, doc, []byte(body))
					} else {
						hash, err = cl.PutJSON(ctx, kill9Namespace, doc, []byte(body))
					}
					if err != nil {
						return // the kill landed; anything unacknowledged may be lost
					}
					if len(hash) == 64 {
						acked.record(doc, v)
					}
				}
			}(w)
		}

		if iterations == 1 {
			// One kill (the -short smoke) has no later iterations to make up for a kill that
			// lands before the load got going - on a Windows runner 150 ms can be one write. So
			// the one kill waits for some acknowledged writes, then comes at a random moment.
			for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
				acked.mu.Lock()
				n := acked.n
				acked.mu.Unlock()
				if n >= 10 {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(time.Duration(150+rng.Intn(900)) * time.Millisecond)
		srv.kill9()
		cancel()
		wg.Wait()

		srv.start()
		if report := verifyAcked(t, srv, acked); report != "" {
			srv.stop()
			t.Fatalf("iteration %d of %d (seed %d): %s", it+1, iterations, seed, report)
		}
		srv.stop()
	}
	if acked.n < iterations*10 {
		t.Fatalf("only %d writes acknowledged over %d iterations - the load never got going", acked.n, iterations)
	}
	t.Logf("%d kills, %d acknowledged writes, %d documents, all intact; slowest restart %v",
		iterations, acked.n, len(acked.last), srv.slowestStart.Round(time.Millisecond))
	failed = false
}

// verifyAcked reads every acknowledged document and returns a description of every way it found
// them wrong, or "" when all are intact. It checks all of them before reporting, because one
// document missing, all of them missing and reads that error are different bugs.
func verifyAcked(t *testing.T, srv *killServer, acked *ackedWrites) string {
	t.Helper()
	acked.mu.Lock()
	want := make(map[string]int, len(acked.last))
	for d, v := range acked.last {
		want[d] = v
	}
	acked.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := client.Connect(ctx, srv.sqlAddr, "")
	if err != nil {
		return fmt.Sprintf("cannot connect after restart: %v", err)
	}
	defer cl.Close()

	var missing, behind, readErrs []string
	servedAt := map[string]bool{}
	docs := make([]string, 0, len(want))
	for d := range want {
		docs = append(docs, d)
	}
	sort.Strings(docs)
	for _, doc := range docs {
		body, commit, err := cl.GetJSON(ctx, kill9Namespace, doc)
		servedAt[commit] = true
		switch {
		case errors.Is(err, client.ErrNotFound):
			missing = append(missing, doc)
		case err != nil:
			readErrs = append(readErrs, fmt.Sprintf("%s: %v", doc, err))
		default:
			var got struct{ V int }
			if err := json.Unmarshal(body, &got); err != nil {
				readErrs = append(readErrs, fmt.Sprintf("%s: unparseable %q", doc, body))
			} else if got.V < want[doc] {
				behind = append(behind, fmt.Sprintf("%s: got v=%d, acknowledged v=%d", doc, got.V, want[doc]))
			}
		}
	}
	if len(missing)+len(behind)+len(readErrs) == 0 {
		return ""
	}
	commits := make([]string, 0, len(servedAt))
	for c := range servedAt {
		commits = append(commits, c)
	}
	return fmt.Sprintf("of %d acknowledged documents: %d missing %v; %d rolled back %v; %d unreadable %v; reads served at %v",
		len(want), len(missing), missing, len(behind), behind, len(readErrs), readErrs, commits)
}
