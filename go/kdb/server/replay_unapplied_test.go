package server

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Issue #43, U6c: replay stores, and does not apply, a logged commit that does not extend main's
// head. For a peer's side branch that is intended; in a single-writer log it should never
// happen, and when it did, nothing said so. Open now counts these commits, reports them through
// the runtime's recovery report, and logs at WARN every one that main still does not reach once
// the whole log has been read.

// syncBuffer is a bytes.Buffer safe for a slog handler that background goroutines also log to.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs routes the default slog logger (and the log package through it) into a buffer
// until the test ends.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func warnLines(logs string) []string {
	var out []string
	for _, l := range strings.Split(logs, "\n") {
		if strings.Contains(l, "level=WARN") && strings.Contains(l, "not applied") {
			out = append(out, l)
		}
	}
	return out
}

func reopenFromLog(t *testing.T, dir, ns string) *embed.EmbeddedKdbRuntime {
	t.Helper()
	removeCheckpointFiles(t, dir)
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	return rt
}

func TestReplayReportsALoggedCommitMainNeverReached(t *testing.T) {
	dir := t.TempDir()
	ns := "app/data"
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	local, err := srv.Upsert(ns, mustRandomUUID(t), `{"from":"local"}`, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	// A commit that does not extend main, logged as peer sync logs a peer's commit ahead of the
	// merge that adopts it - and no merge follows.
	peer := newPeerFixture(t, srv)
	genesis, _ := peer.dag.Head()
	side := pushDoc(t, peer.dag, peer.storage, ns, genesis, mustRandomUUID(t), `{"from":"peer"}`)
	for _, c := range []document.Commit{side} {
		if err := srv.dag.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
		if err := srv.persister.Persist(c); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	logs := captureLogs(t)
	reopened := reopenFromLog(t, dir, ns)
	if h, _ := reopened.DAG.Head(); h != local.Hash {
		t.Fatalf("which commits replay applies must not change: head %s, want %s", h.Hex(), local.Hash.Hex())
	}
	rep := reopened.Recovery()
	if !rep.ReplayedInFull || rep.CommitsNotApplied != 1 || rep.CommitsOffMain != 1 {
		t.Fatalf("recovery report %+v, want a full replay with 1 commit not applied, 1 off main", rep)
	}
	warns := warnLines(logs.String())
	if len(warns) != 1 {
		t.Fatalf("want one WARN line for the commit main never reached, got %d:\n%s", len(warns), logs.String())
	}
	for _, want := range []string{ns, side.Hash.Hex(), local.Hash.Hex()} {
		if !strings.Contains(warns[0], want) {
			t.Fatalf("WARN line does not name %s:\n%s", want, warns[0])
		}
	}
}

func TestReplayCountsAMergedPeerCommitWithoutWarning(t *testing.T) {
	dir := t.TempDir()
	ns := "app/data"
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	if _, err := srv.Upsert(ns, mustRandomUUID(t), `{"from":"local"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	winner := newPeerFixture(t, srv)
	_, peerCommit := winner.write(`{"from":"peer"}`)
	if _, err := winner.push(peerCommit); err != nil {
		t.Fatalf("disjoint push: %v", err)
	}
	head, _ := srv.dag.Head()
	rt.Close()

	logs := captureLogs(t)
	reopened := reopenFromLog(t, dir, ns)
	if h, _ := reopened.DAG.Head(); h != head {
		t.Fatalf("head %s after replay, want %s", h.Hex(), head.Hex())
	}
	rep := reopened.Recovery()
	if rep.CommitsNotApplied != 1 || rep.CommitsOffMain != 0 {
		t.Fatalf("recovery report %+v, want the merged peer commit counted as not applied and none off main", rep)
	}
	if w := warnLines(logs.String()); len(w) != 0 {
		t.Fatalf("a peer commit main merged is expected, not a warning:\n%s", strings.Join(w, "\n"))
	}
}

func TestSingleWriterReplayReportsNothingUnapplied(t *testing.T) {
	dir := t.TempDir()
	ns := "app/data"
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	for i := 0; i < 5; i++ {
		if _, err := srv.Upsert(ns, mustRandomUUID(t), `{"n":1}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	logs := captureLogs(t)
	reopened := reopenFromLog(t, dir, ns)
	rep := reopened.Recovery()
	if !rep.ReplayedInFull || rep.CommitsNotApplied != 0 || rep.CommitsOffMain != 0 {
		t.Fatalf("recovery report %+v, want a full replay that applied every commit", rep)
	}
	if w := warnLines(logs.String()); len(w) != 0 {
		t.Fatalf("unexpected WARN:\n%s", strings.Join(w, "\n"))
	}
}
