package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// Index snapshots are written off the commit path: in the background once commits go quiet,
// and on close. These tests pin that a snapshot is never trusted unless it is exactly the
// index's content at the head it names - whatever moment a kill lands at.

const flushTestFields = 6

func flushTestDocID(i int) codec.UUID {
	id, err := codec.ParseUUID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
	if err != nil {
		panic(err)
	}
	return id
}

// flushTestDoc is document i's body in version v: fields g0..g5 with a few distinct values each,
// and a field left out now and then so a write can also take a document out of an index.
func flushTestDoc(i, v int) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"n":%d`, i)
	for j := 0; j < flushTestFields; j++ {
		if (i+v+j)%7 == 0 {
			continue
		}
		fmt.Fprintf(&b, `,"g%d":"x%d"`, j, (i*(j+1)+v)%5)
	}
	b.WriteString("}")
	return b.String()
}

func createFlushTestIndexes(t testing.TB, p *RegistryIndexProvider) {
	t.Helper()
	for j := 0; j < flushTestFields; j++ {
		using := "HASH"
		if j%2 == 1 {
			using = "BTREE"
		}
		if err := p.CreateIndex(sql.StmtCreateIndex{
			Name: fmt.Sprintf("by_g%d", j), Table: "docs", Fields: []sql.IndexField{{Path: fmt.Sprintf("g%d", j)}}, Using: using,
		}, sql.QueryContext{NamespaceID: "app/data"}); err != nil {
			t.Fatal(err)
		}
	}
}

// flushUpsert writes document i's version v as a whole-document replace. (A merge-patch Upsert
// that leaves an indexed field out is not used here: the index is given the patch, not the
// merged document, so it drops a field the stored document keeps - a separate defect.)
func flushUpsert(t testing.TB, srv *KdbServerRuntime, i, v int) {
	t.Helper()
	flushCommit(t, srv, document.WriteOp{DocID: flushTestDocID(i), Patch: flushTestDoc(i, v), Replace: true})
}

func flushCommit(t testing.TB, srv *KdbServerRuntime, ops ...document.Op) {
	t.Helper()
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{ID: mustNewTxID(t), BaseVersion: head, Timestamp: codec.TimestampNow(), Operations: ops}
	if _, err := srv.Commit("app/data", tx, "flush-test", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func flushDelete(t testing.TB, srv *KdbServerRuntime, i int) {
	t.Helper()
	flushCommit(t, srv, document.DeleteOp{DocID: flushTestDocID(i)})
}

// verifyIndexesMatchScan checks every single-field index against the documents at head: each
// value's lookup lists exactly the documents carrying it, and nothing else is indexed.
// explainIndexMismatch says, for each document only one side lists, what the scan read and
// whether that body is the version head's tree names: a mismatch there is storage returning a
// wrong version, not an index holding a wrong entry.
func explainIndexMismatch(srv *KdbServerRuntime, head codec.Hash, got, want []string, bodies map[codec.UUID]string) string {
	in := func(xs []string) map[string]bool {
		m := map[string]bool{}
		for _, x := range xs {
			m[x] = true
		}
		return m
	}
	g, w := in(got), in(want)
	tree, terr := srv.HeadTree()
	var b strings.Builder
	fmt.Fprintf(&b, "head %s", head.Hex())
	for _, side := range []struct {
		name string
		ids  []string
		in   map[string]bool
	}{{"only in index", got, w}, {"only in scan", want, g}} {
		for _, s := range side.ids {
			if side.in[s] {
				continue
			}
			id, _ := codec.ParseUUID(s)
			body := bodies[id]
			bodyHash, herr := document.Document{ID: id, JSON: body}.ContentHash()
			treeHash, inTree := codec.Hash{}, false
			if terr == nil {
				treeHash, inTree = tree.HashFor(id)
			}
			fmt.Fprintf(&b, "\n  %s %s: scan body %q (hash %s, err %v); head tree names %s (present %v)",
				side.name, s, body, bodyHash.Hex()[:12], herr, treeHash.Hex()[:12], inTree)
		}
	}
	return b.String()
}

func verifyIndexesMatchScan(t testing.TB, srv *KdbServerRuntime, p *RegistryIndexProvider) {
	t.Helper()
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	docs := map[codec.UUID]map[string]any{}
	bodies := map[codec.UUID]string{}
	if err := srv.scanAtCommit(head)(func(id codec.UUID, body string) bool {
		var m map[string]any
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			t.Fatal(err)
		}
		docs[id] = m
		bodies[id] = body
		return true
	}); err != nil {
		t.Fatal(err)
	}
	descs := p.Registry().Descriptors()
	if len(descs) == 0 {
		t.Fatal("no indexes registered")
	}
	for _, d := range descs {
		store, _ := p.Registry().ByID(d.IndexID)
		want := map[string][]string{}
		var all []string
		for id, m := range docs {
			if v, ok := m[d.FieldName].(string); ok {
				want[v] = append(want[v], id.String())
				all = append(all, id.String())
			}
		}
		for k := 0; k < 5; k++ {
			v := fmt.Sprintf("x%d", k)
			ids, err := store.Lookup(index.StringKey{Value: v}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := idStrings(ids), sorted(want[v]); strings.Join(got, ",") != strings.Join(exp, ",") {
				t.Fatalf("index %s, %s = %s: got %d docs, want %d\ngot  %v\nwant %v\n%s", d.IndexName(), d.FieldName, v, len(got), len(exp), got, exp,
					explainIndexMismatch(srv, head, got, exp, bodies))
			}
		}
		if d.Type == index.IndexTypeBTree {
			ids, err := store.Range(nil, nil, nil, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := uniq(idStrings(ids)), sorted(all); strings.Join(got, ",") != strings.Join(exp, ",") {
				t.Fatalf("index %s holds %d docs, want %d", d.IndexName(), len(got), len(exp))
			}
		}
	}
}

func idStrings(ids []codec.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	sort.Strings(out)
	return out
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func uniq(s []string) []string {
	var out []string
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// manifestsAtHead counts the registered indexes whose manifest names the DAG head - the ones
// the next open restores rather than rebuilds.
func manifestsAtHead(t testing.TB, srv *KdbServerRuntime, p *RegistryIndexProvider) int {
	t.Helper()
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range p.Registry().Descriptors() {
		m, err := index.ReadManifest(index.IndexDir(p.dir, d.IndexID))
		if err == nil && m.HeadCommitHex == head.Hex() {
			n++
		}
	}
	return n
}

func withFlushTiming(t *testing.T, idle, gap time.Duration) {
	oldIdle, oldGap, oldBackoff := indexFlushIdle, indexFlushMinGap, indexFlushBackoff
	indexFlushIdle, indexFlushMinGap, indexFlushBackoff = idle, gap, 0
	t.Cleanup(func() { indexFlushIdle, indexFlushMinGap, indexFlushBackoff = oldIdle, oldGap, oldBackoff })
}

// TestIndexSnapshotsAreNotWrittenOnTheCommitPath: hundreds of commits write no snapshot while
// commits keep coming; a graceful close writes them all at head, and the reopen restores them.
func TestIndexSnapshotsAreNotWrittenOnTheCommitPath(t *testing.T) {
	withFlushTiming(t, time.Hour, 0)
	dir := t.TempDir()
	srv, p := openDiskIndexedRuntime(t, dir)
	createFlushTestIndexes(t, p)
	for i := 0; i < 200; i++ {
		flushUpsert(t, srv, i%80, i)
	}
	if n := manifestsAtHead(t, srv, p); n != 0 {
		t.Fatalf("%d snapshots written at head while commits were arriving; want none", n)
	}
	verifyIndexesMatchScan(t, srv, p)
	srv.Release()

	srv2, p2 := openDiskIndexedRuntime(t, dir)
	defer srv2.Release()
	if n := manifestsAtHead(t, srv2, p2); n != flushTestFields {
		t.Fatalf("after a graceful close %d of %d snapshots name head", n, flushTestFields)
	}
	verifyIndexesMatchScan(t, srv2, p2)
}

// TestIndexSnapshotsFlushInTheBackgroundOnceQuiet: with commits stopped, the flusher writes
// every snapshot at head on its own, and a later commit touching nothing rewrites only manifests.
func TestIndexSnapshotsFlushInTheBackgroundOnceQuiet(t *testing.T) {
	withFlushTiming(t, 20*time.Millisecond, 0)
	srv, p := openDiskIndexedRuntime(t, t.TempDir())
	defer srv.Release()
	createFlushTestIndexes(t, p)
	for i := 0; i < 50; i++ {
		flushUpsert(t, srv, i, 0)
	}
	waitForManifestsAtHead(t, srv, p)

	snapshotTimes := func() map[string]time.Time {
		out := map[string]time.Time{}
		for _, d := range p.Registry().Descriptors() {
			fi, err := os.Stat(filepath.Join(index.IndexDir(p.dir, d.IndexID), index.SnapshotFileName))
			if err != nil {
				t.Fatal(err)
			}
			out[d.IndexName()] = fi.ModTime()
		}
		return out
	}
	before := snapshotTimes()
	time.Sleep(20 * time.Millisecond) // mtime granularity
	// A transaction with no operations moves head without touching any store.
	flushCommit(t, srv)
	waitForManifestsAtHead(t, srv, p)
	for name, mt := range snapshotTimes() {
		if !mt.Equal(before[name]) {
			t.Errorf("index %s: snapshot rewritten for a commit that did not change it", name)
		}
	}
}

func waitForManifestsAtHead(t *testing.T, srv *KdbServerRuntime, p *RegistryIndexProvider) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for manifestsAtHead(t, srv, p) != len(p.Registry().Descriptors()) {
		if time.Now().After(deadline) {
			t.Fatalf("snapshots not flushed in the background: %d of %d at head",
				manifestsAtHead(t, srv, p), len(p.Registry().Descriptors()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestIndexPartialFlushRecoversCorrectly simulates kills inside a flush: some stores' snapshot
// and manifest written at the new head, one store's snapshot written but its manifest not, a
// temp file left behind. The reopen must restore the first, rebuild the rest, and answer right.
func TestIndexPartialFlushRecoversCorrectly(t *testing.T) {
	withFlushTiming(t, time.Hour, 0)
	dir := t.TempDir()
	srv, p := openDiskIndexedRuntime(t, dir)
	createFlushTestIndexes(t, p)
	for i := 0; i < 60; i++ {
		flushUpsert(t, srv, i, 0)
	}
	if err := p.Flush(); err != nil { // every store on disk at this head
		t.Fatal(err)
	}
	for i := 0; i < 60; i += 2 {
		flushUpsert(t, srv, i, 1) // values move
	}
	for i := 1; i < 60; i += 9 {
		flushDelete(t, srv, i)
	}

	// The flush the kill interrupts: the first two stores completely, the third's snapshot only.
	p.applyMu.Lock()
	capture, ok, err := p.registry.CaptureSnapshots(nil)
	p.applyMu.Unlock()
	if err != nil || !ok {
		t.Fatalf("capture: ok=%v err=%v", ok, err)
	}
	if _, err := index.WriteSnapshots(p.dir, index.SnapshotCapture{Head: capture.Head, Stores: capture.Stores[:2]}); err != nil {
		t.Fatal(err)
	}
	third := capture.Stores[2]
	thirdDir := index.IndexDir(p.dir, third.Desc.IndexID)
	if err := index.WriteFileAtomic(filepath.Join(thirdDir, index.SnapshotFileName), third.Data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(thirdDir, "."+index.SnapshotFileName+".tmp-killed"), third.Data[:len(third.Data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	if got := manifestsAtHead(t, srv, p); got != 2 {
		t.Fatalf("setup: %d manifests at head, want 2", got)
	}
	p.abandon() // the kill: nothing more is written
	srv.Release()

	srv2, p2 := openDiskIndexedRuntime(t, dir)
	defer srv2.Release()
	verifyIndexesMatchScan(t, srv2, p2)
}

// TestIndexRecreatedAtTheSameHeadIgnoresTheOldSnapshot: DROP INDEX then CREATE INDEX of the
// same name and type on another field, with no commit in between, and a kill before any flush.
// The dropped index's snapshot names the current head; it must not be loaded as the new one.
func TestIndexRecreatedAtTheSameHeadIgnoresTheOldSnapshot(t *testing.T) {
	withFlushTiming(t, time.Hour, 0)
	dir := t.TempDir()
	srv, p := openDiskIndexedRuntime(t, dir)
	createFlushTestIndexes(t, p)
	for i := 0; i < 40; i++ {
		flushUpsert(t, srv, i, 0)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := sql.QueryContext{NamespaceID: "app/data"}
	if err := p.DropIndex(sql.StmtDropIndex{Name: "by_g0"}, ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.CreateIndex(sql.StmtCreateIndex{
		Name: "by_g0", Table: "docs", Fields: []sql.IndexField{{Path: "g3"}}, Using: "HASH",
	}, ctx); err != nil {
		t.Fatal(err)
	}
	p.abandon()
	srv.Release()

	srv2, p2 := openDiskIndexedRuntime(t, dir)
	defer srv2.Release()
	verifyIndexesMatchScan(t, srv2, p2)
}

// TestIndexSnapshotsSurviveKill9 runs a writer in a child process with the flusher writing
// snapshots as eagerly as it can, SIGKILLs it at random moments - mid-commit, mid-flush, while
// idle - and after each kill checks that every index reopens to exactly the documents at head.
// KDB_INDEX_KILL9_ITERATIONS sets the number of kills (default 10).
func TestIndexSnapshotsSurviveKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns and kills a child process repeatedly")
	}
	iterations := 10
	if v, err := strconv.Atoi(os.Getenv("KDB_INDEX_KILL9_ITERATIONS")); err == nil && v > 0 {
		iterations = v
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	restored := 0
	for it := 0; it < iterations; it++ {
		cmd := exec.Command(exe, "-test.run=^TestIndexKill9Child$", "-test.v")
		cmd.Env = append(os.Environ(), "KDB_INDEX_KILL9_CHILD_DIR="+dir, fmt.Sprintf("KDB_INDEX_KILL9_SEED=%d", rng.Int63()))
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(stdout)
			signalled := false
			for sc.Scan() {
				if !signalled && strings.Contains(sc.Text(), "KILL9-CHILD-READY") {
					close(ready)
					signalled = true
				}
			}
			if !signalled {
				close(ready)
			}
		}()
		select {
		case <-ready:
		case <-time.After(60 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatal("child never became ready")
		}
		time.Sleep(time.Duration(50+rng.Intn(1500)) * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()

		rt, err := embed.OpenFileRuntime(dir, "app", "app/data", schema.None())
		if err != nil {
			t.Fatal(err)
		}
		srv := NewKdbServerRuntime(rt)
		atHead := countManifestsAtHeadOnDisk(t, srv)
		p, err := srv.OpenIndexes(stores.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Registry().Descriptors()) > 0 {
			verifyIndexesMatchScan(t, srv, p)
		}
		restored += atHead
		t.Logf("kill %d: %d of %d index snapshots named head and were restored, the rest rebuilt", it+1, atHead, len(p.Registry().Descriptors()))
		// Leave the directory exactly as the kill did for the next child.
		p.abandon()
		srv.Release()
	}
	t.Logf("%d snapshots restored across %d kills", restored, iterations)
}

// countManifestsAtHeadOnDisk reads the index directory as the kill left it, before OpenIndexes
// repairs anything: how many catalogued indexes have a manifest naming the DAG head.
func countManifestsAtHeadOnDisk(t testing.TB, srv *KdbServerRuntime) int {
	t.Helper()
	dir := srv.indexDir()
	cat, err := index.LoadCatalog(dir)
	if err != nil {
		return 0
	}
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range cat.Indexes {
		if m, err := index.ReadManifest(index.IndexDir(dir, d.IndexID)); err == nil && m.HeadCommitHex == head.Hex() {
			n++
		}
	}
	return n
}

// TestIndexKill9Child is TestIndexSnapshotsSurviveKill9's child: it only runs when that test
// starts it. It writes in bursts with quiet spells between them, the flusher writing snapshots
// in every spell, until it is killed.
func TestIndexKill9Child(t *testing.T) {
	dir := os.Getenv("KDB_INDEX_KILL9_CHILD_DIR")
	if dir == "" {
		t.Skip("child of TestIndexSnapshotsSurviveKill9")
	}
	seed, _ := strconv.ParseInt(os.Getenv("KDB_INDEX_KILL9_SEED"), 10, 64)
	rng := rand.New(rand.NewSource(seed))
	indexFlushIdle, indexFlushMinGap, indexFlushBackoff = time.Millisecond, 0, 0
	srv, p := openDiskIndexedRuntime(t, dir)
	if len(p.Registry().Descriptors()) == 0 {
		createFlushTestIndexes(t, p)
	}
	fmt.Println("KILL9-CHILD-READY")
	for v := 0; ; v++ {
		burst := 1 + rng.Intn(40)
		for k := 0; k < burst; k++ {
			i := rng.Intn(120)
			if rng.Intn(10) == 0 {
				flushDelete(t, srv, i)
			} else {
				flushUpsert(t, srv, i, v*100+k)
			}
		}
		// Mostly short pauses, which the flusher's writes outlast; sometimes long enough for a
		// flush to finish, so kills land before, inside and after one.
		pause := time.Duration(rng.Intn(60)) * time.Millisecond
		if rng.Intn(3) == 0 {
			pause = time.Duration(200+rng.Intn(600)) * time.Millisecond
		}
		time.Sleep(pause)
	}
}

func mustNewTxID(t testing.TB) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
