package embed

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

func treeFileOpts(resident int64) FileRuntimeOptions {
	o := FileRuntimeOptions{}
	o.Storage.TreeFile = true
	o.Storage.TreeResidentBytes = resident
	return o
}

func treeSegments(t *testing.T, root, ns string) []string {
	t.Helper()
	ents, _ := os.ReadDir(treeDirPath(root, ns))
	var out []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".kdbt") {
			out = append(out, e.Name())
		}
	}
	return out
}

func newest(names []string) int {
	n := 0
	for _, s := range names {
		if v, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(s, "tree-"), ".kdbt")); err == nil && v > n {
			n = v
		}
	}
	return n
}

func treeDocID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }

// The live tree through the real runtime with tree.file on: written, checkpointed at close,
// reopened by mapping the segment the checkpoint tagged (no build), every document reading back
// as written - and the same data reopened with tree.file off reads the same.
func TestTreeFileLifecycleThroughRuntime(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/treefile"
	const docs = 3000
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), treeFileOpts(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < docs+1000; i++ { // the last 1000 update earlier documents
		b, _ := json.Marshal(map[string]any{"id": treeDocID(i % docs), "rev": i})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	eng := rt.Storage.(*engine.ServerEngine)
	live := eng.LiveTree()
	if bound, _ := live.Paged(); !bound {
		t.Fatal("the live tree is not bound to the tree file")
	}
	wantHash := live.TreeHash
	rt.Close()
	segs := treeSegments(t, root, ns)
	if len(segs) == 0 {
		t.Fatal("no tree segment after close")
	}

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), treeFileOpts(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	live2 := rt2.Storage.(*engine.ServerEngine).LiveTree()
	if _, mapped := live2.Paged(); !mapped || live2.TreeHash != wantHash {
		t.Fatalf("reopen: mapped=%v, hash matches=%v", mapped, live2.TreeHash == wantHash)
	}
	// The open maps the checkpoint's chain - writing nothing - and deletes the earlier run's other
	// segments, leaving one chain of at most the default maximum length.
	got := treeSegments(t, root, ns)
	if newest(got) != newest(segs) {
		t.Fatalf("reopen wrote a segment instead of mapping the checkpoint's: newest %d -> %d", newest(segs), newest(got))
	}
	if len(got) > document.DefaultTreeMaxSegments {
		t.Fatalf("%d segment files left after reopen: %v", len(got), got)
	}
	head := headTree(t, rt2)
	for i := 0; i < docs; i++ {
		id, _ := codec.UUIDFromString(treeDocID(i))
		d, err := rt2.Storage.GetDocument(ns, id, head)
		if err != nil || d == nil {
			t.Fatalf("doc %d: %v found=%v", i, err, d != nil)
		}
	}
	// More writes on the mapped tree, then a reopen with the feature off reads them all.
	for i := 0; i < 200; i++ {
		b, _ := json.Marshal(map[string]any{"id": treeDocID(docs + i), "rev": i})
		if _, err := PutJSONDocument(rt2, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	wantHash = rt2.Storage.(*engine.ServerEngine).LiveTree().TreeHash
	rt2.Close()

	rt3, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rt3.Close()
	live3 := rt3.Storage.(*engine.ServerEngine).LiveTree()
	if bound, _ := live3.Paged(); bound || live3.TreeHash != wantHash || live3.Size() != docs+200 {
		t.Fatalf("reopen without tree.file: bound=%v size=%d hash matches=%v", bound, live3.Size(), live3.TreeHash == wantHash)
	}
}

// A damaged segment is never trusted: the open rebuilds from the checkpoint and the tree is the
// same.
func TestTreeFileDamagedSegmentRebuilds(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/treefile-damage"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), treeFileOpts(0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		b, _ := json.Marshal(map[string]any{"id": treeDocID(i), "rev": i})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	want := rt.Storage.(*engine.ServerEngine).LiveTree().TreeHash
	rt.Close()
	for _, s := range treeSegments(t, root, ns) {
		p := filepath.Join(treeDirPath(root, ns), s)
		b, _ := os.ReadFile(p)
		b[len(b)/2] ^= 0xff
		os.WriteFile(p, b, 0o644)
	}
	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), treeFileOpts(0))
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if got := rt2.Storage.(*engine.ServerEngine).LiveTree().TreeHash; got != want {
		t.Fatal("tree differs after reopening over a damaged segment")
	}
}

// A read-only runtime maps the segment the writer's checkpoint tagged, and writes or deletes
// nothing in the tree directory.
func TestTreeFileReadOnlyMapsWithoutWriting(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/treefile-ro"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), treeFileOpts(0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 800; i++ {
		b, _ := json.Marshal(map[string]any{"id": treeDocID(i), "rev": i})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	want := rt.Storage.(*engine.ServerEngine).LiveTree().TreeHash
	rt.Close()
	before := treeSegments(t, root, ns)

	opts := treeFileOpts(0)
	opts.ReadOnly = true
	ro, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	live := ro.Storage.(*engine.ServerEngine).LiveTree()
	if _, mapped := live.Paged(); !mapped || live.TreeHash != want {
		t.Fatalf("read-only open: mapped=%v, hash matches=%v", mapped, live.TreeHash == want)
	}
	ro.Close()
	if after := treeSegments(t, root, ns); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("a read-only runtime changed the tree directory: %v -> %v", before, after)
	}
}

// The engine's history trees are never updated, so on their own they would keep every segment
// they refer into mapped and on disk. The engine rebases them whenever a segment lands, so
// through many compactions only the newest chain stays - while every historical tree the store
// holds still reads as it did.
func TestTreeFileHistoryDoesNotPinSegments(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/treefile-history"
	opts := treeFileOpts(64 << 10)
	opts.Storage.TreeMaxSegments = 2
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	eng := rt.Storage.(*engine.ServerEngine)
	for i := 0; i < 3000; i++ {
		b, _ := json.Marshal(map[string]any{"id": treeDocID(i % 1500), "rev": i})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	// Let the last write-out and its rebase land, and the collector release what they freed.
	deadline := time.Now().Add(10 * time.Second)
	files := treeSegments(t, root, ns)
	for time.Now().Before(deadline) && len(files) > document.DefaultTreeMaxSegments {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		files = treeSegments(t, root, ns)
	}
	for i := 0; i < 20; i++ { // and settle
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	files = treeSegments(t, root, ns)
	t.Logf("%d segment files on disk with the namespace open: %v; history trees held: %d", len(files), files, eng.HistoryTreesResident())
	if len(files) > 2*opts.Storage.TreeMaxSegments+1 {
		t.Fatalf("%d segment files on disk: history is pinning superseded segments", len(files))
	}
	// Every historical tree still held reads exactly its documents.
	for _, tr := range eng.HistoryTrees() {
		n := 0
		tr.Walk(func(codec.UUID, codec.Hash) bool { n++; return true })
		if n != tr.Size() {
			t.Fatalf("a held tree walks %d entries, sized %d", n, tr.Size())
		}
	}
}

// A closed namespace with a paged tree leaves nothing behind in the heap: no path from the
// tree file's callbacks or the segments' cleanups keeps the engine or the tree alive.
func TestTreeFileClosedNamespaceIsCollected(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/treefile-closed"
	opts := treeFileOpts(64 << 10)
	opts.Storage.TreeMaxSegments = 2
	settle := func() uint64 {
		var ms runtime.MemStats
		for i := 0; i < 10; i++ {
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
		}
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}
	before := settle()
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		b, _ := json.Marshal(map[string]any{"id": treeDocID(i % 1000), "rev": i, "pad": strings.Repeat("x", 200)})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	open := settle()
	rt.Close()
	after := settle()
	t.Logf("heap: %.1f MB before, %.1f MB open, %.1f MB after close", float64(before)/1e6, float64(open)/1e6, float64(after)/1e6)
	if int64(after)-int64(before) > (int64(open)-int64(before))/4 {
		t.Fatalf("a closed namespace keeps %.1f MB of the %.1f MB it held", float64(int64(after)-int64(before))/1e6, float64(int64(open)-int64(before))/1e6)
	}
}
