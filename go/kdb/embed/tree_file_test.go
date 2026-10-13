package embed

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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
