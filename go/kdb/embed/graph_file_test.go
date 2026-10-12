package embed

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

func graphOpts(rebuild int) FileRuntimeOptions {
	o := FileRuntimeOptions{}
	o.Storage.MemoryBudgetBytes = 1 << 20
	o.Storage.Graph = dag.GraphSettings{FileEnabled: true, RebuildCommits: rebuild}
	return o
}

func historyOf(t *testing.T, rt *EmbeddedKdbRuntime) []document.Commit {
	t.Helper()
	d := dagOf(rt)
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := d.WalkWithOperations(head, nil, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	var out []document.Commit
	for _, e := range entries {
		if f, ok := e.(dag.FullEntry); ok {
			out = append(out, f.Commit)
		}
	}
	return out
}

func writeRevs(t *testing.T, rt *EmbeddedKdbRuntime, ns string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		b, _ := json.Marshal(map[string]any{"id": fmt.Sprintf("00000000-0000-4000-8000-%012d", i%7), "rev": i})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
}

// The whole lifecycle through the real runtime: the file appears once the resident tier passes
// the threshold, survives close and reopen, and history - operations included, which the file
// does not carry - reads back identically.
func TestGraphFileLifecycleThroughRuntime(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphfile"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(20))
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 0, 30)
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	d := dagOf(rt)
	if !d.GraphFileActive() {
		t.Fatal("no graph file after passing the rebuild threshold")
	}
	if st := rt.CommitGraphStatus(); st.FileCommits < 30 || st.ResidentCommits != 0 {
		t.Fatalf("after rebuild: %+v", st)
	}
	writeRevs(t, rt, ns, 30, 35) // resident tail on top of the file
	want := historyOf(t, rt)
	if len(want) < 35 {
		t.Fatalf("history has %d commits", len(want))
	}
	rt.Close()

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(20))
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	d2 := dagOf(rt2)
	if !d2.GraphFileActive() {
		t.Fatal("graph file not attached on reopen")
	}
	if st := rt2.CommitGraphStatus(); st.FileCommits < 30 {
		t.Fatalf("reopened graph shows %d commits", st.FileCommits)
	}
	if got := historyOf(t, rt2); !reflect.DeepEqual(got, want) {
		t.Fatalf("history changed across reopen with the file attached: %d vs %d commits", len(got), len(want))
	}
	// Idempotent retry detection still resolves for a commit that now lives in the file.
	oldest := want[len(want)-1]
	if c, ok := d2.GetCommitByTransactionID(oldest.TransactionID); !ok || c.Hash != oldest.Hash {
		t.Fatal("transaction lookup lost for a commit in the file")
	}
	writeRevs(t, rt2, ns, 35, 40)
	if err := rt2.CompactCommitGraph(); err != nil {
		t.Fatal(err)
	}
	if st := rt2.CommitGraphStatus(); st.Segments != 1 || st.ResidentCommits != 0 {
		t.Fatalf("after an explicit compaction: %+v", st)
	}
}

// A damaged file costs a rebuild, never data.
func TestGraphFileDamagedOnDiskStillOpens(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphdamage"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 0, 15)
	rt.Maintain()
	want := historyOf(t, rt)
	if len(want) < 15 {
		t.Fatalf("history has %d commits", len(want))
	}
	rt.Close()

	segs, _ := filepath.Glob(filepath.Join(graphDirPath(root, ns), "graph-*.kdbg"))
	if len(segs) == 0 {
		t.Fatal("no graph segment written")
	}
	path := segs[len(segs)-1]
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/3] ^= 0x5A
	os.WriteFile(path, raw, 0o644)

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if got := historyOf(t, rt2); !reflect.DeepEqual(got, want) {
		t.Fatal("history changed after opening over a damaged graph file")
	}
	// Rebuilt at open: the DAG holds more than the threshold resident.
	if !dagOf(rt2).GraphFileActive() {
		t.Fatal("damaged file was not replaced by a rebuild")
	}
}

// Turning the feature off ignores a file left behind.
func TestGraphFileIgnoredWhenDisabled(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphoff"
	rt, _ := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(5))
	writeRevs(t, rt, ns, 0, 10)
	rt.Maintain()
	want := historyOf(t, rt)
	if len(want) < 10 {
		t.Fatalf("history has %d commits", len(want))
	}
	rt.Close()

	off := FileRuntimeOptions{}
	off.Storage.MemoryBudgetBytes = 1 << 20
	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), off)
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if dagOf(rt2).GraphFileActive() {
		t.Fatal("file attached with the feature off")
	}
	if got := historyOf(t, rt2); !reflect.DeepEqual(got, want) {
		t.Fatal("history differs with the feature off")
	}
	_ = codec.Hash{}
}

func skipIfNoMmap(t *testing.T) {
	t.Helper()
	if !mmapSupported() {
		t.Skip("no mmap on this platform")
	}
}

func mmapSupported() bool {
	f, err := os.CreateTemp("", "mmapprobe")
	if err != nil {
		return false
	}
	defer os.Remove(f.Name())
	f.Write([]byte{1})
	f.Close()
	m, err := mmapfile.Open(f.Name())
	if err != nil {
		return false
	}
	m.Close()
	return true
}
