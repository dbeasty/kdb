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
	if !mmapfile.Supported {
		return false
	}
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

// With segments attached, the checkpoint names them and carries only the resident tier; a reopen
// maps them and restores the rest, with history - operations included - unchanged.
func TestGraphCheckpointReferencesSegments(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphcp"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(20))
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 0, 60)
	if _, err := rt.Maintain(); err != nil { // checkpoint, then the first graph write
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 60, 65)
	want := historyOf(t, rt)
	rt.Close() // writes a checkpoint that names the segment

	cp, ok := readCheckpoint(rt.shim, ns)
	if !ok {
		t.Fatal("no checkpoint after close")
	}
	if len(cp.GraphRefs) == 0 {
		t.Fatal("checkpoint does not reference the graph segments")
	}
	if n := len(cp.State.Commits); n > 10 {
		t.Fatalf("checkpoint carries %d commits; it should carry only the resident tier", n)
	}

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(20))
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if rt2.Recovery().ReplayedInFull {
		t.Fatal("reopen replayed the log instead of restoring from the checkpoint")
	}
	if got := historyOf(t, rt2); !reflect.DeepEqual(got, want) {
		t.Fatalf("history differs after a graph-referencing restore: %d vs %d commits", len(got), len(want))
	}
	writeRevs(t, rt2, ns, 65, 70)
}

// A segment the checkpoint on disk names must survive a full rewrite that supersedes it, until a
// newer checkpoint stops naming it - otherwise a crash in between leaves a checkpoint whose
// segments are gone.
func TestGraphCheckpointPinsItsSegments(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphpin"
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 0, 20)
	rt.Maintain() // graph written
	rt.Close()    // the checkpoint written on close names it
	cp, _ := readCheckpoint(rt.shim, ns)
	if len(cp.GraphRefs) == 0 {
		t.Fatal("checkpoint does not name a segment")
	}
	named := filepath.Join(graphDirPath(root, ns), fmt.Sprintf("graph-%016d.kdbg", cp.GraphRefs[0].Seq))

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt2, ns, 20, 25)
	if err := rt2.CompactCommitGraph(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(named); err != nil {
		t.Fatal("a full rewrite deleted the segment the checkpoint on disk names")
	}
	rt2.Close() // new checkpoint names the new segment; the old one may go
	if _, err := os.Stat(named); !os.IsNotExist(err) {
		t.Fatalf("superseded segment still on disk after a newer checkpoint (%v)", err)
	}
}

// A checkpoint whose named segment has gone is unusable; open replays the log and loses nothing.
func TestGraphCheckpointMissingSegmentReplays(t *testing.T) {
	skipIfNoMmap(t)
	root := t.TempDir()
	const ns = "test/graphmissing"
	rt, _ := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	writeRevs(t, rt, ns, 0, 25)
	rt.Maintain()
	want := historyOf(t, rt)
	rt.Close()
	os.RemoveAll(graphDirPath(root, ns))

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), graphOpts(10))
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if !rt2.Recovery().ReplayedInFull {
		t.Fatal("open trusted a checkpoint whose segments are gone")
	}
	if got := historyOf(t, rt2); !reflect.DeepEqual(got, want) {
		t.Fatal("history lost when the named segments were missing")
	}
}

// A checkpoint stamps the segments it fingerprints, so an open over an unchanged log can trust
// them without reading the log; a segment changed in place loses that and is compared in full.
func TestCheckpointStampsLetOpenSkipTheScan(t *testing.T) {
	root := t.TempDir()
	const ns = "test/stamps"
	opts := FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.DeltaMaxSegmentBytes = 4 << 10 // several segments
	rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	writeRevs(t, rt, ns, 0, 80)
	rt.Close()

	cp, ok := readCheckpoint(rt.shim, ns)
	if !ok || len(cp.Segments) < 2 {
		t.Fatalf("want a checkpoint over several segments, have %d", len(cp.Segments))
	}
	for _, sg := range cp.Segments {
		if !sg.HasStamp {
			t.Fatalf("segment %d fingerprinted without a stamp", sg.Sequence)
		}
	}

	rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !unchangedSinceCheckpoint(cp, rt2.deltaReader) {
		t.Fatal("an untouched log was not recognised as unchanged")
	}
	rt2.Close()

	// Touch one covered segment in place: same bytes, new mtime. Unchanged must now say no, and
	// the full comparison - which still matches, since the bytes are the same - takes over.
	cp, _ = readCheckpoint(rt2.shim, ns)
	seg := filepath.Join(root, "ns", ns, "delta", fmt.Sprintf("%020d.seg", cp.Segments[0].Sequence))
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	rt3, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt3.Close()
	if unchangedSinceCheckpoint(cp, rt3.deltaReader) {
		t.Fatal("a rewritten segment was trusted on its stamp")
	}
	if ok, why := checkpointMatchesLog(cp, rt3.deltaReader); !ok {
		t.Fatalf("the full comparison rejected identical bytes: %s", why)
	}
	if rt3.Recovery().ReplayedInFull {
		t.Fatal("identical bytes under a new mtime forced a full replay")
	}
}

// checkpointVersionOf reads the format version a checkpoint was written as.
func checkpointVersionOf(t *testing.T, raw []byte) int32 {
	t.Helper()
	v, err := codec.DecodeBytes(raw, checkpointType, checkpointRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return v.(codec.RecordValue).Fields[1].(codec.Int32Value).V
}

func testCheckpointCommits(t *testing.T, n int) []dag.CheckpointCommit {
	t.Helper()
	out := make([]dag.CheckpointCommit, n)
	var prev codec.Hash
	for i := range out {
		var h codec.Hash
		h.Bytes[0], h.Bytes[1], h.Bytes[2] = 0xc0, byte(i>>8), byte(i)
		c := document.Commit{NamespaceID: "ns", Hash: h, Message: fmt.Sprintf("c%d", i),
			Timestamp: codec.TimestampFromEpochMicros(int64(1_700_000_000_000_000 + i))}
		if i > 0 {
			c.ParentHashes = []codec.Hash{prev}
		}
		out[i], prev = dag.CheckpointCommit{Commit: c, OperationCount: i % 3}, h
	}
	return out
}

// A namespace up to legacyCheckpointMaxEntries is checkpointed as version 2 - the records-only
// format v0.6.x reads, so a v0.6.2 binary opening a cleanly stopped data dir has no log to replay
// (its replay loses writes: test_upgrade.py::test_rollback_to_v062_after_a_clean_stop) - and it
// round-trips through this build, commits handed over packed included. Past the bound, or with
// graph segments, it is version 4.
func TestCheckpointFormatFollowsNamespaceSize(t *testing.T) {
	entries := map[codec.UUID]codec.Hash{}
	for i := 0; i < 50; i++ {
		id, _ := codec.RandomUUID()
		var h codec.Hash
		h.Bytes[1] = byte(i)
		entries[id] = h
	}
	tree, _ := document.BuildDocumentTree(entries)
	commits := testCheckpointCommits(t, 40)
	packed, err := dag.PackCheckpointCommits(commits[:30])
	if err != nil {
		t.Fatal(err)
	}
	cp := namespaceCheckpoint{NamespaceID: "ns", ThroughSequence: 3, LiveTree: tree,
		State: dag.CheckpointState{PackedCommits: packed, Commits: commits[30:]}}

	raw, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if v := checkpointVersionOf(t, raw); v != checkpointFormatVersion {
		t.Fatalf("a %d-document, %d-commit namespace was checkpointed as version %d, want %d (v0.6.x reads only that)",
			tree.Size(), len(commits), v, checkpointFormatVersion)
	}
	root, _ := codec.DecodeBytes(raw, checkpointType, checkpointRegistry())
	for _, f := range []int{13, 14} {
		if b, ok := root.(codec.RecordValue).Fields[f].(codec.BytesValue); ok && len(b.V) > 0 {
			t.Fatalf("version 2 checkpoint carries field %d, which v0.6.x does not know", f)
		}
	}
	got, err := decodeCheckpoint("ns", raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.LiveTree.TreeHash != tree.TreeHash || got.LiveTree.Size() != 50 {
		t.Fatal("live tree did not round-trip through version 2")
	}
	if len(got.State.PackedCommits) != 0 || len(got.State.Commits) != len(commits) {
		t.Fatalf("version 2 restored %d commits (%d packed bytes), want %d", len(got.State.Commits), len(got.State.PackedCommits), len(commits))
	}
	for i, want := range commits {
		g := got.State.Commits[i]
		if g.Commit.Hash != want.Commit.Hash || g.Commit.Message != want.Commit.Message ||
			g.Commit.Timestamp != want.Commit.Timestamp || g.OperationCount != want.OperationCount ||
			fmt.Sprint(g.Commit.ParentHashes) != fmt.Sprint(want.Commit.ParentHashes) {
			t.Fatalf("commit %d did not round-trip through version 2:\n got %+v\nwant %+v", i, g, want)
		}
	}

	// Past the bound: packed, version 4, and still round-trips.
	big := testCheckpointCommits(t, legacyCheckpointMaxEntries)
	bigPacked, err := dag.PackCheckpointCommits(big)
	if err != nil {
		t.Fatal(err)
	}
	cpBig := namespaceCheckpoint{NamespaceID: "ns", LiveTree: tree, State: dag.CheckpointState{PackedCommits: bigPacked}}
	raw, err = encodeCheckpoint(cpBig)
	if err != nil {
		t.Fatal(err)
	}
	if v := checkpointVersionOf(t, raw); v != checkpointPackedFormatVersion {
		t.Fatalf("a namespace past legacyCheckpointMaxEntries was checkpointed as version %d, want %d", v, checkpointPackedFormatVersion)
	}
	got, err = decodeCheckpoint("ns", raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.LiveTree.TreeHash != tree.TreeHash || string(got.State.PackedCommits) != string(bigPacked) {
		t.Fatal("version 4 checkpoint did not round-trip")
	}

	// Graph segments: version 2 cannot name them.
	cpGraph := cp
	cpGraph.GraphRefs = []dag.GraphSegmentRef{{Seq: 1, CRC: 7, Records: 3}}
	raw, err = encodeCheckpoint(cpGraph)
	if err != nil {
		t.Fatal(err)
	}
	if v := checkpointVersionOf(t, raw); v != checkpointPackedFormatVersion {
		t.Fatalf("a checkpoint naming graph segments was written as version %d, want %d", v, checkpointPackedFormatVersion)
	}
}
