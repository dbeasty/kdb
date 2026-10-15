package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

const largeCommitNS = "app/data"

func openLargeCommitRuntime(t *testing.T, dir string, threshold int64) (*KdbServerRuntime, *engine.ServerEngine) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DocumentCacheBytes = 1 << 20
	opts.Storage.BoundedLiveBodies = true
	opts.Storage.HistoryStrategy = storage.HistoryStrategyObjects
	opts.Storage.LargeCommitBodyBytes = threshold
	rt, err := embed.OpenFileRuntimeWithOptions(dir, "app", largeCommitNS, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skip("not a server engine")
	}
	return NewKdbServerRuntime(rt), eng
}

// importCommit writes n documents of about bodyBytes each in one commit, as a migration's bulk
// import does, and returns their ids and bodies.
func importCommit(t *testing.T, srv *KdbServerRuntime, n, bodyBytes int) ([]codec.UUID, map[codec.UUID]string) {
	t.Helper()
	base, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]codec.UUID, n)
	bodies := map[codec.UUID]string{}
	ops := make([]document.Op, n)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
		bodies[ids[i]] = fmt.Sprintf(`{"n":%d,"pad":%q}`, i, strings.Repeat(string(rune('a'+i%26)), bodyBytes))
		ops[i] = document.WriteOp{DocID: ids[i], Patch: bodies[ids[i]], Replace: true}
	}
	tx := document.Transaction{ID: mustRandomUUID(t), BaseVersion: base, Operations: ops, Timestamp: codec.TimestampNow()}
	if _, err := srv.Commit(largeCommitNS, tx, "import", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	return ids, bodies
}

// readAll reads every id at head, checks its body, and returns how many frames the reads had to
// decompress (frame-cache misses).
func readAll(t *testing.T, srv *KdbServerRuntime, eng *engine.ServerEngine, ids []codec.UUID, bodies map[codec.UUID]string) int64 {
	t.Helper()
	tree, ok := srv.headTreeHash()
	if !ok {
		t.Fatal("no head")
	}
	before := eng.DeltaFrameCache().Stats().Misses
	for i, id := range ids {
		got, err := srv.Runtime.Storage.GetDocument(largeCommitNS, id, tree)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.JSON != bodies[id] {
			t.Fatalf("document %d read back wrong (nil=%v)", i, got == nil)
		}
	}
	return eng.DeltaFrameCache().Stats().Misses - before
}

// U4: the documents of a bulk-write commit are read back one at a time without decompressing the
// commit's frame for each - they were filed by value when the commit was logged - including after
// a reopen.
func TestLargeCommitBodiesAreReadWithoutTheirFrame(t *testing.T) {
	dir := t.TempDir()
	srv, eng := openLargeCommitRuntime(t, dir, 0)
	ids, bodies := importCommit(t, srv, 300, 4000) // ~1.2 MB in one commit: far past 64 KiB
	if got := eng.LargeCommitBodiesFiled(); got != int64(len(ids)) {
		t.Fatalf("%d bodies filed by value, want %d", got, len(ids))
	}
	if misses := readAll(t, srv, eng, ids, bodies); misses > 2 {
		t.Fatalf("reading %d documents of one import decompressed %d frames, want ~0", len(ids), misses)
	}
	srv.Runtime.Close()

	srv, eng = openLargeCommitRuntime(t, dir, 0)
	defer srv.Runtime.Close()
	if misses := readAll(t, srv, eng, ids, bodies); misses > 2 {
		t.Fatalf("after reopen, reading %d documents decompressed %d frames, want ~0", len(ids), misses)
	}
}

// A namespace written before bodies were filed by value (threshold off: location records only)
// gets them re-filed by the first cold read that meets the large frame: the frame is decompressed
// once for all its documents, not once per document.
func TestLargeFrameIsDecompressedOnceForAllItsDocuments(t *testing.T) {
	dir := t.TempDir()
	srv, eng := openLargeCommitRuntime(t, dir, -1)
	ids, bodies := importCommit(t, srv, 300, 4000)
	if got := eng.LargeCommitBodiesFiled(); got != 0 {
		t.Fatalf("threshold off, yet %d bodies filed by value", got)
	}
	srv.Runtime.Close()

	srv, eng = openLargeCommitRuntime(t, dir, 0)
	defer srv.Runtime.Close()
	if misses := readAll(t, srv, eng, ids, bodies); misses > 3 {
		t.Fatalf("first pass over %d documents of one large frame decompressed it %d times, want ~1", len(ids), misses)
	}
	if eng.LargeFrameBodiesRefiled() < int64(len(ids)) {
		t.Fatalf("%d versions re-filed by value, want at least %d", eng.LargeFrameBodiesRefiled(), len(ids))
	}
	if misses := readAll(t, srv, eng, ids, bodies); misses > 0 {
		t.Fatalf("second pass decompressed %d frames, want 0", misses)
	}
}

// Small commits keep filing locations: one document's frame is cheap to decompress, and a
// second copy of every ordinary write would double the disk the log already pays for.
func TestSmallCommitsKeepLocationRecords(t *testing.T) {
	dir := t.TempDir()
	srv, eng := openLargeCommitRuntime(t, dir, 0)
	defer srv.Runtime.Close()
	importCommit(t, srv, 3, 2000)
	if got := eng.LargeCommitBodiesFiled(); got != 0 {
		t.Fatalf("a 6 KB commit filed %d bodies by value", got)
	}
}
