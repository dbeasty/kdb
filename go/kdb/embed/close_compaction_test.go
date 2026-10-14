package embed_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

func closeCompactionRuntime(t *testing.T, root string, closeBytes int64, background, readOnly bool) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{ReadOnly: readOnly}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeFull
	opts.Storage.CloseCompactionBytes = closeBytes
	opts.Storage.DisableBackgroundCompaction = !background
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func blobEngine(t *testing.T, rt *embed.EmbeddedKdbRuntime) *engine.ServerEngine {
	t.Helper()
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Fatalf("storage is %T", rt.Storage)
	}
	return eng
}

// rewriteSessions runs n open/write/close cycles, each rewriting the same documents, so every
// close flushes a table and every table after the first holds versions the next one supersedes.
func rewriteSessions(t *testing.T, root string, n int, closeBytes int64) (ids []codec.UUID, latest map[codec.UUID]string) {
	t.Helper()
	latest = map[codec.UUID]string{}
	for s := 0; s < n; s++ {
		rt := closeCompactionRuntime(t, root, closeBytes, false, false)
		for d := 0; d < 20; d++ {
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", d)
			body := fmt.Sprintf(`{"id":%q,"session":%d}`, id, s)
			res, err := embed.PutJSONDocument(rt, "app/docs", body)
			if err != nil {
				t.Fatal(err)
			}
			if s == 0 {
				ids = append(ids, res.DocID)
			}
			latest[res.DocID] = body
		}
		rt.Close()
	}
	return ids, latest
}

func requireLatest(t *testing.T, rt *embed.EmbeddedKdbRuntime, latest map[codec.UUID]string, where string) {
	t.Helper()
	tree := headTree(t, rt)
	for id, want := range latest {
		d, err := rt.Storage.GetDocument("app/docs", id, tree)
		if err != nil || d == nil {
			t.Fatalf("%s: %s: %v, found=%v", where, id, err, d != nil)
		}
		if got := d.JSON; got != want {
			t.Fatalf("%s: %s reads %s, want %s", where, id, got, want)
		}
	}
}

// A store over the close limit is left as it is by close - every version still readable - and
// merged by the next maintenance pass.
func TestCloseLeavesAStoreOverTheLimitToMaintenance(t *testing.T) {
	root := t.TempDir()
	_, latest := rewriteSessions(t, root, 6, 1) // 1 byte: close never merges

	rt := closeCompactionRuntime(t, root, 1, false, false)
	eng := blobEngine(t, rt)
	if n := eng.BlobTableCount(); n < 6 {
		t.Fatalf("%d tables after 6 sessions whose closes were over the limit, want at least 6", n)
	}
	requireLatest(t, rt, latest, "reopened with tables unmerged")

	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if n := eng.BlobTableCount(); n != 1 {
		t.Fatalf("%d tables after maintenance, want 1 (%+v)", n, res)
	}
	requireLatest(t, rt, latest, "after maintenance")
	rt.Close()
}

// Under the default limit a small namespace is still merged on every close, as it always was:
// the table count stays below the trigger however many sessions it has.
func TestCloseStillMergesASmallStore(t *testing.T) {
	root := t.TempDir()
	_, latest := rewriteSessions(t, root, 8, 0)

	rt := closeCompactionRuntime(t, root, 0, false, false)
	defer rt.Close()
	if n := blobEngine(t, rt).BlobTableCount(); n >= 4 {
		t.Fatalf("%d tables after 8 sessions under the default close limit; close should have merged them", n)
	}
	requireLatest(t, rt, latest, "reopened")
}

// A writable open that finds a backlog merges it in the background; a read-only one never does.
func TestOpenMergesABacklogInTheBackground(t *testing.T) {
	root := t.TempDir()
	_, latest := rewriteSessions(t, root, 6, 1)

	reader := closeCompactionRuntime(t, root, 0, true, true)
	before := blobEngine(t, reader).BlobTableCount()
	reader.Close()
	tables, _ := sstableSidecarFiles(t, root, "app/docs")
	if len(tables) != before {
		t.Fatalf("a read-only open changed the tables on disk: %d registered, %d after", before, len(tables))
	}

	rt := closeCompactionRuntime(t, root, 1, true, false)
	defer rt.Close()
	eng := blobEngine(t, rt)
	deadline := time.Now().Add(30 * time.Second)
	for eng.BlobTableCount() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("still %d tables 30s after open; the background merge did not run", eng.BlobTableCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	requireLatest(t, rt, latest, "after the background merge")
}

// Closing straight after an open that started a background merge returns, and leaves a store
// that opens with every document - whether the merge finished first or was cancelled.
func TestCloseDuringABackgroundMerge(t *testing.T) {
	root := t.TempDir()
	_, latest := rewriteSessions(t, root, 6, 1)
	for i := 0; i < 3; i++ {
		rt := closeCompactionRuntime(t, root, 1, true, false)
		rt.Close()
	}
	rt := closeCompactionRuntime(t, root, 1, false, false)
	defer rt.Close()
	requireLatest(t, rt, latest, "after closes that raced a background merge")
}
