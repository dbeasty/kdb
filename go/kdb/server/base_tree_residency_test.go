package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// tinyTreeCacheServer opens a disk-backed runtime whose historical-tree cache holds
// approximately one tree, so any tree that stops being the live one is evicted almost
// immediately. That is the condition the fix has to survive; a memory-backed runtime
// evicts nothing and cannot show this at all.
func tinyTreeCacheServer(t *testing.T, ns string) (*KdbServerRuntime, *engine.ServerEngine) {
	t.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(
		t.TempDir(), "app", ns, schema.None(),
		embed.FileRuntimeOptions{Storage: embed.StorageOptions{
			HistoryTreeCacheBytes: 8 << 10,
			Durability:            storage.DurabilityMemoryOnly,
		}},
	)
	if err != nil {
		t.Fatalf("OpenFileRuntimeWithOptions: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skipf("not the server engine, got %T", rt.Storage)
	}
	srv := NewKdbServerRuntime(rt)
	srv.SetWriteQueueCapacityForTest(4096)
	return srv, eng
}

// TestConcurrentWritersDoNotRebuildTheirBaseTree is the regression test for the second half
// of the write collapse in docs/benchmarks/2026-09-07-perf-rerun-7947cce.md.
//
// Concurrent writers each resolve a base version and then queue behind a capacity-1 write
// gate. Every writer ahead in the queue publishes a newer tree, so by the time a writer runs,
// its own base tree has been evicted from the bounded history cache by its competitors - and
// the schema phase then rebuilds it from tree objects, on the commit path, once per operation.
// That rebuild was 40% of commit CPU and is what pushed the queue past DefaultWriteTimeout.
//
// Asserted as a rebuild count rather than a duration: the defect is "the commit path resolves
// a tree it should have been holding", which is a property, where a timing threshold would
// only be a guess about the machine.
func TestConcurrentWritersDoNotRebuildTheirBaseTree(t *testing.T) {
	const ns = "app/data"
	const writers = 32
	srv, eng := tinyTreeCacheServer(t, ns)

	// Enough documents that a tree is worth more than the cache holds, so eviction is real
	// and the rebuild it forces is the expensive kind.
	for i := 0; i < 64; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"seed":%d}`, i), auth.Principal{}); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}

	before := eng.HistoryTreeRebuilds()

	// Each writer targets a document nothing else touches, so a stale base version produces no
	// conflict and what is measured is only the cost of resolving it.
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			docID, err := codec.RandomUUID()
			if err != nil {
				errs <- err
				return
			}
			<-start
			base, err := srv.Runtime.DAG.Head()
			if err != nil {
				errs <- err
				return
			}
			tx := document.Transaction{
				ID:          mustRandomUUID(t),
				BaseVersion: base,
				Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: fmt.Sprintf(`{"w":%d}`, w)}},
				Timestamp:   codec.TimestampNow(),
			}
			if _, err := srv.Commit(ns, tx, fmt.Sprintf("sess-%d", w), auth.Principal{}); err != nil {
				errs <- fmt.Errorf("writer %d: %w", w, err)
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent commit failed: %v", err)
	}

	rebuilds := eng.HistoryTreeRebuilds() - before
	t.Logf("%d concurrent writers caused %d historical tree rebuilds", writers, rebuilds)

	// With the base tree pinned for the life of the transaction a writer resolves its base
	// from the cache every time, so the expected count is zero; the allowance covers a writer
	// whose base was already gone when it took its pin, which is legal and self-correcting.
	// Without the pin this is one or more rebuilds per writer.
	if limit := int64(writers / 4); rebuilds > limit {
		t.Fatalf("%d writers caused %d base-tree rebuilds (limit %d); "+
			"queued writers are resolving base versions that were evicted while they waited - "+
			"see KdbServerRuntime.runTransaction's PinTree call", writers, rebuilds, limit)
	}
}
