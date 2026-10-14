package embed_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

const boundedNS = "app/bounded"

func boundedRuntime(tb testing.TB, root string, bounded bool, mode storage.HistoryMode) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DocumentCacheBytes = 1 << 20
	opts.Storage.BoundedLiveBodies = bounded
	opts.Storage.HistoryMode = mode
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", boundedNS, schema.None(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

// writeLiveSet writes n distinct documents of about bodyBytes each and returns their ids and the
// exact JSON written.
func writeLiveSet(tb testing.TB, rt *embed.EmbeddedKdbRuntime, n, bodyBytes int) ([]codec.UUID, map[codec.UUID]string) {
	tb.Helper()
	ids := make([]codec.UUID, 0, n)
	texts := map[codec.UUID]string{}
	for i := 0; i < n; i++ {
		docID, _ := codec.RandomUUID()
		b, _ := json.Marshal(map[string]any{"id": docID.String(), "n": i, "pad": strings.Repeat(string(rune('a'+i%26)), bodyBytes)})
		res, err := embed.PutJSONDocument(rt, boundedNS, string(b))
		if err != nil {
			tb.Fatal(err)
		}
		id, _, err := document.ResolveID(string(b))
		if err != nil {
			tb.Fatal(err)
		}
		_ = res
		ids = append(ids, id)
		texts[id] = string(b)
	}
	return ids, texts
}

func headTree(tb testing.TB, rt *embed.EmbeddedKdbRuntime) codec.Hash {
	tb.Helper()
	h, err := rt.DAG.Head()
	if err != nil {
		tb.Fatal(err)
	}
	c, err := rt.DAG.GetCommitOrThrow(h)
	if err != nil {
		tb.Fatal(err)
	}
	return c.DocumentTreeHash
}

func requireAllLive(tb testing.TB, rt *embed.EmbeddedKdbRuntime, ids []codec.UUID, texts map[codec.UUID]string, where string) {
	tb.Helper()
	tree := headTree(tb, rt)
	for i, id := range ids {
		got, err := rt.Storage.GetDocument(boundedNS, id, tree)
		if err != nil {
			tb.Fatalf("%s: document %d: %v", where, i, err)
		}
		if got == nil {
			tb.Fatalf("%s: live document %d not found - an evicted live version was not read back", where, i)
		}
		if got.JSON != texts[id] {
			tb.Fatalf("%s: document %d read back the wrong content", where, i)
		}
	}
}

// Live data far larger than the document cache: with bounded live bodies the version store stays
// near its budget - which, at 64 shards of at least 64 KiB each, cannot go below 4 MiB - every live document still reads back exactly, including after a reopen, and the
// cold path is what served them. Without the flag the store holds every live body, as before.
func TestBoundedLiveBodiesServeLiveDataLargerThanTheCache(t *testing.T) {
	if testing.Short() {
		t.Skip("heap measurement; runs nightly, or without -short")
	}
	for _, mode := range []storage.HistoryMode{storage.HistoryModeFull, storage.HistoryModeNone} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			root := t.TempDir()
			rt := boundedRuntime(t, root, true, mode)
			ids, texts := writeLiveSet(t, rt, 2400, 5000) // ~12 MB of live bodies, twice the 6 MiB ceiling; a 1 MiB cache, floored at 4 MiB
			eng, ok := rt.Storage.(*engine.ServerEngine)
			if !ok {
				t.Skip("not a server engine")
			}
			if n := eng.PendingDurablePins(); n > 64 {
				t.Fatalf("%d versions still pinned waiting for their commits to be logged", n)
			}
			if live := eng.MemoryBreakdown().DocumentBodiesLive; live > 6<<20 {
				t.Fatalf("version store holds %d live body bytes of ~12 MB live data against a 4 MiB floor - live versions are not being evicted", live)
			}
			requireAllLive(t, rt, ids, texts, "after writing")
			if eng.ColdDocumentLoads() == 0 {
				t.Fatal("no live document was read back from durable storage, so none was evicted")
			}
			if mode == storage.HistoryModeNone {
				if _, err := rt.Maintain(); err != nil {
					t.Fatalf("maintenance (truncation) with evicted live bodies: %v", err)
				}
				requireAllLive(t, rt, ids, texts, "after truncation")
			}
			rt.Close()
			reopened := boundedRuntime(t, root, true, mode)
			defer reopened.Close()
			requireAllLive(t, reopened, ids, texts, "after reopen")
			if e2, ok := reopened.Storage.(*engine.ServerEngine); ok {
				if live := e2.MemoryBreakdown().DocumentBodiesLive; live > 6<<20 {
					t.Fatalf("after reopen and a full read the version store holds %d live body bytes against a 4 MiB floor", live)
				}
			}
		})
	}
}

// The default keeps every live version resident: nothing about a namespace that did not ask for
// bounded live bodies changes.
func TestLiveBodiesStayPinnedWithoutTheFlag(t *testing.T) {
	rt := boundedRuntime(t, t.TempDir(), false, storage.HistoryModeFull)
	defer rt.Close()
	ids, texts := writeLiveSet(t, rt, 400, 5000) // ~2 MB of live bodies, 1 MB cache
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skip("not a server engine")
	}
	if live := eng.MemoryBreakdown().DocumentBodiesLive; live < 1800_000 {
		t.Fatalf("without the flag the live set should stay resident, got %d live body bytes", live)
	}
	requireAllLive(t, rt, ids, texts, "pinned")
}

// Writers and readers at once, under the race detector: a live version is never read back wrong
// or missing while it moves between pinned, evictable and evicted.
func TestBoundedLiveBodiesConcurrentReadersAndWriters(t *testing.T) {
	rt := boundedRuntime(t, t.TempDir(), true, storage.HistoryModeFull)
	defer rt.Close()
	ids, texts := writeLiveSet(t, rt, 300, 4000)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	// One writer: PutJSONDocument refuses a commit whose branch moved while it was prepared, so
	// two would only race each other. What matters here is reads during writes.
	for w := 0; w < 1; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				docID, _ := codec.RandomUUID()
				b, _ := json.Marshal(map[string]any{"id": docID.String(), "w": w, "i": i, "pad": strings.Repeat("z", 4000)})
				if _, err := embed.PutJSONDocument(rt, boundedNS, string(b)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for k := 0; k < 3; k++ {
				tree := headTree(t, rt)
				for i := r; i < len(ids); i += 4 {
					got, err := rt.Storage.GetDocument(boundedNS, ids[i], tree)
					if err != nil {
						errs <- err
						return
					}
					mu.Lock()
					want := texts[ids[i]]
					mu.Unlock()
					if got == nil || got.JSON != want {
						errs <- fmt.Errorf("reader %d: document %d missing or wrong", r, i)
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
