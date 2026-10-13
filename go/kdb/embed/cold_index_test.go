package embed_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/deltaindex"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// coldIndexRuntime is boundedRuntime with the knobs these tests turn: durability, strategy,
// checkpoints and the delta segment size.
func coldIndexRuntime(tb testing.TB, root string, mutate func(*embed.FileRuntimeOptions)) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DocumentCacheBytes = 1 << 20
	opts.Storage.BoundedLiveBodies = true
	// Async: these tests are about where versions are found, not fsync, and a sync commit per
	// document is seconds of fsync on a laptop and minutes on a slow CI runner.
	opts.Storage.Durability = storage.DurabilityAsync
	if mutate != nil {
		mutate(&opts)
	}
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", boundedNS, schema.None(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

func serverEngine(tb testing.TB, rt *embed.EmbeddedKdbRuntime) *engine.ServerEngine {
	tb.Helper()
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		tb.Skip("not a server engine")
	}
	return eng
}

func coldIndexDir(root string) string {
	return filepath.Join(root, "ns", filepath.FromSlash(boundedNS), "deltaidx")
}

func sidecarFiles(tb testing.TB, root string) []string {
	tb.Helper()
	m, _ := filepath.Glob(filepath.Join(coldIndexDir(root), "*"+deltaindex.SidecarSuffix))
	return m
}

// Under the objects strategy every logged version has a location record, so cold reads never
// index the delta log - even when an async writer runs far ahead of the commit log. The engine
// used to keep at most 256 commits' worth of versions waiting for their log position while the
// log can lag by twice that, so a fast async writer lost most of its location records and the
// first cold read built an index of the whole log (225 MB at 1M documents).
func TestColdReadsResolveByLocationAfterAFastAsyncWriter(t *testing.T) {
	root := t.TempDir()
	rt := coldIndexRuntime(t, root, nil)
	ids, texts := writeLiveSet(t, rt, 3000, 2500) // ~7.5 MB of live bodies against a 4 MiB floor
	requireAllLive(t, rt, ids, texts, "after writing")
	rt.Close()

	reopened := coldIndexRuntime(t, root, nil)
	defer reopened.Close()
	requireAllLive(t, reopened, ids, texts, "after reopen")
	eng := serverEngine(t, reopened)
	if eng.ColdDocumentLoads() == 0 {
		t.Fatal("nothing was read back from durable storage, so nothing was evicted")
	}
	if st := eng.ColdIndexStats(); st.Segments != 0 {
		t.Fatalf("the cold loader indexed %d delta segments (%d versions): some version had no location record", st.Segments, st.Keys)
	}
}

// Replay files location records for the commits it reads back: a commit replayed at open may be
// one whose position its writer never filed (a crash, or a lost stash), and nothing else would.
// Simulated here by removing the blob store - and with it every location record - before a full
// replay.
func TestReplayFilesLocationRecords(t *testing.T) {
	root := t.TempDir()
	noCheckpoints := func(o *embed.FileRuntimeOptions) { o.Storage.DisableCheckpoints = true }
	rt := coldIndexRuntime(t, root, noCheckpoints)
	ids, texts := writeLiveSet(t, rt, 2500, 2500)
	rt.Close()
	if err := os.RemoveAll(filepath.Join(root, "ns", filepath.FromSlash(boundedNS), "sstable")); err != nil {
		t.Fatal(err)
	}

	reopened := coldIndexRuntime(t, root, noCheckpoints)
	defer reopened.Close()
	requireAllLive(t, reopened, ids, texts, "after a full replay")
	eng := serverEngine(t, reopened)
	if eng.ColdDocumentLoads() == 0 {
		t.Fatal("nothing was read back from durable storage, so nothing was evicted")
	}
	if st := eng.ColdIndexStats(); st.Segments != 0 {
		t.Fatalf("the cold loader indexed %d delta segments: replay did not file location records", st.Segments)
	}
}

// Where the cold loader is still the way back - the replay strategy keeps no location records,
// and history=none truncates the log under it - every version is still found, its index lives in
// mapped per-segment sidecars with only the newest segment on the heap, the sidecars are reused
// after a reopen, and those of truncated segments are removed.
func TestColdIndexSidecarsServeTheReplayStrategy(t *testing.T) {
	for _, mode := range []storage.HistoryMode{storage.HistoryModeFull, storage.HistoryModeNone} {
		t.Run(mode.String(), func(t *testing.T) {
			root := t.TempDir()
			opts := func(o *embed.FileRuntimeOptions) {
				o.Storage.HistoryMode = mode
				o.Storage.HistoryStrategy = storage.HistoryStrategyReplay
				o.Storage.DeltaMaxSegmentBytes = 16 << 10 // many segments: the bodies compress to ~100 bytes a commit
				if mode == storage.HistoryModeNone {
					o.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
				}
			}
			rt := coldIndexRuntime(t, root, opts)
			ids, texts := writeLiveSet(t, rt, 2500, 2500)
			requireAllLive(t, rt, ids, texts, "after writing")
			eng := serverEngine(t, rt)
			if mode == storage.HistoryModeFull {
				st := eng.ColdIndexStats()
				if st.MappedSegs == 0 || st.MappedSegs != st.Segments-1 {
					t.Fatalf("cold index %+v: want every segment but the newest mapped", st)
				}
				if st.HeapBytes*int64(st.Segments) > st.MappedBytes*2 {
					t.Fatalf("cold index %+v: the heap part is not just the newest segment", st)
				}
				if len(sidecarFiles(t, root)) == 0 {
					t.Fatal("no sidecars on disk")
				}
			}
			if mode == storage.HistoryModeNone {
				before := len(sidecarFiles(t, root))
				res, err := rt.Maintain()
				if err != nil {
					t.Fatalf("maintenance (truncation): %v", err)
				}
				if res.Removed == 0 {
					t.Fatal("maintenance truncated nothing")
				}
				requireAllLive(t, rt, ids, texts, "after truncation")
				after := len(sidecarFiles(t, root))
				t.Logf("truncated %d segments; sidecars %d -> %d", res.Removed, before, after)
				if after > before-res.Removed+1 {
					t.Fatalf("%d sidecars before truncating %d segments, %d after: the deleted segments' sidecars stayed", before, res.Removed, after)
				}
			}
			rt.Close()
			reopened := coldIndexRuntime(t, root, opts)
			defer reopened.Close()
			requireAllLive(t, reopened, ids, texts, "after reopen")
			// Every sidecar on disk names a segment that is still in the log.
			segs, _ := filepath.Glob(filepath.Join(root, "ns", filepath.FromSlash(boundedNS), "delta", "*.seg"))
			if len(segs) == 0 {
				t.Fatal("no delta segments on disk")
			}
			listed := map[string]bool{}
			for _, s := range segs {
				listed[strings.TrimSuffix(filepath.Base(s), ".seg")+".versions"+deltaindex.SidecarSuffix] = true
			}
			for _, f := range sidecarFiles(t, root) {
				if !listed[filepath.Base(f)] {
					t.Fatalf("sidecar %s outlived its segment", filepath.Base(f))
				}
			}
		})
	}
}
