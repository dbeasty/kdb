package engine

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// TestColdLoaderThroughFrameCache reads every version of a varied log (repeated content across
// segments, several writes of one document per commit, unparsable patches, deletes) back through
// the cold loader's single-document path, with no cache, a cache small enough to evict
// constantly, and one that holds everything - twice each, so both misses and hits are checked.
func TestColdLoaderThroughFrameCache(t *testing.T) {
	for name, shim := range map[string]storage.PlatformIOShim{"memory": storio.NewInMemoryPlatformIO(), "file": osShim(t)} {
		t.Run(name, func(t *testing.T) {
			reader, versions := writeColdLog(t, shim, storage.CompressionZSTD, 1500, 9)
			for _, budget := range []int64{0, 64 << 10, 16 << 20} {
				reader.SetFrameCache(delta.NewFrameCache(budget))
				l := newDeltaColdLoader(reader)
				for pass := 0; pass < 2; pass++ {
					for _, v := range versions {
						h, err := document.Document{ID: v.docID, JSON: v.text}.ContentHash()
						if err != nil {
							t.Fatal(err)
						}
						doc, ok, err := l.load(v.docID, h)
						if err != nil || !ok || doc.ID != v.docID || doc.JSON != v.text {
							t.Fatalf("budget %d pass %d: %v ok=%v err=%v got %q want %q", budget, pass, v.docID, ok, err, doc.JSON, v.text)
						}
					}
				}
				// A version that was never written is not found, not an error.
				if _, ok, err := l.load(versions[0].docID, codec.Hash{}); ok || err != nil {
					t.Fatalf("unknown version: ok=%v err=%v", ok, err)
				}
				st := reader.FrameCache().Stats()
				if budget > 0 && (st.Hits == 0 || reader.FrameCache().Bytes() > budget) {
					t.Fatalf("budget %d: %+v, %d bytes", budget, st, reader.FrameCache().Bytes())
				}
			}
		})
	}
}

// TestFrameCacheFollowsTheMemoryBudget: the namespace's frame cache is sized from the hot-tier
// budget, re-cut when the arbiter re-cuts the namespace, reported in its demand, and left alone
// when its size was set explicitly.
func TestFrameCacheFollowsTheMemoryBudget(t *testing.T) {
	shim := storio.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{IOShim: shim, GlobalMemoryBudgetBytes: 64 << 20}
	eng := NewServerEngine(coldTestNS, cfg, nil)
	eng.installFrameReads(coldTestNS, delta.Factory{Config: cfg}.OpenReader(coldTestNS), cfg)
	if got := eng.DeltaFrameCache().Budget(); got != 8<<20 {
		t.Fatalf("frame cache budget %d, want 8 MiB", got)
	}
	eng.SetMemoryBudgetBytes(16 << 20)
	if got := eng.DeltaFrameCache().Budget(); got != 2<<20 {
		t.Fatalf("after re-cut: %d, want 2 MiB", got)
	}
	cfg.DeltaFrameCacheBytes = 1 << 20
	pinned := NewServerEngine(coldTestNS, cfg, nil)
	pinned.installFrameReads(coldTestNS, delta.Factory{Config: cfg}.OpenReader(coldTestNS), cfg)
	pinned.SetMemoryBudgetBytes(16 << 20)
	if got := pinned.DeltaFrameCache().Budget(); got != 1<<20 {
		t.Fatalf("explicit size overridden: %d", got)
	}
	cfg.DeltaFrameCacheBytes = -1
	off := NewServerEngine(coldTestNS, cfg, nil)
	off.installFrameReads(coldTestNS, delta.Factory{Config: cfg}.OpenReader(coldTestNS), cfg)
	off.SetMemoryBudgetBytes(16 << 20)
	if off.DeltaFrameCache() != nil {
		t.Fatal("disabled cache exists")
	}
}
