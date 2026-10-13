package engine

import (
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// frameReads is how the engine reads single versions out of its namespace's delta log: a reader
// that decodes one document out of a framed commit, and the frame cache in front of it.
type frameReads struct {
	namespaceID string
	reader      storage.DeltaDocumentReader
	cache       *delta.FrameCache
}

// installFrameReads puts a frame cache sized from config in front of reader's single-frame reads -
// every cold path that reads one frame: the cold loader, location records and the DAG's commit
// operations loader all go through this reader - and has location records read just the document
// they name rather than decoding the whole commit.
func (e *ServerEngine) installFrameReads(namespaceID string, reader *delta.DefaultReader, config storage.StorageEngineConfig) {
	cache := delta.NewFrameCache(storage.ResolvedDeltaFrameCacheBytes(config))
	reader.SetFrameCache(cache)
	e.frameReads.Store(&frameReads{namespaceID: namespaceID, reader: reader, cache: cache})
}

// DeltaFrameCache reports the namespace's delta frame cache, or nil. For tests and reporting.
func (e *ServerEngine) DeltaFrameCache() *delta.FrameCache {
	if f := e.frameReads.Load(); f != nil {
		return f.cache
	}
	return nil
}

// rebudgetFrameCache re-cuts the frame cache from a namespace budget, unless its size was set
// explicitly. See SetMemoryBudgetBytes.
func (e *ServerEngine) rebudgetFrameCache(budgetBytes int64) {
	if e.config.DeltaFrameCacheBytes != 0 {
		return
	}
	e.DeltaFrameCache().SetBudget(int64(float64(budgetBytes) * storage.DefaultDeltaFrameCacheFraction))
}
