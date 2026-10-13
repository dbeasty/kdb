package embed

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// logMemoryBreakdown logs what the storage engine says it holds, for TestMemoryProfile.
func logMemoryBreakdown(t *testing.T, rt *EmbeddedKdbRuntime) {
	t.Helper()
	if e, ok := rt.Storage.(*engine.ServerEngine); ok {
		b := e.MemoryBreakdown()
		t.Logf("breakdown: bodies %s (live %s), doc index %s, versions %d, placeholders %d, live tree ~%s, history trees %s, memtable %s, "+
			"sstable indexes %s heap + %s mapped, sstable block cache %s",
			mb(b.DocumentBodies), mb(b.DocumentBodiesLive), mb(b.DocumentIndex), b.DocumentVersions, b.DocumentPlaceholders,
			mb(b.LiveTree), mb(b.HistoryTrees), mb(b.Memtable), mb(b.SSTableIndexes), mb(b.SSTableIndexesMapped), mb(b.SSTableBlockCache))
		ci := e.ColdIndexStats()
		t.Logf("cold index: %d segments (%d mapped), %d versions, heap %s, mapped %s; cold loads %d",
			ci.Segments, ci.MappedSegs, ci.Keys, mb(ci.HeapBytes), mb(ci.MappedBytes), e.ColdDocumentLoads())
	}
}
