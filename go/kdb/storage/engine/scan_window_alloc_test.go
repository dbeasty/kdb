package engine_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

func smallScanFixture(tb testing.TB, docs int) (*engine.ServerEngine, codec.Hash) {
	tb.Helper()
	e := engine.NewServerEngine("ns", storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1 << 20, IOShim: io.NewInMemoryPlatformIO()}, nil)
	for i := 0; i < docs; i++ {
		doc, err := document.FromJSON(fmt.Sprintf(`{"i":%d}`, i))
		if err != nil {
			tb.Fatal(err)
		}
		if err := e.PutDocument("ns", doc); err != nil {
			tb.Fatal(err)
		}
	}
	tree, err := e.CommitTree("ns", document.EmptyDocumentTree().TreeHash)
	if err != nil {
		tb.Fatal(err)
	}
	return e, tree.TreeHash
}

func scanAll(tb testing.TB, e *engine.ServerEngine, at codec.Hash) int {
	n := 0
	if err := e.ScanDocuments("ns", at, 256, func(batch []document.Document) error {
		n += len(batch)
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return n
}

// The scan window used to be allocated at its full 32768-entry capacity (1.5 MiB) on every scan,
// so a 100-document namespace paid the same as a 100k one. It must scale with the tree.
func TestScanOfSmallNamespaceDoesNotAllocateAFullWindow(t *testing.T) {
	const docs = 100
	e, at := smallScanFixture(t, docs)
	if got := scanAll(t, e, at); got != docs {
		t.Fatalf("scan saw %d documents, want %d", got, docs)
	}

	const runs = 20
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		scanAll(t, e, at)
	}
	runtime.ReadMemStats(&after)
	perScan := (after.TotalAlloc - before.TotalAlloc) / runs
	const limit = 256 << 10
	if perScan > limit {
		t.Fatalf("a %d-document scan allocated %d B, want under %d B (a full window is %d B)", docs, perScan, limit, 32768*48)
	}
}

func BenchmarkScanSmallNamespace(b *testing.B) {
	e, at := smallScanFixture(b, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanAll(b, e, at)
	}
}
