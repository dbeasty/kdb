package engine

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// TestTreeObjectWritesStayLinearInCommitCount guards the commit path
// against the objects strategy going quadratic in the namespace.
//
// A full tree object costs O(documents). What decides how often one is
// written therefore decides the order of the whole write path: on a fixed
// interval of commits, each commit carries documents/interval of it, which
// grows without bound as the namespace does. That shipped, and cost 63% of
// commit CPU at 10,000 documents - see
// docs/benchmarks/2026-09-07-perf-rerun-7947cce.md.
//
// Doubling the commits should therefore roughly double what the tree
// objects write, not quadruple it. The bytes are measured rather than the
// time so this fails on the defect rather than on a slow machine.
func TestTreeObjectWritesStayLinearInCommitCount(t *testing.T) {
	measure := func(commits int) int64 {
		// A budget nothing will reach, so the memtable holds every object
		// written instead of flushing part of it out of view.
		cfg := storage.StorageEngineConfig{
			GlobalMemoryBudgetBytes: 1 << 30,
			IOShim:                  io.NewInMemoryPlatformIO(),
			HistoryStrategy:         storage.HistoryStrategyObjects,
		}
		e := NewServerEngine("ns", cfg, nil)
		parent := document.EmptyDocumentTree().TreeHash
		for i := 0; i < commits; i++ {
			doc, err := document.FromJSON(fmt.Sprintf(`{"v":%d}`, i))
			if err != nil {
				t.Fatal(err)
			}
			if err := e.PutDocument("ns", doc); err != nil {
				t.Fatal(err)
			}
			tree, err := e.CommitTree("ns", parent)
			if err != nil {
				t.Fatal(err)
			}
			parent = tree.TreeHash
		}
		return e.memTable.SizeBytes()
	}

	const base = 750
	small := measure(base)
	large := measure(2 * base)
	ratio := float64(large) / float64(small)
	t.Logf("tree objects: %d commits wrote %d bytes, %d commits wrote %d bytes (%.2fx)",
		base, small, 2*base, large, ratio)

	// Linear doubles, quadratic quadruples. 3 leaves room for the fixed
	// per-commit costs that are in this measurement too, while staying far
	// below what the defect produced (measured at 3.5x and climbing with
	// the base).
	if ratio > 3 {
		t.Fatalf("doubling the commits multiplied tree-object bytes by %.2f - "+
			"the full-object interval is not scaling with the namespace", ratio)
	}
}
