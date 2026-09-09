package engine

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// TestRebuildStopsAtAResidentAncestor is the bound on what one historical resolution costs.
//
// treeChainFullAt lets a chain run to `size` entries before a full object is written - that is
// what keeps the *write* path constant per commit. The price is that grounding a rebuild on
// that full object applies the whole namespace forward, so a single resolution was
// O(namespace) no matter how close a usable tree was sitting. Rare and enormous rather than
// frequent and small, which is exactly the shape a per-commit average hides: at 108,000
// documents, fourteen such resolutions were 75% of a benchmark's CPU while every other commit
// was fine.
//
// Asserted as objects fetched rather than as a duration, because the claim is about the walk's
// length and not about how fast the machine is.
func TestRebuildStopsAtAResidentAncestor(t *testing.T) {
	const commits = 900
	const gap = 5 // how far the target sits below the ancestor left resident

	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 1 << 30,
		IOShim:                  io.NewInMemoryPlatformIO(),
		HistoryStrategy:         storage.HistoryStrategyObjects,
	}
	e := NewServerEngine("ns", cfg, nil)

	trees := make([]document.DocumentTree, 0, commits)
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
		trees = append(trees, tree)
		parent = tree.TreeHash
	}

	ancestor := trees[len(trees)-1-gap]
	target := trees[len(trees)-1]

	// A fresh store holding nothing but the ancestor. Replacing it rather than shrinking the
	// budget is deliberate: eviction always keeps the most recently used entry, which is the
	// target itself, so a shrunk store leaves the target resident and TreeAt never rebuilds -
	// the assertion below then passes without exercising anything.
	e.treesByHash = newBoundedTreeStore(1 << 30)
	e.treesByHash.Put(ancestor)
	// The live snapshot would answer for the target directly and skip the walk entirely, which
	// is not the path under test.
	e.latestTree.Store(&treeSnapshot{hash: ancestor.TreeHash, tree: ancestor})

	if _, ok := e.treesByHash.Peek(target.TreeHash); ok {
		t.Fatal("target is still cached; this test would not exercise a rebuild")
	}

	before := e.HistoryTreeChainSteps()
	got, ok, err := e.TreeAt(target.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("target tree did not resolve")
	}
	if got.TreeHash != target.TreeHash {
		t.Fatalf("resolved the wrong tree: got %x want %x", got.TreeHash.Bytes, target.TreeHash.Bytes)
	}
	if got.Size() != target.Size() {
		t.Fatalf("resolved tree holds %d entries, want %d", got.Size(), target.Size())
	}
	steps := e.HistoryTreeChainSteps() - before

	t.Logf("rebuild %d commits below a resident ancestor fetched %d objects (namespace %d)",
		gap, steps, target.Size())

	// Walking to the ancestor is `gap` objects. The ceiling leaves room for the chain not
	// lining up exactly with the commit sequence, while staying far below the hundreds of
	// objects a walk to the last full object would fetch on a namespace this size.
	if steps > gap+3 {
		t.Fatalf("rebuilding a tree %d commits below a resident ancestor fetched %d objects; "+
			"the walk is grounding on the last full object instead of stopping at the ancestor, "+
			"which makes one resolution cost the whole namespace", gap, steps)
	}
}

// TestRebuildWithNoResidentAncestorStillResolves is the other half: the shortcut must be an
// optimisation, not a requirement. With nothing resident to stop at, the walk still has to
// reach a full object and produce the right tree.
func TestRebuildWithNoResidentAncestorStillResolves(t *testing.T) {
	const commits = 300

	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 1 << 30,
		IOShim:                  io.NewInMemoryPlatformIO(),
		HistoryStrategy:         storage.HistoryStrategyObjects,
	}
	e := NewServerEngine("ns", cfg, nil)

	var target document.DocumentTree
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
		if i == commits/2 {
			target = tree
		}
		parent = tree.TreeHash
	}

	// Nothing cached at all, and a live snapshot that names a tree unrelated to the target.
	e.treesByHash = newBoundedTreeStore(1)
	e.latestTree.Store(&treeSnapshot{
		hash: codec.Hash{}, tree: document.EmptyDocumentTree(),
	})

	got, ok, err := e.TreeAt(target.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a tree with no resident ancestor did not resolve")
	}
	if got.TreeHash != target.TreeHash {
		t.Fatalf("resolved the wrong tree: got %x want %x", got.TreeHash.Bytes, target.TreeHash.Bytes)
	}
	if got.Size() != target.Size() {
		t.Fatalf("resolved tree holds %d entries, want %d", got.Size(), target.Size())
	}
}
