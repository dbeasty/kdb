package embed_test

import (
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Reading history on a *reopened* file-backed namespace is the case these tests exist for, and it
// is the one that unit tests over a memory runtime can never reach: nothing is ever evicted there,
// so every tree is still resident and every path looks fine.
//
// A checkpoint restores the live tree and the commit graph, not every tree the namespace has ever
// had - the rest are derivable from the delta log and rebuilt on demand. So after a reopen, a
// caller asking for an older tree is asking for something that has to be reconstructed first.
//
// Note which API is under test. Raw dag.Diff resolves trees through dag.DocumentTreeStore, which
// the DAG calls *while holding its own lock*, so that path deliberately does not rebuild: the
// rebuild walks commits and would re-enter the DAG, deadlocking against a waiting writer (see
// ServerEngine.TreeAt's doc comment). embed.DiffCommits is the supported way to diff two points -
// it resolves both trees outside the lock through the adapter and then compares them with the pure
// dag.DiffTrees. These tests target that, because that is the contract callers actually have.

func TestHistoryDiffWorksAfterReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")

	var commits []codec.Hash
	func() {
		rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rt.Close()
		for _, body := range []string{
			`{"id":"doc-a","v":1}`,
			`{"id":"doc-b","v":1}`,
			`{"id":"doc-a","v":2}`,
			`{"id":"doc-c","v":1}`,
			`{"id":"doc-b","v":2}`,
		} {
			res, err := embed.PutJSONDocument(rt, "demo/users", body)
			if err != nil {
				t.Fatalf("put %s: %v", body, err)
			}
			commits = append(commits, res.Commit)
		}
	}()

	// The reopen is the whole point: before it, everything is still resident from having just been
	// written and the interesting path is never taken.
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rt.Close()

	t.Run("across the whole history", func(t *testing.T) {
		diff, err := embed.DiffCommits(rt, commits[0].Hex(), commits[len(commits)-1].Hex())
		if err != nil {
			t.Fatalf("DiffCommits over reopened history: %v\n"+
				"Both trees are derivable from the delta log; resolution must reach the rebuild.", err)
		}
		if len(diff.Entries) == 0 {
			t.Fatal("a diff across five commits touching three documents cannot be empty")
		}
	})

	t.Run("each commit against its parent", func(t *testing.T) {
		for i := 1; i < len(commits); i++ {
			if _, err := embed.DiffCommits(rt, commits[i-1].Hex(), commits[i].Hex()); err != nil {
				t.Errorf("DiffCommits(%s, %s): %v",
					commits[i-1].Hex()[:8], commits[i].Hex()[:8], err)
			}
		}
	})

	t.Run("by revision specification", func(t *testing.T) {
		// The specs the history API accepts must survive a reopen too, not just raw hashes.
		if _, err := embed.DiffCommits(rt, "head~2", "head"); err != nil {
			t.Errorf("DiffCommits(head~2, head) after reopen: %v", err)
		}
	})

	t.Run("a revision that names nothing is refused, not guessed", func(t *testing.T) {
		if _, err := embed.DiffCommits(rt, "head~999", "head"); err == nil {
			t.Fatal("walking past the root must be an error rather than silently clamping to it")
		}
	})
}

// TestHistoryDiffReportsModifications guards the semantics the control plane's commit view
// renders: a document written twice is modified the second time, not added again.
func TestHistoryDiffReportsModifications(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rt.Close()

	first, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"doc-a","v":1}`)
	if err != nil {
		t.Fatal(err)
	}
	second, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"doc-a","v":2}`)
	if err != nil {
		t.Fatal(err)
	}

	diff, err := embed.DiffCommits(rt, first.Commit.Hex(), second.Commit.Hex())
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	var modified int
	for _, e := range diff.Entries {
		if m, ok := e.(dag.DiffModified); ok && m.DocID == first.DocID {
			modified++
		}
	}
	if modified != 1 {
		t.Fatalf("writing the same document id twice must read as one modification, got %d "+
			"modified entries in %v", modified, diff.Entries)
	}
}
