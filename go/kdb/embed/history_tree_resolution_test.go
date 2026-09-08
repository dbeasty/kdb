package embed_test

import (
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// TestDagDiffWorksAfterReopen is the regression test for the defect this file is named after:
// ServerEngine.GetTree - the dag.DocumentTreeStore implementation the DAG calls to resolve a
// document tree - stopped at the bounded in-memory store and never reached the on-demand rebuild.
//
// The rebuild existed, but it hung off treeAt, which only the storage read path called. So a
// caller arriving through the DAG got a plain miss for a tree that was simply not resident yet,
// and dag.Diff - which resolves both commits' trees that way - failed with "from tree missing"
// for every commit but the newest as soon as the namespace was reopened. That is precisely when
// someone wants to read history, so the failure was invisible in memory-backed tests and total in
// production.
//
// The reopen is the whole point of the test: before it, everything is still resident from having
// just been written, and the bug cannot reproduce.
func TestDagDiffWorksAfterReopen(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "data")

	var commits []codec.Hash
	func() {
		rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rt.Close()
		// Enough commits that the oldest trees are well behind the live one.
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

	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rt.Close()

	d, ok := rt.DAG.(*embed.PersistingCommitDAG)
	if !ok {
		t.Fatalf("expected a persisting DAG on a file-backed runtime, got %T", rt.DAG)
	}
	inner := d.Delegate()

	// The oldest pair is the one whose trees are least likely to be resident after a reopen.
	t.Run("diff across the whole history", func(t *testing.T) {
		diff, err := inner.Diff(commits[0], commits[len(commits)-1])
		if err != nil {
			t.Fatalf("Diff over reopened history: %v\n"+
				"This is the regression: both trees are derivable from the delta log, and "+
				"GetTree must reach the rebuild rather than reporting a miss.", err)
		}
		// doc-a and doc-b were written twice, doc-c once: three documents exist at the end, and
		// the first commit already contained doc-a.
		if len(diff.Entries) == 0 {
			t.Fatal("a diff across five commits that touched three documents cannot be empty")
		}
	})

	t.Run("diff each commit against its parent", func(t *testing.T) {
		for i := 1; i < len(commits); i++ {
			if _, err := inner.Diff(commits[i-1], commits[i]); err != nil {
				t.Errorf("Diff(%s, %s): %v",
					commits[i-1].Hex()[:8], commits[i].Hex()[:8], err)
			}
		}
	})

	t.Run("a tree no commit claims is still a clean miss", func(t *testing.T) {
		// The rebuild must not turn an unknown hash into an expensive walk or a spurious hit.
		var bogus codec.Hash
		for i := range bogus.Bytes {
			bogus.Bytes[i] = 0xAB
		}
		if _, ok := inner.GetDocumentTree(bogus); ok {
			t.Fatal("a tree hash no commit claims must not resolve")
		}
	})
}

// TestDiffReportsModificationsNotJustAdditions guards the semantics the control plane's
// operation-based diff depends on: a document written twice is modified the second time, not
// added again.
func TestDiffReportsModificationsNotJustAdditions(t *testing.T) {
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

	inner := rt.DAG.(*embed.PersistingCommitDAG).Delegate()
	diff, err := inner.Diff(first.Commit, second.Commit)
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
