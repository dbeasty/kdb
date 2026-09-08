package embed_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func revertRuntime(t *testing.T, root string, s storage.HistoryStrategy) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryStrategy = s
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func putDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id, body string) codec.Hash {
	t.Helper()
	res, err := embed.PutJSONDocument(rt, "app/docs", fmt.Sprintf(`{"id":%q,"v":%q}`, id, body))
	if err != nil {
		t.Fatal(err)
	}
	return res.Commit
}

func readDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id string) (string, bool) {
	t.Helper()
	docID, _, err := document.ResolveID(fmt.Sprintf(`{"id":%q}`, id))
	if err != nil {
		t.Fatal(err)
	}
	head, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	c, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := rt.Storage.GetDocument("app/docs", docID, c.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		return "", false
	}
	return doc.JSON, true
}

// The defining property: a revert restores the target's exact document
// tree, so the new commit's tree hash equals the target's.
func TestRevertRestoresTheTargetTreeExactly(t *testing.T) {
	for _, strategy := range []storage.HistoryStrategy{
		storage.HistoryStrategyObjects, storage.HistoryStrategyReplay,
	} {
		t.Run(strategy.String(), func(t *testing.T) {
			rt := revertRuntime(t, t.TempDir(), strategy)
			defer rt.Close()

			putDoc(t, rt, "a", "1")
			target := putDoc(t, rt, "b", "1")
			targetCommit, err := rt.DAG.GetCommitOrThrow(target)
			if err != nil {
				t.Fatal(err)
			}

			putDoc(t, rt, "a", "2")
			putDoc(t, rt, "c", "1")

			res, err := embed.RevertTo(rt, "app/docs", target.Hex())
			if err != nil {
				t.Fatal(err)
			}
			if res.Commit == target {
				t.Fatal("a revert must write a new commit, not move to the old one")
			}
			newCommit, err := rt.DAG.GetCommitOrThrow(res.Commit)
			if err != nil {
				t.Fatal(err)
			}
			if newCommit.DocumentTreeHash != targetCommit.DocumentTreeHash {
				t.Fatal("the reverted tree is not the target's tree")
			}
			if res.Restored != 1 || res.Removed != 1 {
				t.Fatalf("want 1 restored and 1 removed, got %d and %d", res.Restored, res.Removed)
			}

			if body, ok := readDoc(t, rt, "a"); !ok || !strings.Contains(body, `"v":"1"`) {
				t.Fatalf("document a was not restored: %q", body)
			}
			if _, ok := readDoc(t, rt, "c"); ok {
				t.Fatal("document c was added after the target and should be gone")
			}
			if _, ok := readDoc(t, rt, "b"); !ok {
				t.Fatal("document b existed at the target and should still be here")
			}
		})
	}
}

// History moves forward: the reverted-away state is still reachable, and
// the revert is itself revertible.
func TestRevertIsItselfRevertible(t *testing.T) {
	rt := revertRuntime(t, t.TempDir(), storage.HistoryStrategyObjects)
	defer rt.Close()

	putDoc(t, rt, "a", "1")
	target := putDoc(t, rt, "a", "2")
	beforeRevert := putDoc(t, rt, "a", "3")

	if _, err := embed.RevertTo(rt, "app/docs", target.Hex()); err != nil {
		t.Fatal(err)
	}
	if body, _ := readDoc(t, rt, "a"); !strings.Contains(body, `"v":"2"`) {
		t.Fatalf("after revert: %q", body)
	}
	if _, err := embed.RevertTo(rt, "app/docs", beforeRevert.Hex()); err != nil {
		t.Fatal(err)
	}
	if body, _ := readDoc(t, rt, "a"); !strings.Contains(body, `"v":"3"`) {
		t.Fatalf("after reverting the revert: %q", body)
	}
}

func TestRevertAcceptsRelativeRevisions(t *testing.T) {
	rt := revertRuntime(t, t.TempDir(), storage.HistoryStrategyObjects)
	defer rt.Close()

	putDoc(t, rt, "a", "1")
	putDoc(t, rt, "a", "2")
	putDoc(t, rt, "a", "3")

	// head~1 is the commit that wrote v2 - undo of exactly one commit.
	if _, err := embed.RevertTo(rt, "app/docs", "head~1"); err != nil {
		t.Fatal(err)
	}
	if body, _ := readDoc(t, rt, "a"); !strings.Contains(body, `"v":"2"`) {
		t.Fatalf("head~1 revert landed on %q", body)
	}
}

func TestRevertToHeadIsANoOp(t *testing.T) {
	rt := revertRuntime(t, t.TempDir(), storage.HistoryStrategyObjects)
	defer rt.Close()

	putDoc(t, rt, "a", "1")
	head, _ := rt.DAG.Head()
	res, err := embed.RevertTo(rt, "app/docs", "head")
	if err != nil {
		t.Fatal(err)
	}
	if res.Commit != head {
		t.Fatal("reverting to head should not write a commit")
	}
	now, _ := rt.DAG.Head()
	if now != head {
		t.Fatal("head moved on a no-op revert")
	}
}

func TestRevertToAnUnknownRevisionFails(t *testing.T) {
	rt := revertRuntime(t, t.TempDir(), storage.HistoryStrategyObjects)
	defer rt.Close()

	putDoc(t, rt, "a", "1")
	head, _ := rt.DAG.Head()
	if _, err := embed.RevertTo(rt, "app/docs", "head~50"); err == nil {
		t.Fatal("reverting past the root should fail")
	}
	if now, _ := rt.DAG.Head(); now != head {
		t.Fatal("a failed revert moved head")
	}
}

// Reverting survives a restart, which is what makes it a real undo rather
// than a session-local view.
func TestRevertSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	rt := revertRuntime(t, root, storage.HistoryStrategyObjects)
	putDoc(t, rt, "a", "1")
	target := putDoc(t, rt, "a", "2")
	putDoc(t, rt, "a", "3")
	if _, err := embed.RevertTo(rt, "app/docs", target.Hex()); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	rt = revertRuntime(t, root, storage.HistoryStrategyObjects)
	defer rt.Close()
	if body, ok := readDoc(t, rt, "a"); !ok || !strings.Contains(body, `"v":"2"`) {
		t.Fatalf("after reopen: %q", body)
	}
}

// documentIDOf resolves the document id a body carries, for tests that
// need to read one back by id.
func documentIDOf(jsonText string) (codec.UUID, bool, error) {
	return document.ResolveID(jsonText)
}
