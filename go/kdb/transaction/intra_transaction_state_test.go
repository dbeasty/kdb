package transaction_test

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// A transaction sees its own earlier operations.
//
// Staging used to resolve every operation against the baseline tree independently, so a
// transaction was blind to what it had already done. That cost two things: a second write to one
// document silently dropped the first one's fields, and there was no way to express replacement at
// all - which is what the control UI's editor needs to remove a key.
//
// The commit fold has always read a delete followed by a write as "the document is exactly this"
// (embed.applyCommitToTree cancels the delete). Staging is what disagreed.

// docKeys is the shape of a stored document, which is what these tests are actually about: whether
// a key that should have gone is gone.
func docKeys(t *testing.T, raw string) []string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("stored document is not JSON: %v (%s)", err, raw)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestDeleteThenWriteReplacesTheDocument(t *testing.T) {
	ns := "app/replace"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

	base, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	original, err := document.FromJSON(`{"keep":"yes","drop":"me","also":"gone"}`)
	if err != nil {
		t.Fatal(err)
	}
	first := mustCommit(t, engine, newTx(base,
		document.WriteOp{DocID: original.ID, Patch: original.JSON}), d, store)
	head := first.(transaction.ResultSuccess).Commit

	// The replacement: remove the document, then write the document it should be.
	replacement := `{"keep":"yes","fresh":"value"}`
	res := mustCommit(t, engine, newTx(head.Hash,
		document.DeleteOp{DocID: original.ID},
		document.WriteOp{DocID: original.ID, Patch: replacement}), d, store)
	commit := res.(transaction.ResultSuccess).Commit

	stored, err := store.GetDocument(ns, original.ID, commit.DocumentTreeHash)
	if err != nil || stored == nil {
		t.Fatalf("the document should still exist after a replace: %v", err)
	}
	got := docKeys(t, stored.JSON)
	want := []string{"fresh", "keep"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("a replace should leave exactly the written keys.\n got: %v\nwant: %v\n"+
			"stored: %s", got, want, stored.JSON)
	}
}

// The same shape without the delete still merges. Replacement has to be something a caller asks
// for, not a change to what an ordinary write means.
func TestAWriteOnItsOwnStillMerges(t *testing.T) {
	ns := "app/merge"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	base, _ := d.Head()

	original, err := document.FromJSON(`{"keep":"yes","drop":"me"}`)
	if err != nil {
		t.Fatal(err)
	}
	first := mustCommit(t, engine, newTx(base,
		document.WriteOp{DocID: original.ID, Patch: original.JSON}), d, store)
	head := first.(transaction.ResultSuccess).Commit

	res := mustCommit(t, engine, newTx(head.Hash,
		document.WriteOp{DocID: original.ID, Patch: `{"fresh":"value"}`}), d, store)
	commit := res.(transaction.ResultSuccess).Commit

	stored, _ := store.GetDocument(ns, original.ID, commit.DocumentTreeHash)
	if stored == nil {
		t.Fatal("no document")
	}
	got := docKeys(t, stored.JSON)
	want := []string{"drop", "fresh", "keep"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("a plain write must still merge.\n got: %v\nwant: %v", got, want)
	}
}

// TestASecondWriteSeesTheFirst: two writes to one document in one transaction. The second used to
// merge over the *original*, so the first one's fields simply vanished from the commit.
func TestASecondWriteSeesTheFirst(t *testing.T) {
	ns := "app/twowrites"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	base, _ := d.Head()

	doc, err := document.FromJSON(`{"a":1}`)
	if err != nil {
		t.Fatal(err)
	}
	res := mustCommit(t, engine, newTx(base,
		document.WriteOp{DocID: doc.ID, Patch: doc.JSON},
		document.WriteOp{DocID: doc.ID, Patch: `{"b":2}`}), d, store)
	commit := res.(transaction.ResultSuccess).Commit

	stored, _ := store.GetDocument(ns, doc.ID, commit.DocumentTreeHash)
	if stored == nil {
		t.Fatal("no document")
	}
	got := docKeys(t, stored.JSON)
	want := []string{"a", "b"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("the second write should build on the first.\n got: %v\nwant: %v\nstored: %s",
			got, want, stored.JSON)
	}
}

// TestReplaceSurvivesTheCommitFold is the property that makes this usable rather than merely
// correct in memory: what the commit records has to rebuild to the same document, because that is
// what a restart and every historical read go through. The stored operation carries the whole
// document (see operationsAsStored), so a replay of this commit must not resurrect the dropped key.
func TestReplaceSurvivesTheCommitFold(t *testing.T) {
	ns := "app/replayreplace"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	base, _ := d.Head()

	original, err := document.FromJSON(`{"keep":"yes","drop":"me"}`)
	if err != nil {
		t.Fatal(err)
	}
	first := mustCommit(t, engine, newTx(base,
		document.WriteOp{DocID: original.ID, Patch: original.JSON}), d, store)
	head := first.(transaction.ResultSuccess).Commit

	res := mustCommit(t, engine, newTx(head.Hash,
		document.DeleteOp{DocID: original.ID},
		document.WriteOp{DocID: original.ID, Patch: `{"keep":"yes"}`}), d, store)
	commit := res.(transaction.ResultSuccess).Commit

	// Rebuild the document from the operations the commit actually recorded, the way replay and
	// the historical-tree fold do: a later write cancels an earlier delete, and the write's
	// payload is the whole document.
	var rebuilt string
	deleted := false
	for _, op := range commit.Operations {
		switch o := op.(type) {
		case document.DeleteOp:
			deleted, rebuilt = true, ""
		case document.WriteOp:
			deleted, rebuilt = false, o.Patch
		}
	}
	if deleted {
		t.Fatal("the fold should end with the document present, not deleted")
	}
	got := docKeys(t, rebuilt)
	if strings.Join(got, ",") != "keep" {
		t.Errorf("replaying this commit rebuilds %v, not the replaced document.\nrecorded: %s",
			got, rebuilt)
	}
}
