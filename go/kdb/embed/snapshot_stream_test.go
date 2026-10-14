package embed_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// StreamTreeBodies, read from a real delta log: every version the head tree names is handed over
// with its exact body, and nothing else - not a version a later write replaced, not a deleted
// document's - across a reopen too, when nothing is resident.
func TestStreamTreeBodiesFromTheLog(t *testing.T) {
	root := t.TempDir()
	rt := boundedRuntime(t, root, true, storage.HistoryModeFull)
	ids, texts := writeLiveSet(t, rt, 300, 200)
	for i, id := range ids[:60] { // rewritten: the first version is in the log, but no longer live
		body := fmt.Sprintf(`{"id":%q,"rewritten":%d}`, id.String(), i)
		if _, err := embed.PutJSONDocument(rt, boundedNS, body); err != nil {
			t.Fatal(err)
		}
		texts[id] = body
	}
	for _, id := range ids[60:90] { // deleted
		head, _ := rt.DAG.Head()
		c, _ := rt.DAG.GetCommitOrThrow(head)
		if err := rt.Storage.DeleteDocument(boundedNS, id); err != nil {
			t.Fatal(err)
		}
		tree, err := rt.Storage.CommitTree(boundedNS, c.DocumentTreeHash)
		if err != nil {
			t.Fatal(err)
		}
		txID, _ := codec.RandomUUID()
		tx := document.Transaction{ID: txID, BaseVersion: head, Operations: []document.Op{document.DeleteOp{DocID: id}}, Timestamp: codec.TimestampNow(), AuthorNodeID: txID}
		if _, err := rt.DAG.AppendCommit(tx, head, tree, nil, ""); err != nil {
			t.Fatal(err)
		}
		delete(texts, id)
	}
	check := func(rt *embed.EmbeddedKdbRuntime, where string) {
		t.Helper()
		eng, ok := rt.Storage.(*engine.ServerEngine)
		if !ok {
			t.Skip("not a server engine")
		}
		tree, ok, err := eng.TreeAt(headTree(t, rt))
		if err != nil || !ok {
			t.Fatalf("%s: head tree: %v", where, err)
		}
		got := map[codec.UUID]string{}
		if err := eng.StreamTreeBodies(boundedNS, tree, func(id codec.UUID, body string) error {
			if prev, seen := got[id]; seen && prev != body {
				t.Fatalf("%s: document %s handed over with two different bodies", where, id)
			}
			got[id] = body
			return nil
		}); err != nil {
			t.Fatalf("%s: %v", where, err)
		}
		if len(got) != len(texts) {
			t.Fatalf("%s: streamed %d documents, the tree names %d", where, len(got), len(texts))
		}
		for id, want := range texts {
			if got[id] != want {
				t.Fatalf("%s: document %s streamed %q, want %q", where, id, got[id], want)
			}
		}
	}
	check(rt, "open")
	rt.Close()
	reopened := boundedRuntime(t, root, true, storage.HistoryModeFull)
	defer reopened.Close()
	check(reopened, "reopened")
}
