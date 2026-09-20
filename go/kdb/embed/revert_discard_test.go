package embed_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// A revert that fails partway must leave nothing behind.
//
// PutDocument and DeleteDocument stage into the engine's pending set, which is
// invisible to reads until something calls CommitTree. So an abandoned revert
// looks harmless — right up until the next unrelated write to that namespace
// commits, flushes the pending set wholesale, and records the abandoned
// revert's documents under that write's commit message. The failure is silent,
// arrives later, and is attributed to the wrong write.

// failingPut delegates everything to a real adapter but refuses the nth
// PutDocument, so a revert can be stopped after it has already staged.
type failingPut struct {
	storage.Adapter
	failOn int
	calls  int
}

func (f *failingPut) PutDocument(namespaceID string, doc document.Document) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("storage refused this write")
	}
	return f.Adapter.PutDocument(namespaceID, doc)
}

func TestAFailedRevertStagesNothingForTheNextWrite(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("t", "t/docs", schema.None())
	if err != nil {
		t.Fatalf("opening a runtime: %v", err)
	}
	ns := rt.DefaultNamespace

	ids := make([]codec.UUID, 3)
	for i := range ids {
		if ids[i], err = codec.RandomUUID(); err != nil {
			t.Fatal(err)
		}
	}
	put := func(id codec.UUID, v int) {
		t.Helper()
		if _, err := embed.PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":%q,"v":%d}`, id.String(), v)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	read := func(id codec.UUID) string {
		t.Helper()
		head, err := rt.DAG.Head()
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		body, _, err := embed.DocumentAt(rt, ns, id, head)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return body
	}

	// Two documents at v1 — the point we will try, and fail, to get back to.
	put(ids[0], 1)
	put(ids[1], 1)
	point, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}

	// Move both on, and add a third that a revert would have to delete.
	put(ids[0], 2)
	put(ids[1], 2)
	put(ids[2], 1)

	// Refuse the second document the revert tries to restore, so the first is
	// already staged when it gives up.
	real := rt.Storage
	rt.Storage = &failingPut{Adapter: real, failOn: 2}
	if _, err := embed.RevertTo(rt, ns, point.Hex()); err == nil {
		t.Fatal("the revert was supposed to fail")
	} else if !strings.Contains(err.Error(), "storage refused") {
		t.Fatalf("failed for the wrong reason: %v", err)
	}
	rt.Storage = real

	// An ordinary, unrelated write. This is the moment the leak would surface:
	// its CommitTree flushes whatever is pending.
	put(ids[2], 2)

	for i, want := range []int{2, 2} {
		if got := read(ids[i]); !strings.Contains(got, fmt.Sprintf(`"v":%d`, want)) {
			t.Errorf("document %d reads %s after a failed revert and one unrelated write; "+
				"the abandoned revert was committed under it", i, got)
		}
	}
	if got := read(ids[2]); !strings.Contains(got, `"v":2`) {
		t.Errorf("the unrelated write itself did not land: %s", got)
	}
}
