package index_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
)

// appendCommit appends one empty commit on top of parent and returns its hash.
func appendCommit(t testing.TB, d *dag.InMemoryCommitDag, parent codec.Hash) codec.Hash {
	t.Helper()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: parent,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
	c, err := d.AppendCommit(tx, parent, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatalf("AppendCommit: %v", err)
	}
	return c.Hash
}

// TestLookupReflectsNewEventsAfterCaching guards the memoized bucket replay against the
// obvious way a cache goes wrong: serving a result that predates the write.
func TestLookupReflectsNewEventsAfterCaching(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/cache")
	head, _ := d.Head()
	eng := index.NewVersionedEngine(d)
	key := index.StringKey{Value: "k"}

	if ids, err := eng.Lookup(key, nil); err != nil || len(ids) != 0 {
		t.Fatalf("lookup on empty index: ids=%v err=%v", ids, err)
	}

	docA, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: docA, Key: key, CommitHash: head})
	ids, err := eng.Lookup(key, nil)
	if err != nil || len(ids) != 1 || ids[0] != docA {
		t.Fatalf("lookup after put: ids=%v err=%v", ids, err)
	}

	docB, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: docB, Key: key, CommitHash: head})
	if ids, _ := eng.Lookup(key, nil); len(ids) != 2 {
		t.Fatalf("lookup after second put: got %d ids, want 2", len(ids))
	}

	_ = eng.Delete(docA, head)
	if ids, _ := eng.Lookup(key, nil); len(ids) != 1 || ids[0] != docB {
		t.Fatalf("lookup after delete: got %v, want only docB", ids)
	}

	_ = eng.Clear()
	if ids, _ := eng.Lookup(key, nil); len(ids) != 0 {
		t.Fatalf("lookup after clear: got %v, want none", ids)
	}
}

// TestLookupIsPerCutoff confirms the memo is keyed by cutoff commit: an as-of read must not
// be served the head's bucket set, or vice versa.
func TestLookupIsPerCutoff(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/cutoff")
	genesis, _ := d.Head()
	eng := index.NewVersionedEngine(d)
	key := index.StringKey{Value: "k"}

	older, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: older, Key: key, CommitHash: genesis})
	if ids, _ := eng.Lookup(key, &genesis); len(ids) != 1 {
		t.Fatalf("lookup at genesis: got %v, want 1 id", ids)
	}

	next := appendCommit(t, d, genesis)
	newer, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: newer, Key: key, CommitHash: next})

	if ids, _ := eng.Lookup(key, &genesis); len(ids) != 1 || ids[0] != older {
		t.Fatalf("lookup as of genesis must not see the later commit's entry: got %v", ids)
	}
	if ids, _ := eng.Lookup(key, nil); len(ids) != 2 {
		t.Fatalf("lookup at head: got %d ids, want 2", len(ids))
	}
}

// TestLookupReflectsAncestryChange is the case a naive (cutoff, event-count) memo gets wrong:
// neither the cutoff nor the event log changes, but the commit graph does. Squashing away the
// commit between genesis and the cutoff makes genesis unreachable, so an entry pinned to
// genesis stops being visible - and the memo has to notice.
func TestLookupReflectsAncestryChange(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/ancestry")
	genesis, _ := d.Head()
	middle := appendCommit(t, d, genesis)
	tip := appendCommit(t, d, middle)

	eng := index.NewVersionedEngine(d)
	key := index.StringKey{Value: "k"}
	docID, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: docID, Key: key, CommitHash: genesis})

	if ids, _ := eng.Lookup(key, &tip); len(ids) != 1 {
		t.Fatalf("lookup at tip before squash: got %v, want 1 id", ids)
	}

	if _, err := d.Squash([]codec.Hash{middle}, genesis, document.EmptyDocumentTree(), nil, "squash"); err != nil {
		t.Fatalf("Squash: %v", err)
	}

	if ids, _ := eng.Lookup(key, &tip); len(ids) != 0 {
		t.Fatalf("genesis is no longer an ancestor of the cutoff, so its entry must be invisible: got %v", ids)
	}
}

// BenchmarkVersionedEngineLookup measures a repeated lookup against an index with a
// non-trivial event log and commit history - the shape that used to pay a full replay
// (rebuild + sort + one ancestor-closure walk per event) on every single call.
func BenchmarkVersionedEngineLookup(b *testing.B) {
	d, _ := dag.NewInMemoryCommitDag("ns/bench")
	head, _ := d.Head()
	for i := 0; i < 200; i++ {
		head = appendCommit(b, d, head)
	}
	eng := index.NewVersionedEngine(d)
	for i := 0; i < 2000; i++ {
		docID, _ := codec.RandomUUID()
		_ = eng.Put(index.Entry{
			DocID:      docID,
			Key:        index.StringKey{Value: fmt.Sprintf("key-%d", i%64)},
			CommitHash: head,
		})
	}
	key := index.StringKey{Value: "key-7"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := eng.Lookup(key, nil); err != nil {
			b.Fatal(err)
		}
	}
}
