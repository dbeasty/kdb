package index

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// A queue whose claims rewrite each job's sort key leaves an empty key behind per claim, right
// where the next claim's walk starts. The key table is compacted once empty keys pass a quarter of
// the live ones (at least 4096), so they stay bounded instead of growing with every claim - even
// when the index's other documents keep most keys live, which used to stop compaction until empty
// keys were the majority.
func TestEmptyKeysFromChurnedSortKeysAreCompacted(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/churn")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	id, _ := codec.RandomUUID()
	s := NewMemoryStoreWithOptions(Descriptor{IndexID: id, NamespaceID: "ns/churn", FieldName: "_c",
		Fields: []string{"_c", "status", "updatedAt"}, Type: IndexTypeBTree}, d, MemoryOptions{HistoryEvents: 2048})
	commit := func() codec.Hash {
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		c, err := d.AppendCommit(document.Transaction{ID: txID, BaseVersion: head, Timestamp: codec.TimestampNow(), AuthorNodeID: author},
			head, document.EmptyDocumentTree(), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		head = c.Hash
		return head
	}
	put := func(doc codec.UUID, body string) {
		if err := s.PutDocument(doc, commit(), body); err != nil {
			t.Fatal(err)
		}
		s.NeedsRebuild() // what the server's commit path does after each commit: moves the head
	}
	// Many live keys that never change, as a busy collection's tasks keep theirs.
	for i := 0; i < 20_000; i++ {
		doc, _ := codec.RandomUUID()
		put(doc, fmt.Sprintf(`{"_c":"tasks","status":"open","updatedAt":"2026-01-01T00:00:%05d"}`, i))
	}
	jobs := make([]codec.UUID, 50)
	for i := range jobs {
		jobs[i], _ = codec.RandomUUID()
	}
	live := 20_000 + len(jobs)
	for c := 0; c < 60_000; c++ {
		put(jobs[c%len(jobs)], fmt.Sprintf(`{"_c":"jobs","status":"pending","updatedAt":"2026-02-01T%09d"}`, c))
	}
	s.log.mu.Lock()
	keys := s.log.keys.len()
	s.log.mu.Unlock()
	// Without the trigger, 60k claims leave ~60k empty keys (the old rule waited for 2 x live).
	if bound := live + 2*max(4096, live/4) + 2*2048; keys > bound {
		t.Fatalf("%d keys after 60k sort-key rewrites over %d live documents, want at most %d", keys, live, bound)
	}
	ids, err := s.Lookup(CompositeKey{Parts: []Key{StringKey{Value: "jobs"}, StringKey{Value: "pending"}, StringKey{Value: "2026-02-01T000059999"}}}, nil)
	if err != nil || len(ids) != 1 {
		t.Fatalf("the newest job key after compaction: %v, %v", ids, err)
	}
}
