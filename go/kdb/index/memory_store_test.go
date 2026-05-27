package index_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
)

func TestMemoryStoreLookupAndSnapshot(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/index")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	desc := index.Descriptor{
		IndexID:       mustUUID(t),
		NamespaceID:   "ns/index",
		FieldName:     "status",
		Fields:        []string{"status"},
		Type:          index.IndexTypeHash,
		CreatedAtHash: head,
	}
	store := index.NewMemoryStore(desc, d)
	docID, _ := codec.RandomUUID()
	key := index.StringKey{Value: "active"}
	if err := store.Put(index.Entry{DocID: docID, Key: key, CommitHash: head}); err != nil {
		t.Fatal(err)
	}
	ids, err := store.Lookup(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != docID {
		t.Fatalf("lookup: got %v", ids)
	}
	snap, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	store2 := index.NewMemoryStore(desc, d)
	if err := store2.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	ids2, _ := store2.Lookup(key, nil)
	if len(ids2) != 1 {
		t.Fatalf("restored lookup: %v", ids2)
	}
}

func TestVersionedEngineRange(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/ver")
	head, _ := d.Head()
	eng := index.NewVersionedEngine(d)
	docA, _ := codec.RandomUUID()
	docB, _ := codec.RandomUUID()
	_ = eng.Put(index.Entry{DocID: docA, Key: index.StringKey{Value: "a"}, CommitHash: head})
	_ = eng.Put(index.Entry{DocID: docB, Key: index.StringKey{Value: "b"}, CommitHash: head})
	ids, err := eng.Range(index.StringKey{Value: "a"}, index.StringKey{Value: "z"}, nil, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("range: got %d ids", len(ids))
	}
	buckets, err := eng.HeadBuckets()
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 {
		t.Fatalf("head buckets: %d", len(buckets))
	}
}

func mustUUID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestMemoryStoreDeletePrunes(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/del")
	head, _ := d.Head()
	desc := index.Descriptor{FieldName: "x", Type: index.IndexTypeHash, CreatedAtHash: head}
	store := index.NewMemoryStore(desc, d)
	docID, _ := codec.RandomUUID()
	key := index.StringKey{Value: "k"}
	_ = store.Put(index.Entry{DocID: docID, Key: key, CommitHash: head})
	_ = store.Delete(docID, head)
	ids, _ := store.Lookup(key, nil)
	if len(ids) != 0 {
		t.Fatalf("expected empty after delete, got %v", ids)
	}
}
