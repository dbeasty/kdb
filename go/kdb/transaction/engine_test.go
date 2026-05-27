package transaction_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

func TestCommitSingleWriteSucceeds(t *testing.T) {
	ns := "app/tx"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	base, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"v":"a"}`)
	if err != nil {
		t.Fatal(err)
	}
	tx := newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON})
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	res, err := engine.Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected success, got %T", res)
	}
	if len(success.Commit.Operations) != 1 {
		t.Fatalf("expected 1 op, got %d", len(success.Commit.Operations))
	}
}

func TestStrictConflictOnConcurrentWrite(t *testing.T) {
	ns := "app/conflict"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()
	docID, _ := codec.RandomUUID()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

	tx1 := newTx(base, document.WriteOp{DocID: docID, Patch: `{"v":"1"}`})
	if _, ok := mustCommit(t, engine, tx1, d, store).(transaction.ResultSuccess); !ok {
		t.Fatal("tx1 failed")
	}
	head, _ := d.Head()
	tx2a := newTx(head, document.WriteOp{DocID: docID, Patch: `{"v":"2a"}`})
	tx2b := newTx(head, document.WriteOp{DocID: docID, Patch: `{"v":"2b"}`})
	if _, ok := mustCommit(t, engine, tx2a, d, store).(transaction.ResultSuccess); !ok {
		t.Fatal("tx2a failed")
	}
	res, err := engine.Commit(tx2b, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(transaction.ResultConflict); !ok {
		t.Fatalf("expected conflict, got %T", res)
	}
}

func TestFileWriteMissingBlobRejected(t *testing.T) {
	ns := "app/file"
	d, _ := dag.NewInMemoryCommitDag(ns)
	store := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()
	missing, _ := codec.HashFromHex("0000000000000000000000000000000000000000000000000000000000000001")
	tx := newTx(base, document.FileWriteOp{Path: "attachments/x", BlobHash: missing})
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	res, err := engine.Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	schemaErr, ok := res.(transaction.ResultSchemaError)
	if !ok {
		t.Fatalf("expected schema error, got %T", res)
	}
	if len(schemaErr.Violations) == 0 {
		t.Fatal("expected violations")
	}
}

func TestReplayIdempotent(t *testing.T) {
	ns := "app/idempotent"
	d, _ := dag.NewInMemoryCommitDag(ns)
	store := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()
	doc, _ := document.FromJSON(`{"v":"x"}`)
	tx := newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON})
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	landed := mustCommit(t, engine, tx, d, store).(transaction.ResultSuccess)
	first, err := engine.Replay(tx, d, store, schema.None(), landed.Commit.Hash, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Replay(tx, d, store, schema.None(), landed.Commit.Hash, "")
	if err != nil {
		t.Fatal(err)
	}
	f1 := first.(transaction.ResultSuccess)
	f2 := second.(transaction.ResultSuccess)
	if f1.Commit.Hash != f2.Commit.Hash {
		t.Fatalf("replay not idempotent: %s vs %s", f1.Commit.Hash.Hex(), f2.Commit.Hash.Hex())
	}
}

func newTx(base codec.Hash, ops ...document.Op) document.Transaction {
	id, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	return document.Transaction{
		ID:           id,
		BaseVersion:  base,
		Operations:   ops,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
}

func mustCommit(t *testing.T, engine transaction.Engine, tx document.Transaction, d *dag.InMemoryCommitDag, store *mem.InMemoryStorageAdapter) transaction.TransactionResult {
	t.Helper()
	res, err := engine.Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return res
}
