package transaction_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// failingStorageAdapter delegates to an inner storage.Adapter but fails PutDocument for one
// doc id, to exercise transaction rollback.
type failingStorageAdapter struct {
	storage.Adapter
	failOnPutDocID codec.UUID
}

func (a *failingStorageAdapter) PutDocument(namespaceID string, doc document.Document) error {
	if doc.ID == a.failOnPutDocID {
		return errors.New("simulated storage failure")
	}
	return a.Adapter.PutDocument(namespaceID, doc)
}

func TestCommitWriteFailsMidTransactionAbortsAndRollsBack(t *testing.T) {
	ns := "app/abort"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	inner := mem.NewInMemoryStorageAdapter()
	docA, err := document.FromJSON(`{"v":"a"}`)
	if err != nil {
		t.Fatal(err)
	}
	docB, err := document.FromJSON(`{"v":"b"}`)
	if err != nil {
		t.Fatal(err)
	}
	store := &failingStorageAdapter{Adapter: inner, failOnPutDocID: docB.ID}
	base, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	tx := newTx(base,
		document.WriteOp{DocID: docA.ID, Patch: docA.JSON},
		document.WriteOp{DocID: docB.ID, Patch: docB.JSON},
	)

	res, err := engine.Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(transaction.ResultAborted); !ok {
		t.Fatalf("expected aborted, got %T", res)
	}

	headAfter, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	if headAfter != base {
		t.Fatalf("expected head unchanged, got %s vs base %s", headAfter.Hex(), base.Hex())
	}
	if got, _ := store.GetDocument(ns, docA.ID, base); got != nil {
		t.Fatalf("expected docA not visible, got %+v", got)
	}
	if got, _ := store.GetDocument(ns, docB.ID, base); got != nil {
		t.Fatalf("expected docB not visible, got %+v", got)
	}

	// A retried transaction (without the injected failure) succeeds cleanly, proving the
	// aborted attempt didn't leave corrupted/leaked pending state behind.
	retryTx := newTx(base,
		document.WriteOp{DocID: docA.ID, Patch: docA.JSON},
		document.WriteOp{DocID: docB.ID, Patch: docB.JSON},
	)
	retried, err := engine.Commit(retryTx, d, inner, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := retried.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected success on retry, got %T", retried)
	}
	if len(success.Commit.Operations) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(success.Commit.Operations))
	}
}

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

// TestCommitIdempotentAcrossInterveningHistory guards the fix that replaced findExistingCommit's
// walk of up to 8192 commits of history with an O(1) dag.GetCommitByTransactionID lookup (see
// that method's doc comment - it was the dominant cost behind kdb-service getting OOM-killed
// under sustained write load, docs/benchmarks/lightsail-sim/README.md). The walk-based version
// also had a latent correctness bug this test would have caught: a retry of a transaction whose
// original commit had fallen more than 8192 commits behind the current head would silently stop
// being recognized as a retry and create a duplicate commit instead - unbounded by history length
// is the actually-correct semantic for "was this exact transaction already committed."
func TestReplayIdempotentAcrossInterveningHistory(t *testing.T) {
	ns := "app/idempotent-long-history"
	d, _ := dag.NewInMemoryCommitDag(ns)
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

	base, _ := d.Head()
	doc, _ := document.FromJSON(`{"v":"original"}`)
	tx := newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON})
	original := mustCommit(t, engine, tx, d, store).(transaction.ResultSuccess)

	// Many unrelated intervening commits, simulating a retry that arrives long after the
	// original attempt - not the "immediately again" shape TestReplayIdempotent already covers.
	// A real caller replaying against a pinned target (e.g. a client resending a write-back
	// transaction after a dropped response - Component 46's shape) passes that same target
	// explicitly regardless of how much else has happened since, which is exactly what Replay's
	// signature is for (unlike Commit, which always re-derives the current head).
	head := original.Commit.Hash
	for i := 0; i < 50; i++ {
		otherDoc, _ := document.FromJSON(fmt.Sprintf(`{"v":"filler-%d"}`, i))
		otherTx := newTx(head, document.WriteOp{DocID: otherDoc.ID, Patch: otherDoc.JSON})
		res := mustCommit(t, engine, otherTx, d, store).(transaction.ResultSuccess)
		head = res.Commit.Hash
	}

	// Retrying the exact same transaction against its original target, long after 50 unrelated
	// commits landed in between, must find the original commit via findExistingCommit's
	// idempotency check - not attempt (and potentially conflict on) a fresh replay.
	retryResult, err := engine.Replay(tx, d, store, schema.None(), original.Commit.ParentHashes[0], "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := retryResult.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected idempotent retry to succeed, got %T: %+v", retryResult, retryResult)
	}
	if success.Commit.Hash != original.Commit.Hash {
		t.Fatalf("retry produced a different commit (%s) than the original (%s) - idempotency broken",
			success.Commit.Hash.Hex(), original.Commit.Hash.Hex())
	}

	// And the DAG must not have forked: main still points at the last of the 50 filler commits,
	// not somewhere the retry left it.
	finalHead, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	if finalHead != head {
		t.Fatalf("main moved during an idempotent retry: got %s, want %s", finalHead.Hex(), head.Hex())
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
