package transaction_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// failingReads is an adapter whose document reads fail for one id - a cold body read hitting a
// damaged segment, say. Not-found is (nil, nil); this is (nil, err), a different answer.
type failingReads struct {
	*mem.InMemoryStorageAdapter
	failID codec.UUID
	fail   bool
}

var errInjectedRead = errors.New("injected read failure")

func (f *failingReads) GetDocument(ns string, id codec.UUID, at codec.Hash) (*document.Document, error) {
	if f.fail && id == f.failID {
		return nil, errInjectedRead
	}
	return f.InMemoryStorageAdapter.GetDocument(ns, id, at)
}

func seedFailing(t *testing.T) (*dag.InMemoryCommitDag, *failingReads, codec.UUID, codec.Hash) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("app/readfail")
	if err != nil {
		t.Fatal(err)
	}
	store := &failingReads{InMemoryStorageAdapter: mem.NewInMemoryStorageAdapter()}
	base, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"a":1,"b":2,"c":3}`)
	if err != nil {
		t.Fatal(err)
	}
	seed := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	res, err := seed.Commit(newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON}), d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	store.failID = doc.ID
	store.fail = true
	return d, store, doc.ID, res.(transaction.ResultSuccess).Commit.Hash
}

// A merge patch over a document whose read failed must not be staged as the whole document:
// that silently drops every field the patch does not name.
func TestWriteOverUnreadableDocumentFailsInsteadOfTruncating(t *testing.T) {
	for _, policy := range []transaction.ConflictPolicy{transaction.ConflictPolicyLastWrite, transaction.ConflictPolicyStrict} {
		d, store, id, head := seedFailing(t)
		engine := transaction.NewEngine(policy, nil)
		res, err := engine.Commit(newTx(head, document.WriteOp{DocID: id, Patch: `{"a":9}`}), d, store, schema.None(), nil, "")
		if !errors.Is(err, errInjectedRead) {
			t.Fatalf("policy %v: commit over an unreadable document: got result %T, err %v; want the read error", policy, res, err)
		}
		if h, _ := d.Head(); h != head {
			t.Fatalf("policy %v: head moved after a failed read", policy)
		}
	}
}

// A delete under STRICT reads base and target to detect a concurrent change; two failed reads
// used to compare equal (both nil) and pass.
func TestStrictDeleteOverUnreadableDocumentFails(t *testing.T) {
	d, store, id, head := seedFailing(t)
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	_, err := engine.Commit(newTx(head, document.DeleteOp{DocID: id}), d, store, schema.None(), nil, "")
	if !errors.Is(err, errInjectedRead) {
		t.Fatalf("strict delete over an unreadable document: err %v; want the read error", err)
	}
}

// insert-if-absent must not read a failed lookup as "absent".
func TestExpectAbsentOverUnreadableDocumentFails(t *testing.T) {
	d, store, id, head := seedFailing(t)
	engine := transaction.NewEngineWithOptions(transaction.ConflictPolicyLastWrite, nil, transaction.EngineOptions{Preconditions: true})
	tx := newTx(head, document.WriteOp{DocID: id, Patch: `{"a":9}`, Replace: true})
	tx.Preconditions = []document.Precondition{{OpIndex: 0, Kind: document.ExpectAbsent}}
	_, err := engine.Commit(tx, d, store, schema.None(), nil, "")
	if !errors.Is(err, errInjectedRead) {
		t.Fatalf("insert-if-absent over an unreadable document: err %v; want the read error", err)
	}
}
