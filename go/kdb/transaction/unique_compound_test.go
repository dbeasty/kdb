package transaction_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// compoundSchema declares UNIQUE (org, slug) alongside a single-field UNIQUE email, so tests can
// prove the two mechanisms are the same mechanism.
func compoundSchema(t *testing.T) schema.KdbSchema {
	t.Helper()
	sch, err := schema.BuildWithConstraints([]schema.Field{
		schema.MustField("org", schema.StringType{}, false, false, false),
		schema.MustField("slug", schema.StringType{}, false, false, false),
		schema.MustField("email", schema.StringType{}, false, true, true),
	}, []schema.UniqueConstraint{{Fields: []string{"org", "slug"}}}, 1, codec.Timestamp{}, "compound")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// TestUniqueKeysForCompoundTupleIsOneKey: a document with every part present claims exactly one
// key for the compound constraint, whose Fields is the ordered tuple and whose Value is the
// canonical JSON array of the parts.
func TestUniqueKeysForCompoundTupleIsOneKey(t *testing.T) {
	sch := compoundSchema(t)
	keys, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"slug":"home","org":"acme","n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected one compound key (no email present), got %v", keys)
	}
	if got := keys[0].FieldNames(); len(got) != 2 || got[0] != "org" || got[1] != "slug" {
		t.Fatalf("fields = %v", got)
	}
	if keys[0].FieldName() != "(org, slug)" {
		t.Fatalf("display name = %q", keys[0].FieldName())
	}
	if keys[0].Value != `["acme","home"]` {
		t.Fatalf("value = %q", keys[0].Value)
	}
}

// TestUniqueKeysForCompoundIsSparse pins §9.6's sparse rule: any absent or null part means the
// tuple claims nothing, even when the other parts are present.
func TestUniqueKeysForCompoundIsSparse(t *testing.T) {
	sch := compoundSchema(t)
	for _, body := range []string{`{"org":"acme"}`, `{"slug":"home"}`, `{"org":"acme","slug":null}`, `{"org":null,"slug":"home"}`, `{}`} {
		keys, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, body))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if len(keys) != 0 {
			t.Fatalf("%s: expected no claimed keys, got %v", body, keys)
		}
	}
}

// TestUniqueKeysForSingleFieldIsTheOneTupleCase: the single-field UNIQUE flag produces a 1-tuple
// key through the same path, so its behaviour is unchanged by the generalisation - and a document
// with both an email and a full (org, slug) claims both keys.
func TestUniqueKeysForSingleFieldIsTheOneTupleCase(t *testing.T) {
	sch := compoundSchema(t)
	keys, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"org":"acme","slug":"home","email":"a@b.c"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected email + compound keys, got %v", keys)
	}
	if keys[0].FieldName() != "email" || keys[0].Value != `["a@b.c"]` {
		t.Fatalf("single-field key = %+v", keys[0])
	}
	if keys[1].FieldName() != "(org, slug)" {
		t.Fatalf("compound key = %+v", keys[1])
	}
}

// TestCompoundUniqueCanonicalizesEachPart: `1` and `1.0` collide inside a tuple exactly as they do
// for a single field.
func TestCompoundUniqueCanonicalizesEachPart(t *testing.T) {
	sch, err := schema.BuildWithConstraints([]schema.Field{
		schema.MustField("a", schema.Float64Type{}, false, false, false),
		schema.MustField("b", schema.StringType{}, false, false, false),
	}, []schema.UniqueConstraint{{Fields: []string{"a", "b"}}}, 1, codec.Timestamp{}, "")
	if err != nil {
		t.Fatal(err)
	}
	x, _ := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"a":1,"b":"z"}`))
	y, _ := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"b":"z","a":1.0}`))
	if len(x) != 1 || len(y) != 1 || x[0] != y[0] {
		t.Fatalf("expected identical keys, got %v and %v", x, y)
	}
}

func compoundEngine(t *testing.T) (transaction.Engine, *dag.InMemoryCommitDag, *mem.InMemoryStorageAdapter, *transaction.UniqueKeyRegistry) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(uniqueNS)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	reg := transaction.NewUniqueKeyRegistry()
	eng := transaction.NewEngineWithOptions(transaction.ConflictPolicyLastWrite, nil, transaction.EngineOptions{UniqueKeys: reg})
	return eng, d, store, reg
}

func commitWrite(t *testing.T, eng transaction.Engine, d *dag.InMemoryCommitDag, store *mem.InMemoryStorageAdapter, sch schema.KdbSchema, id codec.UUID, body string) transaction.TransactionResult {
	t.Helper()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := (&transaction.Builder{NamespaceID: uniqueNS, BaseVersion: head, Schema: sch}).Write(id, body).Build(codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Commit(tx, d, store, sch, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func commitDelete(t *testing.T, eng transaction.Engine, d *dag.InMemoryCommitDag, store *mem.InMemoryStorageAdapter, sch schema.KdbSchema, id codec.UUID) transaction.TransactionResult {
	t.Helper()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := (&transaction.Builder{NamespaceID: uniqueNS, BaseVersion: head, Schema: sch}).Delete(id).Build(codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Commit(tx, d, store, sch, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestCompoundUniqueClaimAndRetractAtCommit drives the engine end to end: the first document
// claims (acme, home); a second document with the same tuple is rejected with a UNIQUE
// violation naming the tuple; a different tuple in the same org is fine; and deleting the first
// document retracts the claim so the tuple can be taken again.
func TestCompoundUniqueClaimAndRetractAtCommit(t *testing.T) {
	sch := compoundSchema(t)
	eng, d, store, reg := compoundEngine(t)
	first, _ := codec.RandomUUID()
	second, _ := codec.RandomUUID()

	if res := commitWrite(t, eng, d, store, sch, first, `{"org":"acme","slug":"home"}`); !isSuccess(res) {
		t.Fatalf("first claim: %+v", res)
	}
	if reg.Len() != 1 {
		t.Fatalf("registry len = %d", reg.Len())
	}
	res := commitWrite(t, eng, d, store, sch, second, `{"org":"acme","slug":"home"}`)
	se, ok := res.(transaction.ResultSchemaError)
	if !ok {
		t.Fatalf("expected a schema error for a duplicate tuple, got %T", res)
	}
	if v := se.Violations[0].Violations[0]; v.FieldName != "(org, slug)" || v.Detail == "" {
		t.Fatalf("violation = %+v", v)
	}
	if res := commitWrite(t, eng, d, store, sch, second, `{"org":"acme","slug":"about"}`); !isSuccess(res) {
		t.Fatalf("distinct tuple in the same org: %+v", res)
	}
	if res := commitDelete(t, eng, d, store, sch, first); !isSuccess(res) {
		t.Fatalf("delete: %+v", res)
	}
	third, _ := codec.RandomUUID()
	if res := commitWrite(t, eng, d, store, sch, third, `{"org":"acme","slug":"home"}`); !isSuccess(res) {
		t.Fatalf("re-claim after delete: %+v", res)
	}
}

// TestCompoundUniqueSparseDocumentsNeverCollide: many documents may share a partial tuple.
func TestCompoundUniqueSparseDocumentsNeverCollide(t *testing.T) {
	sch := compoundSchema(t)
	eng, d, store, reg := compoundEngine(t)
	for i := 0; i < 3; i++ {
		id, _ := codec.RandomUUID()
		if res := commitWrite(t, eng, d, store, sch, id, `{"org":"acme"}`); !isSuccess(res) {
			t.Fatalf("sparse doc %d: %+v", i, res)
		}
	}
	if reg.Len() != 0 {
		t.Fatalf("sparse documents claimed %d keys", reg.Len())
	}
}

// TestRegistryRebuildWithCompoundConstraints: a rebuild over stored data claims one key per full
// tuple and reports a stored duplicate tuple naming the constraint.
func TestRegistryRebuildWithCompoundConstraints(t *testing.T) {
	sch := compoundSchema(t)
	store := mem.NewInMemoryStorageAdapter()
	for _, body := range []string{`{"org":"acme","slug":"a"}`, `{"org":"acme","slug":"b"}`, `{"org":"acme"}`} {
		if err := store.PutDocument(uniqueNS, doc(t, body)); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := store.CommitTree(uniqueNS, codec.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	r := transaction.NewUniqueKeyRegistry()
	if err := r.Rebuild(uniqueNS, store, tree.TreeHash, sch); err != nil {
		t.Fatalf("clean rebuild: %v", err)
	}
	if r.Len() != 2 {
		t.Fatalf("expected 2 claimed tuples, got %d", r.Len())
	}
	if err := store.PutDocument(uniqueNS, doc(t, `{"org":"acme","slug":"a"}`)); err != nil {
		t.Fatal(err)
	}
	tree, err = store.CommitTree(uniqueNS, tree.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Rebuild(uniqueNS, store, tree.TreeHash, sch)
	var dup *transaction.UniqueConstraintError
	if !errors.As(err, &dup) {
		t.Fatalf("expected a UniqueConstraintError from a dirty rebuild, got %v", err)
	}
	if dup.Key.FieldName() != "(org, slug)" {
		t.Fatalf("duplicate names %q, want the tuple", dup.Key.FieldName())
	}
}

func isSuccess(res transaction.TransactionResult) bool {
	_, ok := res.(transaction.ResultSuccess)
	return ok
}
