package transaction_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

const uniqueNS = "app/data"

func uniqueSchema(t *testing.T) schema.KdbSchema {
	t.Helper()
	sch, err := schema.Build([]schema.Field{
		schema.MustField("email", schema.StringType{}, false, true, true),
		schema.MustField("name", schema.StringType{}, false, false, false),
	}, 1, codec.Timestamp{}, "unique email")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func doc(t *testing.T, json string) document.Document {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return document.Document{ID: id, JSON: json}
}

// TestUniqueKeysForSkipsAbsentAndNull pins the NULL semantics: an optional unique field that a
// document omits claims nothing, so many documents may omit it. The alternative - treating
// absence as a claimable value - would let exactly one document in the namespace leave the field
// out, which is not what "optional and unique" means anywhere else.
func TestUniqueKeysForSkipsAbsentAndNull(t *testing.T) {
	sch := uniqueSchema(t)
	for _, body := range []string{`{"name":"a"}`, `{"email":null,"name":"a"}`} {
		keys, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, body))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if len(keys) != 0 {
			t.Fatalf("%s: expected no claimed keys, got %v", body, keys)
		}
	}
	keys, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"email":"a@b.c"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].FieldName() != "email" {
		t.Fatalf("expected one email key, got %v", keys)
	}
}

// TestUniqueKeyCanonicalizationIgnoresSpelling proves two documents whose field decodes to the
// same value collide even when the JSON that produced it differs - otherwise a client could
// evade a unique constraint just by writing 1.0 where another wrote 1.
func TestUniqueKeyCanonicalizationIgnoresSpelling(t *testing.T) {
	sch, err := schema.Build([]schema.Field{
		schema.MustField("n", schema.Float64Type{}, false, true, true),
	}, 1, codec.Timestamp{}, "unique number")
	if err != nil {
		t.Fatal(err)
	}
	a, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"n": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := transaction.UniqueKeysFor(uniqueNS, sch, doc(t, `{"n": 1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected one key each, got %v and %v", a, b)
	}
	if a[0] != b[0] {
		t.Fatalf("1 and 1.0 produced different keys: %q vs %q", a[0].Value, b[0].Value)
	}
}

// TestRegistryApplyRetractsThenClaims covers the ordering Apply promises: a value moving from one
// document to another must not transiently vanish, and a retraction recorded against a document
// that no longer owns the key must not free somebody else's claim.
func TestRegistryApplyRetractsThenClaims(t *testing.T) {
	r := transaction.NewUniqueKeyRegistry()
	key := transaction.NewUniqueKey(uniqueNS, []string{"email"}, `["a@b.c"]`)
	first, _ := codec.RandomUUID()
	second, _ := codec.RandomUUID()

	r.Apply(nil, map[transaction.UniqueKey]codec.UUID{key: first})
	if owner, ok := r.Owner(key); !ok || owner != first {
		t.Fatalf("expected %s to own the key, got %v/%v", first, owner, ok)
	}

	// Move the value to `second`, retracting `first`.
	r.Apply(
		map[transaction.UniqueKey]codec.UUID{key: first},
		map[transaction.UniqueKey]codec.UUID{key: second},
	)
	if owner, ok := r.Owner(key); !ok || owner != second {
		t.Fatalf("expected %s to own the key after the move, got %v/%v", second, owner, ok)
	}

	// A stale retraction naming the previous owner must be ignored, not honored.
	r.Apply(map[transaction.UniqueKey]codec.UUID{key: first}, nil)
	if owner, ok := r.Owner(key); !ok || owner != second {
		t.Fatalf("a stale retraction freed the current owner's key: %v/%v", owner, ok)
	}
}

// TestRegistryRebuildReportsExistingDuplicates proves a rebuild does not silently pick a winner
// when stored data already violates a constraint the schema declares. Silently keeping the
// first-seen owner would make the violation permanent and invisible.
func TestRegistryRebuildReportsExistingDuplicates(t *testing.T) {
	store := mem.NewInMemoryStorageAdapter()
	sch := uniqueSchema(t)
	for i := 0; i < 2; i++ {
		if err := store.PutDocument(uniqueNS, doc(t, `{"email":"dup@b.c"}`)); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := store.CommitTree(uniqueNS, codec.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	r := transaction.NewUniqueKeyRegistry()
	err = r.Rebuild(uniqueNS, store, tree.TreeHash, sch)
	var dup *transaction.UniqueConstraintError
	if !errors.As(err, &dup) {
		t.Fatalf("expected a UniqueConstraintError from a dirty rebuild, got %v", err)
	}
	if dup.Key.FieldName() != "email" {
		t.Fatalf("expected the duplicate to name the email field, got %+v", dup)
	}
}

// TestRegistryRebuildIsCleanOnDistinctValues is the negative control for the test above: a
// rebuild over data that satisfies the constraint must succeed and claim every value.
func TestRegistryRebuildIsCleanOnDistinctValues(t *testing.T) {
	store := mem.NewInMemoryStorageAdapter()
	sch := uniqueSchema(t)
	for _, email := range []string{"a@x.c", "b@x.c", "c@x.c"} {
		if err := store.PutDocument(uniqueNS, doc(t, `{"email":"`+email+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := store.CommitTree(uniqueNS, codec.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	r := transaction.NewUniqueKeyRegistry()
	if err := r.Rebuild(uniqueNS, store, tree.TreeHash, sch); err != nil {
		t.Fatalf("clean rebuild failed: %v", err)
	}
	if r.Len() != 3 {
		t.Fatalf("expected 3 claimed keys, got %d", r.Len())
	}
}
