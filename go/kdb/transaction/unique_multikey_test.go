package transaction_test

import (
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/transaction"
)

// Arrays are multikey under a unique constraint, as in a MongoDB unique index: each element is a
// claim of its own. Before this, an array was one opaque value, so `["a","b"]` and `["b","c"]`
// were "different" and both went in although they share "b" - a unique rule over a list (one
// external login linked to one account) could not be stated at all.

func claimedValues(t *testing.T, body string) []string {
	t.Helper()
	keys, err := transaction.UniqueKeysFor(uniqueNS, compoundSchema(t), doc(t, body))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, k := range keys {
		out = append(out, k.FieldName()+"="+k.Value)
	}
	sort.Strings(out)
	return out
}

func TestUniqueKeysForClaimsEachArrayElement(t *testing.T) {
	got := claimedValues(t, `{"email":["a@b.c","d@e.f"]}`)
	want := []string{`email=["a@b.c"]`, `email=["d@e.f"]`}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestUniqueKeysForCompoundWithAnArrayPartClaimsEachCombination(t *testing.T) {
	got := claimedValues(t, `{"org":"acme","slug":["home","about"]}`)
	if len(got) != 2 {
		t.Fatalf("expected one key per element of the array part, got %v", got)
	}
	got = claimedValues(t, `{"org":["acme","initech"],"slug":["home","about"]}`)
	if len(got) != 4 {
		t.Fatalf("expected the cartesian product of two array parts, got %v", got)
	}
}

func TestUniqueKeysForArrayEdgeCases(t *testing.T) {
	for body, want := range map[string]int{
		`{"email":[]}`:                   0, // an empty list claims nothing, as absence does
		`{"email":[null]}`:               0, // null elements are skipped, as a null value is
		`{"email":["a@b.c","a@b.c"]}`:    1, // a document repeating an element claims it once
		`{"email":["a@b.c",null]}`:       1,
		`{"org":"acme","slug":[]}`:       0, // an empty part leaves the tuple incomplete
		`{"email":[["nested"],"a@b.c"]}`: 2, // a nested array is one element's value
		`{"email":[{"k":1},{"k":1.0}]}`:  1, // elements are canonicalized like whole values
	} {
		if got := claimedValues(t, body); len(got) != want {
			t.Errorf("%s: claimed %v, want %d keys", body, got, want)
		}
	}
}
