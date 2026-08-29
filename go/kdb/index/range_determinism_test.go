package index_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
)

func newEngineWithDocs(t *testing.T, ns string, key index.Key, n int) (*index.VersionedEngine, []codec.UUID) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	eng := index.NewVersionedEngine(d)
	ids := make([]codec.UUID, 0, n)
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Put(index.Entry{DocID: id, Key: key, CommitHash: head}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return eng, ids
}

func idsEqual(a, b []codec.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A bucket is a map, so ranging it directly gave a different order on every call. Without a
// limit that is invisible - the same set comes back, just shuffled - but with one it changes
// *which* documents are returned, so a limited range query answered differently each time and
// paging through an index could show a document twice or skip it entirely.
func TestRangeWithLimitIsRepeatable(t *testing.T) {
	key := index.StringKey{Value: "same"}
	eng, _ := newEngineWithDocs(t, "ns/range-determinism", key, 10)

	first, err := eng.Range(nil, nil, nil, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("got %d ids, want 3", len(first))
	}
	for i := 0; i < 40; i++ {
		again, err := eng.Range(nil, nil, nil, 3, true)
		if err != nil {
			t.Fatal(err)
		}
		if !idsEqual(first, again) {
			t.Fatalf("call %d returned a different set of documents:\n first %v\n again %v",
				i+2, first, again)
		}
	}
}

// Paging must partition the documents rather than overlap or skip: taking the first k, then the
// first k+1, the second call's prefix has to be the first call's result.
func TestRangeLimitsAreNestedPrefixes(t *testing.T) {
	key := index.StringKey{Value: "same"}
	eng, ids := newEngineWithDocs(t, "ns/range-prefix", key, 8)

	prev, err := eng.Range(nil, nil, nil, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	for k := 2; k <= len(ids); k++ {
		got, err := eng.Range(nil, nil, nil, k, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != k {
			t.Fatalf("limit %d returned %d ids", k, len(got))
		}
		if !idsEqual(prev, got[:len(prev)]) {
			t.Fatalf("limit %d is not an extension of limit %d:\n %v\n %v", k, len(prev), prev, got)
		}
		prev = got
	}
}

func TestLookupIsRepeatable(t *testing.T) {
	key := index.StringKey{Value: "same"}
	eng, ids := newEngineWithDocs(t, "ns/lookup-determinism", key, 10)

	first, err := eng.Lookup(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(ids) {
		t.Fatalf("lookup returned %d ids, want %d", len(first), len(ids))
	}
	for i := 0; i < 40; i++ {
		again, err := eng.Lookup(key, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !idsEqual(first, again) {
			t.Fatalf("call %d returned a different order:\n first %v\n again %v", i+2, first, again)
		}
	}
}

// A non-positive limit means "no limit" - a caller passing 0 wants everything, not one row.
func TestRangeTreatsNonPositiveLimitAsUnlimited(t *testing.T) {
	key := index.StringKey{Value: "same"}
	eng, ids := newEngineWithDocs(t, "ns/range-limit-zero", key, 6)

	for _, limit := range []int{0, -1, -100} {
		got, err := eng.Range(nil, nil, nil, limit, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(ids) {
			t.Errorf("limit %d returned %d ids, want all %d", limit, len(got), len(ids))
		}
	}
}

// A limit larger than the number of matches returns everything, not padding or an error.
func TestRangeLimitBeyondTheDataReturnsEverything(t *testing.T) {
	key := index.StringKey{Value: "same"}
	eng, ids := newEngineWithDocs(t, "ns/range-limit-large", key, 4)

	got, err := eng.Range(nil, nil, nil, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("got %d ids, want %d", len(got), len(ids))
	}
}

// Key bounds are inclusive at both ends, and the direction flag reverses key order.
func TestRangeKeyBoundsAndDirection(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/range-keys")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	eng := index.NewVersionedEngine(d)

	byKey := map[string]codec.UUID{}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		id, _ := codec.RandomUUID()
		byKey[k] = id
		if err := eng.Put(index.Entry{DocID: id, Key: index.StringKey{Value: k}, CommitHash: head}); err != nil {
			t.Fatal(err)
		}
	}

	asc, err := eng.Range(index.StringKey{Value: "b"}, index.StringKey{Value: "d"}, nil, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []codec.UUID{byKey["b"], byKey["c"], byKey["d"]}
	if !idsEqual(asc, want) {
		t.Fatalf("ascending [b,d] gave %v, want %v", asc, want)
	}

	desc, err := eng.Range(index.StringKey{Value: "b"}, index.StringKey{Value: "d"}, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	want = []codec.UUID{byKey["d"], byKey["c"], byKey["b"]}
	if !idsEqual(desc, want) {
		t.Fatalf("descending [b,d] gave %v, want %v", desc, want)
	}

	// An open lower bound reaches the first key, an open upper bound the last.
	openLow, err := eng.Range(nil, index.StringKey{Value: "b"}, nil, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if !idsEqual(openLow, []codec.UUID{byKey["a"], byKey["b"]}) {
		t.Fatalf("open lower bound gave %v", openLow)
	}
	openHigh, err := eng.Range(index.StringKey{Value: "d"}, nil, nil, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if !idsEqual(openHigh, []codec.UUID{byKey["d"], byKey["e"]}) {
		t.Fatalf("open upper bound gave %v", openHigh)
	}

	// A range that matches nothing is empty rather than everything.
	empty, err := eng.Range(index.StringKey{Value: "x"}, index.StringKey{Value: "z"}, nil, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("a range matching nothing gave %v", empty)
	}
}
