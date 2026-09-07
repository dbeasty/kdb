package index_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
)

// TestCompositeAndVectorKeysDoNotPanic guards the landmine the event log used to carry: keys
// were used directly as Go map keys, and CompositeKey and VectorKey hold slices, so indexing
// a composite or a vector panicked with "hash of unhashable type" the first time one was Put.
// Bucketing by the canonical key string fixes it.
func TestCompositeAndVectorKeysDoNotPanic(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/unhashable")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	desc := index.Descriptor{FieldName: "a", Fields: []string{"a", "b"}, Type: index.IndexTypeHash, CreatedAtHash: head}
	store := index.NewMemoryStore(desc, d)

	composite := index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "x"}, index.Int64Key{Value: 7}}}
	docA, _ := codec.RandomUUID()
	if err := store.Put(index.Entry{DocID: docA, Key: composite, CommitHash: head}); err != nil {
		t.Fatal(err)
	}
	ids, err := store.Lookup(composite, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != docA {
		t.Fatalf("composite lookup gave %v, want the one document", ids)
	}

	eng := index.NewVersionedEngine(d)
	docB, _ := codec.RandomUUID()
	vec := index.NewVectorKey([]float32{0.5, -1.25, 3})
	if err := eng.Put(index.Entry{DocID: docB, Key: vec, CommitHash: head}); err != nil {
		t.Fatal(err)
	}
	if ids, err := eng.Lookup(vec, nil); err != nil || len(ids) != 1 {
		t.Fatalf("vector key lookup gave %v (err %v), want the one document", ids, err)
	}
}

// TestKeyStringIsInjective: two keys share a bucket exactly when they are equal, so a
// composite whose parts merely concatenate to the same text must not collide.
func TestKeyStringIsInjective(t *testing.T) {
	pairs := [][2]index.Key{
		{index.StringKey{Value: "a,b"}, index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a"}, index.StringKey{Value: "b"}}}},
		{index.StringKey{Value: "1"}, index.Int64Key{Value: 1}},
		{index.Int32Key{Value: 1}, index.Int64Key{Value: 1}},
		{index.Int64Key{Value: 1}, index.Float64Key{Value: 1}},
		{index.NullKey{}, index.StringKey{Value: "NULL"}},
		{index.BoolKey{Value: true}, index.StringKey{Value: "true"}},
		{index.NewVectorKey([]float32{1, 2}), index.NewVectorKey([]float32{1, 2, 0})},
		{
			index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a,b"}, index.StringKey{Value: "c"}}},
			index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a"}, index.StringKey{Value: "b,c"}}},
		},
	}
	for _, p := range pairs {
		if index.KeyString(p[0]) == index.KeyString(p[1]) {
			t.Errorf("distinct keys %#v and %#v share the string %q", p[0], p[1], index.KeyString(p[0]))
		}
	}
	same := index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a"}, index.Int64Key{Value: 2}}}
	other := index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a"}, index.Int64Key{Value: 2}}}
	if index.KeyString(same) != index.KeyString(other) {
		t.Error("equal composite keys must share a bucket")
	}
}

// TestCompareKeysOrdersVectors: CompareKeys used to fall through to 0 for VectorKey, which
// made every vector key compare equal and range scans over them arbitrary.
func TestCompareKeysOrdersVectors(t *testing.T) {
	a := index.NewVectorKey([]float32{1, 2})
	b := index.NewVectorKey([]float32{1, 3})
	shorter := index.NewVectorKey([]float32{1})
	if index.CompareKeys(a, b) >= 0 {
		t.Error("[1 2] must sort before [1 3]")
	}
	if index.CompareKeys(b, a) <= 0 {
		t.Error("comparison must be antisymmetric")
	}
	if index.CompareKeys(a, index.NewVectorKey([]float32{1, 2})) != 0 {
		t.Error("equal vectors compare equal")
	}
	if index.CompareKeys(shorter, a) >= 0 {
		t.Error("a prefix sorts before the longer vector")
	}
}

// TestSnapshotRoundTripsEveryKeyType: encoding a VectorKey used to panic outright, which
// meant one vector entry made the whole index unsnapshottable.
func TestSnapshotRoundTripsEveryKeyType(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/snapshot-keys")
	head, _ := d.Head()
	id, _ := codec.RandomUUID()
	keys := []index.Key{
		index.NullKey{},
		index.BoolKey{Value: true},
		index.Int32Key{Value: -7},
		index.Int64Key{Value: 1 << 40},
		index.Float64Key{Value: 1.5},
		index.TimestampKey{EpochMillis: 1725500000000},
		index.StringKey{Value: `pipe| comma, backslash\ done`},
		index.UUIDKey{ID: id},
		index.NewVectorKey([]float32{0.25, -0.5, 8}),
		index.CompositeKey{Parts: []index.Key{index.StringKey{Value: "a,b"}, index.Int64Key{Value: 3}}},
	}
	eng := index.NewVersionedEngine(d)
	for _, k := range keys {
		docID, _ := codec.RandomUUID()
		if err := eng.Put(index.Entry{DocID: docID, Key: k, CommitHash: head}); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := eng.SnapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	restored := index.NewVersionedEngine(d)
	if err := restored.RestoreSnapshotBytes(snap); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		before, err := eng.Lookup(k, nil)
		if err != nil {
			t.Fatal(err)
		}
		after, err := restored.Lookup(k, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) != 1 || len(after) != 1 || before[0] != after[0] {
			t.Errorf("key %s: before %v, after %v", index.KeyString(k), before, after)
		}
	}
}
