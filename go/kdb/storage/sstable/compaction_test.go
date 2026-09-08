package sstable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func testShim(t *testing.T) *storio.InMemoryPlatformIO {
	t.Helper()
	return storio.NewInMemoryPlatformIO()
}

// keyFor makes a content-addressed key the way the engine does, so the
// merge is tested on the shape of key it actually sees.
func keyFor(value []byte) codec.Hash {
	var h codec.Hash
	copy(h.Bytes[:], value)
	return h
}

func writeTable(t *testing.T, shim *storio.InMemoryPlatformIO, ns string, level int, puts map[string]string, deletes []string) Handle {
	t.Helper()
	w := NewDefaultWriter(shim, ns, level)
	for k, v := range puts {
		w.Put(keyFor([]byte(k)), []byte(v))
	}
	for _, k := range deletes {
		w.Delete(keyFor([]byte(k)))
	}
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestCompactMergesEveryKey(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	a := writeTable(t, shim, ns, 0, map[string]string{"a": "1", "b": "2"}, nil)
	b := writeTable(t, shim, ns, 0, map[string]string{"c": "3"}, nil)

	merged, err := Compact(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := NewDefaultReader(shim, merged)
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, err := r.Get(keyFor([]byte(k)))
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Fatalf("%s came back as %q, want %q", k, got, want)
		}
	}
}

// The same key in two tables collapses to one entry rather than being
// written twice - which is the whole point, since a store that rewrites
// the same documents is nothing but duplicates.
func TestCompactCollapsesDuplicates(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	var inputs []Handle
	for i := 0; i < 5; i++ {
		inputs = append(inputs, writeTable(t, shim, ns, 0, map[string]string{"a": "1", "b": "2"}, nil))
	}
	merged, err := Compact(shim, ns, 1, inputs, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	index, err := NewDefaultReader(shim, merged).Index()
	if err != nil {
		t.Fatal(err)
	}
	if len(index) != 2 {
		t.Fatalf("five copies of two keys merged to %d entries", len(index))
	}
}

// A later table's opinion wins, which for values is a formality and for
// tombstones is the point.
func TestCompactLastWriterWins(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	older := writeTable(t, shim, ns, 0, map[string]string{"a": "old"}, nil)
	newer := writeTable(t, shim, ns, 0, map[string]string{"a": "new"}, nil)

	merged, err := Compact(shim, ns, 1, []Handle{older, newer}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := NewDefaultReader(shim, merged).Get(keyFor([]byte("a")))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("the older value won: %q", got)
	}
}

func TestCompactKeepsTombstonesUnlessToldOtherwise(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	withValue := writeTable(t, shim, ns, 0, map[string]string{"a": "1"}, nil)
	withDelete := writeTable(t, shim, ns, 0, nil, []string{"a"})

	// Kept: the merge might not cover every table holding the key, so
	// dropping the tombstone would resurrect it.
	merged, err := Compact(shim, ns, 1, []Handle{withValue, withDelete}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, deleted, found, err := NewDefaultReader(shim, merged).Lookup(keyFor([]byte("a")))
	if err != nil {
		t.Fatal(err)
	}
	if !found || !deleted {
		t.Fatal("the tombstone did not survive a partial compaction")
	}

	// Dropped: this merge covered everything, so nothing older can hold
	// the key and the tombstone has nothing left to shadow.
	merged, err = Compact(shim, ns, 1, []Handle{withValue, withDelete}, true, nil)
	if err != ErrNothingToCompact {
		if err != nil {
			t.Fatal(err)
		}
		_, _, found, err = NewDefaultReader(shim, merged).Lookup(keyFor([]byte("a")))
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatal("a full compaction should drop the tombstone with nothing left to shadow")
		}
	}
}

// A tombstone must not resurrect a value through the merge: the deleted
// key stays deleted when other keys are carried through.
func TestCompactDoesNotResurrectADeletedKey(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	older := writeTable(t, shim, ns, 0, map[string]string{"a": "1", "b": "2"}, nil)
	newer := writeTable(t, shim, ns, 0, map[string]string{"c": "3"}, []string{"a"})

	merged, err := Compact(shim, ns, 1, []Handle{older, newer}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := NewDefaultReader(shim, merged)
	if _, deleted, found, _ := r.Lookup(keyFor([]byte("a"))); !found || !deleted {
		t.Fatal("the deleted key came back")
	}
	if got, _ := r.Get(keyFor([]byte("b"))); string(got) != "2" {
		t.Fatalf("an untouched key was lost: %q", got)
	}
}

// The same inputs must produce the same file, or a compaction cannot be
// verified or repeated.
func TestCompactIsDeterministic(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	a := writeTable(t, shim, ns, 0, map[string]string{"a": "1", "b": "2", "c": "3"}, nil)
	b := writeTable(t, shim, ns, 0, map[string]string{"d": "4"}, nil)

	first, err := Compact(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Compact(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.FileHash != second.FileHash {
		t.Fatal("two compactions of the same inputs produced different files")
	}
}

func TestCompactRefusesNoInputs(t *testing.T) {
	shim := testShim(t)
	if _, err := Compact(shim, "app/docs", 1, nil, false, nil); err == nil {
		t.Fatal("compacting nothing should be an error")
	}
}

// The store-level pass: the tables collapse to one, the old files go, and
// every value is still readable.
func TestBlobStoreCompactReplacesItsTables(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < 6; i++ {
		store.AddTable(writeTable(t, shim, ns, 0, map[string]string{
			"a": "1", "b": "2", fmt.Sprintf("k%d", i): fmt.Sprintf("v%d", i),
		}, nil))
	}
	before, err := shim.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}

	res, err := store.Compact(DefaultCompactionTrigger, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Merged != 6 {
		t.Fatalf("merged %d tables, want 6", res.Merged)
	}
	if res.Removed != 6 {
		t.Fatalf("removed %d tables, want 6", res.Removed)
	}
	if got := len(store.Tables()); got != 1 {
		t.Fatalf("the store holds %d tables after compaction", got)
	}
	after, err := shim.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Fatalf("segment count did not fall: %d -> %d", len(before), len(after))
	}

	for k, want := range map[string]string{"a": "1", "b": "2", "k0": "v0", "k5": "v5"} {
		if got := store.Get(keyFor([]byte(k))); string(got) != want {
			t.Fatalf("%s reads %q after compaction, want %q", k, got, want)
		}
	}
}

func TestBlobStoreCompactDoesNothingBelowTheTrigger(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"a": "1"}, nil))
	store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"b": "2"}, nil))

	res, err := store.Compact(4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Merged != 0 || res.Removed != 0 {
		t.Fatalf("compaction ran below its trigger: %+v", res)
	}
	if len(store.Tables()) != 2 {
		t.Fatal("the tables were disturbed")
	}
}

// Compaction is repeatable: running it again on an already-merged store
// converges rather than rewriting on every call.
func TestBlobStoreCompactConverges(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < 5; i++ {
		store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"a": "1"}, nil))
	}
	if _, err := store.Compact(4, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		res, err := store.Compact(4, nil)
		if err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		if res.Merged != 0 {
			t.Fatalf("pass %d merged %d tables; one table is already compact", i, res.Merged)
		}
	}
}

// Search order: level 0 is newest, so a table compacted down a level is
// shadowed by anything flushed after it.
func TestSearchOrderPrefersTheNewestLevel(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	old := writeTable(t, shim, ns, 1, map[string]string{"a": "old"}, nil)
	fresh := writeTable(t, shim, ns, 0, map[string]string{"a": "new"}, nil)
	// Registered deepest-last, so insertion order alone would give the
	// wrong answer.
	store.AddTable(fresh)
	store.AddTable(old)

	if got := store.Get(keyFor([]byte("a"))); string(got) != "new" {
		t.Fatalf("a compacted-down table shadowed a newer flush: %q", got)
	}
}

// The bug this file's two-pass merge exists to make impossible: a single
// streaming pass dropped a tombstone by skipping it, which left an earlier
// input's value for that key standing - so a full compaction *resurrected*
// exactly the keys it was meant to reclaim.
func TestDroppingATombstoneAlsoDropsTheValueUnderIt(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	older := writeTable(t, shim, ns, 0, map[string]string{"gone": "value", "kept": "value"}, nil)
	newer := writeTable(t, shim, ns, 0, nil, []string{"gone"})

	merged, err := Compact(shim, ns, 1, []Handle{older, newer}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := NewDefaultReader(shim, merged)
	if _, _, found, err := r.Lookup(keyFor([]byte("gone"))); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("a dropped tombstone resurrected the value beneath it")
	}
	if got, _ := r.Get(keyFor([]byte("kept"))); string(got) != "value" {
		t.Fatalf("an untouched key was lost: %q", got)
	}
}

// And the same through the store, which is the path that actually sets
// dropTombstones.
func TestBlobStoreCompactDoesNotResurrectDeletedKeys(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"gone": "v", "a": "1"}, nil))
	store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"b": "2"}, nil))
	store.AddTable(writeTable(t, shim, ns, 0, nil, []string{"gone"}))
	store.AddTable(writeTable(t, shim, ns, 0, map[string]string{"c": "3"}, nil))

	if _, deleted, found := store.Lookup(keyFor([]byte("gone"))); !found || !deleted {
		t.Fatal("the key should read as deleted before compaction")
	}
	if _, err := store.Compact(2, nil); err != nil {
		t.Fatal(err)
	}
	if v, _, found := store.Lookup(keyFor([]byte("gone"))); found {
		t.Fatalf("compaction resurrected a deleted key as %q", v)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got := store.Get(keyFor([]byte(k))); string(got) != want {
			t.Fatalf("%s reads %q, want %q", k, got, want)
		}
	}
}
