package sstable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
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

	merged, err := compactOne(shim, ns, 1, []Handle{a, b}, false, nil)
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
	merged, err := compactOne(shim, ns, 1, inputs, false, nil)
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

	merged, err := compactOne(shim, ns, 1, []Handle{older, newer}, false, nil)
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
	merged, err := compactOne(shim, ns, 1, []Handle{withValue, withDelete}, false, nil)
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
	merged, err = compactOne(shim, ns, 1, []Handle{withValue, withDelete}, true, nil)
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

	merged, err := compactOne(shim, ns, 1, []Handle{older, newer}, false, nil)
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

	first, err := compactOne(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := compactOne(shim, ns, 1, []Handle{a, b}, false, nil)
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

	merged, err := compactOne(shim, ns, 1, []Handle{older, newer}, true, nil)
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

// A cancelled merge writes nothing and leaves every table registered and readable: what closing
// a namespace relies on when it stops a background merge rather than waiting for it.
func TestBlobStoreCompactContextCancelledLeavesTheTables(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < 6; i++ {
		store.AddTable(writeTable(t, shim, ns, 0, map[string]string{fmt.Sprintf("k%d", i): fmt.Sprintf("v%d", i)}, nil))
	}
	before, err := shim.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.CompactContext(ctx, DefaultCompactionTrigger, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled merge returned %v, want context.Canceled", err)
	}
	after, err := shim.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || len(store.Tables()) != 6 {
		t.Fatalf("a cancelled merge changed the store: %d -> %d segments, %d tables registered",
			len(before), len(after), len(store.Tables()))
	}
	for i := 0; i < 6; i++ {
		if got := store.Get(keyFor([]byte(fmt.Sprintf("k%d", i)))); string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d reads %q after a cancelled merge", i, got)
		}
	}
	// And the store merges normally afterwards.
	if res, err := store.Compact(DefaultCompactionTrigger, nil); err != nil || res.Merged != 6 {
		t.Fatalf("merge after a cancelled one: %+v, %v", res, err)
	}
}

// bigTable writes n keys whose values are incompressible and valueBytes long, so a merge of it
// appends many finishBufferBytes batches - unlike a table of short values, whose merge writes
// nothing at all until its footer.
func bigTable(t *testing.T, shim storage.PlatformIOShim, ns string, n, valueBytes int, seed int64, deletes []string) Handle {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	w := NewDefaultWriter(shim, ns, 0)
	for i := 0; i < n; i++ {
		v := make([]byte, valueBytes)
		rng.Read(v)
		w.Put(keyFor([]byte(fmt.Sprintf("key-%d-%06d", seed, i))), v)
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

func readSegment(t *testing.T, shim storage.PlatformIOShim, name string) []byte {
	t.Helper()
	size, err := shim.(storage.SegmentSizer).SegmentSize(name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := shim.ReadFromSegment(name, 0, int(size))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// noRenameShim hides the in-memory shim's RenameSegment, as a shim written against
// storage.PlatformIOShim alone would lack it.
type noRenameShim struct{ storage.PlatformIOShim }

func (s noRenameShim) SegmentSize(name string) (int64, error) {
	return s.PlatformIOShim.(storage.SegmentSizer).SegmentSize(name)
}

// The merge streams its output, and the stream must lay out exactly the file a writer holding
// every entry until Finish lays out - same blocks, same footer, same file hash - or tables merged
// by this build would differ from what every other build (and Kotlin) writes for the same entries.
func TestStreamedCompactionIsByteIdenticalToFinish(t *testing.T) {
	for _, tc := range []struct {
		name string
		shim func() storage.PlatformIOShim
	}{
		{"renames", func() storage.PlatformIOShim { return testShim(t) }},
		{"no-rename", func() storage.PlatformIOShim { return noRenameShim{testShim(t)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := tc.shim()
			ns := "app/docs"
			// Several megabytes, so the merge appends many batches; tombstones kept, so the
			// footer's fourth field and the hash's marker byte are in the comparison too.
			a := bigTable(t, shim, ns, 1500, 2000, 1, nil)
			b := bigTable(t, shim, ns, 1500, 2000, 2, []string{"key-1-000007", "key-1-000100", "gone"})
			merged, err := compactOne(shim, ns, 1, []Handle{a, b}, false, nil)
			if err != nil {
				t.Fatal(err)
			}

			// The same final entries, sorted, through the buffering writer.
			final := map[codec.Hash]BlockHandle{}
			for _, in := range []Handle{a, b} {
				index, err := NewDefaultReader(shim, in).Index()
				if err != nil {
					t.Fatal(err)
				}
				for k, bh := range index {
					final[k] = bh
				}
			}
			owner := func(k codec.Hash) Handle {
				if _, ok := mustIndex(t, shim, b)[k]; ok {
					return b
				}
				return a
			}
			w := NewDefaultWriter(shim, ns, 1)
			for _, k := range sortedKeys(final) {
				if final[k].Deleted {
					w.Delete(k)
					continue
				}
				v, err := NewDefaultReader(shim, owner(k)).ReadBlockAt(final[k])
				if err != nil {
					t.Fatal(err)
				}
				w.Put(k, v)
			}
			want, err := w.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if merged.FileHash != want.FileHash {
				t.Fatalf("file hash %s, want %s", merged.FileHash.Hex(), want.FileHash.Hex())
			}
			got, exp := readSegment(t, shim, merged.SegmentName), readSegment(t, shim, want.SegmentName)
			if len(exp) < 3*finishBufferBytes {
				t.Fatalf("only %d bytes: the merge did not span several append batches", len(exp))
			}
			if !bytes.Equal(got, exp) {
				t.Fatalf("streamed table differs from the buffered one (%d vs %d bytes)", len(got), len(exp))
			}
			if !strings.HasPrefix(merged.SegmentName, storio.SegmentNameBuilder.NamespacePrefix(ns)+"sstable/L1/") {
				t.Fatalf("merged table is named %s", merged.SegmentName)
			}
		})
	}
}

func mustIndex(t *testing.T, shim storage.PlatformIOShim, h Handle) map[codec.Hash]BlockHandle {
	t.Helper()
	index, err := NewDefaultReader(shim, h).Index()
	if err != nil {
		t.Fatal(err)
	}
	return index
}

// Cancelled part-way through the key loop, after blocks have been appended: the partial output
// is deleted, so the merge leaves no file behind. Run against a shim that renames, where the
// output is under its partial name, and one that cannot, where it is under the table's own name.
func TestCompactContextStopsMidMerge(t *testing.T) {
	for _, tc := range []struct {
		name string
		shim func() storage.PlatformIOShim
	}{
		{"renames", func() storage.PlatformIOShim { return testShim(t) }},
		{"no-rename", func() storage.PlatformIOShim { return noRenameShim{testShim(t)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := tc.shim()
			ns := "app/docs"
			in := bigTable(t, shim, ns, 3*compactionCheckEvery, 1000, 1, nil)
			before, _ := shim.ListSegments(ns)
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			var during []string
			keep := func(codec.Hash) bool {
				if calls++; calls == compactionCheckEvery+1 {
					during, _ = shim.ListSegments(ns)
					cancel()
				}
				return true
			}
			if _, err := CompactContext(ctx, shim, ns, 1, []Handle{in}, false, keep); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want context.Canceled", err)
			}
			if len(during) != len(before)+1 {
				t.Fatalf("no output was being written when the merge was cancelled: %v", during)
			}
			if _, renames := shim.(storage.SegmentRenamer); renames {
				for _, name := range during {
					if name != in.SegmentName && !strings.HasPrefix(name, storio.SegmentNameBuilder.SSTablePartialPrefix(ns)) {
						t.Fatalf("a merge in progress wrote under %s, a name DiscoverTables registers", name)
					}
				}
			}
			if after, _ := shim.ListSegments(ns); len(after) != len(before) {
				t.Fatalf("a merge cancelled mid-way left a file: %v -> %v", before, after)
			}
		})
	}
}

// crashingShim stands in for a process that dies mid-merge: once crashAfter appends to a partial
// table have gone through, the next one panics and nothing the merge does afterwards - its
// cleanup included - reaches storage.
type crashingShim struct {
	*storio.InMemoryPlatformIO
	crashAfter int
	appends    int
	crashed    bool
}

func (s *crashingShim) AppendToSegment(name string, b []byte) (int64, error) {
	if s.crashed {
		return 0, errors.New("process is dead")
	}
	if strings.Contains(name, "sstable-partial/") {
		if s.appends++; s.appends > s.crashAfter {
			s.crashed = true
			panic("crash")
		}
	}
	return s.InMemoryPlatformIO.AppendToSegment(name, b)
}

func (s *crashingShim) DeleteSegment(name string) error {
	if s.crashed {
		return errors.New("process is dead")
	}
	return s.InMemoryPlatformIO.DeleteSegment(name)
}

// A merge that dies mid-write leaves its partial table on disk, and the next open must not
// register it: a half-written table has no footer, or worse, a footer-shaped tail. The next merge
// then removes it.
func TestCrashMidMergeLeavesNothingDiscoverable(t *testing.T) {
	shim := &crashingShim{InMemoryPlatformIO: testShim(t), crashAfter: 2}
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < DefaultCompactionTrigger; i++ {
		store.AddTable(bigTable(t, shim, ns, 500, 2000, int64(i), nil))
	}
	inputs := map[string]bool{}
	for _, h := range store.Tables() {
		inputs[h.SegmentName] = true
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the merge did not reach the crash")
			}
		}()
		_, _ = store.Compact(DefaultCompactionTrigger, nil)
	}()
	partials := func() (n int) {
		names, _ := shim.InMemoryPlatformIO.ListSegments(ns)
		for _, name := range names {
			if strings.HasPrefix(name, storio.SegmentNameBuilder.SSTablePartialPrefix(ns)) {
				n++
			}
		}
		return n
	}
	if partials() != 1 {
		t.Fatalf("the crash left %d partial tables, want the one it was writing", partials())
	}

	// The next process.
	shim.crashed = false
	reopened := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	if err := reopened.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Tables(); len(got) != len(inputs) {
		t.Fatalf("reopen registered %d tables, want the %d inputs", len(got), len(inputs))
	}
	for _, h := range reopened.Tables() {
		if !inputs[h.SegmentName] {
			t.Fatalf("reopen registered %s, which the crashed merge wrote", h.SegmentName)
		}
	}
	shim.crashAfter = 1 << 30
	if res, err := reopened.Compact(DefaultCompactionTrigger, nil); err != nil || res.Merged != len(inputs) {
		t.Fatalf("merge after the crash: %+v, %v", res, err)
	}
	if partials() != 0 {
		t.Fatal("the merge after the crash left the crashed merge's partial table")
	}
	if got := reopened.Get(keyFor([]byte("key-3-000499"))); len(got) != 2000 {
		t.Fatalf("a key reads %d bytes after the merges", len(got))
	}
}

// TableBytes adds up what the registered tables occupy.
func TestBlobStoreTableBytes(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	if n, ok := store.TableBytes(); !ok || n != 0 {
		t.Fatalf("empty store: %d, %v", n, ok)
	}
	var want int64
	for i := 0; i < 3; i++ {
		h := writeTable(t, shim, ns, 0, map[string]string{fmt.Sprintf("k%d", i): "value"}, nil)
		store.AddTable(h)
		n, err := shim.SegmentSize(h.SegmentName)
		if err != nil {
			t.Fatal(err)
		}
		want += n
	}
	if n, ok := store.TableBytes(); !ok || n != want {
		t.Fatalf("TableBytes = %d, %v; want %d", n, ok, want)
	}
}

// sortedKeys orders a key set by its bytes, as a compaction orders its output.
func sortedKeys[V any](m map[codec.Hash]V) []codec.Hash {
	out := make([]codec.Hash, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Bytes, out[j].Bytes
		for x := range a {
			if a[x] != b[x] {
				return a[x] < b[x]
			}
		}
		return false
	})
	return out
}

// compactOne is Compact for a merge small enough to write a single table.
func compactOne(shim storage.PlatformIOShim, ns string, level int, inputs []Handle, dropTombstones bool, keep func(codec.Hash) bool) (Handle, error) {
	out, err := Compact(shim, ns, level, inputs, dropTombstones, keep)
	if err != nil {
		return Handle{}, err
	}
	if len(out) != 1 {
		return Handle{}, fmt.Errorf("the merge wrote %d tables, want 1", len(out))
	}
	return out[0], nil
}
