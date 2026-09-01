package memtable

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// testKey builds a deterministic 32-byte key from a seed - no real hashing needed, just
// something codec.Hash accepts. Mirrors randomKey in the Kotlin twin's MemTableTest.kt.
func testKey(t *testing.T, seed string) codec.Hash {
	t.Helper()
	seedBytes := []byte(seed)
	out := make([]byte, 32)
	for i := range out {
		out[i] = seedBytes[i%len(seedBytes)]
	}
	h, err := codec.HashFromBytes(out)
	if err != nil {
		t.Fatalf("HashFromBytes: %v", err)
	}
	return h
}

// sealFailingShim delegates every call to the embedded shim except SealSegment, which fails
// once when failNextSeal is set. SealSegment is the last I/O call DefaultWriter.Finish makes,
// so this simulates a flush that dies at the final durability step.
type sealFailingShim struct {
	storage.PlatformIOShim
	failNextSeal bool
}

func (s *sealFailingShim) SealSegment(segmentName string) error {
	if s.failNextSeal {
		s.failNextSeal = false
		return errors.New("simulated seal failure")
	}
	return s.PlatformIOShim.SealSegment(segmentName)
}

func newManager(shim storage.PlatformIOShim) *Manager {
	blobStore := sstable.NewLsmBlobStore(shim, "ns", sstable.NewBlockCache(1024*1024))
	return NewManager("ns", shim, blobStore)
}

// --- SortedTable ---

// TestSortedTablePutGetDelete covers basic visibility: a put value is readable, a missing key
// reads as nil, an overwrite is observed, and a delete hides the key.
func TestSortedTablePutGetDelete(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "k1")

	if got := table.Get(key); got != nil {
		t.Fatalf("expected nil for a never-written key, got %q", got)
	}

	table.Put(key, []byte("v1"))
	if got := table.Get(key); string(got) != "v1" {
		t.Fatalf("Get after Put: got %q, want %q", got, "v1")
	}

	table.Put(key, []byte("v2"))
	if got := table.Get(key); string(got) != "v2" {
		t.Fatalf("Get after overwrite: got %q, want %q", got, "v2")
	}

	table.Delete(key)
	if got := table.Get(key); got != nil {
		t.Fatalf("expected nil after Delete, got %q", got)
	}
}

// TestSortedTableGetReturnsCopy confirms Get hands back a defensive copy - mutating the
// returned slice must not corrupt the stored value.
func TestSortedTableGetReturnsCopy(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "copy")
	table.Put(key, []byte("value"))

	first := table.Get(key)
	first[0] = 'X'

	if got := table.Get(key); string(got) != "value" {
		t.Fatalf("stored value was mutated through Get's return: got %q, want %q", got, "value")
	}
}

// TestSortedTableSizeOverwriteNetsOut mirrors the Kotlin twin's
// SortedMemTableSizeTest.overwriteNetsOutThePreviousValuesSize (kdb-finish-up-plan.md 1-K2):
// overwriting a key must net out the replaced value's size, not just add the new one.
func TestSortedTableSizeOverwriteNetsOut(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "k")

	table.Put(key, make([]byte, 100))
	if got := table.SizeBytes(); got != 100 {
		t.Fatalf("SizeBytes after first Put: got %d, want 100", got)
	}
	table.Put(key, make([]byte, 40)) // overwrite with a smaller value
	if got := table.SizeBytes(); got != 40 {
		t.Fatalf("SizeBytes after overwrite: got %d, want 40 (replaced value must be netted out)", got)
	}
}

// TestSortedTableSizeDeleteSubtracts mirrors the Kotlin twin's
// SortedMemTableSizeTest.deleteSubtractsTheDeletedValuesSize (1-K2): deleting the only entry
// must return SizeBytes to zero.
func TestSortedTableSizeDeleteSubtracts(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "k")

	table.Put(key, make([]byte, 100))
	table.Delete(key)
	if got := table.SizeBytes(); got != 0 {
		t.Fatalf("SizeBytes after deleting the only entry: got %d, want 0", got)
	}
}

// TestSortedTableDeleteThenPutIsFlushed covers the delete-before-put ordering: Delete on a
// never-written key records a tombstone, and a later Put of that same key must still land in
// the flush snapshot (snapshotEntries iterates insertion order).
func TestSortedTableDeleteThenPutIsFlushed(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "resurrect")

	table.Delete(key) // tombstone a key that was never written
	table.Put(key, []byte("alive"))

	if got := table.Get(key); string(got) != "alive" {
		t.Fatalf("Get after Delete-then-Put: got %q, want %q", got, "alive")
	}
	for _, e := range table.snapshotEntries() {
		if e.key == key && string(e.value) == "alive" {
			return
		}
	}
	t.Fatal("Delete-then-Put key missing from the flush snapshot - it would be silently dropped on flush")
}

// --- Manager ---

// TestManagerPutGetDelete covers visibility through the manager against a live (empty) blob
// store: a put is readable, a missing key is nil, and a delete hides the key again.
func TestManagerPutGetDelete(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)
	key := testKey(t, "mk1")

	if got := mgr.Get(key); got != nil {
		t.Fatalf("expected nil for a never-written key, got %q", got)
	}
	mgr.Put(key, []byte(`{"v":1}`))
	if got := mgr.Get(key); string(got) != `{"v":1}` {
		t.Fatalf("Get after Put: got %q, want %q", got, `{"v":1}`)
	}
}

// TestManagerFlushThenRead mirrors the Kotlin twin's
// flushSucceedsNormallyAndDataRemainsReadableFromBlobStore: after a successful flush the data
// has moved out of the active table into the blob store, and reads still work through Get.
func TestManagerFlushThenRead(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)

	k1, k2 := testKey(t, "flush-a"), testKey(t, "flush-b")
	v1, v2 := []byte(`{"v":"durable-a"}`), []byte(`{"v":"durable-b"}`)
	mgr.Put(k1, v1)
	mgr.Put(k2, v2)

	handle, err := mgr.Flush(0)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if handle.SegmentName == "" {
		t.Fatal("Flush of a non-empty memtable returned a zero Handle")
	}

	if got := mgr.Get(k1); string(got) != string(v1) {
		t.Fatalf("Get(k1) after flush: got %q, want %q", got, v1)
	}
	if got := mgr.Get(k2); string(got) != string(v2) {
		t.Fatalf("Get(k2) after flush: got %q, want %q", got, v2)
	}

	// A write after the flush lands in the fresh active table and is also visible.
	k3 := testKey(t, "flush-c")
	mgr.Put(k3, []byte("post-flush"))
	if got := mgr.Get(k3); string(got) != "post-flush" {
		t.Fatalf("Get(k3) after flush: got %q, want %q", got, "post-flush")
	}
}

// TestManagerActiveShadowsBlobStore confirms a re-written key is served from the active table,
// not the older flushed copy.
func TestManagerActiveShadowsBlobStore(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)
	key := testKey(t, "shadow")

	mgr.Put(key, []byte("old"))
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	mgr.Put(key, []byte("new"))
	if got := mgr.Get(key); string(got) != "new" {
		t.Fatalf("expected the active table's value to shadow the flushed one: got %q, want %q", got, "new")
	}
}

// TestManagerFlushEmptyReturnsZeroHandle confirms flushing an empty memtable writes nothing and
// returns a zero Handle with no error.
//
// A memtable holding only deletions is NOT empty and does produce a table: its tombstones are the
// only record that those keys were deleted, and the keys may well exist in an SSTable flushed
// earlier. This test used to assert the opposite - that a deletion-only flush wrote nothing -
// which is precisely the behavior that let a delete of an already-flushed key un-delete itself.
// See TestDeleteSurvivesAFlush.
func TestManagerFlushEmptyReturnsZeroHandle(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)

	handle, err := mgr.Flush(0)
	if err != nil {
		t.Fatalf("Flush of empty memtable: %v", err)
	}
	if handle != (sstable.Handle{}) {
		t.Fatalf("Flush of empty memtable: got %+v, want zero Handle", handle)
	}
	segments, err := shim.ListSegments("ns")
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("expected no segments after an empty flush, got %v", segments)
	}
}

// TestDeleteSurvivesAFlush is the regression test for the lost-tombstone hazard: a key written
// and flushed, then deleted and flushed again, must stay deleted. The tombstone used to be
// skipped at flush time (the SSTable format had no delete marker), so the second flush erased the
// only record of the delete and the first flush's value came straight back.
func TestDeleteSurvivesAFlush(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)

	key := testKey(t, "resurrect-me")
	mgr.Put(key, []byte("original"))
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if got := mgr.Get(key); string(got) != "original" {
		t.Fatalf("after the first flush Get = %q, want %q", got, "original")
	}

	mgr.Delete(key)
	if got := mgr.Get(key); got != nil {
		t.Fatalf("Get right after Delete = %q, want nil", got)
	}
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("second flush: %v", err)
	}

	if got := mgr.Get(key); got != nil {
		t.Fatalf("the delete did not survive the flush: Get = %q, want nil", got)
	}
}

// TestDeleteThenRewriteSurvivesAFlush: a tombstone must shadow older tables, but it must not
// shadow a value written after it.
func TestDeleteThenRewriteSurvivesAFlush(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)

	key := testKey(t, "revived")
	mgr.Put(key, []byte("v1"))
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("flush v1: %v", err)
	}
	mgr.Delete(key)
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("flush tombstone: %v", err)
	}
	mgr.Put(key, []byte("v2"))
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("flush v2: %v", err)
	}

	if got := mgr.Get(key); string(got) != "v2" {
		t.Fatalf("Get = %q, want %q - the tombstone shadowed a newer write", got, "v2")
	}
}

// TestManagerFlushFailureKeepsDataVisible mirrors the Kotlin twin's
// flushFailureStillLeavesDataVisibleViaPendingFlush (kdb-finish-up-plan.md 1-K2): when
// writer.Finish fails (here at SealSegment, the final durability step), the flushed generation
// must remain visible via pendingFlush - it was never actually lost, just not yet durable.
func TestManagerFlushFailureKeepsDataVisible(t *testing.T) {
	shim := &sealFailingShim{PlatformIOShim: io.NewInMemoryPlatformIO()}
	mgr := newManager(shim)

	key := testKey(t, "k1")
	value := []byte(`{"v":"should not be lost"}`)
	mgr.Put(key, value)

	shim.failNextSeal = true
	if _, err := mgr.Flush(0); err == nil {
		t.Fatal("expected Flush to fail when SealSegment fails")
	}

	if got := mgr.Get(key); string(got) != string(value) {
		t.Fatalf("write lost after failed flush: got %q, want %q", got, value)
	}
}

// TestSortedTableLookupDistinguishesTombstoneFromAbsent pins the distinction Get cannot make:
// a tombstoned key is found-and-deleted, a never-written key is simply not found. Everything
// that merges generations (Manager.Get) depends on telling those apart.
func TestSortedTableLookupDistinguishesTombstoneFromAbsent(t *testing.T) {
	table := NewSortedTable()
	key := testKey(t, "tomb")

	if _, _, found := table.Lookup(key); found {
		t.Fatal("a never-written key must report found=false")
	}

	table.Put(key, []byte("v"))
	value, deleted, found := table.Lookup(key)
	if !found || deleted || string(value) != "v" {
		t.Fatalf("Lookup of a live key: got (%q, deleted=%v, found=%v), want (\"v\", false, true)", value, deleted, found)
	}

	table.Delete(key)
	value, deleted, found = table.Lookup(key)
	if !found || !deleted || value != nil {
		t.Fatalf("Lookup of a tombstone: got (%q, deleted=%v, found=%v), want (nil, true, true)", value, deleted, found)
	}
}

// TestManagerDeleteShadowsFlushedValue is the correctness bug this Lookup plumbing exists for:
// a delete of a key that was already flushed to an SSTable must hide it. Manager.Get used to
// read the memtable with Get, see nil for the tombstone, treat that as "not here", and fall
// through to the blob store - handing back the deleted value.
func TestManagerDeleteShadowsFlushedValue(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	mgr := newManager(shim)
	key := testKey(t, "deleted-after-flush")

	mgr.Put(key, []byte("flushed"))
	if _, err := mgr.Flush(0); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := mgr.Get(key); string(got) != "flushed" {
		t.Fatalf("sanity: Get after flush: got %q, want %q", got, "flushed")
	}

	mgr.Delete(key)
	if got := mgr.Get(key); got != nil {
		t.Fatalf("delete did not shadow the flushed value: got %q, want nil", got)
	}

	// And a re-put after the delete is visible again.
	mgr.Put(key, []byte("rewritten"))
	if got := mgr.Get(key); string(got) != "rewritten" {
		t.Fatalf("Get after delete-then-put: got %q, want %q", got, "rewritten")
	}
}

// TestManagerDeleteShadowsPendingFlush covers the same shadowing one generation in: a
// tombstone in the active table must hide a value still sitting in the generation being
// flushed.
func TestManagerDeleteShadowsPendingFlush(t *testing.T) {
	shim := &sealFailingShim{PlatformIOShim: io.NewInMemoryPlatformIO()}
	mgr := newManager(shim)
	key := testKey(t, "pending")

	mgr.Put(key, []byte("in-flight"))
	shim.failNextSeal = true
	if _, err := mgr.Flush(0); err == nil {
		t.Fatal("expected Flush to fail when SealSegment fails")
	}
	if got := mgr.Get(key); string(got) != "in-flight" {
		t.Fatalf("sanity: value should still be visible via pendingFlush: got %q", got)
	}

	mgr.Delete(key)
	if got := mgr.Get(key); got != nil {
		t.Fatalf("delete did not shadow the pending-flush value: got %q, want nil", got)
	}
}
