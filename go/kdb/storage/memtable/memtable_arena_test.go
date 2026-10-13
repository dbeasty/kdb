package memtable

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// previousSortedTable is the memtable as it was before the arena (map[Hash]slot{[]byte} plus an
// insertion-order key slice), kept verbatim as the reference the new table's flush output is
// compared against. Do not "fix" it: its value is that it is the old behaviour.
type previousSortedTable struct {
	mu    sync.Mutex
	order []codec.Hash
	vals  map[codec.Hash]previousSlot
}

type previousSlot struct {
	value   []byte
	deleted bool
}

func (m *previousSortedTable) Put(key codec.Hash, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vals[key]; !ok {
		m.order = append(m.order, key)
	}
	m.vals[key] = previousSlot{value: value}
}

func (m *previousSortedTable) Delete(key codec.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vals[key]; !ok {
		m.order = append(m.order, key)
	}
	m.vals[key] = previousSlot{deleted: true}
}

func (m *previousSortedTable) snapshotEntries() []entry {
	out := make([]entry, 0, len(m.order))
	for _, k := range m.order {
		s := m.vals[k]
		out = append(out, entry{key: k, value: s.value, deleted: s.deleted})
	}
	return out
}

// writeEntries is Manager.Flush's write loop, for the reference table.
func writeEntries(t *testing.T, shim storage.PlatformIOShim, entries []entry) sstable.Handle {
	t.Helper()
	w := sstable.NewDefaultWriter(shim, "ns", 0)
	for _, e := range entries {
		if e.deleted {
			w.Delete(e.key)
		} else {
			w.Put(e.key, e.value)
		}
	}
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func segmentBytes(t *testing.T, shim *io.InMemoryPlatformIO, name string) []byte {
	t.Helper()
	size, err := shim.SegmentSize(name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := shim.ReadFromSegment(name, 0, int(size))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func seqKey(n int) codec.Hash {
	var h codec.Hash
	binary.BigEndian.PutUint64(h.Bytes[:], uint64(n)*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(h.Bytes[24:], uint64(n))
	return h
}

// The SSTable a flush writes is byte-for-byte what the previous table would have written for
// the same sequence of puts, overwrites, deletes and resurrections - including values large
// enough for a chunk of their own and arena compactions in between. SSTables are read by the
// Kotlin implementation, so this is a format guarantee, not a nicety.
func TestSortedTableFlushMatchesPreviousImplementation(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		rng := rand.New(rand.NewSource(seed))
		prev := &previousSortedTable{vals: map[codec.Hash]previousSlot{}}
		shim := io.NewInMemoryPlatformIO()
		mgr := newManager(shim)
		for step := 0; step < 5000; step++ {
			k := seqKey(rng.Intn(800))
			switch op := rng.Intn(10); {
			case op < 7:
				size := rng.Intn(300)
				if rng.Intn(200) == 0 {
					size = valueArenaLargeValue + rng.Intn(10000)
				}
				v := make([]byte, size)
				rng.Read(v)
				prev.Put(k, append([]byte(nil), v...))
				mgr.Put(k, v)
				rng.Read(v) // the table must have copied: the caller reusing its buffer changes nothing
			default:
				prev.Delete(k)
				mgr.Delete(k)
			}
		}
		refShim := io.NewInMemoryPlatformIO()
		want := segmentBytes(t, refShim, writeEntries(t, refShim, prev.snapshotEntries()).SegmentName)
		h, err := mgr.Flush(0)
		if err != nil {
			t.Fatal(err)
		}
		got := segmentBytes(t, shim, h.SegmentName)
		if !bytes.Equal(got, want) {
			t.Fatalf("seed %d: flushed SSTable differs from the previous implementation's (%d vs %d bytes)", seed, len(got), len(want))
		}
	}
}

// Lookup, tombstones and size accounting against a model, with compaction forced by repeated
// overwrites of different sizes.
func TestSortedTableModelAndCompaction(t *testing.T) {
	table := NewSortedTable()
	model := map[codec.Hash][]byte{}
	deleted := map[codec.Hash]bool{}
	var order []codec.Hash
	seen := map[codec.Hash]bool{}
	rng := rand.New(rand.NewSource(9))
	for step := 0; step < 20000; step++ {
		k := seqKey(rng.Intn(50))
		if !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
		if rng.Intn(5) == 0 {
			table.Delete(k)
			delete(model, k)
			deleted[k] = true
			continue
		}
		v := make([]byte, 1+rng.Intn(2000))
		rng.Read(v)
		table.Put(k, v)
		model[k] = v
		delete(deleted, k)
	}
	var live int64
	for k, v := range model {
		got, del, found := table.Lookup(k)
		if !found || del || !bytes.Equal(got, v) {
			t.Fatalf("Lookup(%s) wrong after compactions", k.Hex())
		}
		live += int64(len(v))
	}
	for k := range deleted {
		if _, del, found := table.Lookup(k); !found || !del {
			t.Fatalf("tombstone for %s lost", k.Hex())
		}
	}
	if want := live + int64(table.Len())*RecordOverheadBytes; table.SizeBytes() != want {
		t.Fatalf("SizeBytes %d, want %d", table.SizeBytes(), want)
	}
	// 20000 overwrites of up to 2 KB into 50 keys: without compaction the arena would hold ~16 MB.
	if fp := table.FootprintBytes(); fp > 4*(live+1<<20) {
		t.Fatalf("footprint %d for %d live bytes: dead values are not being compacted", fp, live)
	}
	// Order is first insertion, and survives everything above.
	snap := table.snapshotEntries()
	if len(snap) != len(order) {
		t.Fatalf("snapshot has %d entries, want %d", len(snap), len(order))
	}
	for i, e := range snap {
		if e.key != order[i] {
			t.Fatalf("snapshot entry %d out of first-insertion order", i)
		}
	}
}

// A flush snapshot handed out before further writes and compactions keeps its bytes.
func TestSortedTableSnapshotStableAcrossCompaction(t *testing.T) {
	table := NewSortedTable()
	k := seqKey(1)
	table.Put(k, bytes.Repeat([]byte{'a'}, 1000))
	snap := table.snapshotEntries()
	for i := 0; i < 500; i++ {
		table.Put(k, bytes.Repeat([]byte{byte('b' + i%20)}, 900+i%50)) // forces compactions
	}
	if !bytes.Equal(snap[0].value, bytes.Repeat([]byte{'a'}, 1000)) {
		t.Fatal("snapshot value changed under a later write")
	}
}

// SizeBytes and FootprintBytes track what the table really adds to the heap.
func TestSortedTableAccountingMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	const n, size = 200_000, 150
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = bytes.Repeat([]byte{byte(i)}, size)
	}
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	table := NewSortedTable()
	for i := range vals {
		table.Put(seqKey(i), vals[i])
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	heap := int64(after.HeapAlloc - before.HeapAlloc)
	runtime.KeepAlive(vals)
	fp, sz := table.FootprintBytes(), table.SizeBytes()
	t.Logf("%d values of %d B: heap %.1f MB, footprint %.1f MB, SizeBytes %.1f MB (%.0f B/key)",
		n, size, float64(heap)/1e6, float64(fp)/1e6, float64(sz)/1e6, float64(heap)/n)
	if r := float64(fp) / float64(heap); r < 0.8 || r > 1.25 {
		t.Fatalf("FootprintBytes is %.2fx the heap", r)
	}
	if r := float64(sz) / float64(heap); r < 0.7 || r > 1.25 {
		t.Fatalf("SizeBytes is %.2fx the heap", r)
	}
}
