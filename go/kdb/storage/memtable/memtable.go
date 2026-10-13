package memtable

import (
	"bytes"
	"sync"
	"unsafe"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// Table is an in-memory sorted blob map.
type Table interface {
	Put(key codec.Hash, value []byte)
	Get(key codec.Hash) []byte
	Lookup(key codec.Hash) (value []byte, deleted, found bool)
	Delete(key codec.Hash)
	SizeBytes() int64
}

// SortedTable is the in-memory blob generation: every key in first-insertion order, values in an
// append-only arena.
//
// Despite the name it is not sorted and never was: a flush writes keys in the order they were
// first put, and that order is part of what an SSTable's bytes are (blocks are laid out in it,
// and its footer index lists them in it), so it is kept exactly rather than sorted - flush
// output is byte-identical to the map-and-slice table this replaced
// (TestSortedTableFlushMatchesPreviousImplementation).
//
// Layout: values live in chunks of one arena, records (key, location, tombstone flag) in one
// slice in insertion order, and the key -> record map holds only fixed-size values. Nothing in
// the table holds a pointer per entry, so however many versions a generation holds, the
// collector scans a handful of slice headers - the old table had a []byte header per value in
// its map and a key slice beside it. Cassandra's memtable_allocation_type: offheap_objects does
// the same thing for the same reason, with slabs instead of chunks.
//
// The arena's chunks are append-only: a byte is never written twice. Readers outside the lock
// (a flush writing out a generation that is no longer active) therefore see stable bytes, and
// compaction copies live values into new chunks rather than moving them in place.
type SortedTable struct {
	mu    sync.Mutex
	index map[codec.Hash]uint32 // key -> position in recs
	recs  []memRec
	arena valueArena
	// live is the sum of live value lengths; SizeBytes adds the per-record overhead to it.
	live int64
}

// memRec is one key's state: where its value is, or that it is a tombstone. A tombstone is
// deleted=true with no value, which is deliberately distinguishable from "no entry at all" - the
// whole point of Lookup.
type memRec struct {
	key     codec.Hash
	chunk   uint32
	off     uint32
	n       uint32
	deleted bool
}

// RecordOverheadBytes is what one key costs a SortedTable beyond its value's bytes: its record
// (the 32-byte key, the value's location and the tombstone flag) and its index entry
// (a map[codec.Hash]uint32 slot at the swiss table's typical load). FootprintBytes includes it;
// SizeBytes, the flush trigger, does not (see there).
const RecordOverheadBytes = int64(unsafe.Sizeof(memRec{})) + 56

// NewSortedTable returns an empty memtable.
func NewSortedTable() *SortedTable {
	return &SortedTable{index: make(map[codec.Hash]uint32)}
}

// Put writes value for key, copying it into the table. SizeBytes nets out any value being
// replaced (including a tombstone's zero) rather than only ever growing. Re-putting the bytes a
// key already holds - the common case for content-addressed keys - is a no-op.
func (m *SortedTable) Put(key codec.Hash, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i, ok := m.index[key]; ok {
		r := &m.recs[i]
		if !r.deleted && bytes.Equal(m.arena.bytes(r.chunk, r.off, r.n), value) {
			return
		}
		m.release(r)
		r.chunk, r.off = m.arena.append(value)
		r.n, r.deleted = uint32(len(value)), false
		m.live += int64(len(value))
		m.maybeCompact()
		return
	}
	c, off := m.arena.append(value)
	m.index[key] = uint32(len(m.recs))
	m.recs = append(m.recs, memRec{key: key, chunk: c, off: off, n: uint32(len(value))})
	m.live += int64(len(value))
}

// release counts r's current value dead.
func (m *SortedTable) release(r *memRec) {
	if r.deleted {
		return
	}
	m.live -= int64(r.n)
	m.arena.dead += int64(r.n)
}

// maybeCompact rewrites the arena once overwrites and deletes have left more of it dead than
// live. Rare - a content-addressed key is overwritten only with the bytes it already holds -
// but without it a generation that kept overwriting one key would grow without SizeBytes
// noticing.
func (m *SortedTable) maybeCompact() {
	if m.arena.dead < valueArenaCompactMin || m.arena.dead < m.live {
		return
	}
	var fresh valueArena
	for i := range m.recs {
		r := &m.recs[i]
		if r.deleted {
			continue
		}
		r.chunk, r.off = fresh.append(m.arena.bytes(r.chunk, r.off, r.n))
	}
	m.arena = fresh
}

// Get returns the value for key, or nil if the key is absent *or* tombstoned. Callers that
// need to tell those two apart - anything that would otherwise fall through to an older
// generation - must use Lookup instead.
func (m *SortedTable) Get(key codec.Hash) []byte {
	value, _, _ := m.Lookup(key)
	return value
}

// Lookup reports what this table knows about key. found=false means the table has never seen
// the key and the caller should keep searching older generations. found=true with
// deleted=true is a tombstone: the key was deleted here and older generations must NOT be
// consulted - reading through a tombstone is exactly how a delete used to fail to shadow an
// entry already flushed to an SSTable.
//
// The value is a copy the caller owns.
func (m *SortedTable) Lookup(key codec.Hash) (value []byte, deleted, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.index[key]
	if !ok {
		return nil, false, false
	}
	r := &m.recs[i]
	if r.deleted {
		return nil, true, true
	}
	return append([]byte(nil), m.arena.bytes(r.chunk, r.off, r.n)...), false, true
}

// lookupShared is Lookup without the copy: the value is the arena's bytes, capacity-capped so an
// append cannot reach the next value. Safe to hand out because arena bytes are never written
// twice (compaction copies into new chunks), but the caller must treat it as read-only - which
// Manager.Get's contract already requires.
func (m *SortedTable) lookupShared(key codec.Hash) (value []byte, deleted, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.index[key]
	if !ok {
		return nil, false, false
	}
	r := &m.recs[i]
	if r.deleted {
		return nil, true, true
	}
	if r.n == 0 {
		return nil, false, true // what Lookup's copy of an empty value has always been
	}
	return m.arena.bytes(r.chunk, r.off, r.n), false, true
}

// has is Lookup without the value: what Contains needs, without copying a value out to throw
// it away.
func (m *SortedTable) has(key codec.Hash) (deleted, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.index[key]
	if !ok {
		return false, false
	}
	return m.recs[i].deleted, true
}

// Delete tombstones key, subtracting any value it replaces from SizeBytes. A key deleted
// before it was ever written still joins the insertion order, so a later Put of it is not
// silently dropped from the flush snapshot.
func (m *SortedTable) Delete(key codec.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i, ok := m.index[key]; ok {
		r := &m.recs[i]
		m.release(r)
		r.chunk, r.off, r.n, r.deleted = 0, 0, 0, true
		m.maybeCompact()
		return
	}
	m.index[key] = uint32(len(m.recs))
	m.recs = append(m.recs, memRec{key: key, deleted: true})
}

// SizeBytes is the flush trigger: live value bytes, with replaced and deleted values netted out
// (the Kotlin twin's definition). It deliberately does not count per-key overhead even though
// that is real memory - FootprintBytes reports it, and the memory accounting uses that. Charging
// RecordOverheadBytes here was tried and measured: under the objects strategy most values are
// ~20-byte location records, so the threshold was reached ~6x sooner, the memtable flushed ~6x
// as often, and heavy-multi-user updates lost 18-20% throughput
// (docs/benchmarks/2026-10-13-live-body-arena.md).
func (m *SortedTable) SizeBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.live
}

// FootprintBytes is the memory the table occupies: arena chunks (including dead bytes and
// unfilled tails), the record slice and the index. For accounting; SizeBytes is the flush
// trigger.
func (m *SortedTable) FootprintBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.arena.capBytes + int64(cap(m.recs))*int64(unsafe.Sizeof(memRec{})) + int64(len(m.index))*56
}

// Len is the number of keys the table holds, tombstones included.
func (m *SortedTable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recs)
}

// snapshotEntries lists the table in insertion order. The values alias the arena, which is
// safe because arena bytes are never rewritten (see SortedTable); callers must not modify them.
func (m *SortedTable) snapshotEntries() []entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]entry, 0, len(m.recs))
	for i := range m.recs {
		r := &m.recs[i]
		if r.deleted {
			out = append(out, entry{key: r.key, deleted: true})
			continue
		}
		out = append(out, entry{key: r.key, value: m.arena.bytes(r.chunk, r.off, r.n)})
	}
	return out
}

type entry struct {
	key     codec.Hash
	value   []byte
	deleted bool
}

// valueArena is an append-only sequence of chunks. Values up to valueArenaLargeValue share
// chunks of up to valueArenaMaxChunk, which start small and double, so an idle generation costs
// a few kilobytes; a larger value gets a chunk of its own, exactly sized.
type valueArena struct {
	chunks    [][]byte
	cur       int // chunk small values are appended to; meaningful only when chunks is non-empty
	nextChunk int
	capBytes  int64
	dead      int64
}

const (
	valueArenaMinChunk   = 4 << 10
	valueArenaMaxChunk   = 256 << 10
	valueArenaLargeValue = valueArenaMaxChunk / 4
	valueArenaCompactMin = 64 << 10
)

func (a *valueArena) append(v []byte) (chunk, off uint32) {
	n := len(v)
	if n == 0 {
		return 0, 0
	}
	if n > valueArenaLargeValue {
		a.chunks = append(a.chunks, append(make([]byte, 0, n), v...))
		a.capBytes += int64(n)
		return uint32(len(a.chunks) - 1), 0
	}
	if len(a.chunks) == 0 || a.cur < 0 || cap(a.chunks[a.cur])-len(a.chunks[a.cur]) < n {
		if a.nextChunk == 0 {
			a.nextChunk = valueArenaMinChunk
		}
		size := max(a.nextChunk, n)
		if a.nextChunk < valueArenaMaxChunk {
			a.nextChunk *= 2
		}
		a.chunks = append(a.chunks, make([]byte, 0, size))
		a.capBytes += int64(size)
		a.cur = len(a.chunks) - 1
	}
	b := a.chunks[a.cur]
	o := len(b)
	a.chunks[a.cur] = append(b, v...) // within capacity: never reallocates
	return uint32(a.cur), uint32(o)
}

// bytes returns the value at (chunk, off, n), capped so an append by the caller cannot write
// into the arena.
func (a *valueArena) bytes(chunk, off, n uint32) []byte {
	if n == 0 {
		return []byte{}
	}
	return a.chunks[chunk][off : off+n : off+n]
}

var _ Table = (*SortedTable)(nil)

// Manager coordinates active and flushing memtables with LSM blob storage.
type Manager struct {
	namespaceID string
	io          storage.PlatformIOShim
	blobStore   *sstable.LsmBlobStore

	mu           sync.Mutex
	active       *SortedTable
	pendingFlush *SortedTable
}

// NewManager returns a memtable manager for a namespace.
func NewManager(namespaceID string, io storage.PlatformIOShim, blobStore *sstable.LsmBlobStore) *Manager {
	return &Manager{
		namespaceID: namespaceID,
		io:          io,
		blobStore:   blobStore,
		active:      NewSortedTable(),
	}
}

func (mgr *Manager) Put(key codec.Hash, value []byte) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.active.Put(key, value)
}

// SizeBytes is how much the active generation is holding. The memtable
// grows without bound until something flushes it, so a writer that keeps
// putting has to watch this - see engine.ServerEngine.maybeFlushMemtable.
func (mgr *Manager) SizeBytes() int64 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.active.SizeBytes()
}

// FootprintBytes is the memory both generations occupy - the active one and any generation
// still being flushed. For accounting; SizeBytes is the flush trigger.
func (mgr *Manager) FootprintBytes() int64 {
	mgr.mu.Lock()
	active, pending := mgr.active, mgr.pendingFlush
	mgr.mu.Unlock()
	b := active.FootprintBytes()
	if pending != nil {
		b += pending.FootprintBytes()
	}
	return b
}

// Delete tombstones key in the active memtable. The tombstone hides any value in the generation
// being flushed and in the blob store, and survives the flush: Flush writes it into the SSTable
// as a real delete marker (see sstable.BlockHandle.Deleted), so a delete of an already-flushed
// key stays deleted. It used to hold only for as long as the tombstone lived in memory - the
// flush dropped it and the value came straight back from the SSTable underneath.
func (mgr *Manager) Delete(key codec.Hash) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.active.Delete(key)
}

// Get returns the value for key from the newest generation that has one. The result is
// read-only: a value served from an SSTable may be the block cache's shared copy (see
// sstable.LsmBlobStore.Lookup), and one served from a memtable is the arena's own bytes
// (lookupShared), so neither is copied per hit.
func (mgr *Manager) Get(key codec.Hash) []byte {
	mgr.mu.Lock()
	active := mgr.active
	pending := mgr.pendingFlush
	mgr.mu.Unlock()
	if v, deleted, found := active.lookupShared(key); found {
		if deleted {
			return nil
		}
		return v
	}
	if pending != nil {
		if v, deleted, found := pending.lookupShared(key); found {
			if deleted {
				return nil
			}
			return v
		}
	}
	// LsmBlobStore.Lookup stops at the newest table with an opinion, tombstone included, rather
	// than searching for the first non-nil value.
	v, _, _ := mgr.blobStore.Lookup(key)
	return v
}

// Contains reports whether Get would find a value for key, without reading it out of an
// SSTable - see sstable.LsmBlobStore.Contains.
func (mgr *Manager) Contains(key codec.Hash) bool {
	mgr.mu.Lock()
	active := mgr.active
	pending := mgr.pendingFlush
	mgr.mu.Unlock()
	if deleted, found := active.has(key); found {
		return !deleted
	}
	if pending != nil {
		if deleted, found := pending.has(key); found {
			return !deleted
		}
	}
	return mgr.blobStore.Contains(key)
}

// Flush writes the active memtable out as an SSTable and starts a fresh one. pendingFlush is
// cleared only once the write has actually succeeded: clearing it before Finish (the call
// that can fail on I/O) left the flushed-but-not-yet-durable generation reachable from
// neither active (already swapped), pendingFlush, nor the blob store (AddTable never
// reached), so Get reported every write in it as simply absent. On failure the generation
// stays visible through pendingFlush until the caller decides how to recover; it is still not
// durable, and a subsequent successful flush replaces pendingFlush with its own snapshot.
// Mirrors MemTableManager.flush in kdb-storage-memtable.
func (mgr *Manager) Flush(level int) (sstable.Handle, error) {
	mgr.mu.Lock()
	snap := mgr.active
	mgr.pendingFlush = snap
	mgr.active = NewSortedTable()
	mgr.mu.Unlock()

	writer := sstable.NewDefaultWriter(mgr.io, mgr.namespaceID, level)
	count := 0
	for _, e := range snap.snapshotEntries() {
		// Tombstones are written, not skipped. Skipping them is what made a delete of an
		// already-flushed key temporary: the tombstone lived only in the memtable, so the next
		// flush erased the only record that the key had been deleted, and the value reappeared
		// from the SSTable it had been flushed into earlier.
		if e.deleted {
			writer.Delete(e.key)
		} else {
			writer.Put(e.key, e.value)
		}
		count++
	}
	if count == 0 {
		mgr.clearPending(snap)
		return sstable.Handle{}, nil
	}
	handle, err := writer.Finish()
	if err != nil {
		return sstable.Handle{}, err
	}
	mgr.blobStore.AddTable(handle)
	mgr.clearPending(snap)
	return handle, nil
}

// clearPending drops the pending-flush generation, but only if it is still the one this flush
// staged - a later flush may already have replaced it.
func (mgr *Manager) clearPending(snap *SortedTable) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if mgr.pendingFlush == snap {
		mgr.pendingFlush = nil
	}
}
