package memtable

import (
	"sync"

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

// SortedTable is a linked-order memtable.
type SortedTable struct {
	mu    sync.Mutex
	order []codec.Hash
	vals  map[codec.Hash]slot
	bytes int64
}

// slot is one key's state in the table. A tombstone is deleted=true with a nil value, which
// is deliberately distinguishable from "no entry at all" - the whole point of Lookup.
type slot struct {
	value   []byte
	deleted bool
}

// NewSortedTable returns an empty memtable.
func NewSortedTable() *SortedTable {
	return &SortedTable{vals: make(map[codec.Hash]slot)}
}

// Put writes value for key. SizeBytes nets out any value being replaced (including a
// tombstone's zero) rather than only ever growing.
func (m *SortedTable) Put(key codec.Hash, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.vals[key]
	if !ok {
		m.order = append(m.order, key)
	}
	m.vals[key] = slot{value: value}
	m.bytes += int64(len(value)) - int64(len(old.value))
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
func (m *SortedTable) Lookup(key codec.Hash) (value []byte, deleted, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.vals[key]
	if !ok {
		return nil, false, false
	}
	if s.deleted {
		return nil, true, true
	}
	return append([]byte(nil), s.value...), false, true
}

// Delete tombstones key, subtracting any value it replaces from SizeBytes. A key deleted
// before it was ever written still joins the insertion order, so a later Put of it is not
// silently dropped from the flush snapshot.
func (m *SortedTable) Delete(key codec.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.vals[key]
	if !ok {
		m.order = append(m.order, key)
	}
	m.vals[key] = slot{deleted: true}
	m.bytes -= int64(len(old.value))
}

func (m *SortedTable) SizeBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bytes
}

func (m *SortedTable) snapshotEntries() []entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]entry, 0, len(m.order))
	for _, k := range m.order {
		s := m.vals[k]
		out = append(out, entry{key: k, value: s.value, deleted: s.deleted})
	}
	return out
}

type entry struct {
	key     codec.Hash
	value   []byte
	deleted bool
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

// Delete tombstones key in the active memtable. The tombstone hides any value in the
// generation being flushed and in the blob store, but is dropped at the next flush: the
// SSTable format has no deleted marker, so a delete of a key that is already in an SSTable
// only holds for as long as the tombstone lives in memory. Persisting tombstones needs an
// SSTable format change, which per docs/go-porting.md has to originate on the Kotlin side.
func (mgr *Manager) Delete(key codec.Hash) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.active.Delete(key)
}

func (mgr *Manager) Get(key codec.Hash) []byte {
	mgr.mu.Lock()
	active := mgr.active
	pending := mgr.pendingFlush
	mgr.mu.Unlock()
	if v, deleted, found := active.Lookup(key); found {
		if deleted {
			return nil
		}
		return v
	}
	if pending != nil {
		if v, deleted, found := pending.Lookup(key); found {
			if deleted {
				return nil
			}
			return v
		}
	}
	return mgr.blobStore.Get(key)
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
		if e.deleted {
			continue
		}
		writer.Put(e.key, e.value)
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
