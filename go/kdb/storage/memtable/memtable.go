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
	Delete(key codec.Hash)
	SizeBytes() int64
}

// SortedTable is a linked-order memtable.
type SortedTable struct {
	mu    sync.Mutex
	order []codec.Hash
	vals  map[codec.Hash][]byte
	bytes int64
}

// NewSortedTable returns an empty memtable.
func NewSortedTable() *SortedTable {
	return &SortedTable{vals: make(map[codec.Hash][]byte)}
}

func (m *SortedTable) Put(key codec.Hash, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vals[key]; !ok {
		m.order = append(m.order, key)
	}
	m.vals[key] = value
	m.bytes += int64(len(value))
}

func (m *SortedTable) Get(key codec.Hash) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.vals[key]
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}

func (m *SortedTable) Delete(key codec.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.vals[key] = nil
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
		out = append(out, entry{key: k, value: m.vals[k]})
	}
	return out
}

type entry struct {
	key   codec.Hash
	value []byte
}

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

func (mgr *Manager) Get(key codec.Hash) []byte {
	mgr.mu.Lock()
	active := mgr.active
	pending := mgr.pendingFlush
	mgr.mu.Unlock()
	if v := active.Get(key); v != nil {
		return v
	}
	if pending != nil {
		if v := pending.Get(key); v != nil {
			return v
		}
	}
	return mgr.blobStore.Get(key)
}

func (mgr *Manager) Flush(level int) (sstable.Handle, error) {
	mgr.mu.Lock()
	snap := mgr.active
	mgr.pendingFlush = snap
	mgr.active = NewSortedTable()
	mgr.mu.Unlock()

	writer := sstable.NewDefaultWriter(mgr.io, mgr.namespaceID, level)
	count := 0
	for _, e := range snap.snapshotEntries() {
		if e.value != nil {
			writer.Put(e.key, e.value)
			count++
		}
	}
	mgr.mu.Lock()
	mgr.pendingFlush = nil
	mgr.mu.Unlock()
	if count == 0 {
		return sstable.Handle{}, nil
	}
	handle, err := writer.Finish()
	if err != nil {
		return sstable.Handle{}, err
	}
	mgr.blobStore.AddTable(handle)
	return handle, nil
}
