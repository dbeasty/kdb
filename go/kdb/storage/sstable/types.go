package sstable

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// BlockHandle points at a compressed block in a segment, or - when Deleted is set - records that
// the key was deleted and no block exists for it at all.
type BlockHandle struct {
	Offset           int64
	CompressedSize   int
	UncompressedSize int
	// Deleted marks a tombstone: this table says the key is gone, and older tables must not be
	// consulted for it. Before the format carried this, a delete of a key that had already been
	// flushed held only for as long as its tombstone lived in the memtable - the next flush
	// dropped it, and the value came back from the SSTable underneath.
	Deleted bool
}

// Handle references a sealed SSTable file.
type Handle struct {
	FileHash    codec.Hash
	Level       int
	SegmentName string
}

// Writer builds an SSTable segment.
type Writer interface {
	Put(key codec.Hash, value []byte)
	// Delete records a tombstone for key: this table asserts the key is gone, shadowing whatever
	// older tables hold for it.
	Delete(key codec.Hash)
	Finish() (Handle, error)
}

// Reader reads keys from an SSTable.
type Reader interface {
	Get(key codec.Hash) ([]byte, error)
	// Lookup reports what this table knows about key. found=false means the table has never seen
	// it and the caller should keep searching older tables; found=true with deleted=true is a
	// tombstone, and the search must stop there. Get cannot express the difference - it returns
	// nil for both - which is exactly how a delete used to fall through to an older generation.
	Lookup(key codec.Hash) (value []byte, deleted, found bool, err error)
}

// BlockCache is an LRU-ish block cache keyed by hash and offset.
type BlockCache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	order    []cacheKey
	data     map[cacheKey][]byte
}

type cacheKey struct {
	hash   codec.Hash
	offset int64
}

// NewBlockCache returns a cache with the given byte capacity.
func NewBlockCache(capacityBytes int64) *BlockCache {
	return &BlockCache{
		capacity: capacityBytes,
		data:     make(map[cacheKey][]byte),
	}
}

// Get loads or fetches a block.
func (c *BlockCache) Get(key codec.Hash, offset int64, loader func() ([]byte, error)) ([]byte, error) {
	k := cacheKey{hash: key, offset: offset}
	c.mu.Lock()
	if v, ok := c.data[k]; ok {
		c.mu.Unlock()
		return append([]byte(nil), v...), nil
	}
	c.mu.Unlock()
	v, err := loader()
	if err != nil {
		return nil, err
	}
	c.put(k, v)
	return v, nil
}

func (c *BlockCache) put(k cacheKey, v []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used += int64(len(v))
	c.data[k] = append([]byte(nil), v...)
	c.order = append(c.order, k)
	for c.used > c.capacity && len(c.order) > 0 {
		eldest := c.order[0]
		c.order = c.order[1:]
		if old, ok := c.data[eldest]; ok {
			c.used -= int64(len(old))
			delete(c.data, eldest)
		}
	}
}

// LsmBlobStore layers SSTables for blob lookup.
type LsmBlobStore struct {
	io          storage.PlatformIOShim
	namespaceID string
	cache       *BlockCache

	// mu guards tables: AddTable runs on the flush path while Get runs on the read path, and
	// nothing above serializes the two - appending to the slice under a concurrent read was a
	// straight data race.
	mu     sync.Mutex
	tables []Handle
}

// NewLsmBlobStore returns an LSM blob store.
func NewLsmBlobStore(io storage.PlatformIOShim, namespaceID string, cache *BlockCache) *LsmBlobStore {
	return &LsmBlobStore{io: io, namespaceID: namespaceID, cache: cache}
}

// Get searches tables newest-first, stopping at the first table that has an opinion about key -
// including a tombstone, which means the key is gone and the older tables underneath must not be
// consulted. Reading through a tombstone (which is what searching for the first non-nil value
// did) is how a flushed delete used to resurrect the value it deleted.
func (s *LsmBlobStore) Get(key codec.Hash) []byte {
	value, _, _ := s.Lookup(key)
	return value
}

// Lookup is Get with the distinction Get cannot express: whether the key is absent from every
// table, or present as a tombstone.
func (s *LsmBlobStore) Lookup(key codec.Hash) (value []byte, deleted, found bool) {
	s.mu.Lock()
	tables := s.tables
	s.mu.Unlock()
	for i := len(tables) - 1; i >= 0; i-- {
		reader := NewDefaultReader(s.io, tables[i])
		v, del, ok, err := reader.Lookup(key)
		if err != nil || !ok {
			// A read error is treated the way it always has been - as "this table cannot answer" -
			// so one damaged segment does not make the whole store unreadable.
			continue
		}
		if del {
			return nil, true, true
		}
		return v, false, true
	}
	return nil, false, false
}

// AddTable registers a flushed table.
func (s *LsmBlobStore) AddTable(handle Handle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy rather than append in place so a Get holding the old slice header keeps reading a
	// stable backing array.
	s.tables = append(append([]Handle(nil), s.tables...), handle)
}
