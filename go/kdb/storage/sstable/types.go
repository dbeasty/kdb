package sstable

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// BlockHandle points at a compressed block in a segment.
type BlockHandle struct {
	Offset           int64
	CompressedSize   int
	UncompressedSize int
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
	Finish() (Handle, error)
}

// Reader reads keys from an SSTable.
type Reader interface {
	Get(key codec.Hash) ([]byte, error)
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
	tables      []Handle
}

// NewLsmBlobStore returns an LSM blob store.
func NewLsmBlobStore(io storage.PlatformIOShim, namespaceID string, cache *BlockCache) *LsmBlobStore {
	return &LsmBlobStore{io: io, namespaceID: namespaceID, cache: cache}
}

// Get searches tables newest-first.
func (s *LsmBlobStore) Get(key codec.Hash) []byte {
	for i := len(s.tables) - 1; i >= 0; i-- {
		reader := NewDefaultReader(s.io, s.tables[i])
		if v, err := reader.Get(key); err == nil && v != nil {
			return v
		}
	}
	return nil
}

// AddTable registers a flushed table.
func (s *LsmBlobStore) AddTable(handle Handle) {
	s.tables = append(s.tables, handle)
}
