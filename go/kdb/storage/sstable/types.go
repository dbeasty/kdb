package sstable

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
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
	for _, h := range s.searchOrder() {
		reader := NewDefaultReader(s.io, h)
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

// searchOrder returns the tables newest-first.
//
// Level ascending, then reverse insertion order within a level: level 0 is
// where a memtable flush lands, and compaction writes its output one level
// down, so a table at a higher level is older than everything above it.
// Before compaction existed every table was level 0 and this was plain
// insertion order, which is what it still reduces to for a store that has
// never compacted.
func (s *LsmBlobStore) searchOrder() []Handle {
	s.mu.Lock()
	tables := append([]Handle(nil), s.tables...)
	s.mu.Unlock()
	order := make([]int, len(tables))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		ia, ib := order[a], order[b]
		if tables[ia].Level != tables[ib].Level {
			return tables[ia].Level < tables[ib].Level
		}
		return ia > ib
	})
	out := make([]Handle, len(tables))
	for i, idx := range order {
		out[i] = tables[idx]
	}
	return out
}

// Tables returns the registered tables. For compaction and for tests; not
// on any read path.
func (s *LsmBlobStore) Tables() []Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Handle(nil), s.tables...)
}

// CompactionResult reports one compaction pass.
type CompactionResult struct {
	// Merged is how many tables went in, Removed how many were actually
	// deleted afterwards. They differ only when a deletion failed.
	Merged  int
	Removed int
	// Dropped is how many keys the keep predicate refused - superseded
	// document versions that will never be asked for again.
	Dropped int
	// Output names the table that replaced them, empty when the merge had
	// nothing to write.
	Output string
}

// Compact merges every registered table into one, when there are more than
// trigger of them, keeping only the keys keep accepts.
//
// keep is what turns this from a deduplicator into a garbage collector,
// and the difference decides whether the store is bounded at all. Merging
// alone only removes keys written more than once; a namespace that writes
// a *new* version of a document on every commit produces a distinct
// content hash every time, so nothing is a duplicate and the merged table
// is exactly as large as the sum of its inputs. Only dropping versions
// nothing can reach again makes the store a function of what is stored.
//
// A nil keep keeps everything, which is the honest default for a caller
// that cannot say what is reachable.
//
// The ordering is the safety argument, and it is the same one delta-log
// truncation makes: write the merged table, swap it in, *then* delete the
// inputs. A crash between the swap and the deletions leaves the inputs on
// disk alongside their replacement, which the next open rediscovers - and
// that is harmless, because every key in this store is the hash of its own
// value, so an old table and the merged table cannot disagree about one.
//
// The exception, stated rather than hidden: a *tombstone* in the merged
// table can be shadowed by a leftover input holding the value it deleted,
// because a leftover input is discovered at level 0 and the merged output
// sits below it. Nothing in this codebase writes a tombstone to an SSTable
// today (memtable.Manager.Delete has no production caller), so the window
// is currently unreachable - but anything that starts writing them has to
// close it, most simply by making discovery prefer the newest file rather
// than listing order.
func (s *LsmBlobStore) Compact(trigger int, keep func(codec.Hash) bool) (CompactionResult, error) {
	if trigger < 2 {
		trigger = 2
	}
	inputs := s.Tables()
	if len(inputs) < trigger {
		return CompactionResult{}, nil
	}
	// Oldest first, so a later table's opinion wins in the merge.
	ordered := s.searchOrder()
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}
	// One level below the deepest input, so the output is older than
	// anything flushed after it and newer than nothing that survives.
	outLevel := 0
	for _, h := range ordered {
		if h.Level+1 > outLevel {
			outLevel = h.Level + 1
		}
	}
	// Every table in the store is going in, so a tombstone here cannot be
	// shadowing a key in a table that is staying: dropping them is safe
	// and is the only way a delete ever reclaims its own space.
	// Counted before the merge so the result can say how much was
	// reclaimed rather than only how many files were rewritten.
	dropped := 0
	if keep != nil {
		seen := make(map[codec.Hash]struct{})
		for _, h := range ordered {
			index, err := NewDefaultReader(s.io, h).Index()
			if err != nil {
				continue
			}
			for k := range index {
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = struct{}{}
				if !keep(k) {
					dropped++
				}
			}
		}
	}
	merged, err := Compact(s.io, s.namespaceID, outLevel, ordered, true, keep)
	switch {
	case err == ErrNothingToCompact:
		merged = Handle{}
	case err != nil:
		return CompactionResult{}, err
	}

	s.mu.Lock()
	if merged.SegmentName != "" {
		s.tables = []Handle{merged}
	} else {
		s.tables = nil
	}
	s.mu.Unlock()

	result := CompactionResult{Merged: len(ordered), Output: merged.SegmentName, Dropped: dropped}
	for _, in := range ordered {
		if in.SegmentName == merged.SegmentName {
			continue
		}
		if err := s.io.DeleteSegment(in.SegmentName); err != nil {
			// The table is already unreachable through this store; leaving
			// the file behind costs disk, not correctness, and the next
			// compaction will try again.
			continue
		}
		result.Removed++
	}
	return result, nil
}

// AddTable registers a flushed table.
func (s *LsmBlobStore) AddTable(handle Handle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy rather than append in place so a Get holding the old slice header keeps reading a
	// stable backing array.
	s.tables = append(append([]Handle(nil), s.tables...), handle)
}

// DiscoverTables registers every SSTable already on disk for this
// namespace, so a process that did not write them can still read them.
//
// Without this the table list starts empty on every open and everything
// previously flushed is invisible - which made the blob store durable in
// name only, since a value survived being written but not being reopened.
// That is fine for a pure cache and fatal for the tree objects that let a
// historical read resolve by address (see engine/tree_objects.go).
//
// Registration order is file-listing order, not write order, which for
// this store is not the problem it would be for a general LSM: every key
// here is the hash of the value stored under it, so two tables holding the
// same key hold identical bytes and "which is newer" cannot change an
// answer. Tombstones would break that, and nothing tombstones a
// content-addressed object.
func (s *LsmBlobStore) DiscoverTables() error {
	names, err := s.io.ListSegments(s.namespaceID)
	if err != nil {
		return err
	}
	prefix := storio.SegmentNameBuilder.NamespacePrefix(s.namespaceID) + "sstable/"
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		level := 0
		rest := name[len(prefix):]
		if slash := strings.IndexByte(rest, '/'); slash > 1 && rest[0] == 'L' {
			if n, convErr := strconv.Atoi(rest[1:slash]); convErr == nil {
				level = n
			}
		}
		s.AddTable(Handle{Level: level, SegmentName: name})
	}
	return nil
}
