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
	// ordered is tables newest-first, rebuilt only when tables changes. Lookup used to sort a
	// fresh copy on every probe. Never mutated in place once published, so a caller may range
	// over the slice it was handed after the lock is released.
	ordered []Handle
	// indexes caches each table's parsed footer index; also guarded by mu (see tableIndexCache).
	indexes tableIndexCache

	// readers counts, per segment, the lookups currently holding that table (see pin), and
	// doomed names the tables a compaction has swapped out whose files wait for their last
	// reader to let go. Both guarded by mu.
	//
	// Compaction used to delete its inputs the moment it swapped them out, and a lookup that had
	// taken the table list just before the swap went on to read files that were gone. Lookup
	// treats an unreadable table as one that cannot answer, so a key the merged table held came
	// back as not found. A reader now pins every table it may read; a compaction deletes an input
	// at once only when nothing holds it, and otherwise leaves the deletion to the last release.
	readers map[string]int
	doomed  map[string]struct{}
	// compactMu serializes Compact. Two at once would both merge the same inputs, and the second
	// would fail reading a file the first had already deleted.
	compactMu sync.Mutex

	// afterSearchOrder, when set, runs between a lookup taking its table list and reading any
	// table - the window a concurrent compaction used to delete an input in. Tests only.
	afterSearchOrder func()
}

// NewLsmBlobStore returns an LSM blob store.
func NewLsmBlobStore(io storage.PlatformIOShim, namespaceID string, cache *BlockCache) *LsmBlobStore {
	return &LsmBlobStore{
		io:          io,
		namespaceID: namespaceID,
		cache:       cache,
		indexes:     newTableIndexCache(DefaultIndexCacheBytes),
		readers:     make(map[string]int),
		doomed:      make(map[string]struct{}),
	}
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
//
// The returned value is read-only and may be shared: with a block cache, every lookup of a key
// gets the same cached slice rather than a copy, so a caller that modified it would corrupt every
// later read of that block. The same holds for Get.
//
// Each table's index comes from the store's index cache, so a probe costs a map lookup per
// table plus one block read on a hit. It used to build a fresh reader per table per probe, and
// each fresh reader read the whole file to learn its size and then re-read and re-parsed the
// footer - time proportional to the table, on every probe (issue #43's slow recovery).
func (s *LsmBlobStore) Lookup(key codec.Hash) (value []byte, deleted, found bool) {
	order := s.pin()
	defer s.release(order)
	if s.afterSearchOrder != nil {
		s.afterSearchOrder()
	}
	for _, h := range order {
		index, err := s.indexFor(h)
		if err != nil {
			// A read error is treated the way it always has been - as "this table cannot answer" -
			// so one damaged segment does not make the whole store unreadable.
			continue
		}
		bh, ok := index[key]
		if !ok {
			continue
		}
		if bh.Deleted {
			return nil, true, true
		}
		v, err := s.readBlock(h, bh)
		if err != nil {
			continue
		}
		return v, false, true
	}
	return nil, false, false
}

// readBlock returns the decoded block bh names in table h, from the block cache when it is
// there. A block read from disk is cached only while h is still registered - the same rule
// indexFor follows, and for the same reason: Compact unregisters a table and then drops its
// cached blocks, so a lookup that read one of its blocks just before the swap must not put it
// back afterwards, where nothing could ever ask for it again. The check and the insert happen
// under s.mu, which Compact's swap also holds, so the two cannot interleave.
func (s *LsmBlobStore) readBlock(h Handle, bh BlockHandle) ([]byte, error) {
	if v, ok := s.cache.Get(h.SegmentName, bh.Offset); ok {
		return v, nil
	}
	v, err := NewDefaultReader(s.io, h).ReadBlockAt(bh)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		s.mu.Lock()
		if s.registeredLocked(h.SegmentName) {
			s.cache.Put(h.SegmentName, bh.Offset, v)
		}
		s.mu.Unlock()
	}
	return v, nil
}

// BlockCache returns the store's block cache, nil when it has none.
func (s *LsmBlobStore) BlockCache() *BlockCache { return s.cache }

// SetBlockCacheBudget re-bounds the store's block cache. A no-op for a store without one.
func (s *LsmBlobStore) SetBlockCacheBudget(budgetBytes int64) {
	s.cache.SetCapacity(budgetBytes)
}

// BlockCacheBytes reports what the block cache holds, zero for a store without one.
func (s *LsmBlobStore) BlockCacheBytes() int64 { return s.cache.UsedBytes() }

// Contains reports whether the newest table with an opinion about key holds a value for it,
// without reading the value: Lookup's answer to "found and not deleted", from the cached
// indexes alone.
//
// For replay's existence probe (engine.skipExistingObject), which only needs to know a
// content-addressed object is already stored and used to read and decompress every tree object
// it probed to find out. Unlike Lookup it does not check the block is readable, so a damaged
// block counts as present; nothing relies on replay rewriting one, since every object read is
// verified against its content hash and treats a bad block as a miss.
func (s *LsmBlobStore) Contains(key codec.Hash) bool {
	order := s.pin()
	defer s.release(order)
	if s.afterSearchOrder != nil {
		s.afterSearchOrder()
	}
	for _, h := range order {
		index, err := s.indexFor(h)
		if err != nil {
			continue
		}
		if bh, ok := index[key]; ok {
			return !bh.Deleted
		}
	}
	return false
}

// searchOrderLocked returns the tables newest-first. Callers hold s.mu.
//
// Level ascending, then reverse insertion order within a level: level 0 is
// where a memtable flush lands, and compaction writes its output one level
// down, so a table at a higher level is older than everything above it.
// Before compaction existed every table was level 0 and this was plain
// insertion order, which is what it still reduces to for a store that has
// never compacted.
//
// The returned slice is shared and must not be modified.
func (s *LsmBlobStore) searchOrderLocked() []Handle {
	if s.ordered == nil && len(s.tables) > 0 {
		s.ordered = newestFirst(s.tables)
	}
	return s.ordered
}

// pin returns the tables newest-first, as searchOrderLocked does, and holds every one of them until the
// matching release: a compaction that swaps one of them out in the meantime defers deleting its
// file rather than pulling it from under the read. Taking the list and pinning it in one critical
// section is the point - a table cannot be swapped out between the two.
func (s *LsmBlobStore) pin() []Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	order := s.searchOrderLocked()
	for _, h := range order {
		s.readers[h.SegmentName]++
	}
	return order
}

// release lets go of tables taken by pin, and deletes any a compaction has swapped out that no
// reader holds any more. The deletion runs outside the lock, as Compact's own does; a failure
// leaves the file behind, which costs disk until the next open and never an answer (see
// Compact).
func (s *LsmBlobStore) release(order []Handle) {
	var gone []string
	s.mu.Lock()
	for _, h := range order {
		name := h.SegmentName
		if n := s.readers[name]; n > 1 {
			s.readers[name] = n - 1
			continue
		}
		delete(s.readers, name)
		if _, ok := s.doomed[name]; ok {
			delete(s.doomed, name)
			gone = append(gone, name)
		}
	}
	s.mu.Unlock()
	for _, name := range gone {
		_ = s.io.DeleteSegment(name)
	}
}

// setTablesLocked replaces the table list and invalidates the cached search order. Callers hold
// s.mu.
func (s *LsmBlobStore) setTablesLocked(tables []Handle) {
	s.tables = tables
	s.ordered = nil
}

func newestFirst(tables []Handle) []Handle {
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
	// Merged is how many tables went in, Removed how many were deleted
	// before Compact returned, and Deferred how many were still being read
	// and are deleted when their last reader lets go. Merged exceeds
	// Removed+Deferred only when a deletion failed.
	Merged   int
	Removed  int
	Deferred int
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
// "Then delete" may now be later than Compact's return: an input a lookup is still reading is
// deleted when that lookup releases it (see pin). A crash in that window is the same crash as
// one between the swap and the deletions - the inputs survive, the next open registers them
// beside the merged table, and the next compaction merges them in and deletes them - and the
// window is no wider than the longest lookup in flight, which reads one block per table.
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
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	// The inputs are pinned like any reader's tables, so the release below is what deletes them -
	// immediately when no lookup holds one, otherwise when the last one lets go.
	pinned := s.pin()
	defer s.release(pinned)
	if len(pinned) < trigger {
		return CompactionResult{}, nil
	}
	// Oldest first, so a later table's opinion wins in the merge.
	// A copy: the pinned slice is shared with concurrent lookups and is reversed here.
	ordered := append([]Handle(nil), pinned...)
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
			index, err := s.indexFor(h)
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
	// Keep any table registered while the merge ran (a concurrent memtable flush): only the
	// inputs are replaced. Assigning []Handle{merged} outright dropped such a table from the
	// store until the next open rediscovered it.
	merging := make(map[string]struct{}, len(ordered))
	for _, in := range ordered {
		merging[in.SegmentName] = struct{}{}
	}
	var next []Handle
	if merged.SegmentName != "" {
		next = append(next, merged)
	}
	for _, t := range s.tables {
		if _, ok := merging[t.SegmentName]; !ok {
			next = append(next, t)
		}
	}
	s.setTablesLocked(next)
	// Dropped in the same critical section as the swap, so indexFor cannot re-insert one of
	// these after it is gone (it only caches a table that is still registered). A lookup still
	// holding one reads its index from the file, which stays until that lookup releases it.
	//
	// Each input is doomed here, under the lock that also guards pin, so from this point no new
	// reader can take it (it is no longer registered) and the last of the existing ones - this
	// compaction's own release included - deletes it.
	result := CompactionResult{Merged: len(ordered), Output: merged.SegmentName, Dropped: dropped}
	var now []string
	for _, in := range ordered {
		if in.SegmentName == merged.SegmentName {
			continue
		}
		s.indexes.remove(in.SegmentName)
		if s.readers[in.SegmentName] > 1 {
			// Held by a lookup besides this compaction's own pin.
			s.doomed[in.SegmentName] = struct{}{}
			result.Deferred++
			continue
		}
		now = append(now, in.SegmentName)
	}
	s.mu.Unlock()

	// After the unlock, and still complete: the inputs are unregistered, and readBlock caches a
	// block only for a registered table, under the lock the swap held. Every block of theirs that
	// can ever be in the cache is already there, so this cannot miss one.
	for _, in := range ordered {
		if in.SegmentName != merged.SegmentName {
			s.cache.RemoveSegment(in.SegmentName)
		}
	}

	for _, name := range now {
		if err := s.io.DeleteSegment(name); err != nil {
			// The table is already unreachable through this store; leaving
			// the file behind costs disk, not correctness, and the next
			// open rediscovers it and the compaction after that merges it.
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
	s.setTablesLocked(append(append([]Handle(nil), s.tables...), handle))
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
