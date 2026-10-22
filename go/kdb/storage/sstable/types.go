package sstable

import (
	"context"
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

	// sidecar bookkeeping (sidecar_store.go), guarded by mu.
	sidecarState

	// seq names the tables this store's writers finish (NewWriter).
	seq *Sequence

	// afterSearchOrder, when set, runs between a lookup taking its table list and reading any
	// table - the window a concurrent compaction used to delete an input in. Tests only.
	afterSearchOrder func()
}

// NewWriter returns a writer for a table this store will hold: named with the store's sequence, so
// it sorts after every table the store holds (see tiering.go).
func (s *LsmBlobStore) NewWriter(level int) *DefaultWriter {
	w := NewDefaultWriter(s.io, s.namespaceID, level)
	w.seq = s.seq
	return w
}

// NewWriterUnique is NewWriter as NewDefaultWriterUnique.
func (s *LsmBlobStore) NewWriterUnique(level, n int) *DefaultWriter {
	w := NewDefaultWriterUnique(s.io, s.namespaceID, level, n)
	w.seq = s.seq
	return w
}

// NewLsmBlobStore returns an LSM blob store.
func NewLsmBlobStore(io storage.PlatformIOShim, namespaceID string, cache *BlockCache) *LsmBlobStore {
	s := &LsmBlobStore{
		io:          io,
		namespaceID: namespaceID,
		cache:       cache,
		indexes:     newTableIndexCache(DefaultIndexCacheBytes),
		readers:     make(map[string]int),
		doomed:      make(map[string]struct{}),
		seq:         NewSequence(io, namespaceID),
	}
	if r := sidecarResolver(io); r != nil {
		s.sidecarPaths = r
		s.sidecarBuild = true
		s.sidecars = make(map[string]*sidecarSlot)
	}
	return s
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
// Each table answers from its sidecar when it has one (sidecar.go): a Bloom filter check, and a
// binary search of a mapped sorted index only when the filter cannot rule the key out. A table
// without one answers from the store's index cache, so a probe costs a map lookup per table plus
// one block read on a hit. It used to build a fresh reader per table per probe, and
// each fresh reader read the whole file to learn its size and then re-read and re-parsed the
// footer - time proportional to the table, on every probe (issue #43's slow recovery).
func (s *LsmBlobStore) Lookup(key codec.Hash) (value []byte, deleted, found bool) {
	order := s.pin()
	defer s.release(order)
	if s.afterSearchOrder != nil {
		s.afterSearchOrder()
	}
	for _, h := range order {
		bh, ok, err := s.probe(h, &key)
		if err != nil {
			// A read error is treated the way it always has been - as "this table cannot answer" -
			// so one damaged segment does not make the whole store unreadable.
			continue
		}
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
		bh, ok, err := s.probe(h, &key)
		if err != nil {
			continue
		}
		if ok {
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
	var unmap []*sidecarSlot
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
			unmap = append(unmap, s.takeSidecarLocked(name))
		} else if s.closed {
			// The store closed while this lookup held the table: its sidecar was left mapped for
			// the lookup, and is unmapped now that the last one is done.
			unmap = append(unmap, s.takeSidecarLocked(name))
		}
	}
	s.mu.Unlock()
	closeSidecars(unmap)
	for _, name := range gone {
		_ = s.io.DeleteSegment(name)
		s.removeSidecarFile(name)
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
	// Full is set when the merge took every table in the store - the merge that may drop
	// tombstones - rather than a window of its newest runs (tiering.go).
	Full bool
	// Outputs names the tables that replaced them - one run, in key order - empty when the merge
	// had nothing to write.
	Outputs []string
	// Postponed is set when a bounded caller (engine.CompactBlobStoreUpTo) found the tables too
	// large to merge then: the bytes they hold, or -1 when the shim could not say. Zero for a
	// merge that ran or had nothing to do.
	Postponed int64
}

// Compact merges every registered table into one run (see DefaultCompactionTrigger), when they
// form at least trigger runs, keeping only the keys keep accepts.
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
	return s.CompactContext(context.Background(), trigger, keep)
}

// CompactContext is Compact that abandons the merge when ctx is done, returning ctx's error with
// every table left as it was - see sstable.CompactContext for why abandoning is always safe. For
// a merge running in the background of an open namespace, which closing the namespace must be
// able to stop rather than wait out.
func (s *LsmBlobStore) CompactContext(ctx context.Context, trigger int, keep func(codec.Hash) bool) (CompactionResult, error) {
	return s.CompactContextUpTo(ctx, trigger, keep, -1)
}

// CompactContextUpTo is CompactContext for a caller that must stay bounded: when the merge that is
// due would read more than maxBytes of tables, it merges nothing and reports those bytes in
// Postponed (-1 when the shim cannot size them). A negative maxBytes is no bound.
//
// Which tables the merge takes is decided here - see tiering.go - so a bounded caller can still
// merge a window of small new runs in a store too large to merge whole.
func (s *LsmBlobStore) CompactContextUpTo(ctx context.Context, trigger int, keep func(codec.Hash) bool, maxBytes int64) (CompactionResult, error) {
	return s.compactContext(ctx, trigger, keep, maxBytes, false)
}

// CompactAllContext merges every table in the store into one run, whether or not a merge is due:
// a major compaction. It is for the one case the size-tiered policy never reaches on its own - a
// change that shadowed data across the whole store at once (engine.RefileVersions putting
// locations back over body copies), on a store that may never flush enough new runs to make a
// full merge due. Nothing to do with fewer than two runs.
func (s *LsmBlobStore) CompactAllContext(ctx context.Context, keep func(codec.Hash) bool) (CompactionResult, error) {
	return s.compactContext(ctx, 2, keep, -1, true)
}

func (s *LsmBlobStore) compactContext(ctx context.Context, trigger int, keep func(codec.Hash) bool, maxBytes int64, all bool) (CompactionResult, error) {
	if trigger < 2 {
		trigger = 2
	}
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	// The inputs are pinned like any reader's tables, so the release below is what deletes them -
	// immediately when no lookup holds one, otherwise when the last one lets go.
	pinned := s.pin()
	defer s.release(pinned)
	runs := runsNewestFirst(pinned)
	sized := sizeRuns(s.io, runs)
	lo, hi, full := pickMerge(runs, sized, trigger)
	if all {
		lo, hi, full = 0, 0, false
		if len(runs) >= 2 {
			lo, hi, full = 0, len(runs), true
		}
	}
	if hi == 0 {
		return CompactionResult{}, nil
	}
	if maxBytes >= 0 {
		var bytes int64
		for _, r := range runs[lo:hi] {
			bytes += r.bytes
		}
		if !sized {
			return CompactionResult{Postponed: -1}, nil
		}
		if bytes > maxBytes {
			return CompactionResult{Postponed: bytes}, nil
		}
	}
	s.removePartialTables()
	// Oldest first, so a later table's opinion wins in the merge: the window's tables in search
	// order (newest first), reversed.
	var ordered []Handle
	for i := hi - 1; i >= lo; i-- {
		tables := runs[i].tables
		for j := len(tables) - 1; j >= 0; j-- {
			ordered = append(ordered, tables[j])
		}
	}
	// A full merge's output goes one level below the deepest input, so it is older than anything
	// flushed after it and newer than nothing that survives. A window's output takes the window's
	// place instead: the oldest input's level, and its slot in the table list (below), which is
	// what keeps it newer than every run older than the window and older than every run newer.
	outLevel := 0
	if full {
		for _, h := range ordered {
			if h.Level+1 > outLevel {
				outLevel = h.Level + 1
			}
		}
	} else {
		outLevel = ordered[0].Level
	}
	// A full merge takes every table in the store, so a tombstone here cannot
	// be shadowing a key in a table that is staying: dropping them is safe
	// and is the only way a delete ever reclaims its own space. A window's
	// merge keeps them.
	// The merge counts the keys keep refuses, so the result can say how much was reclaimed
	// rather than only how many files were rewritten.
	//
	// Each input is read through the sidecar the store has open for it (building it where it is
	// missing), so the merge reads sorted, mapped indexes instead of loading footers.
	sources, err := openMergeSources(s.io, ordered, s.sidecarFor)
	if err != nil {
		return CompactionResult{}, err
	}
	merged, dropped, err := compact(ctx, s.io, s.namespaceID, outLevel, ordered, sources, full, keep)
	closeMergeSources(sources)
	switch {
	case err == ErrNothingToCompact:
		merged = nil
	case err != nil:
		return CompactionResult{}, err
	}
	outputs := make(map[string]struct{}, len(merged))
	for _, h := range merged {
		outputs[h.SegmentName] = struct{}{}
	}

	s.mu.Lock()
	// Keep any table registered while the merge ran (a concurrent memtable flush): only the
	// inputs are replaced. Assigning the merge's output outright dropped such a table from the
	// store until the next open rediscovered it.
	merging := make(map[string]struct{}, len(ordered))
	for _, in := range ordered {
		merging[in.SegmentName] = struct{}{}
	}
	var next []Handle
	if full {
		next = append(next, merged...)
	}
	for _, t := range s.tables {
		if _, ok := merging[t.SegmentName]; !ok {
			next = append(next, t)
		} else if !full && t.SegmentName == ordered[0].SegmentName {
			next = append(next, merged...) // the window's slot: see outLevel
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
	result := CompactionResult{Merged: len(ordered), Dropped: dropped, Full: full}
	for _, h := range merged {
		result.Outputs = append(result.Outputs, h.SegmentName)
	}
	var now []string
	var unmap []*sidecarSlot
	for _, in := range ordered {
		if _, ok := outputs[in.SegmentName]; ok {
			continue
		}
		s.indexes.remove(in.SegmentName)
		if s.readers[in.SegmentName] > 1 {
			// Held by a lookup besides this compaction's own pin. Its sidecar stays mapped for
			// that lookup and is unmapped by the release that deletes the table.
			s.doomed[in.SegmentName] = struct{}{}
			result.Deferred++
			continue
		}
		now = append(now, in.SegmentName)
		// Only this compaction holds it, and the compaction is done reading it.
		unmap = append(unmap, s.takeSidecarLocked(in.SegmentName))
	}
	s.mu.Unlock()
	closeSidecars(unmap)

	// After the unlock, and still complete: the inputs are unregistered, and readBlock caches a
	// block only for a registered table, under the lock the swap held. Every block of theirs that
	// can ever be in the cache is already there, so this cannot miss one.
	for _, in := range ordered {
		if _, ok := outputs[in.SegmentName]; !ok {
			s.cache.RemoveSegment(in.SegmentName)
		}
	}

	for _, name := range now {
		s.removeSidecarFile(name)
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

// removePartialTables deletes the partial tables a merge left behind when its process died
// mid-write (see CompactContext). Nothing reads them - they are outside the names DiscoverTables
// registers - so they cost only disk, and only until the next merge.
//
// Run by a merge, under compactMu, rather than by DiscoverTables: an open does not know that no
// merge is writing one right now (a read-only open beside a writable one does not), while a merge
// holding compactMu is the only writer of this namespace's partial tables there is.
func (s *LsmBlobStore) removePartialTables() {
	names, err := s.io.ListSegments(s.namespaceID)
	if err != nil {
		return
	}
	prefix := storio.SegmentNameBuilder.SSTablePartialPrefix(s.namespaceID)
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			_ = s.io.DeleteSegment(name)
		}
	}
}

// TableBytes reports how many bytes the registered tables occupy on disk, and false when the
// shim cannot say without reading every table (see storage.SegmentSizer) - a caller deciding
// whether a merge is small enough to run now should then treat it as not small.
func (s *LsmBlobStore) TableBytes() (int64, bool) {
	sizer, ok := s.io.(storage.SegmentSizer)
	if !ok {
		return 0, false
	}
	var total int64
	for _, h := range s.Tables() {
		n, err := sizer.SegmentSize(h.SegmentName)
		if err != nil {
			return 0, false
		}
		total += n
	}
	return total, true
}

// PendingMerge reports the bytes of tables the next merge would read - what CompactContextUpTo
// compares with its bound - and whether one is due at all. bytes is -1 when the shim cannot size
// them. For a caller that wants to know before doing the work a merge needs first.
func (s *LsmBlobStore) PendingMerge(trigger int) (bytes int64, due bool) {
	if trigger < 2 {
		trigger = 2
	}
	s.mu.Lock()
	order := append([]Handle(nil), s.searchOrderLocked()...)
	s.mu.Unlock()
	runs := runsNewestFirst(order)
	sized := sizeRuns(s.io, runs)
	lo, hi, _ := pickMerge(runs, sized, trigger)
	if hi == 0 {
		return 0, false
	}
	if !sized {
		return -1, true
	}
	for _, r := range runs[lo:hi] {
		bytes += r.bytes
	}
	return bytes, true
}

// RunCount reports how many runs the registered tables form - what DefaultCompactionTrigger
// counts. A flushed table is a run of its own; the tables one merge wrote are one run.
func (s *LsmBlobStore) RunCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return countRuns(s.tables)
}

// AddTable registers a flushed table.
func (s *LsmBlobStore) AddTable(handle Handle) {
	s.seq.Observe(handle.SegmentName)
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
// Tables register oldest first, by the age in their names (byAge, see
// tiering.go), so a lookup searches them in the order they were written and
// a tombstone shadows what it did before the restart. Tables named without
// an age - Kotlin's, and this package's older ones - register first, in name
// order.
func (s *LsmBlobStore) DiscoverTables() error {
	names, err := s.io.ListSegments(s.namespaceID)
	if err != nil {
		return err
	}
	prefix := storio.SegmentNameBuilder.NamespacePrefix(s.namespaceID) + "sstable/"
	var tables []string
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			tables = append(tables, name)
		}
	}
	byAge(tables)
	for _, name := range tables {
		level := 0
		rest := name[len(prefix):]
		if slash := strings.IndexByte(rest, '/'); slash > 1 && rest[0] == 'L' {
			if n, convErr := strconv.Atoi(rest[1:slash]); convErr == nil {
				level = n
			}
		}
		s.AddTable(Handle{Level: level, SegmentName: name})
	}
	s.seq.listedAll()
	return nil
}
