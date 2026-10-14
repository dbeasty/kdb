package sstable

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// SSTable compaction: merging the tables a run of memtable flushes leaves
// behind, so on-disk size follows what is stored rather than how many
// times it has been flushed.
//
// Without this, every flush writes another table holding whatever was hot,
// and nothing ever merges or drops them. A namespace rewriting the same
// documents keeps a full copy per flush: measured at 25 documents over 40
// open/write/close cycles, the store grew to 320 KB for a dataset of about
// 13 KB, and it grows without bound. That is the one remaining structure
// whose size is a function of write history rather than of data, and it is
// why history=none could bound its delta log and still not bound its
// footprint.
//
// The merge is unusually simple here because of what the keys are. Every
// key is the SHA-256 of the value stored under it, so two tables holding
// the same key hold identical bytes and "which is newer" cannot change an
// answer for a value. Only tombstones can, and they are handled explicitly
// below.

// DefaultCompactionTrigger is how many tables a store may accumulate
// before a compaction is worth running.
//
// Four, because the cost of merging is proportional to the live data and
// the benefit is proportional to the duplication, so the trigger wants to
// be small - but every compaction rewrites everything, so triggering on
// two would rewrite the whole store on every other flush.
const DefaultCompactionTrigger = 4

// Compact merges inputs into one table at outLevel and returns it.
//
// inputs are given oldest-first, so a later table's opinion about a key
// wins - which for values is a formality (identical keys carry identical
// bytes) and for tombstones is the whole point.
//
// dropTombstones may be set only when inputs is *every* table in the
// store. A tombstone says "this key is gone, do not consult anything
// older", so dropping one while any un-merged table might still hold the
// key would resurrect it. When in doubt, leave it false: keeping a
// tombstone costs one index entry.
//
// keep, when non-nil, is asked about every key and drops the ones it
// refuses. That is how a superseded document version stops costing disk:
// without it a compaction only removes *duplicates*, and a store that
// writes a new version of a document every time still grows without bound
// because every version is a distinct content hash and none of them is a
// duplicate of anything. Passing nil keeps everything, which is what a
// store whose reachability the caller cannot determine must do.
func Compact(
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) (Handle, error) {
	return CompactContext(context.Background(), io, namespaceID, outLevel, inputs, dropTombstones, keep)
}

// compactionCheckEvery is how many keys a merge handles between looks at its context: often
// enough that a cancelled merge stops within milliseconds, rarely enough to cost nothing.
const compactionCheckEvery = 4096

// CompactContext is Compact that gives up when ctx is done, returning ctx's error and leaving
// no table behind. Abandoning is always safe: the output is written under a partial name
// (io.SegmentNameBuilder.SSTablePartial) that nothing registers as a table, and is deleted when
// the merge stops, so the inputs stay exactly as they were. Once the last key is written the
// footer and rename run to the end - they are the short tail of the merge.
func CompactContext(
	ctx context.Context,
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) (Handle, error) {
	h, _, err := compact(ctx, io, namespaceID, outLevel, inputs, dropTombstones, keep)
	return h, err
}

// compact is CompactContext that also reports how many distinct keys keep refused - what
// LsmBlobStore.CompactContext reports as reclaimed, counted here because this is where every key
// is asked about exactly once.
func compact(
	ctx context.Context,
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) (Handle, int, error) {
	if len(inputs) == 0 {
		return Handle{}, 0, fmt.Errorf("sstable: compaction needs at least one input table")
	}

	// Two passes, and the first reads indexes only.
	//
	// A single streaming pass that wrote each input's entries in turn was
	// the obvious implementation and was wrong: dropping a tombstone meant
	// skipping it, which left whatever an *earlier* input had written for
	// that key standing - resurrecting the value the tombstone deleted.
	// Deciding every key's final owner before writing anything makes that
	// impossible to express.
	//
	// Values are deliberately not held here. Every input's index goes into one slice, which is
	// sorted by key and then by where each entry came from, so a key's last entry is its final
	// owner: the latest input, and within an input its last footer line - the same winner a map
	// filled input by input chose. The slice then becomes the output's own index (see below), so
	// a merge holds about 60 bytes per key in all; the map, sorted key list and output index it
	// used to hold were three times that.
	merge, err := loadMergeOrder(ctx, io, inputs)
	if err != nil {
		return Handle{}, 0, err
	}
	sort.Sort(merge)

	// Streamed: each value is read, encoded and appended as its key comes up, so the merge holds
	// the index and one batch of blocks rather than every value in the store. It used to collect
	// them all in a DefaultWriter and write at Finish - 2.7 GB of heap to merge 471 MB of tables.
	//
	// Streaming means a file exists while the merge runs, so it is written under a partial name
	// and renamed once complete: DiscoverTables registers every name under sstable/, and a table
	// a crash left half-written there would be read as one. A shim that cannot rename writes the
	// table's own name directly, which leaves that window open for it; both shims in this
	// codebase rename.
	fileID, err := codec.RandomUUID()
	if err != nil {
		return Handle{}, 0, err
	}
	name := storio.SegmentNameBuilder.SSTable(namespaceID, outLevel, fileID.String())
	writing := name
	renamer, canRename := io.(storage.SegmentRenamer)
	if canRename {
		writing = storio.SegmentNameBuilder.SSTablePartial(namespaceID, fileID.String())
	}
	done := false
	defer func() {
		if !done {
			// Cancelled, failed, or nothing to write: whatever was appended goes. Deleting a
			// segment that was never created is not an error.
			_ = io.DeleteSegment(writing)
		}
	}()

	// Sorted, so the output is deterministic: the same inputs produce the
	// same file hash, which is what makes a compaction verifiable and
	// repeatable rather than merely successful.
	//
	// The output index is written over the merge slice itself. Entry w of the output is appended
	// only after entry r >= w of the merge has been read, so nothing is overwritten before it is
	// used, and the slice never has to grow.
	b := newTableBuilder(io, writing, 0)
	b.index = merge.entries[:0]
	readers := make([]*DefaultReader, len(inputs))
	for i, in := range inputs {
		readers[i] = NewDefaultReader(io, in)
	}
	wrote, dropped := 0, 0
	for r, n := 0, 0; r < len(merge.entries); n++ {
		if n%compactionCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return Handle{}, 0, err
			}
		}
		// The last of this key's entries is its final owner.
		for r+1 < len(merge.entries) && merge.entries[r+1].key == merge.entries[r].key {
			r++
		}
		key, bh, input := merge.entries[r].key, merge.entries[r].handle, merge.input(r)
		r++
		if keep != nil && !keep(key) {
			// Unreachable: the caller says nothing can ask for this key
			// again. Dropping it is the only way a superseded version ever
			// stops costing disk - see LsmBlobStore.Compact for what makes
			// a "no" here safe.
			dropped++
			continue
		}
		if bh.Deleted {
			if dropTombstones {
				continue
			}
			b.delete(key)
			wrote++
			continue
		}
		value, err := readers[input].ReadBlockAt(bh)
		if err != nil {
			return Handle{}, 0, fmt.Errorf(
				"sstable: compaction could not read a block of %s: %w", inputs[input].SegmentName, err)
		}
		if err := b.put(key, value); err != nil {
			return Handle{}, 0, err
		}
		wrote++
	}
	if wrote == 0 {
		// Every input was empty, or was nothing but tombstones being
		// dropped. Finish would write a table with no entries, which is a
		// file that can only ever cost a read.
		return Handle{}, dropped, ErrNothingToCompact
	}
	tableSize, fileHash, err := b.finish()
	if err != nil {
		return Handle{}, 0, err
	}
	if canRename {
		if err := renamer.RenameSegment(writing, name); err != nil {
			return Handle{}, 0, err
		}
		// From here a failure deletes the table under its own name, which is complete but not
		// registered: discovered by a later open it would merely duplicate the inputs' keys.
		writing = name
	}
	if err := io.SealSegment(name); err != nil {
		return Handle{}, 0, err
	}
	done = true
	writeSidecar(io, name, b.index, tableSize, fileHash)
	return Handle{FileHash: fileHash, Level: outLevel, SegmentName: name}, dropped, nil
}

// mergeOrder is every input's index entries in one slice, with each entry's position in load
// order alongside: inputs oldest first, each in its footer's order. Sorting by key and then by
// that position puts a key's entries oldest to newest.
type mergeOrder struct {
	entries []indexEntry
	ords    []uint32
	// ends[i] is one past the last position input i's entries were loaded at.
	ends []int
}

func (m *mergeOrder) Len() int { return len(m.entries) }

func (m *mergeOrder) Less(i, j int) bool {
	if c := bytes.Compare(m.entries[i].key.Bytes[:], m.entries[j].key.Bytes[:]); c != 0 {
		return c < 0
	}
	return m.ords[i] < m.ords[j]
}

func (m *mergeOrder) Swap(i, j int) {
	m.entries[i], m.entries[j] = m.entries[j], m.entries[i]
	m.ords[i], m.ords[j] = m.ords[j], m.ords[i]
}

// input is the input the entry now at position i came from.
func (m *mergeOrder) input(i int) int {
	ord := int(m.ords[i])
	return sort.Search(len(m.ends), func(k int) bool { return ord < m.ends[k] })
}

// loadMergeOrder reads every input's footer index into one mergeOrder, sized up front from the
// footers' lengths so the slice is allocated once.
func loadMergeOrder(ctx context.Context, io storage.PlatformIOShim, inputs []Handle) (*mergeOrder, error) {
	type footer struct {
		size     int64
		indexLen int
	}
	footers := make([]footer, len(inputs))
	capacity := 0
	for i, in := range inputs {
		r := NewDefaultReader(io, in)
		size, err := r.segmentSize()
		if err == nil && size >= 40+footerTrailerSize {
			footers[i].size = size
			footers[i].indexLen, err = r.readFooterIndexLen(size)
		}
		if err != nil {
			// One unreadable table must not cost the whole store its
			// compaction, and must not silently drop the keys it held
			// either - so the compaction is abandoned rather than
			// completed with a hole in it.
			return nil, fmt.Errorf("sstable: compaction could not read %s: %w", in.SegmentName, err)
		}
		capacity += footers[i].indexLen/minFooterLine + 1
	}
	m := &mergeOrder{entries: make([]indexEntry, 0, capacity), ends: make([]int, len(inputs))}
	for i, in := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if footers[i].size > 0 {
			var err error
			m.entries, err = NewDefaultReader(io, in).appendIndexEntries(m.entries, footers[i].size, footers[i].indexLen)
			if err != nil {
				return nil, fmt.Errorf("sstable: compaction could not read %s: %w", in.SegmentName, err)
			}
		}
		m.ends[i] = len(m.entries)
	}
	if len(m.entries) > math.MaxUint32 {
		return nil, fmt.Errorf("sstable: %d entries is more than one compaction can order", len(m.entries))
	}
	m.ords = make([]uint32, len(m.entries))
	for i := range m.ords {
		m.ords[i] = uint32(i)
	}
	return m, nil
}

// ErrNothingToCompact reports a merge whose inputs held nothing worth
// writing out. Not a failure: the caller deletes the inputs and is done.
var ErrNothingToCompact = fmt.Errorf("sstable: the inputs hold nothing to write")
