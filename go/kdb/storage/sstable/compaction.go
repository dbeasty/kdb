package sstable

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
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
	if len(inputs) == 0 {
		return Handle{}, fmt.Errorf("sstable: compaction needs at least one input table")
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
	// Values are deliberately not held here. The index is
	// key -> BlockHandle, tens of bytes per key, so this stays proportional
	// to the key count rather than to the data - the same bargain every
	// other index in this engine makes.
	type owner struct {
		input  int
		handle BlockHandle
	}
	indexes := make([]map[codec.Hash]BlockHandle, len(inputs))
	final := make(map[codec.Hash]owner)
	for i, in := range inputs {
		index, err := NewDefaultReader(io, in).Index()
		if err != nil {
			// One unreadable table must not cost the whole store its
			// compaction, and must not silently drop the keys it held
			// either - so the compaction is abandoned rather than
			// completed with a hole in it.
			return Handle{}, fmt.Errorf("sstable: compaction could not read %s: %w", in.SegmentName, err)
		}
		indexes[i] = index
		for key, bh := range index {
			final[key] = owner{input: i, handle: bh}
		}
	}

	// Sorted, so the output is deterministic: the same inputs produce the
	// same file hash, which is what makes a compaction verifiable and
	// repeatable rather than merely successful.
	writer := NewDefaultWriter(io, namespaceID, outLevel)
	wrote := 0
	for _, key := range sortedKeys(final) {
		o := final[key]
		if keep != nil && !keep(key) {
			// Unreachable: the caller says nothing can ask for this key
			// again. Dropping it is the only way a superseded version ever
			// stops costing disk - see LsmBlobStore.Compact for what makes
			// a "no" here safe.
			continue
		}
		if o.handle.Deleted {
			if dropTombstones {
				continue
			}
			writer.Delete(key)
			wrote++
			continue
		}
		value, err := NewDefaultReader(io, inputs[o.input]).ReadBlockAt(o.handle)
		if err != nil {
			return Handle{}, fmt.Errorf(
				"sstable: compaction could not read a block of %s: %w", inputs[o.input].SegmentName, err)
		}
		writer.Put(key, value)
		wrote++
	}
	if wrote == 0 {
		// Every input was empty, or was nothing but tombstones being
		// dropped. Finish would write a table with no entries, which is a
		// file that can only ever cost a read.
		return Handle{}, ErrNothingToCompact
	}
	return writer.Finish()
}

// ErrNothingToCompact reports a merge whose inputs held nothing worth
// writing out. Not a failure: the caller deletes the inputs and is done.
var ErrNothingToCompact = fmt.Errorf("sstable: the inputs hold nothing to write")

// sortedKeys orders a key set by its bytes, so a compaction's output does
// not depend on Go's map iteration order.
func sortedKeys[V any](m map[codec.Hash]V) []codec.Hash {
	out := make([]codec.Hash, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Bytes, out[j].Bytes
		for x := range a {
			if a[x] != b[x] {
				return a[x] < b[x]
			}
		}
		return false
	})
	return out
}
