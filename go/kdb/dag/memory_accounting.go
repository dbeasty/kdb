package dag

import (
	"unsafe"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Map-entry estimates for Go's swiss-table maps: the key and value, plus a control byte, at the
// table's typical load between half and 7/8 full. Estimates rather than measurements because a
// Go map does not report its size; TestGraphResidentBytesTracksHeap holds them to the heap.
const (
	hashMapEntryOverhead = 24 // control byte and load-factor slack, per entry, on top of key+value
	avgMessageBytes      = 24 // commit messages are short; their text is not tracked per byte
)

// GraphResidentBytes estimates what the commit graph's resident tier holds on the heap, apart
// from commit operations (OperationsResidentBytes) and trees (the storage engine's): the slab of
// resident commit records, the hash -> slot index, the side maps for messages and extra parents,
// the segment tier's hidden/override sets, and the transaction index. Computed from lengths, so
// O(1) and safe for a metrics scrape; mapped segment files are not counted, because they are
// page cache, not heap.
//
// Under GraphMemoryGo the resident tier is a map of whole commits; that is charged per entry
// at the struct's size, which undercounts their parent slices - that mode is a fallback kept
// for comparison, not the default.
func (d *InMemoryCommitDag) GraphResidentBytes() int64 {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := &d.nodes
	const hashKey = int64(unsafe.Sizeof(codec.Hash{}))
	var b int64
	if s.goMode {
		b += int64(len(s.goNodes)) * (hashKey + int64(unsafe.Sizeof(goNode{})) + hashMapEntryOverhead)
	} else {
		b += int64(len(s.chunks)) * slotsPerChunk * graphRecordSize
		b += int64(len(s.index)) * (hashKey + 4 + hashMapEntryOverhead)
		b += int64(cap(s.free)) * 4
	}
	b += int64(len(s.messages)) * (hashKey + 16 + hashMapEntryOverhead + avgMessageBytes)
	b += int64(len(s.extra)) * (hashKey + 24 + hashMapEntryOverhead + 2*hashKey)
	b += int64(len(s.hidden)) * (hashKey + hashMapEntryOverhead)
	b += int64(len(s.genOverride)) * (hashKey + 4 + hashMapEntryOverhead)
	b += int64(len(d.txIndex)) * (int64(unsafe.Sizeof(codec.UUID{})) + hashKey + hashMapEntryOverhead)
	return b
}
