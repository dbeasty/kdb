package engine

import (
	"encoding/binary"

	"github.com/limidus/kdb/go/kdb/codec"
)

// slotIndex maps a content hash to its slot in a shard's slot array. It is an open-addressing
// table of slot numbers only: the key is not stored here, because the slot it names already
// holds it, so a probe compares against slots[i].hash. Four bytes a bucket at a load factor
// kept between 3/8 and 3/4 comes to 5-11 bytes a version, against ~56 for a
// map[codec.Hash]uint32 (36-byte slots plus control bytes, at the swiss table's load) - and
// unlike a Go map it shrinks when the shard does. It holds no pointers.
//
// Linear probing with backward-shift deletion, so there are no tombstones and lookups never
// degrade with churn.
type slotIndex struct {
	table []uint32 // slot+1; 0 is an empty bucket
	n     int
	shift uint8 // 64 - log2(len(table))
}

const slotIndexMinBuckets = 16

// bucketOf spreads a content hash over the table. Content hashes are SHA-256 output, but the
// shard is chosen by byte 0 and test hashes vary in only a few bytes, so fold all four words
// and take the top bits of a Fibonacci multiply rather than trusting any one word.
func (x *slotIndex) bucketOf(h *codec.Hash) uint64 {
	k := binary.LittleEndian.Uint64(h.Bytes[0:8]) ^ binary.LittleEndian.Uint64(h.Bytes[8:16]) ^
		binary.LittleEndian.Uint64(h.Bytes[16:24]) ^ binary.LittleEndian.Uint64(h.Bytes[24:32])
	return (k * 0x9E3779B97F4A7C15) >> x.shift
}

func (x *slotIndex) find(slots []bodyRec, h *codec.Hash) (uint32, bool) {
	if x.n == 0 {
		return 0, false
	}
	mask := uint64(len(x.table) - 1)
	for b := x.bucketOf(h); ; b = (b + 1) & mask {
		v := x.table[b]
		if v == 0 {
			return 0, false
		}
		if slots[v-1].hash == *h {
			return v - 1, true
		}
	}
}

// insert adds h -> slot. h must not be present.
func (x *slotIndex) insert(slots []bodyRec, h *codec.Hash, slot uint32) {
	if (x.n+1)*4 > len(x.table)*3 {
		x.resize(slots, max(slotIndexMinBuckets, len(x.table)*2))
	}
	x.place(h, slot)
	x.n++
}

func (x *slotIndex) place(h *codec.Hash, slot uint32) {
	mask := uint64(len(x.table) - 1)
	b := x.bucketOf(h)
	for x.table[b] != 0 {
		b = (b + 1) & mask
	}
	x.table[b] = slot + 1
}

// remove deletes h, which must be present, shifting later members of its probe run back so the
// table never needs tombstones.
func (x *slotIndex) remove(slots []bodyRec, h *codec.Hash) {
	mask := uint64(len(x.table) - 1)
	b := x.bucketOf(h)
	for slots[x.table[b]-1].hash != *h {
		b = (b + 1) & mask
	}
	hole := b
	for j := (hole + 1) & mask; x.table[j] != 0; j = (j + 1) & mask {
		home := x.bucketOf(&slots[x.table[j]-1].hash)
		// Move j into the hole unless its home lies cyclically in (hole, j].
		if (j > hole && (home <= hole || home > j)) || (j < hole && home <= hole && home > j) {
			x.table[hole] = x.table[j]
			hole = j
		}
	}
	x.table[hole] = 0
	x.n--
	if len(x.table) > slotIndexMinBuckets && x.n*8 < len(x.table) {
		x.resize(slots, len(x.table)/2)
	}
}

// resize rebuilds the table with buckets buckets (a power of two) from the slots it names.
func (x *slotIndex) resize(slots []bodyRec, buckets int) {
	old := x.table
	x.table = make([]uint32, buckets)
	x.shift = uint8(64 - log2(buckets))
	for _, v := range old {
		if v != 0 {
			x.place(&slots[v-1].hash, v-1)
		}
	}
}

// rebuild re-derives the table from scratch over every used slot, for after the slots move.
func (x *slotIndex) rebuild(slots []bodyRec, used int) {
	buckets := slotIndexMinBuckets
	for used*2 > buckets { // at most half full after a rebuild
		buckets *= 2
	}
	x.table = make([]uint32, buckets)
	x.shift = uint8(64 - log2(buckets))
	x.n = 0
	for i := range slots {
		if slots[i].chunk != slotFree {
			x.place(&slots[i].hash, uint32(i))
			x.n++
		}
	}
}

func (x *slotIndex) bytes() int64 { return int64(cap(x.table)) * 4 }

func log2(n int) int {
	l := 0
	for n > 1 {
		n >>= 1
		l++
	}
	return l
}
