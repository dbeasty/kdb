package index

import (
	"bytes"
	"hash/maphash"
	"slices"
)

// keyTable interns the distinct keys of one keyed index. Each key is stored once, as its
// order-preserving encoding (keyenc.go) in a byte arena, and is referred to everywhere else by
// a uint32 ordinal. Nothing in it holds a pointer per key: the arena and offset slices are
// single allocations, and the hash index maps a 64-bit hash to an ordinal (collisions are
// chained through next), so the garbage collector never walks it key by key.
//
// Keys are never removed one at a time - an ordinal can still be named by retained history
// after its bucket empties. compact rebuilds the table without the keys nothing names any more.
type keyTable struct {
	seed  maphash.Seed
	arena []byte
	off   []uint32 // ordinal i occupies arena[off[i]:off[i+1]]
	// byHash maps a key's hash to the first ordinal with that hash; next[i] is the ordinal after i
	// with the same hash, plus one (0 ends the chain).
	byHash map[uint64]uint32
	next   []uint32

	// Sort order, for ranges. Built on the first range and maintained from then on: sorted is
	// every ordinal interned before the last merge, in key order; unsorted are the ones interned
	// since, merged in on the next range.
	ordered  bool
	sorted   []uint32
	unsorted []uint32
}

func newKeyTable() *keyTable {
	return &keyTable{seed: maphash.MakeSeed(), off: []uint32{0}, byHash: make(map[uint64]uint32)}
}

func (t *keyTable) len() int { return len(t.off) - 1 }

func (t *keyTable) bytes(ord uint32) []byte { return t.arena[t.off[ord]:t.off[ord+1]] }

// key decodes ordinal ord back into a Key.
func (t *keyTable) key(ord uint32) Key {
	k, _, err := decodeKeyEnc(t.bytes(ord))
	if err != nil {
		return NullKey{}
	}
	return k
}

// find returns the ordinal of the key encoded as enc, if it is interned.
func (t *keyTable) find(enc []byte) (uint32, bool) {
	h := maphash.Bytes(t.seed, enc)
	first, ok := t.byHash[h]
	if !ok {
		return 0, false
	}
	for ord := first; ; {
		if bytes.Equal(t.bytes(ord), enc) {
			return ord, true
		}
		n := t.next[ord]
		if n == 0 {
			return 0, false
		}
		ord = n - 1
	}
}

// intern returns enc's ordinal, adding it if it is new.
func (t *keyTable) intern(enc []byte) uint32 {
	h := maphash.Bytes(t.seed, enc)
	first, ok := t.byHash[h]
	if ok {
		for ord := first; ; {
			if bytes.Equal(t.bytes(ord), enc) {
				return ord
			}
			n := t.next[ord]
			if n == 0 {
				break
			}
			ord = n - 1
		}
	}
	ord := uint32(t.len())
	t.arena = append(t.arena, enc...)
	t.off = append(t.off, uint32(len(t.arena)))
	if ok {
		// Prepend to the chain: next[ord] points at the old first.
		t.next = append(t.next, first+1)
	} else {
		t.next = append(t.next, 0)
	}
	t.byHash[h] = ord
	if t.ordered {
		t.unsorted = append(t.unsorted, ord)
	}
	return ord
}

func (t *keyTable) less(a, b uint32) int { return bytes.Compare(t.bytes(a), t.bytes(b)) }

// order returns every ordinal in key order, building or refreshing the sort order first.
func (t *keyTable) order() []uint32 {
	if !t.ordered {
		t.ordered = true
		t.sorted = make([]uint32, t.len())
		for i := range t.sorted {
			t.sorted[i] = uint32(i)
		}
		slices.SortFunc(t.sorted, t.less)
		t.unsorted = nil
		return t.sorted
	}
	if len(t.unsorted) > 0 {
		slices.SortFunc(t.unsorted, t.less)
		merged := make([]uint32, 0, len(t.sorted)+len(t.unsorted))
		i, j := 0, 0
		for i < len(t.sorted) && j < len(t.unsorted) {
			if t.less(t.sorted[i], t.unsorted[j]) <= 0 {
				merged = append(merged, t.sorted[i])
				i++
			} else {
				merged = append(merged, t.unsorted[j])
				j++
			}
		}
		merged = append(merged, t.sorted[i:]...)
		merged = append(merged, t.unsorted[j:]...)
		t.sorted, t.unsorted = merged, t.unsorted[:0]
	}
	return t.sorted
}

// lowerBound is the first position in order whose key is >= enc.
func (t *keyTable) lowerBound(order []uint32, enc []byte) int {
	lo, hi := 0, len(order)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(t.bytes(order[mid]), enc) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// upperBound is the first position in order whose key is > enc.
func (t *keyTable) upperBound(order []uint32, enc []byte) int {
	lo, hi := 0, len(order)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(t.bytes(order[mid]), enc) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// compact rebuilds the table with only the ordinals keep reports true for, and returns the
// old-to-new ordinal map (dropped ordinals map to ^uint32(0)).
func (t *keyTable) compact(keep func(ord uint32) bool) []uint32 {
	n := t.len()
	remap := make([]uint32, n)
	fresh := &keyTable{seed: t.seed, off: []uint32{0}, byHash: make(map[uint64]uint32)}
	for i := 0; i < n; i++ {
		if !keep(uint32(i)) {
			remap[i] = ^uint32(0)
			continue
		}
		remap[i] = fresh.intern(t.bytes(uint32(i)))
	}
	if t.ordered {
		fresh.ordered = true
		fresh.sorted = make([]uint32, 0, fresh.len())
		for _, o := range t.order() {
			if r := remap[o]; r != ^uint32(0) {
				fresh.sorted = append(fresh.sorted, r)
			}
		}
	}
	*t = *fresh
	return remap
}

// approxBytes is the table's heap footprint.
func (t *keyTable) approxBytes() int {
	return cap(t.arena) + 4*(cap(t.off)+cap(t.next)+cap(t.sorted)+cap(t.unsorted)) + 24*len(t.byHash)
}
