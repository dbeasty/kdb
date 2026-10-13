package index

import (
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
)

// smallBucketMax is the largest bucket kept as a sorted slice. Above it a bucket becomes a set
// (plus a memoized sorted copy), so filing one more document into a bucket shared by most of the
// namespace stays O(1) rather than a memmove of the whole bucket.
const smallBucketMax = 64

// postingState is the key -> documents map of a keyed index at one cutoff: the bucket of every
// key ordinal, and the keys each document is filed under.
//
// Most keys of most indexes hold exactly one document (an index over an id-like field) and most
// documents are filed under exactly one key (a scalar field). Both of those cases are held in
// flat, pointer-free arrays and maps; only a key shared by several documents, or a document
// filed under several keys (a multikey array field), costs a separate allocation.
type postingState struct {
	cnt  []uint32     // documents filed under each key ordinal
	one  []codec.UUID // the document, for a key with cnt == 1
	many map[uint32]*multiBucket

	docOne  map[codec.UUID]uint32   // the key of a document filed under exactly one
	docMany map[codec.UUID][]uint32 // the keys of a document filed under several
	live    int                     // keys with cnt > 0
}

// multiBucket is a bucket of two or more documents.
type multiBucket struct {
	list   []codec.UUID // sorted; used while set is nil
	set    map[codec.UUID]struct{}
	sorted []codec.UUID // memo of set in order; nil when stale
}

func newPostingState() *postingState {
	return &postingState{
		many:    make(map[uint32]*multiBucket),
		docOne:  make(map[codec.UUID]uint32),
		docMany: make(map[codec.UUID][]uint32),
	}
}

func uuidCmp(a, b codec.UUID) int {
	if a.MSB != b.MSB {
		if uint64(a.MSB) < uint64(b.MSB) {
			return -1
		}
		return 1
	}
	if a.LSB != b.LSB {
		if uint64(a.LSB) < uint64(b.LSB) {
			return -1
		}
		return 1
	}
	return 0
}

func (p *postingState) grow(ord uint32) {
	if int(ord) < len(p.cnt) {
		return
	}
	n := int(ord) + 1
	if c := 2 * len(p.cnt); c > n {
		n = c
	}
	p.cnt = append(p.cnt, make([]uint32, n-len(p.cnt))...)
	p.one = append(p.one, make([]codec.UUID, n-len(p.one))...)
}

func (p *postingState) count(ord uint32) int {
	if int(ord) >= len(p.cnt) {
		return 0
	}
	return int(p.cnt[ord])
}

// docKeys calls fn for each key doc is filed under.
func (p *postingState) docKeys(doc codec.UUID) []uint32 {
	if k, ok := p.docOne[doc]; ok {
		return []uint32{k}
	}
	return p.docMany[doc]
}

func (p *postingState) filed(doc codec.UUID, ord uint32) bool {
	if k, ok := p.docOne[doc]; ok {
		return k == ord
	}
	for _, k := range p.docMany[doc] {
		if k == ord {
			return true
		}
	}
	return false
}

// add files doc under ord and reports whether it was not already filed there.
func (p *postingState) add(doc codec.UUID, ord uint32) bool {
	if p.filed(doc, ord) {
		return false
	}
	if k, ok := p.docOne[doc]; ok {
		delete(p.docOne, doc)
		p.docMany[doc] = []uint32{k, ord}
	} else if ks, ok := p.docMany[doc]; ok {
		p.docMany[doc] = append(ks, ord)
	} else {
		p.docOne[doc] = ord
	}
	p.bucketAdd(doc, ord)
	return true
}

// remove unfiles doc from ord only (the inverse of a successful add).
func (p *postingState) remove(doc codec.UUID, ord uint32) {
	if k, ok := p.docOne[doc]; ok {
		if k != ord {
			return
		}
		delete(p.docOne, doc)
	} else {
		ks := p.docMany[doc]
		i := slices.Index(ks, ord)
		if i < 0 {
			return
		}
		ks = append(ks[:i:i], ks[i+1:]...)
		switch len(ks) {
		case 0:
			delete(p.docMany, doc)
		case 1:
			delete(p.docMany, doc)
			p.docOne[doc] = ks[0]
		default:
			p.docMany[doc] = ks
		}
	}
	p.bucketRemove(doc, ord)
}

// removeDoc unfiles doc from every key and appends those keys, in filing order, to removed.
func (p *postingState) removeDoc(doc codec.UUID, removed []uint32) []uint32 {
	if k, ok := p.docOne[doc]; ok {
		delete(p.docOne, doc)
		p.bucketRemove(doc, k)
		return append(removed, k)
	}
	ks, ok := p.docMany[doc]
	if !ok {
		return removed
	}
	delete(p.docMany, doc)
	for _, k := range ks {
		p.bucketRemove(doc, k)
	}
	return append(removed, ks...)
}

func (p *postingState) bucketAdd(doc codec.UUID, ord uint32) {
	p.grow(ord)
	switch p.cnt[ord] {
	case 0:
		p.one[ord] = doc
		p.live++
	case 1:
		first := p.one[ord]
		p.one[ord] = codec.UUID{}
		list := []codec.UUID{first, doc}
		if uuidCmp(doc, first) < 0 {
			list[0], list[1] = doc, first
		}
		p.many[ord] = &multiBucket{list: list}
	default:
		p.many[ord].add(doc)
	}
	p.cnt[ord]++
}

func (p *postingState) bucketRemove(doc codec.UUID, ord uint32) {
	switch p.count(ord) {
	case 0:
		return
	case 1:
		if p.one[ord] != doc {
			return
		}
		p.one[ord] = codec.UUID{}
		p.live--
	default:
		b := p.many[ord]
		if !b.remove(doc) {
			return
		}
		if p.cnt[ord] == 2 {
			p.one[ord] = b.only()
			delete(p.many, ord)
		}
	}
	p.cnt[ord]--
}

// ids returns ord's documents in UUID order. The slice may be shared: callers that keep or
// modify it must copy.
func (p *postingState) ids(ord uint32) []codec.UUID {
	switch p.count(ord) {
	case 0:
		return nil
	case 1:
		return p.one[int(ord) : int(ord)+1]
	default:
		return p.many[ord].sortedIDs()
	}
}

func (p *postingState) contains(ord uint32, doc codec.UUID) bool {
	switch p.count(ord) {
	case 0:
		return false
	case 1:
		return p.one[ord] == doc
	default:
		return p.many[ord].contains(doc)
	}
}

func (b *multiBucket) add(doc codec.UUID) {
	if b.set != nil {
		b.set[doc] = struct{}{}
		b.sorted = nil
		return
	}
	i, _ := slices.BinarySearchFunc(b.list, doc, uuidCmp)
	b.list = slices.Insert(b.list, i, doc)
	if len(b.list) > smallBucketMax {
		b.set = make(map[codec.UUID]struct{}, 2*len(b.list))
		for _, id := range b.list {
			b.set[id] = struct{}{}
		}
		b.list = nil
	}
}

// remove reports whether doc was in the bucket.
func (b *multiBucket) remove(doc codec.UUID) bool {
	if b.set != nil {
		if _, ok := b.set[doc]; !ok {
			return false
		}
		delete(b.set, doc)
		b.sorted = nil
		if len(b.set) <= smallBucketMax/2 {
			b.list = b.sortedFromSet()
			b.set = nil
		}
		return true
	}
	i, ok := slices.BinarySearchFunc(b.list, doc, uuidCmp)
	if !ok {
		return false
	}
	b.list = slices.Delete(b.list, i, i+1)
	return true
}

func (b *multiBucket) contains(doc codec.UUID) bool {
	if b.set != nil {
		_, ok := b.set[doc]
		return ok
	}
	_, ok := slices.BinarySearchFunc(b.list, doc, uuidCmp)
	return ok
}

// only returns the single remaining document of a bucket about to drop to one.
func (b *multiBucket) only() codec.UUID {
	if b.set != nil {
		for id := range b.set {
			return id
		}
	}
	return b.list[0]
}

func (b *multiBucket) sortedFromSet() []codec.UUID {
	out := make([]codec.UUID, 0, len(b.set))
	for id := range b.set {
		out = append(out, id)
	}
	slices.SortFunc(out, uuidCmp)
	return out
}

func (b *multiBucket) sortedIDs() []codec.UUID {
	if b.set == nil {
		return b.list
	}
	// A low-selectivity key (a tag shared by most documents) is looked up on every query that
	// mentions it; sorting it each time cost more than the query, so the order is memoized
	// until the bucket next changes.
	if b.sorted == nil {
		b.sorted = b.sortedFromSet()
	}
	return b.sorted
}

// remap rewrites every key ordinal through m (see keyTable.compact). Keys mapped to ^0 must be
// empty.
func (p *postingState) remap(m []uint32, n int) {
	cnt := make([]uint32, n)
	one := make([]codec.UUID, n)
	many := make(map[uint32]*multiBucket, len(p.many))
	for old := range p.cnt {
		if old >= len(m) {
			break
		}
		nw := m[old]
		if nw == ^uint32(0) {
			continue
		}
		cnt[nw] = p.cnt[old]
		one[nw] = p.one[old]
		if b, ok := p.many[uint32(old)]; ok {
			many[nw] = b
		}
	}
	p.cnt, p.one, p.many = cnt, one, many
	for doc, k := range p.docOne {
		p.docOne[doc] = m[k]
	}
	for _, ks := range p.docMany {
		for i, k := range ks {
			ks[i] = m[k]
		}
	}
}
