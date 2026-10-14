package attachstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The reference index is a durable note the store keeps for its owner's garbage collection: every
// content hash the history below some commits references (Refs, sorted), those commits (Heads),
// and a token naming the history those commits stand on (Floor). A pass that finds the heads still
// below the current ones and the floor unchanged reads only the commits since and adds what they
// reference, instead of walking all of history; one that finds history changed below them (a
// truncation, an archived or deleted branch) rebuilds it.
//
// Like the mark it lives in its own segments, <ns>/attachment/r-<generation>, a new one written
// under the next generation before the older ones go. A torn or unreadable index reads as the next
// older one, or as none - a rebuild, slower but never wrong. It is read only by a pass and not held
// in memory between passes.

const refIndexMagic = "KDBATR1"

// RefIndex is the reference index (see above).
type RefIndex struct {
	Heads []codec.Hash
	Floor codec.Hash
	Refs  []codec.Hash // sorted, distinct
}

// Has reports whether h is one of the index's references.
func (ix RefIndex) Has(h codec.Hash) bool {
	i := sort.Search(len(ix.Refs), func(i int) bool { return bytes.Compare(ix.Refs[i].Bytes[:], h.Bytes[:]) >= 0 })
	return i < len(ix.Refs) && ix.Refs[i] == h
}

// SortHashes sorts hs and drops repeats, in place.
func SortHashes(hs []codec.Hash) []codec.Hash {
	sort.Slice(hs, func(i, j int) bool { return bytes.Compare(hs[i].Bytes[:], hs[j].Bytes[:]) < 0 })
	out := hs[:0]
	for i, h := range hs {
		if i == 0 || h != hs[i-1] {
			out = append(out, h)
		}
	}
	return out
}

func (s *Store) refIndexName(gen int) string { return fmt.Sprintf("%sr-%06d", s.prefix, gen) }

func (s *Store) isRefIndex(name string) (int, bool) {
	if !strings.HasPrefix(name, s.prefix+"r-") {
		return 0, false
	}
	gen, err := strconv.Atoi(strings.TrimPrefix(name, s.prefix+"r-"))
	return gen, err == nil
}

// refIndexSegments lists the index segments, newest generation first. Must hold mu.
func (s *Store) refIndexSegments() ([]string, []int, error) {
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return nil, nil, err
	}
	var segs []string
	var gens []int
	for _, name := range names {
		if gen, ok := s.isRefIndex(name); ok {
			segs, gens = append(segs, name), append(gens, gen)
		}
	}
	order := make([]int, len(segs))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return gens[order[a]] > gens[order[b]] })
	outS, outG := make([]string, len(segs)), make([]int, len(segs))
	for i, j := range order {
		outS[i], outG[i] = segs[j], gens[j]
	}
	return outS, outG, nil
}

// RefIndex returns the newest readable reference index; ok=false when there is none.
func (s *Store) RefIndex() (RefIndex, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segs, _, err := s.refIndexSegments()
	if err != nil {
		return RefIndex{}, false, err
	}
	for _, name := range segs {
		if ix, ok := s.readRefIndex(name); ok {
			return ix, true, nil
		}
	}
	return RefIndex{}, false, nil
}

func (s *Store) readRefIndex(name string) (RefIndex, bool) {
	// The lengths come first: the magic and head count, then past the heads and floor the
	// reference count.
	head, err := s.shim.ReadFromSegment(name, 0, len(refIndexMagic)+4)
	if err != nil || len(head) != len(refIndexMagic)+4 || string(head[:len(refIndexMagic)]) != refIndexMagic {
		return RefIndex{}, false
	}
	heads := int64(binary.BigEndian.Uint32(head[len(refIndexMagic):]))
	countAt := int64(len(refIndexMagic)+4) + heads*32 + 32
	count, err := s.shim.ReadFromSegment(name, countAt, 8)
	if err != nil || len(count) != 8 {
		return RefIndex{}, false
	}
	refs := binary.BigEndian.Uint64(count)
	if refs > 1<<40 {
		return RefIndex{}, false
	}
	size := countAt + 8 + int64(refs)*32 + 4
	raw, err := s.shim.ReadFromSegment(name, 0, int(size))
	if err != nil || int64(len(raw)) != size {
		return RefIndex{}, false
	}
	body, crc := raw[:len(raw)-4], raw[len(raw)-4:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:4], crc) {
		return RefIndex{}, false
	}
	off := len(refIndexMagic)
	nHeads := int64(binary.BigEndian.Uint32(body[off:]))
	off += 4
	if int64(len(body)-off) < nHeads*32+32+8 {
		return RefIndex{}, false
	}
	var ix RefIndex
	ix.Heads = make([]codec.Hash, nHeads)
	for i := range ix.Heads {
		copy(ix.Heads[i].Bytes[:], body[off:])
		off += 32
	}
	copy(ix.Floor.Bytes[:], body[off:])
	off += 32
	nRefs := binary.BigEndian.Uint64(body[off:])
	off += 8
	if uint64(len(body)-off) != nRefs*32 {
		return RefIndex{}, false
	}
	ix.Refs = make([]codec.Hash, nRefs)
	for i := range ix.Refs {
		copy(ix.Refs[i].Bytes[:], body[off:])
		off += 32
	}
	return ix, true
}

// SetRefIndex durably records ix (its Refs sorted and distinct) as the reference index, replacing
// the previous one.
func (s *Store) SetRefIndex(ix RefIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segs, gens, err := s.refIndexSegments()
	if err != nil {
		return err
	}
	gen := 1
	if len(gens) > 0 {
		gen = gens[0] + 1
	}
	name := s.refIndexName(gen)
	sum := sha256.New()
	buf := make([]byte, 0, 1<<20)
	write := func(final bool) error {
		if len(buf) == 0 {
			return nil
		}
		if !final {
			sum.Write(buf)
		}
		_, err := s.shim.AppendToSegment(name, buf)
		buf = buf[:0]
		return err
	}
	fail := func(err error) error {
		_ = s.shim.DeleteSegment(name)
		return err
	}
	buf = append(buf, refIndexMagic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ix.Heads)))
	for _, h := range ix.Heads {
		buf = append(buf, h.Bytes[:]...)
	}
	buf = append(buf, ix.Floor.Bytes[:]...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(ix.Refs)))
	for _, h := range ix.Refs {
		if len(buf)+32 > cap(buf) {
			if err := write(false); err != nil {
				return fail(err)
			}
		}
		buf = append(buf, h.Bytes[:]...)
	}
	if err := write(false); err != nil {
		return fail(err)
	}
	buf = append(buf, sum.Sum(nil)[:4]...)
	if err := write(true); err != nil {
		return fail(err)
	}
	if err := s.shim.FlushSegment(name); err != nil {
		return fail(err)
	}
	if err := s.shim.SealSegment(name); err != nil {
		return fail(err)
	}
	for _, old := range segs {
		_ = s.shim.DeleteSegment(old) // a leftover is only an older index, read if the new one is not
	}
	return nil
}
