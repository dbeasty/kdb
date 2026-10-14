package attachstore

import (
	"encoding/binary"
	"fmt"
)

// Binary deltas, git-pack style: a target is a sequence of instructions against a base - copy a
// run of the base, or insert literal bytes. A version of an attachment is stored as a delta against
// the version that replaced it (Store.StoreAsDelta), so the newest bytes are always whole and an
// older version costs what changed.
//
// Encoding: uvarint target length, then instructions until the end of the segment:
//
//	0x00 uvarint n, then n literal bytes
//	0x01 uvarint offset, uvarint n: copy base[offset : offset+n]
//
// A delta is read by random access (deltaSource): the instructions are indexed, the literal bytes
// and base ranges read only where a read lands.
//
// Encoding never holds either version whole. The base is indexed at every block-aligned offset by a
// hash of the block there (the block size grows with the base, so the index stays under ~1M
// entries); the target is scanned with a rolling hash of the same width through a small page cache;
// a hit is verified, extended forwards and backwards into the pending literal, and emitted as a
// copy. Output goes to emit in pieces of at most outPiece bytes.

const (
	opLiteral  = 0x00
	opCopy     = 0x01
	deltaPrime = 0x100000001b3
	maxIndex   = 1 << 20
	outPiece   = 1 << 20
	maxLiteral = 1 << 20 // a literal run longer than this is emitted in pieces
	pageBytes  = 1 << 20
	cachePages = 4
)

// errDeltaCorrupt is a stored delta that does not parse: damaged bytes, like any other ErrCorrupt.
var errDeltaCorrupt = fmt.Errorf("%w: corrupt delta", ErrCorrupt)

// pageCache serves single bytes and short runs of a source a page at a time.
type pageCache struct {
	src   source
	pages map[int64][]byte
	order []int64
}

func newPageCache(src source) *pageCache {
	return &pageCache{src: src, pages: map[int64][]byte{}}
}

func (c *pageCache) page(i int64) ([]byte, error) {
	if p, ok := c.pages[i]; ok {
		return p, nil
	}
	n := min(int64(pageBytes), c.src.size()-i*pageBytes)
	var buf []byte
	if len(c.order) >= cachePages {
		oldest := c.order[0]
		c.order = c.order[1:]
		buf = c.pages[oldest][:0]
		delete(c.pages, oldest)
	}
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if err := c.src.readAt(buf, i*pageBytes); err != nil {
		return nil, err
	}
	c.pages[i] = buf
	c.order = append(c.order, i)
	return buf, nil
}

func (c *pageCache) byteAt(off int64) (byte, error) {
	p, err := c.page(off / pageBytes)
	if err != nil {
		return 0, err
	}
	return p[off%pageBytes], nil
}

// encodeDelta writes target as a delta against base to emit.
func encodeDelta(base, target source, emit func([]byte) error) error {
	block := int64(32)
	for base.size()/block > maxIndex {
		block *= 2
	}
	out := binary.AppendUvarint(nil, uint64(target.size()))
	flush := func(force bool) error {
		if len(out) == 0 || (!force && len(out) < outPiece) {
			return nil
		}
		err := emit(out)
		out = out[:0]
		return err
	}
	tc := newPageCache(target)
	literal := func(from, to int64) error {
		for from < to {
			n := min(to-from, maxLiteral)
			out = append(out, opLiteral)
			out = binary.AppendUvarint(out, uint64(n))
			start := len(out)
			out = append(out, make([]byte, n)...)
			if err := target.readAt(out[start:], from); err != nil {
				return err
			}
			from += n
			if err := flush(false); err != nil {
				return err
			}
		}
		return nil
	}
	if base.size() < block || target.size() < block {
		if err := literal(0, target.size()); err != nil {
			return err
		}
		return flush(true)
	}

	// Index the base, a page at a time.
	index := make(map[uint64]int64, base.size()/block)
	pg := make([]byte, 0, pageBytes-pageBytes%block)
	for off := int64(0); off+block <= base.size(); off += int64(cap(pg)) {
		n := min(int64(cap(pg)), base.size()-off)
		n -= n % block
		pg = pg[:n]
		if err := base.readAt(pg, off); err != nil {
			return err
		}
		for b := int64(0); b < n; b += block {
			h := blockHash(pg[b : b+block])
			if _, ok := index[h]; !ok {
				index[h] = off + b
			}
		}
	}
	bc := newPageCache(base)

	pow := uint64(1)
	for i := int64(0); i < block-1; i++ {
		pow *= deltaPrime
	}
	hashAt := func(i int64) (uint64, error) {
		var h uint64
		for k := int64(0); k < block; k++ {
			b, err := tc.byteAt(i + k)
			if err != nil {
				return 0, err
			}
			h = h*deltaPrime + uint64(b)
		}
		return h, nil
	}
	same := func(bo, to int64) (bool, error) {
		for k := int64(0); k < block; k++ {
			x, err := bc.byteAt(bo + k)
			if err != nil {
				return false, err
			}
			y, err := tc.byteAt(to + k)
			if err != nil {
				return false, err
			}
			if x != y {
				return false, nil
			}
		}
		return true, nil
	}

	tn, bn := target.size(), base.size()
	litStart, i := int64(0), int64(0)
	h, err := hashAt(0)
	if err != nil {
		return err
	}
	for i+block <= tn {
		if bo, ok := index[h]; ok {
			match, err := same(bo, i)
			if err != nil {
				return err
			}
			if match {
				start, bstart := i, bo
				for start > litStart && bstart > 0 {
					x, _ := bc.byteAt(bstart - 1)
					y, _ := tc.byteAt(start - 1)
					if x != y {
						break
					}
					start--
					bstart--
				}
				end, bend := i+block, bo+block
				for end < tn && bend < bn {
					x, err := bc.byteAt(bend)
					if err != nil {
						return err
					}
					y, err := tc.byteAt(end)
					if err != nil {
						return err
					}
					if x != y {
						break
					}
					end++
					bend++
				}
				if err := literal(litStart, start); err != nil {
					return err
				}
				out = append(out, opCopy)
				out = binary.AppendUvarint(out, uint64(bstart))
				out = binary.AppendUvarint(out, uint64(end-start))
				if err := flush(false); err != nil {
					return err
				}
				litStart, i = end, end
				if i+block <= tn {
					if h, err = hashAt(i); err != nil {
						return err
					}
				}
				continue
			}
		}
		if i+block >= tn {
			break
		}
		first, err := tc.byteAt(i)
		if err != nil {
			return err
		}
		next, err := tc.byteAt(i + block)
		if err != nil {
			return err
		}
		h = (h-uint64(first)*pow)*deltaPrime + uint64(next)
		i++
	}
	if err := literal(litStart, tn); err != nil {
		return err
	}
	return flush(true)
}

func blockHash(b []byte) uint64 {
	var h uint64
	for _, c := range b {
		h = h*deltaPrime + uint64(c)
	}
	return h
}
