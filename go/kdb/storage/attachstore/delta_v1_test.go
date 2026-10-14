package attachstore

import "encoding/binary"

// The version 1 delta encoder, as it shipped: the format v1 deltas on disk were written in. Kept
// for tests, which write v1 deltas to prove they still read; the store writes v2 (delta.go).

const (
	v1MaxIndex   = 1 << 20
	v1OutPiece   = 1 << 20
	v1MaxLiteral = 1 << 20
	v1PageBytes  = 1 << 20
	v1CachePages = 4
)

// v1PageCache serves single bytes and short runs of a source a page at a time.
type v1PageCache struct {
	src   source
	pages map[int64][]byte
	order []int64
}

func newV1PageCache(src source) *v1PageCache {
	return &v1PageCache{src: src, pages: map[int64][]byte{}}
}

func (c *v1PageCache) page(i int64) ([]byte, error) {
	if p, ok := c.pages[i]; ok {
		return p, nil
	}
	n := min(int64(v1PageBytes), c.src.size()-i*v1PageBytes)
	var buf []byte
	if len(c.order) >= v1CachePages {
		oldest := c.order[0]
		c.order = c.order[1:]
		buf = c.pages[oldest][:0]
		delete(c.pages, oldest)
	}
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if err := c.src.readAt(buf, i*v1PageBytes); err != nil {
		return nil, err
	}
	c.pages[i] = buf
	c.order = append(c.order, i)
	return buf, nil
}

func (c *v1PageCache) byteAt(off int64) (byte, error) {
	p, err := c.page(off / v1PageBytes)
	if err != nil {
		return 0, err
	}
	return p[off%v1PageBytes], nil
}

// encodeDeltaV1 writes target as a delta against base to emit.
func encodeDeltaV1(base, target source, emit func([]byte) error) error {
	block := int64(32)
	for base.size()/block > v1MaxIndex {
		block *= 2
	}
	out := binary.AppendUvarint(nil, uint64(target.size()))
	flush := func(force bool) error {
		if len(out) == 0 || (!force && len(out) < v1OutPiece) {
			return nil
		}
		err := emit(out)
		out = out[:0]
		return err
	}
	tc := newV1PageCache(target)
	literal := func(from, to int64) error {
		for from < to {
			n := min(to-from, v1MaxLiteral)
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
	pg := make([]byte, 0, v1PageBytes-v1PageBytes%block)
	for off := int64(0); off+block <= base.size(); off += int64(cap(pg)) {
		n := min(int64(cap(pg)), base.size()-off)
		n -= n % block
		pg = pg[:n]
		if err := base.readAt(pg, off); err != nil {
			return err
		}
		for b := int64(0); b < n; b += block {
			h := v1BlockHash(pg[b : b+block])
			if _, ok := index[h]; !ok {
				index[h] = off + b
			}
		}
	}
	bc := newV1PageCache(base)

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

func v1BlockHash(b []byte) uint64 {
	var h uint64
	for _, c := range b {
		h = h*deltaPrime + uint64(c)
	}
	return h
}
