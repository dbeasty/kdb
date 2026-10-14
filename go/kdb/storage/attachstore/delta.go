package attachstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Binary deltas, git-pack style: a target is a sequence of instructions against a base - copy a
// run of the base, or insert literal bytes. A version of an attachment is stored as a delta against
// the version that replaced it (Store.DeltaStep), so the newest bytes are always whole and an
// older version costs what changed. Versions of any size are diffed: nothing here holds either
// version, the delta or its instruction list whole.
//
// Instructions (both formats):
//
//	0x00 uvarint n: n literal bytes
//	0x01 uvarint offset, uvarint n: copy base[offset : offset+n]
//
// Format 1 (read only; written before deltas had a size limit lifted): uvarint target length, then
// instructions until the end of the segment, each literal's bytes inline after its instruction.
// Reading it parses every instruction up front, which is fine at the sizes it was written for
// (at most 256 MiB).
//
// Format 2 (pointer flag 4; what the store writes): frames, an index, a footer. Big-endian.
//
//	frame  := the frame's literal bytes, concatenated in instruction order; then its instructions,
//	          whose literals carry no bytes inline (each takes the next n of the frame's literals)
//	index  := one 32-byte entry per frame, ascending: target start, literal offset, instructions
//	          offset, instructions length (segment offsets). A frame covers the target from its
//	          start to the next frame's (the last: to the target's end), exactly.
//	footer := index offset, frame count, target length (u64 each), framedMagic
//
// A read binary-searches the index on disk and parses one frame's instructions (frameOps at most),
// so it holds one frame whatever the version's size. Bytes no index entry points at are ignored.
//
// Encoding (encoder) runs in bounded steps a job carries between calls (deltajob.go):
//
//  1. Chunks first. Both versions keep their 1 MiB chunk leaf hashes; a target chunk whose leaf
//     equals the base's at the same offset is a copy without reading a byte. In-place edits of a
//     huge file cost only the chunks they touch.
//  2. Index the rest of the base: every block-aligned offset in a base chunk that did not match is
//     indexed by a hash of the block there. The block grows with the unmatched bytes so the index
//     stays under Config.DeltaIndexEntries entries - its memory is bounded whatever the size.
//  3. Scan the target with a rolling hash of the same width through a small page cache, where its
//     chunks did not match (content shifted by an insertion, or moved): a hit is verified, extended
//     backwards into the pending literal and forwards (by page compares, and by whole chunks again
//     once the offsets line up), and emitted as a copy.

const (
	opLiteral  = 0x00
	opCopy     = 0x01
	deltaPrime = 0x100000001b3
	minBlock   = 32
	outPiece   = 1 << 20
	maxLiteral = 1 << 20 // a literal run longer than this is emitted in pieces

	defaultIndexEntries = 1 << 20
	defaultPageBytes    = 1 << 20
	defaultFrameOps     = 4096
	// maxFrameOpsBytes bounds a frame's instruction bytes; a reader refuses more. An instruction
	// is at most 21 bytes, so defaultFrameOps never comes near it.
	maxFrameOpsBytes = 1 << 20

	framedMagic     = "KDBDLT2\n"
	frameEntryBytes = 32
	footerBytes     = 32
)

// errDeltaCorrupt is a stored delta that does not parse: damaged bytes, like any other ErrCorrupt.
var errDeltaCorrupt = fmt.Errorf("%w: corrupt delta", ErrCorrupt)

// errDeltaTooBig stops an encoding whose delta is no longer worth storing.
var errDeltaTooBig = errors.New("attachstore: delta too big")

// pager serves single bytes and runs of a source a page at a time, from a few cached pages.
type pager struct {
	src  source
	pb   int64
	idx  [4]int64
	buf  [4][]byte
	used [4]uint64
	tick uint64
	last int
}

func newPager(src source, pageBytes int64) *pager {
	p := &pager{src: src, pb: pageBytes}
	for i := range p.idx {
		p.idx[i] = -1
	}
	return p
}

func (p *pager) page(i int64) ([]byte, error) {
	p.tick++
	if p.idx[p.last] == i {
		p.used[p.last] = p.tick
		return p.buf[p.last], nil
	}
	victim := 0
	for k := range p.idx {
		if p.idx[k] == i {
			p.last, p.used[k] = k, p.tick
			return p.buf[k], nil
		}
		if p.used[k] < p.used[victim] {
			victim = k
		}
	}
	n := min(p.pb, p.src.size()-i*p.pb)
	if i < 0 || n <= 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if int64(cap(p.buf[victim])) < n {
		p.buf[victim] = make([]byte, n)
	}
	b := p.buf[victim][:n]
	p.idx[victim] = -1
	if err := p.src.readAt(b, i*p.pb); err != nil {
		return nil, err
	}
	p.buf[victim], p.idx[victim], p.used[victim], p.last = b, i, p.tick, victim
	return b, nil
}

func (p *pager) at(off int64) (byte, error) {
	i := off / p.pb
	if p.idx[p.last] == i {
		return p.buf[p.last][off-i*p.pb], nil
	}
	b, err := p.page(i)
	if err != nil {
		return 0, err
	}
	return b[off-i*p.pb], nil
}

// span returns the bytes from off to the end of its page.
func (p *pager) span(off int64) ([]byte, error) {
	b, err := p.page(off / p.pb)
	if err != nil {
		return nil, err
	}
	return b[off%p.pb:], nil
}

// mismatch is the length of a and b's common prefix.
func mismatch(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i+4096 <= n && bytes.Equal(a[i:i+4096], b[i:i+4096]) {
		i += 4096
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// leafSet reads a blob's chunk leaf hashes a window at a time.
type leafSet struct {
	src   source // nil: the blob keeps no leaves (it is inline)
	count int64
	start int64
	win   []byte
}

func newLeafSet(s *Store, leaves string, size int64) *leafSet {
	if leaves == "" {
		return &leafSet{}
	}
	count := (size + chunk - 1) / chunk
	return &leafSet{src: segSource{shim: s.shim, name: leaves, n: count * 32}, count: count}
}

func (l *leafSet) get(k int64) (codec.Hash, bool, error) {
	var h codec.Hash
	if l.src == nil || k < 0 || k >= l.count {
		return h, false, nil
	}
	if k < l.start || k >= l.start+int64(len(l.win))/32 {
		n := min(4096, l.count-k)
		if cap(l.win) < int(n*32) {
			l.win = make([]byte, 4096*32)
		}
		l.win = l.win[:n*32]
		if err := l.src.readAt(l.win, k*32); err != nil {
			return h, false, err
		}
		l.start = k
	}
	copy(h.Bytes[:], l.win[(k-l.start)*32:])
	return h, true, nil
}

// encoder is one delta encoding's state. The fields above the per-step ones survive between steps
// (a job keeps them); the sources and caches are set again at the start of each.
type encoder struct {
	tn, bn    int64
	block     int64
	pow       uint64
	index     map[uint64]int64
	entries   int
	frameOps  int
	pageBytes int64
	limit     int64 // the delta is abandoned once it reaches this many bytes

	pos        int64 // target bytes encoded
	contB      int64 // >= 0: target pos lines up with base contB (a copy cut by the last step)
	written    int64 // delta segment bytes, including out
	opsEnd     int64 // target bytes covered by instructions in frames or ops
	frameStart int64
	litOff     int64
	ops        []byte
	nOps       int
	frames     int64
	pend       struct{ b, t, n int64 } // a copy not yet in ops, to merge with the next

	target, base source
	tc, bc       *pager
	tl, bl       *leafSet
	out          []byte
	appendSeg    func([]byte) error // appends to the delta segment
	appendFrame  func([]byte) error // appends an index entry
}

// matched reports whether target chunk k equals the base's chunk k, by their leaves.
func (e *encoder) matched(k int64) (bool, error) {
	a, ok, err := e.tl.get(k)
	if err != nil || !ok {
		return false, err
	}
	b, ok, err := e.bl.get(k)
	return ok && a == b, err
}

// baseUnmatched reports whether base chunk k has no equal target chunk at its offset.
func (e *encoder) baseUnmatched(k int64) (bool, error) {
	ok, err := e.matched(k)
	return !ok, err
}

func (e *encoder) flushOut(force bool) error {
	if len(e.out) == 0 || (!force && len(e.out) < outPiece) {
		return nil
	}
	err := e.appendSeg(e.out)
	e.out = e.out[:0]
	return err
}

func (e *encoder) grow(n int) error {
	e.written += int64(n)
	if e.written >= e.limit {
		return errDeltaTooBig
	}
	return nil
}

func (e *encoder) addOp(op []byte, n int64) error {
	e.ops = append(e.ops, op...)
	e.nOps++
	e.opsEnd += n
	if e.nOps >= e.frameOps {
		return e.closeFrame()
	}
	return nil
}

func (e *encoder) closeFrame() error {
	if e.nOps == 0 {
		return nil
	}
	opsOff := e.written
	e.out = append(e.out, e.ops...)
	if err := e.grow(len(e.ops)); err != nil {
		return err
	}
	var entry [frameEntryBytes]byte
	binary.BigEndian.PutUint64(entry[0:], uint64(e.frameStart))
	binary.BigEndian.PutUint64(entry[8:], uint64(e.litOff))
	binary.BigEndian.PutUint64(entry[16:], uint64(opsOff))
	binary.BigEndian.PutUint64(entry[24:], uint64(len(e.ops)))
	if err := e.appendFrame(entry[:]); err != nil {
		return err
	}
	e.frames++
	e.ops, e.nOps = e.ops[:0], 0
	e.frameStart, e.litOff = e.opsEnd, e.written
	return e.flushOut(false)
}

func (e *encoder) flushCopy() error {
	if e.pend.n == 0 {
		return nil
	}
	op := []byte{opCopy}
	op = binary.AppendUvarint(op, uint64(e.pend.b))
	op = binary.AppendUvarint(op, uint64(e.pend.n))
	n := e.pend.n
	e.pend.n = 0
	return e.addOp(op, n)
}

func (e *encoder) copyOp(b, t, n int64) error {
	if n <= 0 {
		return nil
	}
	if e.pend.n > 0 && e.pend.b+e.pend.n == b && e.pend.t+e.pend.n == t {
		e.pend.n += n
		return nil
	}
	if err := e.flushCopy(); err != nil {
		return err
	}
	e.pend.b, e.pend.t, e.pend.n = b, t, n
	return nil
}

func (e *encoder) literal(from, to int64) error {
	if from >= to {
		return nil
	}
	if err := e.flushCopy(); err != nil {
		return err
	}
	for from < to {
		n := min(to-from, maxLiteral)
		start := len(e.out)
		e.out = append(e.out, make([]byte, n)...)
		if err := e.target.readAt(e.out[start:], from); err != nil {
			return err
		}
		if err := e.grow(int(n)); err != nil {
			return err
		}
		if err := e.addOp(binary.AppendUvarint([]byte{opLiteral}, uint64(n)), n); err != nil {
			return err
		}
		from += n
		if err := e.flushOut(false); err != nil {
			return err
		}
	}
	return nil
}

// hashAt is the block hash of the target at i.
func (e *encoder) hashAt(i int64) (uint64, error) {
	var h uint64
	for k := int64(0); k < e.block; {
		s, err := e.tc.span(i + k)
		if err != nil {
			return 0, err
		}
		s = s[:min(int64(len(s)), e.block-k)]
		for _, c := range s {
			h = h*deltaPrime + uint64(c)
		}
		k += int64(len(s))
	}
	return h, nil
}

// extend is how far base from bo and target from t stay equal, up to limit in the target.
// Where the offsets coincide on a chunk boundary, equal leaves skip a whole chunk unread.
func (e *encoder) extend(t, bo, limit int64) (int64, error) {
	start := t
	for t < e.tn && bo < e.bn && t < limit {
		if t == bo && t%chunk == 0 {
			ok, err := e.matched(t / chunk)
			if err != nil {
				return 0, err
			}
			if ok {
				n := min(chunk, e.tn-t)
				t, bo = t+n, bo+n
				continue
			}
		}
		ts, err := e.tc.span(t)
		if err != nil {
			return 0, err
		}
		bs, err := e.bc.span(bo)
		if err != nil {
			return 0, err
		}
		n := min(int64(len(ts)), int64(len(bs)), limit-t)
		if t == bo {
			n = min(n, chunk-t%chunk) // stop at the boundary, where leaves may take over
		}
		k := int64(mismatch(ts[:n], bs[:n]))
		t, bo = t+k, bo+k
		if k < n {
			break
		}
	}
	return t - start, nil
}

// same reports whether the block at base bo equals the block at target t.
func (e *encoder) same(bo, t int64) (bool, error) {
	n, err := e.extend(t, bo, t+e.block)
	return n >= e.block, err
}

// step encodes target bytes from pos until about budget of them are done or the target ends, and
// reports whether it ended. It leaves no instruction pending: everything up to pos is in frames.
func (e *encoder) step(budget int64) (bool, error) {
	stop := e.tn
	if budget < e.tn-e.pos {
		stop = e.pos + budget
	}
	i, litStart := e.pos, e.pos
	if e.contB >= 0 {
		n, err := e.extend(i, e.contB, stop)
		if err != nil {
			return false, err
		}
		if err := e.copyOp(e.contB, i, n); err != nil {
			return false, err
		}
		i += n
		litStart = i
		e.contB = -1
	}
	// found emits a match of n bytes at base bo and target t, extended both ways.
	found := func(bo, t, n int64) error {
		start, bstart := t, bo
		for start > litStart && bstart > 0 {
			x, err := e.bc.at(bstart - 1)
			if err != nil {
				return err
			}
			y, err := e.tc.at(start - 1)
			if err != nil {
				return err
			}
			if x != y {
				break
			}
			start--
			bstart--
		}
		more, err := e.extend(t+n, bo+n, max(stop, t+n))
		if err != nil {
			return err
		}
		end := t + n + more
		if end >= stop && end < e.tn && bo+n+more < e.bn {
			e.contB = bo + n + more // cut by the budget: the next step goes on with it
		}
		if err := e.literal(litStart, start); err != nil {
			return err
		}
		if err := e.copyOp(bstart, start, end-start); err != nil {
			return err
		}
		litStart, i = end, end
		return nil
	}
	var h uint64
	hashed := false
	for i < e.tn && i < stop {
		if i%chunk == 0 {
			ok, err := e.matched(i / chunk)
			if err != nil {
				return false, err
			}
			if ok {
				if err := found(i, i, min(chunk, e.tn-i)); err != nil {
					return false, err
				}
				hashed = false
				continue
			}
		}
		if e.index == nil || i+e.block > e.tn {
			// Nothing to roll for: on to the next chunk boundary, where leaves may match.
			i = min((i/chunk+1)*chunk, e.tn)
			hashed = false
			continue
		}
		if !hashed {
			var err error
			if h, err = e.hashAt(i); err != nil {
				return false, err
			}
			hashed = true
		}
		if bo, ok := e.index[h]; ok {
			match, err := e.same(bo, i)
			if err != nil {
				return false, err
			}
			if match {
				if err := found(bo, i, e.block); err != nil {
					return false, err
				}
				hashed = false
				continue
			}
		}
		if i+e.block >= e.tn {
			i++
			hashed = false
			continue
		}
		first, err := e.tc.at(i)
		if err != nil {
			return false, err
		}
		next, err := e.tc.at(i + e.block)
		if err != nil {
			return false, err
		}
		h = (h-uint64(first)*e.pow)*deltaPrime + uint64(next)
		i++
	}
	if err := e.literal(litStart, i); err != nil {
		return false, err
	}
	if err := e.flushCopy(); err != nil {
		return false, err
	}
	if err := e.closeFrame(); err != nil {
		return false, err
	}
	e.pos = i
	return i >= e.tn, e.flushOut(true)
}

// indexChunks indexes the base's unmatched chunks from chunk k until about budget bytes are hashed,
// and returns the next chunk to index.
func (e *encoder) indexChunks(k, budget int64) (int64, int64, error) {
	chunks := (e.bn + chunk - 1) / chunk
	used := int64(0)
	for ; k < chunks && used < budget; k++ {
		un, err := e.baseUnmatched(k)
		if err != nil {
			return k, used, err
		}
		if !un {
			continue
		}
		off := (k*chunk + e.block - 1) / e.block * e.block
		for ; off < (k+1)*chunk && off+e.block <= e.bn && len(e.index) < e.entries; off += e.block {
			var h uint64
			for j := int64(0); j < e.block; {
				s, err := e.bc.span(off + j)
				if err != nil {
					return k, used, err
				}
				s = s[:min(int64(len(s)), e.block-j)]
				for _, c := range s {
					h = h*deltaPrime + uint64(c)
				}
				j += int64(len(s))
			}
			if _, ok := e.index[h]; !ok {
				e.index[h] = off
			}
			used += e.block
		}
	}
	return k, used, nil
}

// unmatchedBase is how many base bytes have no equal target chunk at their offset.
func (e *encoder) unmatchedBase() (int64, error) {
	chunks := (e.bn + chunk - 1) / chunk
	var n int64
	for k := int64(0); k < chunks; k++ {
		un, err := e.baseUnmatched(k)
		if err != nil {
			return 0, err
		}
		if un {
			n += min(chunk, e.bn-k*chunk)
		}
	}
	return n, nil
}

// frameEntry is one index entry of a format 2 delta.
type frameEntry struct{ tStart, litOff, opsOff, opsLen int64 }

// framedDelta reads a format 2 delta by random access, one frame's instructions at a time.
type framedDelta struct {
	base     source
	seg      segSource
	n        int64
	indexOff int64
	frames   int64

	mu         sync.Mutex
	k          int64 // the loaded frame, -1 for none
	start, end int64
	ins        []instr
	opsBuf     []byte
}

func openFramedDelta(base source, seg segSource) (*framedDelta, error) {
	if seg.n < footerBytes {
		return nil, errDeltaCorrupt
	}
	var f [footerBytes]byte
	if err := seg.readAt(f[:], seg.n-footerBytes); err != nil {
		return nil, err
	}
	if string(f[24:]) != framedMagic {
		return nil, errDeltaCorrupt
	}
	indexOff := int64(binary.BigEndian.Uint64(f[0:]))
	frames := int64(binary.BigEndian.Uint64(f[8:]))
	n := int64(binary.BigEndian.Uint64(f[16:]))
	if frames <= 0 || frames > (seg.n-footerBytes)/frameEntryBytes || n <= 0 ||
		indexOff != seg.n-footerBytes-frames*frameEntryBytes {
		return nil, errDeltaCorrupt
	}
	return &framedDelta{base: base, seg: seg, n: n, indexOff: indexOff, frames: frames, k: -1}, nil
}

func (d *framedDelta) size() int64 { return d.n }

func (d *framedDelta) entry(k int64) (frameEntry, error) {
	var b [frameEntryBytes]byte
	if err := d.seg.readAt(b[:], d.indexOff+k*frameEntryBytes); err != nil {
		return frameEntry{}, err
	}
	e := frameEntry{
		tStart: int64(binary.BigEndian.Uint64(b[0:])),
		litOff: int64(binary.BigEndian.Uint64(b[8:])),
		opsOff: int64(binary.BigEndian.Uint64(b[16:])),
		opsLen: int64(binary.BigEndian.Uint64(b[24:])),
	}
	if e.tStart < 0 || e.litOff < 0 || e.opsOff < 0 || e.opsLen < 0 {
		return frameEntry{}, errDeltaCorrupt
	}
	return e, nil
}

func (d *framedDelta) load(k int64) error {
	d.k = -1
	e, err := d.entry(k)
	if err != nil {
		return err
	}
	end := d.n
	if k+1 < d.frames {
		next, err := d.entry(k + 1)
		if err != nil {
			return err
		}
		end = next.tStart
	}
	if (k == 0 && e.tStart != 0) || e.tStart >= end || end > d.n || e.opsLen > maxFrameOpsBytes ||
		e.litOff > e.opsOff || e.opsOff > d.indexOff || e.opsLen > d.indexOff-e.opsOff {
		return errDeltaCorrupt
	}
	if cap(d.opsBuf) < int(e.opsLen) {
		d.opsBuf = make([]byte, e.opsLen)
	}
	buf := d.opsBuf[:e.opsLen]
	if err := d.seg.readAt(buf, e.opsOff); err != nil {
		return err
	}
	d.ins = d.ins[:0]
	t, lit := e.tStart, e.litOff
	for len(buf) > 0 {
		op := buf[0]
		buf = buf[1:]
		a, k1 := binary.Uvarint(buf)
		if k1 <= 0 {
			return errDeltaCorrupt
		}
		buf = buf[k1:]
		switch op {
		case opLiteral:
			if a == 0 || a > uint64(e.opsOff-lit) {
				return errDeltaCorrupt
			}
			d.ins = append(d.ins, instr{tOff: t, n: int64(a), off: lit})
			lit += int64(a)
			t += int64(a)
		case opCopy:
			l, k2 := binary.Uvarint(buf)
			if k2 <= 0 || l == 0 || a > uint64(d.base.size()) || l > uint64(d.base.size())-a {
				return errDeltaCorrupt
			}
			buf = buf[k2:]
			d.ins = append(d.ins, instr{tOff: t, n: int64(l), off: int64(a), copy: true})
			t += int64(l)
		default:
			return errDeltaCorrupt
		}
		if t > end {
			return errDeltaCorrupt
		}
	}
	if t != end {
		return errDeltaCorrupt
	}
	d.k, d.start, d.end = k, e.tStart, end
	return nil
}

// find loads the frame holding target offset off.
func (d *framedDelta) find(off int64) error {
	if d.k >= 0 && off >= d.start && off < d.end {
		return nil
	}
	if d.k >= 0 && off >= d.end && d.k+1 < d.frames {
		if err := d.load(d.k + 1); err != nil {
			return err
		}
		if off < d.end {
			return nil
		}
	}
	var serr error
	k := sort.Search(int(d.frames), func(k int) bool {
		e, err := d.entry(int64(k))
		if err != nil {
			serr = err
			return true
		}
		return e.tStart > off
	}) - 1
	if serr != nil {
		return serr
	}
	if k < 0 {
		return errDeltaCorrupt
	}
	if err := d.load(int64(k)); err != nil {
		return err
	}
	if off < d.start || off >= d.end {
		return errDeltaCorrupt
	}
	return nil
}

func (d *framedDelta) readAt(p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > d.n {
		return io.ErrUnexpectedEOF
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(p) > 0 {
		if err := d.find(off); err != nil {
			return err
		}
		i := sort.Search(len(d.ins), func(i int) bool { return d.ins[i].tOff+d.ins[i].n > off })
		for len(p) > 0 && i < len(d.ins) {
			in := d.ins[i]
			within := off - in.tOff
			take := min(int64(len(p)), in.n-within)
			var err error
			if in.copy {
				err = d.base.readAt(p[:take], in.off+within)
			} else {
				err = d.seg.readAt(p[:take], in.off+within)
			}
			if err != nil {
				return err
			}
			p = p[take:]
			off += take
			i++
		}
	}
	return nil
}
