package attachstore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// source is random access to one blob version's bytes, wherever they live.
type source interface {
	size() int64
	// readAt fills p from off; a short source is an error.
	readAt(p []byte, off int64) error
}

type memSource []byte

func (m memSource) size() int64 { return int64(len(m)) }
func (m memSource) readAt(p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > int64(len(m)) {
		return io.ErrUnexpectedEOF
	}
	copy(p, m[off:])
	return nil
}

type segSource struct {
	shim storage.PlatformIOShim
	name string
	n    int64
}

func (s segSource) size() int64 { return s.n }
func (s segSource) readAt(p []byte, off int64) error {
	for len(p) > 0 {
		b, err := s.shim.ReadFromSegment(s.name, off, len(p))
		if err != nil {
			return err
		}
		if len(b) == 0 {
			return io.ErrUnexpectedEOF
		}
		copy(p, b)
		p = p[len(b):]
		off += int64(len(b))
	}
	return nil
}

// instr is one delta instruction: target bytes [tOff, tOff+n) come from base at off (copy) or from
// the delta segment at off (literal).
type instr struct {
	tOff, n, off int64
	copy         bool
}

// deltaSource reads a version stored as a delta by random access through its instructions: a read
// touches only the base ranges and literal bytes it needs, and nothing is rebuilt whole.
type deltaSource struct {
	base source
	seg  segSource
	n    int64
	ins  []instr
}

func (d *deltaSource) size() int64 { return d.n }

func (d *deltaSource) readAt(p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > d.n {
		return io.ErrUnexpectedEOF
	}
	i := sort.Search(len(d.ins), func(i int) bool { return d.ins[i].tOff+d.ins[i].n > off })
	for len(p) > 0 {
		if i >= len(d.ins) {
			return errDeltaCorrupt
		}
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
	return nil
}

// parseDelta reads a delta segment's instructions (not its literal bytes) into a deltaSource.
func parseDelta(base source, seg segSource) (*deltaSource, error) {
	r := bufio.NewReaderSize(&segStream{src: seg}, 64<<10)
	pos := int64(0)
	uv := func() (uint64, error) {
		v, err := binary.ReadUvarint(r)
		if err != nil {
			return 0, errDeltaCorrupt
		}
		pos += int64(uvarintLen(v))
		return v, nil
	}
	n, err := uv()
	if err != nil {
		return nil, err
	}
	d := &deltaSource{base: base, seg: seg, n: int64(n)}
	t := int64(0)
	for pos < seg.n {
		op, err := r.ReadByte()
		if err != nil {
			return nil, errDeltaCorrupt
		}
		pos++
		switch op {
		case opLiteral:
			l, err := uv()
			if err != nil {
				return nil, err
			}
			if l > uint64(seg.n-pos) {
				return nil, errDeltaCorrupt
			}
			d.ins = append(d.ins, instr{tOff: t, n: int64(l), off: pos})
			if _, err := r.Discard(int(l)); err != nil {
				return nil, errDeltaCorrupt
			}
			pos += int64(l)
			t += int64(l)
		case opCopy:
			bo, err := uv()
			if err != nil {
				return nil, err
			}
			l, err := uv()
			if err != nil {
				return nil, err
			}
			if bo > uint64(base.size()) || l > uint64(base.size())-bo {
				return nil, errDeltaCorrupt
			}
			d.ins = append(d.ins, instr{tOff: t, n: int64(l), off: int64(bo), copy: true})
			t += int64(l)
		default:
			return nil, errDeltaCorrupt
		}
	}
	if t != d.n {
		return nil, errDeltaCorrupt
	}
	return d, nil
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// segStream reads a segment front to back.
type segStream struct {
	src segSource
	off int64
}

func (s *segStream) Read(p []byte) (int, error) {
	if s.off >= s.src.n {
		return 0, io.EOF
	}
	n := min(int64(len(p)), s.src.n-s.off)
	if err := s.src.readAt(p[:n], s.off); err != nil {
		return 0, err
	}
	s.off += n
	return int(n), nil
}

// Reader streams one blob version: sequentially (Read) or by range (ReadAt), each chunk checked
// against the content hash before it is handed out. It holds pins on everything it reads from
// until Close, so it finishes on the version it opened whatever happens to the store meanwhile.
type Reader struct {
	s        *Store
	hash     codec.Hash
	n        int64
	src      source
	leaves   string // leaves segment; "" for a small blob, verified whole at open
	pins     []string
	deadline time.Time
	off      int64

	mu      sync.Mutex
	cached  int64 // chunk index in buf, -1 for none
	buf     []byte
	closed  bool
	expired atomic.Bool
}

// Size is the blob's length.
func (r *Reader) Size() int64 { return r.n }

// Hash is the blob's content hash.
func (r *Reader) Hash() codec.Hash { return r.hash }

// ChunkLeaf returns chunk i's leaf hash, for a caller proving a chunk to someone else.
func (r *Reader) ChunkLeaf(i int64) (codec.Hash, error) {
	if r.leaves == "" {
		data := make([]byte, min(int64(document.AttachmentChunkBytes), r.n-i*document.AttachmentChunkBytes))
		if err := r.src.readAt(data, i*document.AttachmentChunkBytes); err != nil {
			return codec.Hash{}, err
		}
		return document.ChunkLeafHash(data), nil
	}
	var h codec.Hash
	if err := (segSource{shim: r.s.shim, name: r.leaves, n: (i + 1) * 32}).readAt(h.Bytes[:], i*32); err != nil {
		return codec.Hash{}, err
	}
	return h, nil
}

// chunkAt returns chunk i, verified.
func (r *Reader) chunkAt(i int64) ([]byte, error) {
	if r.expired.Load() {
		return nil, ErrReadLeaseExpired
	}
	if r.closed {
		return nil, errors.New("attachstore: read after close")
	}
	if r.cached == i {
		return r.buf, nil
	}
	start := i * chunk
	n := min(int64(chunk), r.n-start)
	if cap(r.buf) < int(n) {
		r.buf = make([]byte, chunk)
	}
	buf := r.buf[:n]
	if err := r.src.readAt(buf, start); err != nil {
		return nil, err
	}
	if r.leaves != "" {
		want, err := r.ChunkLeaf(i)
		if err != nil {
			return nil, err
		}
		if document.ChunkLeafHash(buf) != want {
			return nil, fmt.Errorf("%w: chunk %d of %s", ErrCorrupt, i, r.hash.Hex())
		}
	}
	r.buf, r.cached = buf, i
	return buf, nil
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.off >= r.n {
		return 0, io.EOF
	}
	c, err := r.chunkAt(r.off / chunk)
	if err != nil {
		return 0, err
	}
	n := copy(p, c[r.off%chunk:])
	r.off += int64(n)
	return n, nil
}

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if off < 0 {
		return 0, errors.New("attachstore: negative offset")
	}
	done := 0
	for done < len(p) {
		if off >= r.n {
			return done, io.EOF
		}
		c, err := r.chunkAt(off / chunk)
		if err != nil {
			return done, err
		}
		n := copy(p[done:], c[off%chunk:])
		done += n
		off += int64(n)
	}
	return done, nil
}

// Close releases the reader's pins; a segment whose deletion waited on them goes now.
func (r *Reader) Close() error {
	r.mu.Lock()
	closed := r.closed
	r.closed = true
	r.mu.Unlock()
	if !closed && !r.expired.Load() {
		r.s.release(r)
	}
	return nil
}

// pinLocked pins name for r. Must hold mu.
func (s *Store) pinLocked(r *Reader, name string) {
	if name == "" {
		return
	}
	s.pinned[name]++
	r.pins = append(r.pins, name)
}

func (s *Store) release(r *Reader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.readers[r]; !ok {
		return
	}
	delete(s.readers, r)
	for _, name := range r.pins {
		s.pinned[name]--
		if s.pinned[name] > 0 {
			continue
		}
		delete(s.pinned, name)
		if s.deferred[name] {
			delete(s.deferred, name)
			_ = s.shim.DeleteSegment(name)
		}
	}
	r.pins = nil
}

// expireReaders releases readers past their lease; their next read fails with ErrReadLeaseExpired.
func (s *Store) expireReaders(now time.Time) {
	s.mu.Lock()
	var stale []*Reader
	for r := range s.readers {
		if now.After(r.deadline) {
			stale = append(stale, r)
		}
	}
	s.mu.Unlock()
	for _, r := range stale {
		s.release(r)
		r.expired.Store(true)
	}
}

// resolved is one blob version's pointer chain, base last.
type resolved struct {
	hash codec.Hash
	p    pointer
}

// resolveLocked returns h's pointer chain (h first, then its delta bases), or ok=false for a blob
// with no pointer (small, or missing). Must hold mu.
func (s *Store) resolveLocked(h codec.Hash) ([]resolved, bool, error) {
	var chain []resolved
	at := h
	for {
		p, ok := s.pointers[at]
		if !ok {
			if len(chain) == 0 {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("%w: delta base %s of %s", ErrNotFound, at.Hex(), h.Hex())
		}
		chain = append(chain, resolved{at, p})
		if !p.delta {
			return chain, true, nil
		}
		if len(chain) > s.cfg.DeltaMaxDepth+1 {
			return nil, false, fmt.Errorf("attachstore: delta chain at %s is deeper than %d", h.Hex(), s.cfg.DeltaMaxDepth)
		}
		at = p.base
	}
}

// Open opens blob h for reading. The reader is pinned to the version as it is now: segments it
// reads from are not deleted until it is closed (or its lease expires).
func (s *Store) Open(h codec.Hash) (*Reader, error) {
	if h == emptyHash {
		return &Reader{s: s, hash: h, src: memSource(nil), cached: -1, deadline: time.Now().Add(s.cfg.ReadLease)}, nil
	}
	s.mu.Lock()
	if err := s.loadLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	chain, large, err := s.resolveLocked(h)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	r := &Reader{s: s, hash: h, cached: -1, deadline: time.Now().Add(s.cfg.ReadLease)}
	if large {
		for _, c := range chain {
			s.pinLocked(r, c.p.segment)
			s.pinLocked(r, c.p.leaves)
		}
		s.readers[r] = struct{}{}
	}
	s.mu.Unlock()

	if !large {
		if s.small == nil {
			return nil, ErrNotFound
		}
		b, err := s.small.ReadBlob(h)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, ErrNotFound
		}
		if document.AttachmentContentHash(b) != h {
			return nil, fmt.Errorf("%w: %s", ErrCorrupt, h.Hex())
		}
		r.n, r.src = int64(len(b)), memSource(b)
		return r, nil
	}
	src, err := s.sourceFor(chain)
	if err != nil {
		r.Close()
		return nil, err
	}
	if chain[0].p.inline == nil {
		if err := s.verifyLeaves(h, chain[0].p); err != nil {
			r.Close()
			return nil, err
		}
	}
	r.n, r.src, r.leaves = chain[0].p.size, src, chain[0].p.leaves
	if chain[0].p.inline != nil {
		r.leaves = "" // checked whole when its pointer was read
		return r, nil
	}
	return r, nil
}

// sourceFor builds random access to the first version of chain.
func (s *Store) sourceFor(chain []resolved) (source, error) {
	last := chain[len(chain)-1].p
	var src source = segSource{shim: s.shim, name: last.segment, n: last.size}
	if last.inline != nil {
		src = memSource(last.inline)
	}
	for i := len(chain) - 2; i >= 0; i-- {
		p := chain[i].p
		n, err := segmentLength(s.shim, p.segment)
		if err != nil {
			return nil, err
		}
		d, err := parseDelta(src, segSource{shim: s.shim, name: p.segment, n: n})
		if err != nil {
			return nil, err
		}
		if d.n != p.size {
			return nil, errDeltaCorrupt
		}
		src = d
	}
	return src, nil
}

// segmentLength finds a segment's length by probing (the shim reports none).
func segmentLength(shim storage.PlatformIOShim, name string) (int64, error) {
	lo, hi := int64(0), int64(1)
	for {
		b, err := shim.ReadFromSegment(name, hi, 1)
		if err != nil || len(b) == 0 {
			break
		}
		lo, hi = hi, hi*2
	}
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if b, err := shim.ReadFromSegment(name, mid, 1); err == nil && len(b) == 1 {
			lo = mid
		} else {
			hi = mid
		}
	}
	if b, err := shim.ReadFromSegment(name, 0, 1); err != nil {
		return 0, err
	} else if len(b) == 0 {
		return 0, nil
	}
	return lo + 1, nil
}

// verifyLeaves checks once per leaves segment that the stored leaves hash to h, streaming them.
func (s *Store) verifyLeaves(h codec.Hash, p pointer) error {
	s.mu.Lock()
	ok := s.verified[p.leaves]
	s.mu.Unlock()
	if ok {
		return nil
	}
	count := (p.size + chunk - 1) / chunk
	var m document.MerkleBuilder
	seg := segSource{shim: s.shim, name: p.leaves, n: count * 32}
	buf := make([]byte, 32*4096)
	for off := int64(0); off < seg.n; off += int64(len(buf)) {
		n := min(int64(len(buf)), seg.n-off)
		if err := seg.readAt(buf[:n], off); err != nil {
			return fmt.Errorf("%w: leaves of %s: %v", ErrCorrupt, h.Hex(), err)
		}
		for i := int64(0); i < n; i += 32 {
			var leaf codec.Hash
			copy(leaf.Bytes[:], buf[i:i+32])
			m.Add(leaf)
		}
	}
	if document.AttachmentIdentity(m.Root(), p.size) != h {
		return fmt.Errorf("%w: leaves of %s", ErrCorrupt, h.Hex())
	}
	s.mu.Lock()
	s.verified[p.leaves] = true
	s.mu.Unlock()
	return nil
}

// ReadAll returns blob h whole - for tests and small blobs only; a large blob is meant to be
// streamed. It refuses a blob larger than limit.
func (s *Store) ReadAll(h codec.Hash, limit int64) ([]byte, error) {
	r, err := s.Open(h)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if r.Size() > limit {
		return nil, fmt.Errorf("attachstore: blob %s is %d bytes, over %d", h.Hex(), r.Size(), limit)
	}
	var b bytes.Buffer
	if _, err := io.Copy(&b, r); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// StoreAsDelta re-stores old as a delta against base (the version that replaced it) when old is a
// whole standalone blob within DeltaMaxBytes, the chain stays within DeltaMaxDepth, and the delta is
// under half old's size. It reports whether it did. Readers already open on old keep reading the
// whole copy, which is deleted when the last of them closes.
func (s *Store) StoreAsDelta(old, base codec.Hash) (bool, error) {
	if old == base || old == emptyHash || base == emptyHash {
		return false, nil
	}
	s.mu.Lock()
	if err := s.loadLocked(); err != nil {
		s.mu.Unlock()
		return false, err
	}
	p, ok := s.pointers[old]
	baseChain, baseLarge, err := s.resolveLocked(base)
	if err != nil || !ok || !baseLarge || p.delta || p.inline != nil || p.size > s.cfg.DeltaMaxBytes ||
		baseChain[0].p.size > s.cfg.DeltaMaxBytes || len(baseChain) > s.cfg.DeltaMaxDepth {
		s.mu.Unlock()
		return false, err
	}
	s.mu.Unlock()

	oldR, err := s.Open(old)
	if err != nil {
		return false, err
	}
	defer oldR.Close()
	baseR, err := s.Open(base)
	if err != nil {
		return false, err
	}
	defer baseR.Close()

	seg, err := s.newSegmentName()
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	s.uploads[seg] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.uploads, seg)
		s.mu.Unlock()
	}()
	written := int64(0)
	limit := p.size / 2
	errTooBig := errors.New("delta too big")
	err = encodeDelta(baseR.src, oldR.src, func(b []byte) error {
		written += int64(len(b))
		if written >= limit {
			return errTooBig
		}
		_, err := s.shim.AppendToSegment(seg, b)
		return err
	})
	if err != nil {
		_ = s.shim.DeleteSegment(seg)
		if errors.Is(err, errTooBig) {
			return false, nil
		}
		return false, err
	}
	if err := s.shim.FlushSegment(seg); err != nil {
		return false, err
	}
	if err := s.shim.SealSegment(seg); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.pointers[old]
	if !ok || cur.gen != p.gen {
		_ = s.shim.DeleteSegment(seg) // changed under us; leave it be
		return false, nil
	}
	np, err := s.writePointerLocked(old, pointer{segment: seg, leaves: cur.leaves, size: cur.size, delta: true, base: base})
	if err != nil {
		return false, err
	}
	s.pointers[old] = np
	s.deleteOrDeferLocked(cur.name)
	s.deleteOrDeferLocked(cur.segment)
	return true, nil
}
