// Package attachstore stores attachment bytes (docs/kdb-attachments-plan.md): one blob per
// content hash, written in a stream and read in a stream, whatever its size.
//
// Where the bytes go depends on their size, as the target storage system is best at:
//
//   - Small blobs (up to Config.SmallMaxBytes) go to the namespace's ordinary blob store - the WAL,
//     memtable and SSTables - which packs many of them into one file and makes them durable with
//     the same group commit as everything else.
//   - Larger blobs are standalone segments (<ns>/attachment/u-<id>), streamed through the
//     platform I/O shim (local files, or the S3 shim) a chunk at a time, never held whole.
//   - An older version of an attachment can be re-stored as a binary delta against the version that
//     replaced it (StoreAsDelta), git-pack style: the newest bytes stay whole, a history of small
//     edits to a large file costs the edits.
//
// A large blob is found through a pointer segment, <ns>/attachment/h-<hash>.<generation>, naming
// the segment that holds its bytes (and, for a delta, its base). The shim has no rename, so a blob
// is streamed under a random name and the pointer written once its hash is known; replacing a blob
// (full -> delta) writes a pointer of a higher generation before the old one is deleted, so a
// crash at any point leaves a readable blob. Segments no pointer names are garbage (an upload that
// never finished) and Collect deletes them.
package attachstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// ErrNotFound reports a blob the store does not hold.
var ErrNotFound = errors.New("attachstore: blob not found")

// ErrTooLarge reports an upload past Config.MaxBytes.
var ErrTooLarge = errors.New("attachstore: attachment exceeds the size limit")

// Config tunes a Store. Zero fields take the defaults.
type Config struct {
	// SmallMaxBytes is the largest blob kept in the ordinary blob store. Default 256 KiB.
	SmallMaxBytes int64
	// MaxBytes caps one attachment. Default 1 GiB.
	MaxBytes int64
	// ChunkBytes is the unit large blobs are written and read in. Default 1 MiB.
	ChunkBytes int
	// DeltaMaxBytes is the largest version StoreAsDelta will encode; both it and its base are
	// held in memory to diff. Default 64 MiB.
	DeltaMaxBytes int64
	// DeltaMaxDepth bounds a delta chain - how many deltas a read applies. Default 8.
	DeltaMaxDepth int
}

func (c Config) withDefaults() Config {
	if c.SmallMaxBytes <= 0 {
		c.SmallMaxBytes = 256 << 10
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 1 << 30
	}
	if c.ChunkBytes <= 0 {
		c.ChunkBytes = 1 << 20
	}
	if c.DeltaMaxBytes <= 0 {
		c.DeltaMaxBytes = 64 << 20
	}
	if c.DeltaMaxDepth <= 0 {
		c.DeltaMaxDepth = 8
	}
	return c
}

// SmallBlobs is the ordinary blob store small attachments live in.
type SmallBlobs interface {
	ReadBlob(contentHash codec.Hash) ([]byte, error)
	WriteBlob(bytes []byte) (codec.Hash, error)
}

// pointer is what a pointer segment says about one large blob.
type pointer struct {
	gen     int
	name    string // the pointer segment's own name
	segment string // the segment holding the bytes (whole, or a delta)
	size    int64  // the blob's length
	delta   bool
	base    codec.Hash // for a delta: the blob it applies to
}

// Store holds one namespace's attachment blobs.
type Store struct {
	shim   storage.PlatformIOShim
	ns     string
	prefix string // <ns prefix>attachment/
	small  SmallBlobs
	cfg    Config

	mu       sync.Mutex
	loaded   bool
	pointers map[codec.Hash]pointer
	uploads  map[string]struct{} // segments being written, which Collect must leave alone
}

// New returns the store for namespaceID. small may be nil, which keeps every blob standalone.
func New(shim storage.PlatformIOShim, namespaceID string, small SmallBlobs, cfg Config) *Store {
	return &Store{
		shim:     shim,
		ns:       namespaceID,
		prefix:   storio.SegmentNameBuilder.NamespacePrefix(namespaceID) + "attachment/",
		small:    small,
		cfg:      cfg.withDefaults(),
		pointers: map[codec.Hash]pointer{},
		uploads:  map[string]struct{}{},
	}
}

// Config returns the store's effective configuration.
func (s *Store) Config() Config { return s.cfg }

// load reads every pointer segment once. Must hold mu.
func (s *Store) loadLocked() error {
	if s.loaded {
		return nil
	}
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, s.prefix+"h-") {
			continue
		}
		p, h, ok := s.readPointer(name)
		if !ok {
			continue // torn by a crash: its blob is unreachable through it, and Collect retires it
		}
		if cur, have := s.pointers[h]; !have || p.gen > cur.gen {
			s.pointers[h] = p
		}
	}
	s.loaded = true
	return nil
}

const pointerMagic = "KDBATP1"

func (s *Store) pointerName(h codec.Hash, gen int) string {
	return fmt.Sprintf("%sh-%s.%06d", s.prefix, h.Hex(), gen)
}

func encodePointer(p pointer) []byte {
	b := []byte(pointerMagic)
	flags := byte(0)
	if p.delta {
		flags = 1
	}
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, uint64(p.size))
	b = binary.AppendUvarint(b, uint64(len(p.segment)))
	b = append(b, p.segment...)
	if p.delta {
		b = append(b, p.base.Bytes[:]...)
	}
	sum := sha256.Sum256(b)
	return append(b, sum[:4]...)
}

func (s *Store) readPointer(name string) (pointer, codec.Hash, bool) {
	base := strings.TrimPrefix(name, s.prefix+"h-")
	dot := strings.LastIndexByte(base, '.')
	if dot < 0 {
		return pointer{}, codec.Hash{}, false
	}
	h, err := codec.HashFromHex(base[:dot])
	if err != nil {
		return pointer{}, codec.Hash{}, false
	}
	gen, err := strconv.Atoi(base[dot+1:])
	if err != nil {
		return pointer{}, codec.Hash{}, false
	}
	raw, err := s.shim.ReadFromSegment(name, 0, 1024)
	if err != nil || len(raw) < len(pointerMagic)+1+8+1+4 || string(raw[:len(pointerMagic)]) != pointerMagic {
		return pointer{}, codec.Hash{}, false
	}
	body, crc := raw[:len(raw)-4], raw[len(raw)-4:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:4], crc) {
		return pointer{}, codec.Hash{}, false
	}
	off := len(pointerMagic)
	p := pointer{gen: gen, name: name, delta: body[off]&1 != 0}
	off++
	p.size = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	l, k := binary.Uvarint(body[off:])
	if k <= 0 || uint64(len(body)-off-k) < l {
		return pointer{}, codec.Hash{}, false
	}
	off += k
	p.segment = string(body[off : off+int(l)])
	off += int(l)
	if p.delta {
		if len(body)-off != 32 {
			return pointer{}, codec.Hash{}, false
		}
		copy(p.base.Bytes[:], body[off:])
	}
	return p, h, true
}

// writePointer makes p the durable pointer for h. Must hold mu.
func (s *Store) writePointerLocked(h codec.Hash, p pointer) (pointer, error) {
	if cur, ok := s.pointers[h]; ok {
		p.gen = cur.gen + 1
	}
	p.name = s.pointerName(h, p.gen)
	if _, err := s.shim.AppendToSegment(p.name, encodePointer(p)); err != nil {
		return pointer{}, err
	}
	if err := s.shim.FlushSegment(p.name); err != nil {
		return pointer{}, err
	}
	if err := s.shim.SealSegment(p.name); err != nil {
		return pointer{}, err
	}
	return p, nil
}

// Info describes a stored blob.
type Info struct {
	Hash codec.Hash
	Size int64
}

// Upload streams one blob in. Write the bytes, then Finish (or Abort).
type Upload struct {
	s       *Store
	h       hash.Hash
	size    int64
	buf     []byte // the bytes so far, until they outgrow SmallMaxBytes
	segment string // set once spilled
	pending []byte // bytes waiting for a full chunk before they are appended
	done    bool
}

// Begin starts an upload.
func (s *Store) Begin() *Upload {
	return &Upload{s: s, h: sha256.New()}
}

// Write implements io.Writer.
func (u *Upload) Write(p []byte) (int, error) {
	if u.done {
		return 0, errors.New("attachstore: write after finish")
	}
	if u.size+int64(len(p)) > u.s.cfg.MaxBytes {
		return 0, ErrTooLarge
	}
	u.h.Write(p)
	u.size += int64(len(p))
	if u.segment == "" {
		u.buf = append(u.buf, p...)
		if int64(len(u.buf)) <= u.s.cfg.SmallMaxBytes {
			return len(p), nil
		}
		if err := u.spill(); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	u.pending = append(u.pending, p...)
	for len(u.pending) >= u.s.cfg.ChunkBytes {
		if _, err := u.s.shim.AppendToSegment(u.segment, u.pending[:u.s.cfg.ChunkBytes]); err != nil {
			return 0, err
		}
		u.pending = u.pending[u.s.cfg.ChunkBytes:]
	}
	if len(u.pending) == 0 {
		u.pending = nil
	}
	return len(p), nil
}

// spill moves an upload that outgrew the small store into a segment of its own.
func (u *Upload) spill() error {
	id, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	u.segment = u.s.prefix + "u-" + id.String()
	u.s.mu.Lock()
	u.s.uploads[u.segment] = struct{}{}
	u.s.mu.Unlock()
	u.pending, u.buf = u.buf, nil
	return nil
}

// Finish makes the blob durable and returns its hash and size. A blob already stored is not
// stored twice: the new copy is dropped.
func (u *Upload) Finish() (Info, error) {
	if u.done {
		return Info{}, errors.New("attachstore: finish called twice")
	}
	u.done = true
	var h codec.Hash
	copy(h.Bytes[:], u.h.Sum(nil))
	info := Info{Hash: h, Size: u.size}
	if u.size == 0 {
		return info, nil // see emptyHash
	}
	if u.segment == "" {
		if u.s.small == nil {
			if err := u.spill(); err != nil {
				return Info{}, err
			}
		} else {
			got, err := u.s.small.WriteBlob(u.buf)
			if err != nil {
				return Info{}, err
			}
			if got != h {
				return Info{}, fmt.Errorf("attachstore: blob store hashed %s, expected %s", got.Hex(), h.Hex())
			}
			return info, nil
		}
	}
	s := u.s
	defer func() {
		s.mu.Lock()
		delete(s.uploads, u.segment)
		s.mu.Unlock()
	}()
	if len(u.pending) > 0 || u.size == 0 {
		if _, err := s.shim.AppendToSegment(u.segment, u.pending); err != nil {
			return Info{}, err
		}
		u.pending = nil
	}
	if err := s.shim.FlushSegment(u.segment); err != nil {
		return Info{}, err
	}
	if err := s.shim.SealSegment(u.segment); err != nil {
		return Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return Info{}, err
	}
	if _, ok := s.pointers[h]; ok {
		_ = s.shim.DeleteSegment(u.segment) // already stored: keep the one copy
		return info, nil
	}
	p, err := s.writePointerLocked(h, pointer{segment: u.segment, size: u.size})
	if err != nil {
		return Info{}, err
	}
	s.pointers[h] = p
	return info, nil
}

// Abort discards an upload.
func (u *Upload) Abort() {
	if u.done {
		return
	}
	u.done = true
	if u.segment != "" {
		_ = u.s.shim.DeleteSegment(u.segment)
		u.s.mu.Lock()
		delete(u.s.uploads, u.segment)
		u.s.mu.Unlock()
	}
}

// Put stores everything r yields.
func (s *Store) Put(r io.Reader) (Info, error) {
	u := s.Begin()
	buf := make([]byte, min(s.cfg.ChunkBytes, 64<<10))
	if _, err := io.CopyBuffer(u, r, buf); err != nil {
		u.Abort()
		return Info{}, err
	}
	return u.Finish()
}

// emptyHash is the content hash of zero bytes. An empty attachment is never stored - a blob store
// cannot tell zero bytes from none - and always present.
var emptyHash = func() codec.Hash {
	s := sha256.Sum256(nil)
	var h codec.Hash
	copy(h.Bytes[:], s[:])
	return h
}()

// Has reports whether h is stored.
func (s *Store) Has(h codec.Hash) (bool, error) {
	if h == emptyHash {
		return true, nil
	}
	s.mu.Lock()
	err := s.loadLocked()
	_, ok := s.pointers[h]
	s.mu.Unlock()
	if err != nil || ok {
		return ok, err
	}
	if s.small == nil {
		return false, nil
	}
	b, err := s.small.ReadBlob(h)
	return b != nil, err
}

// Open streams the blob h. The reader checks nothing itself: the bytes were hashed when stored,
// and a delta's reconstruction is checked against h before it is returned.
func (s *Store) Open(h codec.Hash) (io.ReadCloser, int64, error) {
	if h == emptyHash {
		return io.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	s.mu.Lock()
	err := s.loadLocked()
	p, ok := s.pointers[h]
	s.mu.Unlock()
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		if s.small != nil {
			b, err := s.small.ReadBlob(h)
			if err != nil {
				return nil, 0, err
			}
			if b != nil {
				return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
			}
		}
		return nil, 0, ErrNotFound
	}
	if !p.delta {
		return &segmentReader{shim: s.shim, segment: p.segment, size: p.size, chunk: s.cfg.ChunkBytes}, p.size, nil
	}
	b, err := s.materialize(h, 0)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

// ReadAll returns blob h whole, for a caller that needs it in memory (a delta, a small blob). It
// refuses a blob larger than limit.
func (s *Store) ReadAll(h codec.Hash, limit int64) ([]byte, error) {
	r, size, err := s.Open(h)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if size > limit {
		return nil, fmt.Errorf("attachstore: blob %s is %d bytes, over %d", h.Hex(), size, limit)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// materialize rebuilds h whole, applying delta chains, and checks the result hashes to h.
func (s *Store) materialize(h codec.Hash, depth int) ([]byte, error) {
	if depth > s.cfg.DeltaMaxDepth+1 {
		return nil, fmt.Errorf("attachstore: delta chain at %s is deeper than %d", h.Hex(), s.cfg.DeltaMaxDepth)
	}
	s.mu.Lock()
	p, ok := s.pointers[h]
	s.mu.Unlock()
	if !ok || !p.delta {
		return s.ReadAll(h, s.cfg.DeltaMaxBytes)
	}
	base, err := s.materialize(p.base, depth+1)
	if err != nil {
		return nil, err
	}
	delta, err := readSegment(s.shim, p.segment, s.cfg.DeltaMaxBytes)
	if err != nil {
		return nil, err
	}
	out, err := applyDelta(base, delta, s.cfg.DeltaMaxBytes)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(out); !bytes.Equal(sum[:], h.Bytes[:]) {
		return nil, fmt.Errorf("attachstore: delta for %s reconstructs other bytes", h.Hex())
	}
	return out, nil
}

// depth is how many deltas reading h applies.
func (s *Store) depthLocked(h codec.Hash) int {
	d := 0
	for {
		p, ok := s.pointers[h]
		if !ok || !p.delta || d > s.cfg.DeltaMaxDepth {
			return d
		}
		d++
		h = p.base
	}
}

// StoreAsDelta re-stores old as a delta against base (the version that replaced it), when old is
// a whole standalone blob within DeltaMaxBytes, the chain stays within DeltaMaxDepth, and the
// delta is under half old's size. It reports whether it did. Reads of old are unaffected except
// in cost; nothing is lost if it fails half way.
func (s *Store) StoreAsDelta(old, base codec.Hash) (bool, error) {
	if old == base {
		return false, nil
	}
	s.mu.Lock()
	if err := s.loadLocked(); err != nil {
		s.mu.Unlock()
		return false, err
	}
	p, ok := s.pointers[old]
	if !ok || p.delta || p.size > s.cfg.DeltaMaxBytes || s.depthLocked(base)+1 > s.cfg.DeltaMaxDepth {
		s.mu.Unlock()
		return false, nil
	}
	if bp, ok := s.pointers[base]; ok && bp.size > s.cfg.DeltaMaxBytes {
		s.mu.Unlock()
		return false, nil
	}
	s.mu.Unlock()
	oldBytes, err := s.ReadAll(old, s.cfg.DeltaMaxBytes)
	if err != nil {
		return false, err
	}
	baseBytes, err := s.materialize(base, 0)
	if err != nil {
		return false, err
	}
	delta := encodeDelta(baseBytes, oldBytes)
	if int64(len(delta)) >= int64(len(oldBytes))/2 {
		return false, nil
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return false, err
	}
	seg := s.prefix + "u-" + id.String()
	s.mu.Lock()
	s.uploads[seg] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.uploads, seg)
		s.mu.Unlock()
	}()
	if _, err := s.shim.AppendToSegment(seg, delta); err != nil {
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
	np, err := s.writePointerLocked(old, pointer{segment: seg, size: p.size, delta: true, base: base})
	if err != nil {
		return false, err
	}
	s.pointers[old] = np
	_ = s.shim.DeleteSegment(cur.name)
	_ = s.shim.DeleteSegment(cur.segment)
	return true, nil
}

// LargeBlobs lists the standalone blobs (small ones are the ordinary blob store's).
func (s *Store) LargeBlobs() ([]codec.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	out := make([]codec.Hash, 0, len(s.pointers))
	for h := range s.pointers {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bytes[:], out[j].Bytes[:]) < 0 })
	return out, nil
}

// Collect deletes every standalone blob keep rejects - except the bases of deltas that are kept -
// and every segment no pointer names (uploads that never finished, pointers superseded by a
// higher generation). It reports how many blobs it removed.
func (s *Store) Collect(keep func(codec.Hash) bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return 0, err
	}
	live := map[codec.Hash]bool{}
	for h := range s.pointers {
		if keep(h) {
			live[h] = true
		}
	}
	// A kept delta keeps its chain.
	for h := range live {
		for p, ok := s.pointers[h]; ok && p.delta && !live[p.base]; p, ok = s.pointers[p.base] {
			live[p.base] = true
		}
	}
	removed := 0
	for h := range s.pointers {
		if !live[h] {
			delete(s.pointers, h)
			removed++
		}
	}
	referenced := map[string]bool{}
	for _, p := range s.pointers {
		referenced[p.name] = true
		referenced[p.segment] = true
	}
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return removed, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, s.prefix) || referenced[name] {
			continue
		}
		if _, writing := s.uploads[name]; writing {
			continue
		}
		_ = s.shim.DeleteSegment(name)
	}
	return removed, nil
}

// segmentReader streams a segment a chunk at a time.
type segmentReader struct {
	shim    storage.PlatformIOShim
	segment string
	size    int64
	off     int64
	chunk   int
	buf     []byte
}

func (r *segmentReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		if r.off >= r.size {
			return 0, io.EOF
		}
		n := int64(r.chunk)
		if r.size-r.off < n {
			n = r.size - r.off
		}
		b, err := r.shim.ReadFromSegment(r.segment, r.off, int(n))
		if err != nil {
			return 0, err
		}
		if len(b) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		r.buf = b
		r.off += int64(len(b))
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *segmentReader) Close() error { return nil }

func readSegment(shim storage.PlatformIOShim, segment string, limit int64) ([]byte, error) {
	var out []byte
	for off := int64(0); ; {
		b, err := shim.ReadFromSegment(segment, off, 1<<20)
		if err != nil {
			return nil, err
		}
		if len(b) == 0 {
			return out, nil
		}
		out = append(out, b...)
		off += int64(len(b))
		if int64(len(out)) > limit {
			return nil, fmt.Errorf("attachstore: segment %s exceeds %d bytes", segment, limit)
		}
	}
}
