// Package attachstore stores attachment bytes (docs/kdb-attachments-plan.md): one blob per content
// hash, written in a stream and read in a stream, of any size.
//
// Content identity. A blob is named by its scheme-1 content hash (document.AttachmentContentHash):
// the root of a Merkle tree over 1 MiB chunks. Chunks are hashed in parallel as an upload streams
// in, and every large blob keeps its chunk (leaf) hashes, so a reader checks each chunk against the
// root before handing it out - corruption is caught at the chunk, never after the whole blob.
//
// Where the bytes go depends on their size, as the target storage system is best at:
//
//   - Small blobs (up to Config.SmallMaxBytes) go to the namespace's ordinary blob store - the WAL,
//     memtable and SSTables - which packs many of them into one file.
//   - Larger blobs are standalone segments (<ns>/attachment/u-<id>), streamed through the platform
//     I/O shim (local files, or the S3 shim) a chunk at a time, never held whole.
//   - An older version can be re-stored as a binary delta against the version that replaced it
//     (StoreAsDelta), git-pack style: the newest bytes stay whole, a history of small edits to a
//     large file costs the edits. A delta is read by random access through its instructions, never
//     rebuilt in memory.
//
// A large blob is found through a pointer segment, <ns>/attachment/h-<hash>.<generation>, naming
// the segments holding its bytes and its leaf hashes (and, for a delta, its base). The shim has no
// rename, so a blob is streamed under a random name and the pointer written once its hash is known;
// replacing a blob (whole -> delta) writes a pointer of a higher generation before the old one goes,
// so a crash at any point leaves a readable blob.
//
// Readers pin. Open pins every segment the read resolves to - its bytes, its leaves, a delta's whole
// base chain - and Close releases them. StoreAsDelta and Collect never delete a pinned segment: they
// queue it, and the last release deletes it. A read in flight therefore finishes on the version it
// started on, whatever commits, maintenance or GC happen meanwhile. Pins are leases
// (Config.ReadLease): a reader that stalls past its lease is released by the next Collect and fails
// with ErrReadLeaseExpired, so a stuck client cannot hold storage forever.
package attachstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// ErrNotFound reports a blob the store does not hold.
var ErrNotFound = errors.New("attachstore: blob not found")

// ErrTooLarge reports an upload past Config.MaxBytes.
var ErrTooLarge = errors.New("attachstore: attachment exceeds the size limit")

// ErrCorrupt reports stored bytes that do not hash to what they are stored under.
var ErrCorrupt = errors.New("attachstore: stored bytes do not match their content hash")

// ErrReadLeaseExpired reports a read that outlived its lease (Config.ReadLease) and was released.
var ErrReadLeaseExpired = errors.New("attachstore: read lease expired")

const chunk = document.AttachmentChunkBytes

// Config tunes a Store. Zero fields take the defaults.
type Config struct {
	// SmallMaxBytes is the largest blob kept in the ordinary blob store. Default 256 KiB; never
	// more than one chunk.
	SmallMaxBytes int64
	// MaxBytes caps one attachment. Zero (the default) is no limit.
	MaxBytes int64
	// DeltaMaxBytes is the largest version StoreAsDelta will encode. Encoding indexes the base and
	// pages through both versions, so it bounds the index, not the bytes held. Default 256 MiB.
	DeltaMaxBytes int64
	// DeltaMaxDepth bounds a delta chain - how many deltas a read goes through. Default 8.
	DeltaMaxDepth int
	// HashWorkers bounds the chunks hashed at once during an upload. Default GOMAXPROCS.
	HashWorkers int
	// ReadLease is how long a reader may hold its pins. Default one hour.
	ReadLease time.Duration
}

func (c Config) withDefaults() Config {
	if c.SmallMaxBytes <= 0 {
		c.SmallMaxBytes = 256 << 10
	}
	if c.SmallMaxBytes > chunk {
		c.SmallMaxBytes = chunk
	}
	if c.DeltaMaxBytes <= 0 {
		c.DeltaMaxBytes = 256 << 20
	}
	if c.DeltaMaxDepth <= 0 {
		c.DeltaMaxDepth = 8
	}
	if c.HashWorkers <= 0 {
		c.HashWorkers = runtime.GOMAXPROCS(0)
	}
	if c.ReadLease <= 0 {
		c.ReadLease = time.Hour
	}
	return c
}

// SmallBlobs is the ordinary blob store small attachments live in, keyed by content hash.
type SmallBlobs interface {
	ReadBlob(key codec.Hash) ([]byte, error)
	WriteKeyedBlob(key codec.Hash, bytes []byte) error
}

// pointer is what a pointer segment says about one large blob.
type pointer struct {
	gen     int
	name    string     // the pointer segment's own name
	segment string     // the segment holding the bytes (whole, or a delta)
	leaves  string     // the segment holding the chunk leaf hashes
	size    int64      // the blob's length
	delta   bool       // segment is a delta against base
	base    codec.Hash // for a delta: the blob it applies to
	// inline holds a small blob's bytes in the pointer itself (no segment, no leaves): one file
	// per small blob, and every attachment byte in the attachment store's own segments, which is
	// what a backup copies. Checked against the content hash when the pointer is read.
	inline []byte
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
	verified map[string]bool     // leaves segments whose leaves are known to hash to their root
	pinned   map[string]int      // segment -> open readers using it
	deferred map[string]bool     // segments to delete once unpinned
	readers  map[*Reader]struct{}
	// The mark (mark.go): the newest readable one, its segment, and the highest generation any
	// mark segment has, readable or not, so the next one is never written over a torn one.
	mark    []codec.Hash
	markSeg string
	markGen int
}

// SegmentPrefix is the name prefix of every segment namespaceID's attachment store writes - for a
// backup, which copies them.
func SegmentPrefix(namespaceID string) string {
	return storio.SegmentNameBuilder.NamespacePrefix(namespaceID) + "attachment/"
}

// New returns the store for namespaceID. small may be nil, which keeps every blob standalone.
func New(shim storage.PlatformIOShim, namespaceID string, small SmallBlobs, cfg Config) *Store {
	return &Store{
		shim:     shim,
		ns:       namespaceID,
		prefix:   SegmentPrefix(namespaceID),
		small:    small,
		cfg:      cfg.withDefaults(),
		pointers: map[codec.Hash]pointer{},
		uploads:  map[string]struct{}{},
		verified: map[string]bool{},
		pinned:   map[string]int{},
		deferred: map[string]bool{},
		readers:  map[*Reader]struct{}{},
	}
}

// Config returns the store's effective configuration.
func (s *Store) Config() Config { return s.cfg }

// emptyHash is the content hash of zero bytes. An empty attachment is never stored - a blob store
// cannot tell zero bytes from none - and always present.
var emptyHash = document.AttachmentContentHash(nil)

// loadLocked reads every pointer segment once. Must hold mu.
func (s *Store) loadLocked() error {
	if s.loaded {
		return nil
	}
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return err
	}
	markGen := -1
	for _, name := range names {
		if strings.HasPrefix(name, s.prefix+"m-") {
			gen, commits, ok := s.readMark(name)
			if g, err := strconv.Atoi(strings.TrimPrefix(name, s.prefix+"m-")); err == nil && g > s.markGen {
				s.markGen = g
			}
			if ok && gen > markGen {
				markGen, s.markSeg, s.mark = gen, name, commits
			}
			continue
		}
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

const pointerMagic = "KDBATP2"

func (s *Store) pointerName(h codec.Hash, gen int) string {
	return fmt.Sprintf("%sh-%s.%06d", s.prefix, h.Hex(), gen)
}

func encodePointer(p pointer) []byte {
	b := []byte(pointerMagic)
	flags := byte(0)
	if p.delta {
		flags = 1
	}
	if p.inline != nil {
		flags |= 2
	}
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, uint64(p.size))
	for _, s := range []string{p.segment, p.leaves} {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	if p.delta {
		b = append(b, p.base.Bytes[:]...)
	}
	b = append(b, p.inline...)
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
	raw, err := s.shim.ReadFromSegment(name, 0, 4096)
	if err != nil || len(raw) < len(pointerMagic)+1+8+2+4 || string(raw[:len(pointerMagic)]) != pointerMagic {
		return pointer{}, codec.Hash{}, false
	}
	if raw[len(pointerMagic)]&2 != 0 {
		// Inline: the header, two empty names, the bytes, the checksum.
		n := int64(binary.BigEndian.Uint64(raw[len(pointerMagic)+1:]))
		if n < 0 || n > 64<<20 {
			return pointer{}, codec.Hash{}, false
		}
		if raw, err = s.shim.ReadFromSegment(name, 0, len(pointerMagic)+1+8+2+int(n)+4); err != nil {
			return pointer{}, codec.Hash{}, false
		}
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
	for _, dst := range []*string{&p.segment, &p.leaves} {
		l, k := binary.Uvarint(body[off:])
		if k <= 0 || uint64(len(body)-off-k) < l {
			return pointer{}, codec.Hash{}, false
		}
		off += k
		*dst = string(body[off : off+int(l)])
		off += int(l)
	}
	if body[len(pointerMagic)]&2 != 0 {
		p.inline = append([]byte{}, body[off:]...)
		if int64(len(p.inline)) != p.size || document.AttachmentContentHash(p.inline) != h {
			return pointer{}, codec.Hash{}, false
		}
		return p, h, true
	}
	if p.delta {
		if len(body)-off != 32 {
			return pointer{}, codec.Hash{}, false
		}
		copy(p.base.Bytes[:], body[off:])
	}
	return p, h, true
}

// writePointerLocked makes p the durable pointer for h. Must hold mu.
func (s *Store) writePointerLocked(h codec.Hash, p pointer) (pointer, error) {
	if cur, ok := s.pointers[h]; ok {
		p.gen = cur.gen + 1
	}
	p.name = s.pointerName(h, p.gen)
	if err := s.writeSegment(p.name, encodePointer(p)); err != nil {
		return pointer{}, err
	}
	return p, nil
}

func (s *Store) writeSegment(name string, data []byte) error {
	if _, err := s.shim.AppendToSegment(name, data); err != nil {
		return err
	}
	if err := s.shim.FlushSegment(name); err != nil {
		return err
	}
	return s.shim.SealSegment(name)
}

func (s *Store) newSegmentName() (string, error) {
	id, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	return s.prefix + "u-" + id.String(), nil
}

// Info describes a stored blob.
type Info struct {
	Hash codec.Hash
	Size int64
}

// Upload streams one blob in. Write the bytes, then Finish (or Abort).
type Upload struct {
	s       *Store
	size    int64
	cur     []byte        // the chunk being filled
	segment string        // set once the upload outgrows the small store
	leaves  []*codec.Hash // one slot per chunk, filled by a hashing worker
	pending sync.WaitGroup
	sem     chan struct{}
	done    bool
	err     error
}

var chunkPool = sync.Pool{New: func() any { b := make([]byte, 0, chunk); return &b }}

// Begin starts an upload.
func (s *Store) Begin() *Upload {
	buf := chunkPool.Get().(*[]byte)
	return &Upload{s: s, cur: (*buf)[:0], sem: make(chan struct{}, s.cfg.HashWorkers)}
}

// Write implements io.Writer.
func (u *Upload) Write(p []byte) (int, error) {
	if u.done {
		return 0, errors.New("attachstore: write after finish")
	}
	if u.err != nil {
		return 0, u.err
	}
	if max := u.s.cfg.MaxBytes; max > 0 && u.size+int64(len(p)) > max {
		return 0, ErrTooLarge
	}
	n := len(p)
	for len(p) > 0 {
		take := min(chunk-len(u.cur), len(p))
		u.cur = append(u.cur, p[:take]...)
		p = p[take:]
		u.size += int64(take)
		if len(u.cur) == chunk {
			if err := u.flushChunk(); err != nil {
				u.err = err
				return 0, err
			}
		}
	}
	return n, nil
}

// flushChunk appends the full chunk in cur to the segment and hashes it in the background.
func (u *Upload) flushChunk() error {
	if u.segment == "" {
		name, err := u.s.newSegmentName()
		if err != nil {
			return err
		}
		u.segment = name
		u.s.mu.Lock()
		u.s.uploads[name] = struct{}{}
		u.s.mu.Unlock()
	}
	if _, err := u.s.shim.AppendToSegment(u.segment, u.cur); err != nil {
		return err
	}
	u.hashChunk(u.cur)
	buf := chunkPool.Get().(*[]byte)
	u.cur = (*buf)[:0]
	return nil
}

// hashChunk computes chunk's leaf on a worker, into a slot of its own, and returns the buffer to
// the pool when done. Finish waits for every worker before reading the slots.
func (u *Upload) hashChunk(c []byte) {
	slot := new(codec.Hash)
	u.leaves = append(u.leaves, slot)
	u.sem <- struct{}{}
	u.pending.Add(1)
	go func() {
		defer u.pending.Done()
		*slot = document.ChunkLeafHash(c)
		c = c[:0]
		chunkPool.Put(&c)
		<-u.sem
	}()
}

// Finish makes the blob durable and returns its content hash and size. A blob already stored is
// not stored twice: the new copy is dropped.
func (u *Upload) Finish() (Info, error) {
	if u.done {
		return Info{}, errors.New("attachstore: finish called twice")
	}
	u.done = true
	if u.err != nil {
		u.cleanup()
		return Info{}, u.err
	}
	s := u.s
	last := u.cur
	if len(last) > 0 || len(u.leaves) == 0 {
		if u.segment != "" && len(last) > 0 {
			if _, err := s.shim.AppendToSegment(u.segment, last); err != nil {
				u.cleanup()
				return Info{}, err
			}
		}
		leaf := document.ChunkLeafHash(last)
		u.leaves = append(u.leaves, &leaf)
	}
	u.pending.Wait()
	var m document.MerkleBuilder
	for _, l := range u.leaves {
		m.Add(*l)
	}
	info := Info{Hash: document.AttachmentIdentity(m.Root(), u.size), Size: u.size}
	if u.size == 0 {
		u.cleanup()
		return info, nil // see emptyHash
	}
	if u.segment == "" && u.size <= s.cfg.SmallMaxBytes {
		// Small: the bytes go in the pointer (see pointer.inline).
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.loadLocked(); err != nil {
			return Info{}, err
		}
		if _, ok := s.pointers[info.Hash]; ok {
			return info, nil
		}
		p, err := s.writePointerLocked(info.Hash, pointer{size: u.size, inline: append([]byte{}, last...)})
		if err != nil {
			return Info{}, err
		}
		s.pointers[info.Hash] = p
		return info, nil
	}
	if u.segment == "" {
		// Smaller than a chunk but no small store (or over its bound): a segment of its own.
		name, err := s.newSegmentName()
		if err != nil {
			return Info{}, err
		}
		u.segment = name
		s.mu.Lock()
		s.uploads[name] = struct{}{}
		s.mu.Unlock()
		if _, err := s.shim.AppendToSegment(name, last); err != nil {
			u.cleanup()
			return Info{}, err
		}
	}
	defer func() {
		s.mu.Lock()
		delete(s.uploads, u.segment)
		s.mu.Unlock()
	}()
	if err := s.shim.FlushSegment(u.segment); err != nil {
		u.cleanup()
		return Info{}, err
	}
	if err := s.shim.SealSegment(u.segment); err != nil {
		u.cleanup()
		return Info{}, err
	}
	leavesSeg, err := s.writeLeaves(u.leaves)
	if err != nil {
		u.cleanup()
		return Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return Info{}, err
	}
	if _, ok := s.pointers[info.Hash]; ok {
		_ = s.shim.DeleteSegment(u.segment) // already stored: keep the one copy
		_ = s.shim.DeleteSegment(leavesSeg)
		return info, nil
	}
	p, err := s.writePointerLocked(info.Hash, pointer{segment: u.segment, leaves: leavesSeg, size: u.size})
	if err != nil {
		return Info{}, err
	}
	s.pointers[info.Hash] = p
	s.verified[leavesSeg] = true
	return info, nil
}

func (s *Store) writeLeaves(leaves []*codec.Hash) (string, error) {
	name, err := s.newSegmentName()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.uploads[name] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.uploads, name)
		s.mu.Unlock()
	}()
	const perWrite = chunk / 32
	for i := 0; i < len(leaves); i += perWrite {
		end := min(i+perWrite, len(leaves))
		buf := make([]byte, 0, (end-i)*32)
		for _, l := range leaves[i:end] {
			buf = append(buf, l.Bytes[:]...)
		}
		if _, err := s.shim.AppendToSegment(name, buf); err != nil {
			return "", err
		}
	}
	if err := s.shim.FlushSegment(name); err != nil {
		return "", err
	}
	return name, s.shim.SealSegment(name)
}

func (u *Upload) cleanup() {
	u.pending.Wait()
	if u.segment != "" {
		_ = u.s.shim.DeleteSegment(u.segment)
		u.s.mu.Lock()
		delete(u.s.uploads, u.segment)
		u.s.mu.Unlock()
	}
}

// Abort discards an upload.
func (u *Upload) Abort() {
	if u.done {
		return
	}
	u.done = true
	u.cleanup()
}

// Put stores everything r yields.
func (s *Store) Put(r io.Reader) (Info, error) {
	u := s.Begin()
	buf := make([]byte, 64<<10)
	if _, err := io.CopyBuffer(u, r, buf); err != nil {
		u.Abort()
		return Info{}, err
	}
	return u.Finish()
}

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

// deleteOrDeferLocked deletes a segment, or queues it while a reader has it pinned. Must hold mu.
func (s *Store) deleteOrDeferLocked(name string) {
	if name == "" {
		return
	}
	if s.pinned[name] > 0 {
		s.deferred[name] = true
		return
	}
	_ = s.shim.DeleteSegment(name)
}

// Collect deletes every standalone blob keep rejects - except the bases of kept deltas - and every
// segment no pointer names (uploads that never finished, pointers superseded by a higher
// generation). Segments an open reader has pinned are deleted when it closes. Readers past their
// lease are released first. It reports how many blobs it removed.
func (s *Store) Collect(keep func(codec.Hash) bool) (int, error) {
	s.expireReaders(time.Now())
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
	for h := range live { // a kept delta keeps its chain
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
	referenced := map[string]bool{s.markSeg: true}
	for _, p := range s.pointers {
		referenced[p.name] = true
		referenced[p.segment] = true
		referenced[p.leaves] = true
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
		s.deleteOrDeferLocked(name)
	}
	return removed, nil
}

// PinnedSegments is how many segments open readers hold, and how many deletions wait on them.
func (s *Store) PinnedSegments() (pinned, deferred int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pinned), len(s.deferred)
}
