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
//
// Uploads are held. Every writer uploads first and commits after - the client API, SQL's STREAM ?,
// a replica's push - so a finished blob no commit references yet is not garbage. A pointer records
// when its upload finished, and Collect keeps an unreferenced blob for Config.UploadGrace after
// that (Hold restarts the grace for a blob a client skips uploading because the store has it). The
// hold ends early once the commit referencing the blob is durable (Committed), or a Collect sees it
// referenced: from then on references alone decide.
package attachstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// ErrCorrupt reports stored bytes that do not hash to what they are stored under: this node's copy
// is damaged. Servers send it as wire.ErrorCodeIntegrity, so a reader can go to another replica.
var ErrCorrupt = errors.New("attachstore: stored bytes do not match their content hash")

var corruptReads atomic.Int64

// CorruptReads is how many reads ReportCorrupt has seen fail with ErrCorrupt, process-wide.
func CorruptReads() int64 { return corruptReads.Load() }

// ReportCorrupt logs err at error level with attrs (slog key/value pairs naming what was read) and
// counts it in CorruptReads if it is ErrCorrupt, and reports whether it was. Whoever turns a read's
// error into a reply calls it, so damaged bytes are seen by an operator, not just by the client.
func ReportCorrupt(err error, attrs ...any) bool {
	if !errors.Is(err, ErrCorrupt) {
		return false
	}
	corruptReads.Add(1)
	slog.Error("kdb: stored attachment bytes are corrupt", append(attrs, "err", err)...)
	return true
}

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
	// DeltaMaxBytes is the largest version StoreAsDelta will encode. Zero (the default) is no
	// limit: encoding holds a bounded index and a few pages, never a version (delta.go).
	DeltaMaxBytes int64
	// DeltaIndexEntries bounds the base index a delta encoding holds (about 40 bytes an entry):
	// the block it indexes grows with the base's unmatched bytes to stay under it. Default 1M.
	DeltaIndexEntries int
	// DeltaStepBytes is how much work - bytes indexed, encoded or verified - one DeltaStep does
	// when its caller names no budget, and so one maintenance pass. Default 4 GiB.
	DeltaStepBytes int64
	// DisableDeltas keeps every version whole, as uploaded: DeltaStep (and StoreAsDelta) store
	// nothing. Deltas stored before it was set still read; InflateStep turns them back into
	// whole copies, which maintenance does while this is set.
	DisableDeltas bool
	// DeltaMaxDepth bounds a delta chain - how many deltas a read goes through. Default 8.
	DeltaMaxDepth int
	// HashWorkers bounds the chunks hashed at once during an upload. Default GOMAXPROCS.
	HashWorkers int
	// ReadLease is how long a reader may hold its pins. Default one hour.
	ReadLease time.Duration
	// UploadGrace is how long Collect keeps a finished blob no commit references yet: the longest
	// an upload may wait for the commit that references it. Default one hour.
	UploadGrace time.Duration
	// PointerCacheBytes bounds the pointers (a small blob's bytes included) kept in memory; the
	// rest are read from their segments through the pointer index (pointer_index.go). Default
	// 4 MiB.
	PointerCacheBytes int64

	// For tests: the pointer index's overlay, in entries, before it is written as a run.
	indexOverlay int
	// For tests: page and frame sizes small enough that a few-MB version takes the paths a
	// multi-GB one does.
	deltaPageBytes int64
	deltaFrameOps  int
}

func (c Config) withDefaults() Config {
	if c.SmallMaxBytes <= 0 {
		c.SmallMaxBytes = 256 << 10
	}
	if c.SmallMaxBytes > chunk {
		c.SmallMaxBytes = chunk
	}
	if c.DeltaIndexEntries <= 0 {
		c.DeltaIndexEntries = defaultIndexEntries
	}
	if c.DeltaStepBytes <= 0 {
		c.DeltaStepBytes = 4 << 30
	}
	if c.deltaPageBytes <= 0 {
		c.deltaPageBytes = defaultPageBytes
	}
	if c.deltaFrameOps <= 0 {
		c.deltaFrameOps = defaultFrameOps
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
	if c.UploadGrace <= 0 {
		c.UploadGrace = time.Hour
	}
	if c.PointerCacheBytes <= 0 {
		c.PointerCacheBytes = 4 << 20
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
	framed  bool       // the delta is format 2 (delta.go)
	base    codec.Hash // for a delta: the blob it applies to
	// inline holds a small blob's bytes in the pointer itself (no segment, no leaves): one file
	// per small blob, and every attachment byte in the attachment store's own segments, which is
	// what a backup copies. Checked against the content hash when the pointer is read.
	inline []byte
	// finished is when the blob's upload last finished (or was last held): the start of its
	// UploadGrace. Zero in a pointer written before the field existed.
	finished time.Time
}

// Store holds one namespace's attachment blobs.
type Store struct {
	shim   storage.PlatformIOShim
	ns     string
	prefix string // <ns prefix>attachment/
	small  SmallBlobs
	cfg    Config

	mu     sync.Mutex
	loaded bool
	// Every blob's pointer, through the index (pointer_index.go), and the recently used ones. An
	// index entry also says whether the blob is held: a finished upload no Collect has seen
	// referenced, and since when.
	idx      *pointerIndex
	cache    *lru[codec.Hash, pointer]
	uploads  map[string]struct{} // segments being written, which Collect must leave alone
	verified *lru[string, bool]  // leaves segments whose leaves are known to hash to their root
	pinned   map[string]int      // segment -> open readers using it
	deferred map[string]bool     // segments to delete once unpinned
	readers  map[*Reader]struct{}
	// The mark (mark.go): the newest readable one, its segment, and the highest generation any
	// mark segment has, readable or not, so the next one is never written over a torn one.
	mark    []codec.Hash
	markSeg string
	markGen int

	// Delta jobs (deltajob.go): deltaMu serialises their steps; job is the one in progress, and
	// declined the pairs whose delta was not worth storing (this process need not try them again).
	deltaMu  sync.Mutex
	job      *deltaJob
	inflate  *inflateJob // inflate.go: a delta being turned back into a whole copy
	declined *lru[[2]codec.Hash, struct{}]
}

// How many leaves segments known good, and declined delta pairs, a store remembers: both only
// save repeating work.
const rememberedEntries = 64 << 10

// SegmentPrefix is the name prefix of every segment namespaceID's attachment store writes - for a
// backup, which copies them.
func SegmentPrefix(namespaceID string) string {
	return storio.SegmentNameBuilder.NamespacePrefix(namespaceID) + "attachment/"
}

// New returns the store for namespaceID. small may be nil, which keeps every blob standalone.
func New(shim storage.PlatformIOShim, namespaceID string, small SmallBlobs, cfg Config) *Store {
	cfg = cfg.withDefaults()
	prefix := SegmentPrefix(namespaceID)
	return &Store{
		shim:     shim,
		ns:       namespaceID,
		prefix:   prefix,
		small:    small,
		cfg:      cfg,
		idx:      newPointerIndex(shim, prefix, cfg.indexOverlay),
		cache:    newLRU[codec.Hash](cfg.PointerCacheBytes, pointerCost),
		uploads:  map[string]struct{}{},
		verified: newLRU[string](rememberedEntries, one[bool]),
		pinned:   map[string]int{},
		deferred: map[string]bool{},
		readers:  map[*Reader]struct{}{},
		declined: newLRU[[2]codec.Hash](rememberedEntries, one[struct{}]),
	}
}

// Config returns the store's effective configuration.
func (s *Store) Config() Config { return s.cfg }

// emptyHash is the content hash of zero bytes. An empty attachment is never stored - a blob store
// cannot tell zero bytes from none - and always present.
var emptyHash = document.AttachmentContentHash(nil)

// loadLocked reads every pointer segment once, into the index. Must hold mu.
func (s *Store) loadLocked() error {
	if s.loaded {
		return nil
	}
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return err
	}
	markGen := -1
	sort.Strings(names)
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
	}
	if err := s.rebuildIndex(names, time.Now()); err != nil {
		return err
	}
	s.loaded = true
	return nil
}

// getLocked returns h's pointer: from the cache, or read from the segment the index names. A
// pointer that no longer reads is ErrCorrupt. Must hold mu, loaded.
func (s *Store) getLocked(h codec.Hash) (pointer, bool, error) {
	if p, ok := s.cache.get(h); ok {
		return p, true, nil
	}
	e, ok, err := s.idx.get(h)
	if err != nil || !ok {
		return pointer{}, false, err
	}
	name := s.pointerName(h, e.gen)
	p, ph, ok := s.readPointer(name)
	if !ok || ph != h {
		return pointer{}, false, fmt.Errorf("%w: pointer %s does not read", ErrCorrupt, name)
	}
	s.cache.put(h, p)
	return p, true, nil
}

// hasLocked reports whether h has a pointer, without reading it. Must hold mu, loaded.
func (s *Store) hasLocked(h codec.Hash) (bool, error) {
	if _, ok := s.cache.get(h); ok {
		return true, nil
	}
	_, ok, err := s.idx.get(h)
	return ok, err
}

// setLocked makes p, just written, h's pointer, held since held (UnixNano; 0 is not held).
// Must hold mu, loaded.
func (s *Store) setLocked(h codec.Hash, p pointer, held int64) {
	s.idx.put(h, entryFor(p, held))
	s.cache.put(h, p)
}

// replaceLocked makes p, just written, h's pointer, keeping h's hold. Must hold mu, loaded.
func (s *Store) replaceLocked(h codec.Hash, p pointer) error {
	e, _, err := s.idx.get(h)
	if err != nil {
		return err
	}
	s.setLocked(h, p, e.held)
	return nil
}

// ResidentPointers is how many pointers the store holds in memory, and how many index entries
// wait in its overlay to be written as a run.
func (s *Store) ResidentPointers() (pointers, overlay int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache.len(), len(s.idx.overlay)
}

// Pointer formats. v3 adds the finish time after the size; v2 is still read.
const (
	pointerMagicV2 = "KDBATP2"
	pointerMagic   = "KDBATP3"
)

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
	if p.framed {
		flags |= 4
	}
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, uint64(p.size))
	b = binary.BigEndian.AppendUint64(b, uint64(p.finished.UnixNano()))
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
	if err != nil || len(raw) < len(pointerMagic) {
		return pointer{}, codec.Hash{}, false
	}
	header := len(pointerMagic) + 1 + 8 // magic, flags, size
	switch string(raw[:len(pointerMagic)]) {
	case pointerMagic:
		header += 8 // finish time
	case pointerMagicV2:
	default:
		return pointer{}, codec.Hash{}, false
	}
	if len(raw) < header+2+4 {
		return pointer{}, codec.Hash{}, false
	}
	if raw[len(pointerMagic)]&2 != 0 {
		// Inline: the header, two empty names, the bytes, the checksum.
		n := int64(binary.BigEndian.Uint64(raw[len(pointerMagic)+1:]))
		if n < 0 || n > 64<<20 {
			return pointer{}, codec.Hash{}, false
		}
		if raw, err = s.shim.ReadFromSegment(name, 0, header+2+int(n)+4); err != nil {
			return pointer{}, codec.Hash{}, false
		}
	}
	body, crc := raw[:len(raw)-4], raw[len(raw)-4:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:4], crc) {
		return pointer{}, codec.Hash{}, false
	}
	off := len(pointerMagic)
	p := pointer{gen: gen, name: name, delta: body[off]&1 != 0, framed: body[off]&4 != 0}
	off++
	p.size = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	if off < header {
		if t := int64(binary.BigEndian.Uint64(body[off:])); t > 0 {
			p.finished = time.Unix(0, t)
		}
		off += 8
	}
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
// A p with no finish time keeps the current pointer's.
func (s *Store) writePointerLocked(h codec.Hash, p pointer) (pointer, error) {
	cur, ok, err := s.getLocked(h)
	if err != nil {
		return pointer{}, err
	}
	if ok {
		p.gen = cur.gen + 1
		if p.finished.IsZero() {
			p.finished = cur.finished
		}
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
		if have, err := s.hasLocked(info.Hash); err != nil || have {
			if err != nil {
				return Info{}, err
			}
			return info, s.holdLocked(info.Hash, time.Now())
		}
		now := time.Now()
		p, err := s.writePointerLocked(info.Hash, pointer{size: u.size, inline: append([]byte{}, last...), finished: now})
		if err != nil {
			return Info{}, err
		}
		s.setLocked(info.Hash, p, now.UnixNano())
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
	have, err := s.hasLocked(info.Hash)
	if err != nil {
		return Info{}, err
	}
	if have {
		_ = s.shim.DeleteSegment(u.segment) // already stored: keep the one copy
		_ = s.shim.DeleteSegment(leavesSeg)
		return info, s.holdLocked(info.Hash, time.Now())
	}
	now := time.Now()
	p, err := s.writePointerLocked(info.Hash, pointer{segment: u.segment, leaves: leavesSeg, size: u.size, finished: now})
	if err != nil {
		return Info{}, err
	}
	s.setLocked(info.Hash, p, now.UnixNano())
	s.verified.put(leavesSeg, true)
	return info, nil
}

// Hold restarts h's upload grace, as if it had just been uploaded: for a client about to commit a
// reference to a blob it did not upload because the store already holds it (a wire upload's
// Expect). It reports whether the store holds h as a standalone blob; a blob in the ordinary blob
// store (written before attachments had their own segments) has no grace to restart.
func (s *Store) Hold(h codec.Hash) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return false, err
	}
	if have, err := s.hasLocked(h); err != nil || !have {
		return false, err
	}
	return true, s.holdLocked(h, time.Now())
}

// Committed ends the hold on blobs a durable commit references: from here on references alone
// decide, so a version replaced before the next Collect is not kept for the rest of its grace.
// (Collect also ends the hold on any blob it finds referenced.)
func (s *Store) Committed(hashes ...codec.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	for _, h := range hashes {
		e, ok, err := s.idx.get(h)
		if err != nil {
			return err
		}
		if ok && e.held != 0 {
			e.held = 0
			s.idx.put(h, e)
		}
	}
	return nil
}

// holdLocked starts h's grace at now. The pointer is rewritten with the new time only once the
// recorded one is half a grace old, so the hold survives a restart without a write per upload of a
// blob uploaded often. Must hold mu; h must have a pointer.
func (s *Store) holdLocked(h codec.Hash, now time.Time) error {
	cur, _, err := s.getLocked(h)
	if err != nil {
		return err
	}
	if now.Sub(cur.finished) < s.cfg.UploadGrace/2 {
		s.setLocked(h, cur, now.UnixNano())
		return nil
	}
	next := cur
	next.finished = now
	p, err := s.writePointerLocked(h, next)
	if err != nil {
		return err
	}
	s.setLocked(h, p, now.UnixNano()) // the old generation is no longer named, and Collect retires it
	return nil
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
	ok := false
	if err == nil {
		ok, err = s.hasLocked(h)
	}
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
	var out []codec.Hash
	err := s.idx.each(func(h codec.Hash, _ indexEntry) error {
		out = append(out, h)
		return nil
	})
	return out, err
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

// Collect deletes every standalone blob keep rejects - except the bases of kept deltas, and blobs
// within their upload grace (Config.UploadGrace) - and every segment no pointer names (uploads that
// never finished, pointers superseded by a higher generation). Segments an open reader has pinned
// are deleted when it closes. Readers past their lease are released first. It reports how many
// blobs it removed.
//
// It streams the pointer index twice - once for the bases kept deltas need that nothing else
// keeps, once to decide - so keep is asked about a blob more than once, and no pointer has to be
// in memory. The segment sweep holds a key of each segment a kept pointer names (pointer_index.go).
func (s *Store) Collect(keep func(codec.Hash) bool) (int, error) {
	now := time.Now()
	s.expireReaders(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return 0, err
	}
	grace := s.cfg.UploadGrace
	// Bases a kept (or held) delta reads through that are neither kept nor held: a kept delta
	// keeps its chain.
	rescued := map[codec.Hash]bool{}
	err := s.idx.each(func(h codec.Hash, e indexEntry) error {
		if !e.delta || !(keep(h) || e.heldAt(now, grace)) {
			return nil
		}
		for b := e.base; !rescued[b] && !keep(b); {
			be, ok, err := s.idx.get(b)
			if err != nil {
				return err
			}
			if !ok || be.heldAt(now, grace) {
				return nil // missing (reads fail as they would have), or held: walked itself
			}
			rescued[b] = true
			if !be.delta {
				return nil
			}
			b = be.base
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	live := func(h codec.Hash) (bool, error) {
		if keep(h) || rescued[h] {
			return true, nil
		}
		e, ok, err := s.idx.get(h)
		return ok && e.heldAt(now, grace), err
	}
	if s.job != nil || s.inflate != nil {
		dropJob, dropInflate := false, false
		if s.job != nil {
			oldLive, err := live(s.job.old)
			if err != nil {
				return 0, err
			}
			baseLive, err := live(s.job.base)
			if err != nil {
				return 0, err
			}
			dropJob = !oldLive || !baseLive
		}
		if s.inflate != nil {
			l, err := live(s.inflate.h)
			if err != nil {
				return 0, err
			}
			dropInflate = !l
		}
		if (dropJob || dropInflate) && s.deltaMu.TryLock() { // no step can be running while we hold it
			if dropJob {
				s.dropJobLocked() // its versions are going
			}
			if dropInflate {
				s.dropInflateLocked()
			}
			s.deltaMu.Unlock()
		}
	}
	removed := 0
	referenced := []nameKey{keyOf(s.markSeg)}
	err = s.idx.each(func(h codec.Hash, e indexEntry) error {
		kept := keep(h)
		if !kept && !rescued[h] && !e.heldAt(now, grace) {
			s.idx.remove(h)
			s.cache.drop(h)
			removed++
			return nil
		}
		if e.held != 0 && (kept || !e.heldAt(now, grace)) {
			e.held = 0 // committed: references decide from here on
			s.idx.put(h, e)
		}
		referenced = append(referenced, keyOf(s.pointerName(h, e.gen)), e.seg, e.leaves)
		return nil
	})
	if err != nil {
		return removed, err
	}
	sort.Slice(referenced, func(i, j int) bool { return bytes.Compare(referenced[i][:], referenced[j][:]) < 0 })
	isReferenced := func(name string) bool {
		k := keyOf(name)
		i := sort.Search(len(referenced), func(i int) bool { return bytes.Compare(referenced[i][:], k[:]) >= 0 })
		return i < len(referenced) && referenced[i] == k
	}
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		return removed, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, s.prefix) || s.idx.isRun(name) || isReferenced(name) {
			continue
		}
		if _, ok := s.isRefIndex(name); ok {
			continue // SetRefIndex retires older ones itself
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
