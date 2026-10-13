// Package deltaindex finds the delta-log frame that holds a key - a document version's content
// hash, or a commit's hash - without keeping one heap entry per key.
//
// The delta log is the only place every historical version and every commit's operations are
// durable, and both the engine's cold loader and embed's commit-operations loader need to go from
// a hash to the one frame that holds it. They used to keep a map with an entry for every key in
// the log, built on the first miss and never evicted: at 1M documents and 2M commits the cold
// loader's map alone was 225 MB of live heap, growing with history rather than with any budget.
//
// This package keeps the index per delta segment instead, as a sorted array of fixed-width
// records (hash, frame offset), searched by binary search, newest segment first:
//
//   - A sealed segment - any but the newest, since a writer never appends to an older segment -
//     has its array written once to a sidecar file beside the log and memory-mapped
//     (storage/mmapfile). Mapped bytes are page cache, not Go heap: the collector never sees
//     them, the kernel pages them, and mmapfile.MappedBytes reports them to the memory guard the
//     way the commit graph's are.
//   - The newest segment, which may still grow, keeps its array on the heap. That is bounded by
//     the segment rotation size, not by history.
//   - On a platform without mmap (Windows), a sealed segment's sidecar is read with positioned
//     reads instead: still off the heap, at about log2(n) 32-byte reads per segment probed.
//   - With no sidecar directory (an in-memory store, or a caller that has none to give), every
//     segment's array stays on the heap. Still pointer-free and about 40 bytes a key.
//
// A sidecar is a cache and never an authority. It records which segment it describes (sequence,
// size, first and last commit) and a CRC over everything; any mismatch, a short or unreadable
// file, or a platform without mmap rebuilds it from the segment, and a sidecar that cannot be
// written leaves that segment on the heap. Callers verify what they read from a frame against
// the hash they asked for, so a bad entry can only cost a miss - and the CRC is what keeps a
// damaged sidecar from producing that miss for a key the log does hold.
//
// The index catches up with the log on a miss, as the maps it replaces did: segments it has not
// seen, and a segment that changed since it was indexed, are indexed again; segments that are
// gone are dropped along with their sidecars.
package deltaindex

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// KeyFunc reports the keys one commit's frame answers for.
type KeyFunc func(c document.Commit, emit func(codec.Hash))

// Options configure an Index.
type Options struct {
	// Kind names what is indexed ("versions", "commits"); it is part of every sidecar's file name,
	// so two indexes over the same log can share a directory.
	Kind string
	// Keys extracts the keys of one commit.
	Keys KeyFunc
}

const (
	recordSize = 32 + 8
	// header: magic(8) seq(8) size(8) first(32) last(32) tail(8) count(8)
	headerSize = 8 + 8 + 8 + 32 + 32 + 8 + 8
	crcSize    = 4
)

var magic = [8]byte{'K', 'D', 'B', 'D', 'I', 'X', 0, 1}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Index maps a key to the newest frame of the log that holds it. Safe for concurrent use.
type Index struct {
	reader storage.DeltaSegmentReader
	opts   Options

	// refreshMu serializes refreshes and guards dir/readOnly; mu guards segs.
	refreshMu sync.Mutex
	dir       string
	readOnly  bool
	closed    bool

	mu sync.RWMutex
	// segs is newest first: the order lookups probe in, so the most recent frame holding a key
	// is the one returned.
	segs []*segIndex
}

type segIndex struct {
	ref storage.DeltaSegmentRef
	// recs is the sorted record array: heap bytes, or the record section of a mapping. Nil for
	// a file-backed sidecar.
	recs    []byte
	mapping *mmapfile.Mapping
	region  *mmapfile.Region
	// file is a sidecar read with positioned reads instead of mapped, on platforms without mmap
	// (Windows): records stay off the heap either way, at a few small reads per lookup.
	file  *os.File
	count int
}

func (s *segIndex) mapped() bool { return s.mapping != nil }

// offHeap reports whether the segment's records live in a sidecar (mapped or file-backed).
func (s *segIndex) offHeap() bool { return s.mapping != nil || s.file != nil }

// keys is how many records the segment's index holds.
func (s *segIndex) keys() int {
	if s.file != nil {
		return s.count
	}
	return len(s.recs) / recordSize
}

// lookup finds key in this segment's records, wherever they are.
func (s *segIndex) lookup(key codec.Hash) (int64, bool, error) {
	if s.file == nil {
		off, ok := search(s.recs, key)
		return off, ok, nil
	}
	var buf [recordSize]byte
	var readErr error
	i := sort.Search(s.count, func(i int) bool {
		if readErr != nil {
			return true
		}
		if _, err := s.file.ReadAt(buf[:32], int64(headerSize+i*recordSize)); err != nil {
			readErr = err
			return true
		}
		return bytes.Compare(buf[:32], key.Bytes[:]) >= 0
	})
	if readErr != nil {
		return 0, false, fmt.Errorf("deltaindex: reading sidecar: %w", readErr)
	}
	if i >= s.count {
		return 0, false, nil
	}
	if _, err := s.file.ReadAt(buf[:], int64(headerSize+i*recordSize)); err != nil {
		return 0, false, fmt.Errorf("deltaindex: reading sidecar: %w", err)
	}
	if !bytes.Equal(buf[:32], key.Bytes[:]) {
		return 0, false, nil
	}
	return int64(binary.LittleEndian.Uint64(buf[32:])), true, nil
}

func (s *segIndex) close() {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	if s.region != nil {
		s.region.Release()
		s.region = nil
	}
	if s.mapping != nil {
		_ = s.mapping.Close()
		s.mapping = nil
	}
	s.recs = nil
}

// New returns an empty index over reader. Nothing is read until the first Lookup.
func New(reader storage.DeltaSegmentReader, opts Options) *Index {
	return &Index{reader: reader, opts: opts}
}

// SetDir gives the index a directory for its sidecars. readOnly uses sidecars it finds there and
// never writes or removes one (a read-only runtime shares the directory with a writer). Safe to
// call at any time; segments indexed before it stay as they are until they next change.
func (x *Index) SetDir(dir string, readOnly bool) {
	x.refreshMu.Lock()
	defer x.refreshMu.Unlock()
	x.dir, x.readOnly = dir, readOnly
}

// Location is where a key was found.
type Location struct {
	Segment     storage.DeltaSegmentRef
	FrameOffset int64
}

// Lookup finds the newest frame holding key. A key the index does not know brings it up to date
// with the log before being reported absent, so a key written to the log is never missed.
func (x *Index) Lookup(key codec.Hash) (Location, bool, error) {
	if loc, ok, err := x.find(key); err != nil || ok {
		return loc, ok, err
	}
	if _, err := x.Refresh(); err != nil {
		return Location{}, false, err
	}
	// Look again even if this refresh changed nothing: another goroutine's refresh may have
	// published the segment holding key between the first look and this one taking refreshMu.
	return x.find(key)
}

func (x *Index) find(key codec.Hash) (loc Location, found bool, err error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	// A mapped sidecar that faults (an I/O error under the page cache) is reported, not crashed
	// on; the caller treats an error like any other failed read.
	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)
	defer func() {
		if r := recover(); r != nil {
			loc, found, err = Location{}, false, fmt.Errorf("deltaindex: fault reading sidecar: %v", r)
		}
	}()
	for _, s := range x.segs {
		off, ok, err := s.lookup(key)
		if err != nil {
			return Location{}, false, err
		}
		if ok {
			return Location{Segment: s.ref, FrameOffset: off}, true, nil
		}
	}
	return Location{}, false, nil
}

// search binary-searches a sorted record array.
func search(recs []byte, key codec.Hash) (int64, bool) {
	n := len(recs) / recordSize
	i := sort.Search(n, func(i int) bool {
		return bytes.Compare(recs[i*recordSize:i*recordSize+32], key.Bytes[:]) >= 0
	})
	if i < n && bytes.Equal(recs[i*recordSize:i*recordSize+32], key.Bytes[:]) {
		return int64(binary.LittleEndian.Uint64(recs[i*recordSize+32:])), true
	}
	return 0, false
}

// Refresh brings the index up to date with the log and reports whether anything changed.
func (x *Index) Refresh() (bool, error) {
	x.refreshMu.Lock()
	defer x.refreshMu.Unlock()
	streamer, ok := x.reader.(storage.DeltaCommitStreamer)
	if !ok {
		// A reader that cannot report frame offsets cannot be asked for one frame later either.
		return false, nil
	}
	listed, err := x.reader.ListSegments()
	if err != nil {
		return false, err
	}
	newest := int64(-1)
	for _, seg := range listed {
		newest = max(newest, seg.SequenceNumber)
	}
	if x.closed {
		return false, nil
	}
	x.mu.RLock()
	have := make(map[int64]*segIndex, len(x.segs))
	current := make(map[*segIndex]bool, len(x.segs))
	for _, s := range x.segs {
		have[s.ref.SequenceNumber] = s
		current[s] = true
	}
	x.mu.RUnlock()

	useDir := x.dir != ""
	next := make([]*segIndex, 0, len(listed))
	var retired []*segIndex
	changed := false
	for _, seg := range listed {
		sealed := seg.SequenceNumber < newest
		old := have[seg.SequenceNumber]
		delete(have, seg.SequenceNumber)
		if old != nil && sameSegment(old.ref, seg) {
			if old.offHeap() || !sealed || !useDir || x.readOnly {
				next = append(next, old)
				continue
			}
			// Sealed since it was indexed: move its records off the heap, no rescan needed.
			if s, ok := x.persist(seg, old.recs); ok {
				next = append(next, s)
				retired = append(retired, old)
				changed = true
				continue
			}
			next = append(next, old)
			continue
		}
		changed = true
		if old != nil {
			retired = append(retired, old)
		}
		if sealed && useDir {
			if s, ok := x.openSidecar(seg); ok {
				next = append(next, s)
				continue
			}
		}
		recs, err := x.scan(streamer, seg)
		if err != nil {
			// Nothing was published: close only what this pass created, never what lookups use.
			for _, s := range next {
				if !current[s] {
					s.close()
				}
			}
			return false, err
		}
		if sealed && useDir && !x.readOnly {
			if s, ok := x.persist(seg, recs); ok {
				next = append(next, s)
				continue
			}
		}
		next = append(next, &segIndex{ref: seg, recs: recs})
	}
	// Whatever is left in have is a segment that is no longer in the log: truncated away, or
	// deleted by a repair. Its sidecar goes with it.
	for _, s := range have {
		changed = true
		// Its sidecar is removed below (removeOrphans), once the index no longer holds it open:
		// Windows will not delete an open file.
		retired = append(retired, s)
	}
	if !changed {
		return false, nil
	}
	slices.SortFunc(next, func(a, b *segIndex) int {
		switch {
		case a.ref.SequenceNumber > b.ref.SequenceNumber:
			return -1
		case a.ref.SequenceNumber < b.ref.SequenceNumber:
			return 1
		}
		return 0
	})
	x.mu.Lock()
	x.segs = next
	x.mu.Unlock()
	// Lookups run under mu, so once the swap is done nothing reads a retired index any more.
	for _, s := range retired {
		s.close()
	}
	if useDir && !x.readOnly {
		x.removeOrphans(listed)
	}
	return true, nil
}

func sameSegment(a, b storage.DeltaSegmentRef) bool {
	return a.SequenceNumber == b.SequenceNumber && a.SizeBytes == b.SizeBytes &&
		a.FirstCommitHash == b.FirstCommitHash && a.LastCommitHash == b.LastCommitHash &&
		a.Tail.Kind == b.Tail.Kind
}

// scan reads seg once and returns its sorted, de-duplicated record array. Past damage where the
// reader can: a damaged frame loses only the keys it held, and callers verify every frame they
// read against the key they asked for.
func (x *Index) scan(streamer storage.DeltaCommitStreamer, seg storage.DeltaSegmentRef) ([]byte, error) {
	type rec struct {
		key codec.Hash
		off int64
	}
	var recs []rec
	stream := streamer.StreamCommits
	if dr, ok := x.reader.(storage.DamageTolerantReader); ok {
		stream = dr.StreamCommitsPastDamage
	}
	err := stream(seg, func(c document.Commit, offset int64) error {
		x.opts.Keys(c, func(h codec.Hash) { recs = append(recs, rec{h, offset}) })
		return nil
	})
	var corrupt *delta.CorruptFrameError
	if err != nil && !errors.As(err, &corrupt) {
		return nil, err
	}
	// By key, then by offset, so the last record of each run is the newest frame holding it.
	slices.SortFunc(recs, func(a, b rec) int {
		if c := bytes.Compare(a.key.Bytes[:], b.key.Bytes[:]); c != 0 {
			return c
		}
		switch {
		case a.off < b.off:
			return -1
		case a.off > b.off:
			return 1
		}
		return 0
	})
	out := make([]byte, 0, len(recs)*recordSize)
	for i, r := range recs {
		if i+1 < len(recs) && recs[i+1].key == r.key {
			continue
		}
		out = append(out, r.key.Bytes[:]...)
		out = binary.LittleEndian.AppendUint64(out, uint64(r.off))
	}
	return slices.Clip(out), nil
}

// SidecarSuffix is the file-name suffix of every sidecar, for tools that list a namespace's files.
const SidecarSuffix = ".kdbx"

func (x *Index) sidecarPath(seq int64) string {
	return filepath.Join(x.dir, fmt.Sprintf("%020d.%s%s", seq, x.opts.Kind, SidecarSuffix))
}

// persist writes recs as seg's sidecar and maps it. On any failure the caller keeps recs on the
// heap: the sidecar is an optimisation.
func (x *Index) persist(seg storage.DeltaSegmentRef, recs []byte) (*segIndex, bool) {
	if err := os.MkdirAll(x.dir, 0o755); err != nil {
		return nil, false
	}
	path := x.sidecarPath(seg.SequenceNumber)
	tmp, err := os.CreateTemp(x.dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return nil, false
	}
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	hdr := encodeHeader(seg, len(recs)/recordSize)
	crc := crc32.Update(crc32.Checksum(hdr, castagnoli), castagnoli, recs)
	var tail [crcSize]byte
	binary.LittleEndian.PutUint32(tail[:], crc)
	for _, b := range [][]byte{hdr, recs, tail[:]} {
		if _, err := tmp.Write(b); err != nil {
			return nil, false
		}
	}
	// No fsync: a sidecar lost to a crash is rebuilt, and one torn by it fails its CRC. The
	// rename is what keeps a reader from ever mapping a half-written file under the final name.
	if err := tmp.Close(); err != nil {
		return nil, false
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, false
	}
	ok = true
	return x.openSidecar(seg)
}

func encodeHeader(seg storage.DeltaSegmentRef, count int) []byte {
	b := make([]byte, 0, headerSize)
	b = append(b, magic[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(seg.SequenceNumber))
	b = binary.LittleEndian.AppendUint64(b, uint64(seg.SizeBytes))
	b = append(b, seg.FirstCommitHash.Bytes[:]...)
	b = append(b, seg.LastCommitHash.Bytes[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(seg.Tail.Kind))
	b = binary.LittleEndian.AppendUint64(b, uint64(count))
	return b
}

// openSidecar maps seg's sidecar if there is one that describes exactly this segment and passes
// its CRC. Anything else - absent, stale, torn, damaged, unmappable - is reported as no sidecar,
// and the caller rebuilds it from the segment.
func (x *Index) openSidecar(seg storage.DeltaSegmentRef) (*segIndex, bool) {
	if !mapSidecars || !mmapfile.Supported {
		return x.openSidecarFile(seg)
	}
	m, err := mmapfile.Open(x.sidecarPath(seg.SequenceNumber))
	if errors.Is(err, mmapfile.ErrUnsupported) {
		return x.openSidecarFile(seg)
	}
	if err != nil {
		return nil, false
	}
	region, err := m.Acquire()
	if err != nil {
		m.Close()
		return nil, false
	}
	valid := false
	var recs []byte
	gerr := region.Guard(func(data []byte) {
		if len(data) < headerSize+crcSize {
			return
		}
		want := encodeHeader(seg, 0)
		hdr := data[:headerSize]
		// Everything but the count must match this segment.
		if !bytes.Equal(hdr[:headerSize-8], want[:headerSize-8]) {
			return
		}
		count := binary.LittleEndian.Uint64(hdr[headerSize-8:])
		if count > uint64(len(data)) || uint64(len(data)) != headerSize+count*recordSize+crcSize {
			return
		}
		body := data[:len(data)-crcSize]
		if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(data[len(data)-crcSize:]) {
			return
		}
		recs = data[headerSize : len(data)-crcSize]
		valid = true
	})
	if gerr != nil || !valid {
		region.Release()
		m.Close()
		return nil, false
	}
	return &segIndex{ref: seg, recs: recs, mapping: m, region: region}, true
}

// mapSidecars is false only in tests, to run the positioned-read path that platforms without
// mmap take.
var mapSidecars = true

// openSidecarFile opens seg's sidecar for positioned reads: the path for a platform without mmap.
// It is validated exactly as a mapped one is - the whole file is read once to check its CRC - and
// then only the records a lookup's binary search touches are read.
func (x *Index) openSidecarFile(seg storage.DeltaSegmentRef) (*segIndex, bool) {
	f, err := os.Open(x.sidecarPath(seg.SequenceNumber))
	if err != nil {
		return nil, false
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	st, err := f.Stat()
	if err != nil || st.Size() < headerSize+crcSize {
		return nil, false
	}
	hdr := make([]byte, headerSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return nil, false
	}
	want := encodeHeader(seg, 0)
	if !bytes.Equal(hdr[:headerSize-8], want[:headerSize-8]) {
		return nil, false
	}
	count := binary.LittleEndian.Uint64(hdr[headerSize-8:])
	size := uint64(st.Size())
	if count > size || size != headerSize+count*recordSize+crcSize {
		return nil, false
	}
	crc := crc32.Checksum(hdr, castagnoli)
	buf := make([]byte, 64<<10)
	for pos, end := int64(headerSize), int64(size-crcSize); pos < end; {
		n := min(int64(len(buf)), end-pos)
		if _, err := f.ReadAt(buf[:n], pos); err != nil {
			return nil, false
		}
		crc = crc32.Update(crc, castagnoli, buf[:n])
		pos += n
	}
	var tail [crcSize]byte
	if _, err := f.ReadAt(tail[:], int64(size-crcSize)); err != nil || binary.LittleEndian.Uint32(tail[:]) != crc {
		return nil, false
	}
	ok = true
	return &segIndex{ref: seg, file: f, count: int(count)}, true
}

// removeOrphans deletes sidecars - of every kind, since indexes over one log share a directory -
// whose segment is no longer listed, and temporary files of this kind a crash left behind.
func (x *Index) removeOrphans(listed []storage.DeltaSegmentRef) {
	entries, err := os.ReadDir(x.dir)
	if err != nil {
		return
	}
	keep := make(map[int64]bool, len(listed))
	for _, s := range listed {
		keep[s.SequenceNumber] = true
	}
	ownTmp := "." + x.opts.Kind + SidecarSuffix + ".tmp"
	for _, e := range entries {
		name := e.Name()
		if strings.Contains(name, ownTmp) {
			// Only this kind's: refreshMu serializes this index's writes, not another's.
			os.Remove(filepath.Join(x.dir, name))
			continue
		}
		if !strings.HasSuffix(name, SidecarSuffix) {
			continue
		}
		dot := strings.IndexByte(name, '.')
		if dot <= 0 {
			continue
		}
		seq, err := strconv.ParseInt(name[:dot], 10, 64)
		if err != nil || keep[seq] {
			continue
		}
		os.Remove(filepath.Join(x.dir, name))
	}
}

// Prune forgets segments that are no longer in the log and removes their sidecars, without
// indexing anything. Sidecars of every kind are removed, but on Windows a file another index
// still holds open cannot be deleted, so every index over the log must be pruned: each one
// closes its own sidecars before removing them. Truncation calls it after deleting segments: a namespace whose cold reads
// are answered elsewhere (the blob store under history=none) may never miss again, and the
// sidecars of deleted segments would otherwise stay on disk until it did.
func (x *Index) Prune() error {
	x.refreshMu.Lock()
	defer x.refreshMu.Unlock()
	if x.closed {
		return nil
	}
	listed, err := x.reader.ListSegments()
	if err != nil {
		return err
	}
	keep := make(map[int64]bool, len(listed))
	for _, s := range listed {
		keep[s.SequenceNumber] = true
	}
	x.mu.Lock()
	var retired []*segIndex
	next := x.segs[:0:0]
	for _, s := range x.segs {
		if keep[s.ref.SequenceNumber] {
			next = append(next, s)
		} else {
			retired = append(retired, s)
		}
	}
	x.segs = next
	x.mu.Unlock()
	for _, s := range retired {
		s.close()
	}
	if x.dir != "" && !x.readOnly {
		x.removeOrphans(listed)
	}
	return nil
}

// Stats describes what the index holds.
type Stats struct {
	Segments   int
	MappedSegs int // sidecars mapped into memory
	FileSegs   int // sidecars read with positioned reads (no mmap on this platform)
	Keys       int64
	// HeapBytes is record arrays on the heap: the newest segment, or every one without a dir.
	HeapBytes   int64
	MappedBytes int64 // record arrays in mapped sidecars
	FileBytes   int64 // record arrays in file-backed sidecars
}

// OffHeapSegs is how many segments' records are in sidecars rather than on the heap.
func (s Stats) OffHeapSegs() int { return s.MappedSegs + s.FileSegs }

// Stats reports the index's current shape.
func (x *Index) Stats() Stats {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var st Stats
	for _, s := range x.segs {
		st.Segments++
		st.Keys += int64(s.keys())
		if s.file != nil {
			st.FileSegs++
			st.FileBytes += int64(s.count) * recordSize
		} else if s.mapped() {
			st.MappedSegs++
			st.MappedBytes += int64(len(s.recs))
		} else {
			st.HeapBytes += int64(cap(s.recs))
		}
	}
	return st
}

// Close releases every mapping. Lookups afterwards find nothing.
func (x *Index) Close() {
	x.refreshMu.Lock()
	defer x.refreshMu.Unlock()
	x.closed = true
	x.mu.Lock()
	segs := x.segs
	x.segs = nil
	x.mu.Unlock()
	for _, s := range segs {
		s.close()
	}
}
