package io

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/fsx"
)

// OSByteStore is a filesystem-backed SegmentByteStore rooted at PlatformIOConfig.RootDirectory.
// Segment names are validated upstream (must start with "ns/").
//
// Append keeps one *os.File open per segment instead of reopening on
// every call: the original implementation did open+write+fstat+close per
// append, which meant every WriteBlob paid for 4 syscalls plus directory
// lookup regardless of any in-process locking work done above it. That
// was found to be the dominant cost once the engine-wide mutex was
// removed from ServerEngine.WriteBlob - see docs/benchmarks/phase0-baseline.md
// Phase 1. Callers (FileBackedPlatformIO) already serialize Append calls
// per segment, so the handle cache itself only needs to guard the map,
// not each write.
type OSByteStore struct {
	root             string
	syncMode         SyncMode
	preallocateBytes int64

	mu      sync.Mutex
	handles map[string]*openSegment
	// readers caches read-only handles so a read does not open, stat and close a file. Reading
	// every document after a restart (a scan, or a cold get) goes through here once per document,
	// and the open dominated it: ~45 of 58 seconds for 100k documents. Bounded, and never evicted
	// while in use: past maxReadHandles a read simply opens its own handle as before.
	readers map[string]*os.File
}

const maxReadHandles = 512

type openSegment struct {
	file *os.File
	size int64 // atomic
	// sealed is set by MarkSealed and cleared by any later append (atomic). Sealing is otherwise
	// advisory - the handle stays open - so this is what lets Stamp vouch for a segment this
	// process wrote and will not append to again.
	sealed int32
}

func NewOSByteStore(config PlatformIOConfig) (*OSByteStore, error) {
	if config.RootDirectory == nil || *config.RootDirectory == "" {
		return nil, fmt.Errorf("os byte store requires root directory")
	}
	return &OSByteStore{
		root:             *config.RootDirectory,
		syncMode:         config.SyncMode,
		preallocateBytes: config.PreallocateBytes,
		handles:          make(map[string]*openSegment),
		readers:          make(map[string]*os.File),
	}, nil
}

// LocalSegmentPath names the file segmentName lives in (see storage.LocalPathResolver).
func (s *OSByteStore) LocalSegmentPath(segmentName string) (string, bool) {
	return s.pathFor(segmentName), true
}

func (s *OSByteStore) pathFor(segmentName string) string {
	return filepath.Join(s.root, filepath.FromSlash(segmentName))
}

// preallocateFor reports the size a newly created segment should be created
// at, or 0 for the default grow-as-you-go behavior.
//
// Scoped to delta segments on purpose. They are the append-per-commit path
// whose fsync cost this exists to remove, and - because OpenWriter always
// starts a new segment - the only ones guaranteed never to be reopened for
// appending, which is what makes a preallocated file safe to hand to a process
// that has the feature switched off. SSTables are written once and legitimately
// read by file length, so padding one would change what a reader sees; the WAL
// is not on the commit-ack path.
func (s *OSByteStore) preallocateFor(segmentName string) int64 {
	if s.preallocateBytes <= 0 {
		return 0
	}
	base := segmentName
	if i := strings.LastIndex(segmentName, "/"); i >= 0 {
		base = segmentName[i+1:]
	}
	if _, ok := ParseDeltaSequencedFileName(base); !ok {
		return 0
	}
	return s.preallocateBytes
}

func (s *OSByteStore) openFor(segmentName string) (*openSegment, error) {
	s.mu.Lock()
	seg, ok := s.handles[segmentName]
	s.mu.Unlock()
	if ok {
		return seg, nil
	}

	p := s.pathFor(segmentName)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	// Create and reopen are distinguished with O_EXCL rather than a prior Stat,
	// which would be a race: preallocation must happen exactly once, on the
	// creating call, and the logical size of a segment we just created is 0 no
	// matter how large the file itself now is.
	//
	// Deliberately not O_APPEND either way: writes are positional (see Append).
	// O_APPEND targets the end of the *file*, which stops being the end of the
	// *data* as soon as a segment is preallocated
	// (docs/kdb-segment-preallocation-plan.md §3.1).
	var logicalSize int64
	f, err := fsx.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	switch {
	case err == nil:
		if n := s.preallocateFor(segmentName); n > 0 {
			if perr := preallocateFile(f, n); perr != nil {
				_ = f.Close()
				return nil, perr
			}
		}
		// Created: no data in it yet, whatever its physical size.
		logicalSize = 0
	case os.IsExist(err):
		f, err = fsx.OpenFile(p, os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		info, serr := f.Stat()
		if serr != nil {
			_ = f.Close()
			return nil, serr
		}
		// Reopening an existing segment: its file size is its logical size,
		// because a *preallocated* segment is never reopened for writing -
		// delta.Factory.OpenWriter always starts a new segment rather than
		// resuming one. Were that to change, this line would need to find the
		// logical end by scanning frames instead (plan doc §3.3), since the
		// file size of a preallocated segment says nothing about how much of
		// it is data.
		logicalSize = info.Size()
	default:
		return nil, err
	}

	s.mu.Lock()
	if existing, raced := s.handles[segmentName]; raced {
		s.mu.Unlock()
		_ = f.Close()
		return existing, nil
	}
	seg = &openSegment{file: f, size: logicalSize}
	s.handles[segmentName] = seg
	s.mu.Unlock()
	return seg, nil
}

func (s *OSByteStore) Append(segmentName string, bytes []byte) (int64, error) {
	seg, err := s.openFor(segmentName)
	if err != nil {
		return 0, err
	}
	// Positional write at the segment's logical end, rather than an O_APPEND
	// write at the file's physical end. The two are the same thing today and
	// stop being the same thing under preallocation, where the file is created
	// at its full size up front and O_APPEND would put the first record 64MiB
	// in - growing the file, which is the exact cost preallocation exists to
	// remove (docs/kdb-segment-preallocation-plan.md §3.1).
	//
	// size is advanced only after the write succeeds, so a failed write leaves
	// the next caller pointed at the same offset rather than at a hole - a hole
	// would read back as an invalid frame and silently truncate the segment at
	// that point on the next scan. Callers are serialized per segment by
	// FileBackedPlatformIO (see this type's doc comment), which is what makes
	// load-then-advance safe here.
	offset := atomic.LoadInt64(&seg.size)
	if atomic.LoadInt32(&seg.sealed) != 0 {
		// Written after its seal (a repair, say): no longer something Stamp may vouch for.
		atomic.StoreInt32(&seg.sealed, 0)
	}
	if _, err := seg.file.WriteAt(bytes, offset); err != nil {
		return 0, err
	}
	return atomic.AddInt64(&seg.size, int64(len(bytes))), nil
}

// Size reports a segment's length without reading it: the logical size this process has
// written when it holds the segment open for appending, and otherwise the file's length.
//
// The file length is what Read clamps to, so it is the same answer reading to the end would
// give - including for a preallocated delta segment reopened by a later process, whose padding
// reads back as zeros either way. SSTables, the reason this exists, are never preallocated.
func (s *OSByteStore) Size(segmentName string) (int64, error) {
	s.mu.Lock()
	seg, ok := s.handles[segmentName]
	s.mu.Unlock()
	if ok {
		return atomic.LoadInt64(&seg.size), nil
	}
	info, err := os.Stat(s.pathFor(segmentName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, &PlatformIOError{Message: "segment does not exist", SegmentName: segmentName, Cause: err}
		}
		return 0, err
	}
	return info.Size(), nil
}

// Stamp reports a segment's size and modification time, or ok=false while this process holds it
// open for appending and has not sealed it: such a segment's contents are still changing, so
// nothing may be cached against them.
func (s *OSByteStore) Stamp(segmentName string) (size, modNanos int64, ok bool, err error) {
	s.mu.Lock()
	seg, open := s.handles[segmentName]
	s.mu.Unlock()
	if open && atomic.LoadInt32(&seg.sealed) == 0 {
		return 0, 0, false, nil
	}
	info, err := os.Stat(s.pathFor(segmentName))
	if err == nil && open {
		// The logical size, which is what a scan reads to; a preallocated file is longer.
		return atomic.LoadInt64(&seg.size), info.ModTime().UnixNano(), true, nil
	}
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, &PlatformIOError{Message: "segment does not exist", SegmentName: segmentName, Cause: err}
		}
		return 0, 0, false, err
	}
	return info.Size(), info.ModTime().UnixNano(), true, nil
}

func (s *OSByteStore) Read(segmentName string, offset int64, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	// A cached handle may be closed under us by Delete/ReleaseHandles/Close between lookup and
	// read; one retry opens a fresh one.
	for attempt := 0; ; attempt++ {
		f, cached, err := s.readHandle(segmentName)
		if err != nil {
			return nil, err
		}
		buf, err := readAtClamped(f, offset, length)
		if !cached {
			_ = f.Close()
		}
		if err != nil && cached && attempt == 0 && errors.Is(err, os.ErrClosed) {
			continue
		}
		return buf, err
	}
}

// ReadInto implements segmentStoreIntoReader: a positional read into dst, sharing the cached read
// handle exactly as Read does. Unlike Read it needs no stat - dst's length is the caller's bound,
// and a short read at the end of the file is simply reported as such.
func (s *OSByteStore) ReadInto(segmentName string, offset int64, dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for attempt := 0; ; attempt++ {
		f, cached, err := s.readHandle(segmentName)
		if err != nil {
			return 0, err
		}
		n, err := f.ReadAt(dst, offset)
		if !cached {
			_ = f.Close()
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return n, nil
		}
		if err != nil && cached && attempt == 0 && errors.Is(err, os.ErrClosed) {
			continue
		}
		return n, err
	}
}

// readHandle returns a handle for reading segmentName and whether it is the shared cached one
// (which the caller must not close).
func (s *OSByteStore) readHandle(segmentName string) (*os.File, bool, error) {
	s.mu.Lock()
	if f, ok := s.readers[segmentName]; ok {
		s.mu.Unlock()
		return f, true, nil
	}
	s.mu.Unlock()
	f, err := fsx.Open(s.pathFor(segmentName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, &PlatformIOError{Message: "segment does not exist", SegmentName: segmentName, Cause: err}
		}
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.readers[segmentName]; ok {
		_ = f.Close()
		return existing, true, nil
	}
	if len(s.readers) >= maxReadHandles {
		return f, false, nil
	}
	s.readers[segmentName] = f
	return f, true, nil
}

// dropReader closes and forgets the cached read handle for segmentName.
func (s *OSByteStore) dropReader(segmentName string) {
	s.mu.Lock()
	f := s.readers[segmentName]
	delete(s.readers, segmentName)
	s.mu.Unlock()
	if f != nil {
		_ = f.Close()
	}
}

// statFreeReadBytes is the largest read readAtClamped serves without first stat-ing the file.
const statFreeReadBytes = 64 << 10

// readAtClamped reads up to length bytes at offset with a positional read, so concurrent readers
// share one handle without disturbing each other's position.
func readAtClamped(f *os.File, offset int64, length int) ([]byte, error) {
	if length <= statFreeReadBytes {
		// A small read - a frame header, an SSTable block, a footer - allocates what it asked
		// for and skips the stat: the clamp below exists for huge upper-bound requests, and for
		// these it cost a syscall per read to save at most a few KiB.
		buf := make([]byte, length)
		n, err := f.ReadAt(buf, offset)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n < length/2 {
			// Mostly past the end of the file: don't hand back (and let the caller retain) a
			// buffer mostly made of nothing.
			out := make([]byte, n)
			copy(out, buf[:n])
			return out, nil
		}
		return buf[:n], nil
	}
	// Clamp to what the file actually holds before allocating. Callers that want
	// "the whole segment" pass a length that is an upper bound rather than a real
	// size - delta.DefaultReader.scanSegmentRef passes 1<<28 - and allocating that
	// literally meant a 256MiB make([]byte) per segment scanned, of which the
	// returned buf[:n] then retained the entire backing array for as long as the
	// caller held the result. ListSegments does this once per segment, so opening
	// a namespace with 20 segments transiently reserved ~5GiB.
	if st, err := f.Stat(); err == nil {
		if remaining := st.Size() - offset; remaining <= 0 {
			return []byte{}, nil
		} else if int64(length) > remaining {
			length = int(remaining)
		}
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (s *OSByteStore) Flush(segmentName string, fsync bool) error {
	if !fsync {
		return nil
	}
	s.mu.Lock()
	seg, ok := s.handles[segmentName]
	s.mu.Unlock()
	if ok {
		return syncFile(seg.file, s.syncMode)
	}
	// No open handle yet (e.g. Flush called before any Append in this
	// process): fall back to a one-off open, matching prior behavior.
	//
	// Opened for writing where possible: Windows refuses FlushFileBuffers on a handle without
	// write access, where unix fsyncs a read-only descriptor happily. A file this process cannot
	// write to keeps the read-only open it always had.
	p := s.pathFor(segmentName)
	f, err := fsx.OpenFile(p, os.O_RDWR, 0o644)
	if os.IsPermission(err) {
		f, err = fsx.OpenFile(p, os.O_RDONLY, 0o644)
	}
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	return syncFile(f, s.syncMode)
}

// Close releases all cached file handles. Safe to call multiple times.
func (s *OSByteStore) Close() error {
	s.mu.Lock()
	handles := s.handles
	s.handles = make(map[string]*openSegment)
	readers := s.readers
	s.readers = make(map[string]*os.File)
	s.mu.Unlock()
	var firstErr error
	for _, f := range readers {
		_ = f.Close()
	}
	for _, seg := range handles {
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *OSByteStore) MarkSealed(segmentName string) error {
	// Sealing is advisory: persisted segments are discovered by listing + scanning, and the
	// handle stays open. It is recorded only so Stamp can tell a finished segment from one still
	// being written.
	s.mu.Lock()
	seg, ok := s.handles[segmentName]
	s.mu.Unlock()
	if ok {
		atomic.StoreInt32(&seg.sealed, 1)
	}
	return nil
}

func (s *OSByteStore) List(prefix string) ([]string, error) {
	rootPrefix := s.pathFor(prefix)
	var out []string
	err := filepath.WalkDir(rootPrefix, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		out = append(out, name)
		return nil
	})
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	return out, err
}

func (s *OSByteStore) Delete(segmentName string) error {
	s.dropReader(segmentName)
	s.mu.Lock()
	seg, ok := s.handles[segmentName]
	if ok {
		delete(s.handles, segmentName)
	}
	s.mu.Unlock()
	if ok {
		_ = seg.file.Close()
	}
	p := s.pathFor(segmentName)
	if err := fsx.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// AvailableBytes reports the free space on the filesystem holding the data root.
//
// It used to return a 0 sentinel, which no caller could tell apart from a full disk. Nothing read
// it then; something does now (the control plane asks whether a promotion's copy will fit), and a
// wrong answer to "is there room" is worse than no answer - so where the platform cannot say, this
// returns an error rather than a number.
// Rename implements segmentStoreRenamer. The handles open on from are closed first - a renamed
// file keeps its descriptors on unix but not everywhere, and from is finished with anyway - and the
// directory holding to is synced afterwards so the rename survives a power loss.
func (s *OSByteStore) Rename(from, to string) error {
	s.dropReader(from)
	s.dropReader(to)
	s.mu.Lock()
	seg, ok := s.handles[from]
	delete(s.handles, from)
	s.mu.Unlock()
	if ok {
		if err := seg.file.Close(); err != nil {
			return err
		}
	}
	dst := s.pathFor(to)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("rename target %s already exists", to)
	}
	if err := fsx.Rename(s.pathFor(from), dst); err != nil {
		return err
	}
	_ = fsx.SyncDir(filepath.Dir(dst))
	return nil
}

func (s *OSByteStore) AvailableBytes() (int64, error) {
	return availableBytes(s.root)
}

// SnapFileName is the single flat file name a snapshot key is stored under inside a data root's
// snap/ directory. Both the directory name and this sanitization must match Kotlin's
// JvmSegmentByteStore.snapFile and NativeSegmentByteStore.snapPath: a snapshot written by one
// runtime has to be found by the other.
//
// Two substitutions, and both are load-bearing. The colon, because the raw key -
// SnapshotKeyBuilder.Enlistment produces "kdb:snap:<id>", checkpoints "kdb:checkpoint:<id>" - is
// not a portable path character. The slash, because a namespace id may contain one and the name
// has to stay a single path component: a nested id used to become real directories, so
// "repro/account" wrote the FILE snap/kdb_checkpoint_repro/account while "repro/account/ro"
// needed that same "account" to be a directory, and whichever came second could never be written
// at all. "%2F" is safe to substitute because ValidateNamespaceID allows only [A-Za-z0-9._-] in a
// segment, so no id can produce a "%" of its own and two ids can never flatten to one name.
func SnapFileName(key string) string {
	return strings.ReplaceAll(strings.ReplaceAll(key, ":", "_"), "/", "%2F")
}

// legacySnapPathFor is where a snapshot written before SnapFileName flattened the slash still
// lives. Only ids containing a slash moved; everything else names the same file under both.
func (s *OSByteStore) legacySnapPathFor(key string) string {
	return filepath.Join(s.root, "snap", filepath.FromSlash(strings.ReplaceAll(key, ":", "_")))
}

func (s *OSByteStore) snapPathFor(key string) string {
	return filepath.Join(s.root, "snap", SnapFileName(key))
}

func (s *OSByteStore) ReadSnapshot(key string) ([]byte, error) {
	b, err := os.ReadFile(s.snapPathFor(key))
	if err == nil {
		return b, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	// Written by a build that nested the slash. For a namespace bootstrapped from a peer's
	// snapshot the checkpoint is the only record of its state and open refuses to proceed
	// without it, so failing to look here would not cost a slow start - it would strand the
	// namespace. WriteSnapshot retires the old file once the new one is in place.
	legacy := s.legacySnapPathFor(key)
	if legacy == s.snapPathFor(key) {
		return nil, nil
	}
	b, err = os.ReadFile(legacy)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return b, nil
}

// WriteSnapshot replaces key's snapshot atomically: written to a temporary file, synced, renamed
// over the old one, and the directory synced. A checkpoint used to be a plain overwrite - fine
// while it was only ever a cache the log could rebuild, and not once it can be the only record
// of state (a namespace bootstrapped from a peer's snapshot), where a torn write would lose it.
func (s *OSByteStore) WriteSnapshot(key string, data []byte) error {
	p := s.snapPathFor(key)
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(p)+".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		fsx.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		fsx.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		fsx.Remove(tmp.Name())
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		fsx.Remove(tmp.Name())
		return err
	}
	if err := fsx.Rename(tmp.Name(), p); err != nil {
		fsx.Remove(tmp.Name())
		return err
	}
	_ = fsx.SyncDir(dir)
	// The new name now holds this key's state, so retire any pre-flattening file. Leaving it
	// would be worse than untidy: ReadSnapshot falls back to it whenever the new name is absent,
	// and promotion moves only the new one - so a promoted namespace would fall back to a
	// checkpoint describing a log that is no longer underneath it.
	if legacy := s.legacySnapPathFor(key); legacy != p {
		_ = fsx.Remove(legacy)
	}
	return nil
}

func (s *OSByteStore) DeleteSnapshot(key string) error {
	if legacy := s.legacySnapPathFor(key); legacy != s.snapPathFor(key) {
		if err := fsx.Remove(legacy); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	p := s.snapPathFor(key)
	if err := fsx.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// HandleReleaser is implemented by stores that keep a file handle per segment they have written:
// ReleaseHandles closes and forgets those under prefix. A host sharing one store across many
// namespaces calls it when it closes one, or every namespace ever opened would keep its handles -
// file descriptors and memory - until the whole host closed.
type HandleReleaser interface {
	ReleaseHandles(prefix string) error
}

// ReleaseHandles implements HandleReleaser. A segment appended to again later is reopened.
func (s *OSByteStore) ReleaseHandles(prefix string) error {
	s.mu.Lock()
	var release []*openSegment
	var releaseReaders []*os.File
	for name, f := range s.readers {
		if strings.HasPrefix(name, prefix) {
			releaseReaders = append(releaseReaders, f)
			delete(s.readers, name)
		}
	}
	for name, seg := range s.handles {
		if strings.HasPrefix(name, prefix) {
			release = append(release, seg)
			delete(s.handles, name)
		}
	}
	s.mu.Unlock()
	for _, f := range releaseReaders {
		_ = f.Close()
	}
	var firstErr error
	for _, seg := range release {
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
