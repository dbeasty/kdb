package io

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
}

type openSegment struct {
	file *os.File
	size int64 // atomic
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
	}, nil
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
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
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
		f, err = os.OpenFile(p, os.O_WRONLY, 0o644)
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
	if _, err := seg.file.WriteAt(bytes, offset); err != nil {
		return 0, err
	}
	return atomic.AddInt64(&seg.size, int64(len(bytes))), nil
}

func (s *OSByteStore) Read(segmentName string, offset int64, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	p := s.pathFor(segmentName)
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &PlatformIOError{Message: "segment does not exist", SegmentName: segmentName, Cause: err}
		}
		return nil, err
	}
	defer f.Close()
	if offset != 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
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
	n, err := io.ReadFull(f, buf)
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
	p := s.pathFor(segmentName)
	f, err := os.OpenFile(p, os.O_RDONLY, 0o644)
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
	s.mu.Unlock()
	var firstErr error
	for _, seg := range handles {
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *OSByteStore) MarkSealed(segmentName string) error {
	// v1: sealing is advisory; persisted segments are discovered by listing + scanning.
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
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
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
func (s *OSByteStore) AvailableBytes() (int64, error) {
	return availableBytes(s.root)
}

// snapPathFor is where an enlistment snapshot lives on disk. Both the directory name and the
// key sanitization must match Kotlin's JvmSegmentByteStore.snapFile: this used to write to
// "snapshots/" with the raw key, so (a) a snapshot written by one runtime was invisible to the
// other, and (b) the raw key - SnapshotKeyBuilder.Enlistment produces "kdb:snap:<id>" - put a
// colon in the file name, which is not a portable path character.
func (s *OSByteStore) snapPathFor(key string) string {
	safeKey := strings.ReplaceAll(key, ":", "_")
	return filepath.Join(s.root, "snap", filepath.FromSlash(safeKey))
}

func (s *OSByteStore) ReadSnapshot(key string) ([]byte, error) {
	p := s.snapPathFor(key)
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return b, nil
}

func (s *OSByteStore) WriteSnapshot(key string, data []byte) error {
	p := s.snapPathFor(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (s *OSByteStore) DeleteSnapshot(key string) error {
	p := s.snapPathFor(key)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
