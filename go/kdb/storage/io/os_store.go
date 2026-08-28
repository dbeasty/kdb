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
	root     string
	syncMode SyncMode

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
		root:     *config.RootDirectory,
		syncMode: config.SyncMode,
		handles:  make(map[string]*openSegment),
	}, nil
}

func (s *OSByteStore) pathFor(segmentName string) string {
	return filepath.Join(s.root, filepath.FromSlash(segmentName))
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
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	s.mu.Lock()
	if existing, raced := s.handles[segmentName]; raced {
		s.mu.Unlock()
		_ = f.Close()
		return existing, nil
	}
	seg = &openSegment{file: f, size: info.Size()}
	s.handles[segmentName] = seg
	s.mu.Unlock()
	return seg, nil
}

func (s *OSByteStore) Append(segmentName string, bytes []byte) (int64, error) {
	seg, err := s.openFor(segmentName)
	if err != nil {
		return 0, err
	}
	if _, err := seg.file.Write(bytes); err != nil {
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
			return nil, nil
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

func (s *OSByteStore) AvailableBytes() (int64, error) {
	// v1: not used for correctness; return sentinel.
	return 0, nil
}

func (s *OSByteStore) ReadSnapshot(key string) ([]byte, error) {
	p := filepath.Join(s.root, "snapshots", key)
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
	p := filepath.Join(s.root, "snapshots", key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (s *OSByteStore) DeleteSnapshot(key string) error {
	p := filepath.Join(s.root, "snapshots", key)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
