package io

import (
	"fmt"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
)

// InMemoryPlatformIO is a pure in-memory PlatformIOShim for tests.
//
// It stands in for FileBackedPlatformIO throughout the test suites, which only works if it
// behaves the same way: a double that is more permissive than the real thing lets a bug pass
// every test and fail in production. See platform_io_conformance_test.go, which runs one suite
// against both.
type InMemoryPlatformIO struct {
	mu        sync.Mutex
	segments  map[string][]byte
	sealed    map[string]struct{}
	snapshots map[string][]byte
}

// NewInMemoryPlatformIO returns an empty in-memory shim.
func NewInMemoryPlatformIO() *InMemoryPlatformIO {
	return &InMemoryPlatformIO{
		segments:  make(map[string][]byte),
		sealed:    make(map[string]struct{}),
		snapshots: make(map[string][]byte),
	}
}

func (s *InMemoryPlatformIO) AppendToSegment(segmentName string, bytes []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Sealing is enforced here as it is on disk. It used to be a no-op, so a write to a sealed
	// segment - a real bug, and the kind sealing exists to catch - succeeded against this shim
	// and failed only against the file-backed one.
	if _, ok := s.sealed[segmentName]; ok {
		return 0, &PlatformIOError{Message: "segment sealed", SegmentName: segmentName}
	}
	cur := s.segments[segmentName]
	next := make([]byte, len(cur)+len(bytes))
	copy(next, cur)
	copy(next[len(cur):], bytes)
	s.segments[segmentName] = next
	return int64(len(next)), nil
}

func (s *InMemoryPlatformIO) ReadFromSegment(segmentName string, offset int64, length int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	full, ok := s.segments[segmentName]
	if !ok {
		return nil, fmt.Errorf("unknown segment %s", segmentName)
	}
	if offset < 0 || int(offset) > len(full) {
		return nil, fmt.Errorf("offset out of range")
	}
	off := int(offset)
	if length < 0 {
		return nil, fmt.Errorf("negative length")
	}
	safeLen := length
	if off+safeLen > len(full) {
		safeLen = len(full) - off
	}
	if safeLen <= 0 {
		return []byte{}, nil
	}
	out := make([]byte, safeLen)
	copy(out, full[off:off+safeLen])
	return out, nil
}

func (s *InMemoryPlatformIO) FlushSegment(string) error { return nil }

func (s *InMemoryPlatformIO) SealSegment(segmentName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed[segmentName] = struct{}{}
	return nil
}

func (s *InMemoryPlatformIO) ListSegments(namespaceID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := SegmentNameBuilder.NamespacePrefix(namespaceID)
	var out []string
	for name := range s.segments {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			out = append(out, name)
		}
	}
	// Sorted, like the file-backed shim and as the names are designed for: segment file names
	// are zero-padded so that lexicographic order is sequence order (see SegmentNameBuilder),
	// and readers rely on it - delta replay identifies the *most recently written* segment as
	// the last one, and only tolerates a torn tail there. Ranging a map gave an arbitrary
	// order, so against this shim a corrupt middle segment could land last and be forgiven,
	// while a genuine torn tail could land mid-list and be treated as fatal.
	sort.Strings(out)
	return out, nil
}

func (s *InMemoryPlatformIO) DeleteSegment(segmentName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.segments, segmentName)
	delete(s.sealed, segmentName)
	return nil
}

func (s *InMemoryPlatformIO) AvailableBytes() (int64, error) {
	return 1 << 62, nil
}

func (s *InMemoryPlatformIO) ReadSnapshot(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.snapshots[key]
	if b == nil {
		return nil, nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (s *InMemoryPlatformIO) WriteSnapshot(key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[key] = append([]byte(nil), data...)
	return nil
}

func (s *InMemoryPlatformIO) DeleteSnapshot(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snapshots, key)
	return nil
}

var _ storage.PlatformIOShim = (*InMemoryPlatformIO)(nil)
