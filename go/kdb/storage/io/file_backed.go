package io

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
)

// FileBackedPlatformIO shares segment logic; platform code supplies SegmentByteStore.
type FileBackedPlatformIO struct {
	config PlatformIOConfig
	store  SegmentByteStore

	globalMu       sync.Mutex
	segmentMu      map[string]*sync.Mutex
	sealedSegments map[string]struct{}
}

// NewFileBackedPlatformIO wraps a SegmentByteStore with shared locking.
func NewFileBackedPlatformIO(config PlatformIOConfig, store SegmentByteStore) *FileBackedPlatformIO {
	return &FileBackedPlatformIO{
		config:         config,
		store:          store,
		segmentMu:      make(map[string]*sync.Mutex),
		sealedSegments: make(map[string]struct{}),
	}
}

func (f *FileBackedPlatformIO) mutexFor(segmentName string) *sync.Mutex {
	f.globalMu.Lock()
	defer f.globalMu.Unlock()
	m, ok := f.segmentMu[segmentName]
	if !ok {
		m = &sync.Mutex{}
		f.segmentMu[segmentName] = m
	}
	return m
}

func (f *FileBackedPlatformIO) AppendToSegment(segmentName string, bytes []byte) (int64, error) {
	if err := ValidateSegmentName(segmentName); err != nil {
		return 0, err
	}
	max := f.config.MaxAppendBytes
	if max <= 0 {
		max = DefaultPlatformIOConfig().MaxAppendBytes
	}
	if len(bytes) > max {
		return 0, &PlatformIOError{
			Message:     "append size exceeds max",
			SegmentName: segmentName,
		}
	}
	mu := f.mutexFor(segmentName)
	mu.Lock()
	defer mu.Unlock()
	f.globalMu.Lock()
	_, sealed := f.sealedSegments[segmentName]
	f.globalMu.Unlock()
	if sealed {
		return 0, &PlatformIOError{Message: "segment sealed", SegmentName: segmentName}
	}
	size, err := f.store.Append(segmentName, bytes)
	if err != nil {
		return 0, &PlatformIOError{Message: "append failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return size, nil
}

func (f *FileBackedPlatformIO) ReadFromSegment(segmentName string, offset int64, length int) ([]byte, error) {
	if err := ValidateSegmentName(segmentName); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, &PlatformIOError{Message: "negative offset or length", SegmentName: segmentName}
	}
	mu := f.mutexFor(segmentName)
	mu.Lock()
	defer mu.Unlock()
	b, err := f.store.Read(segmentName, offset, length)
	if err != nil {
		if _, ok := err.(*PlatformIOError); ok {
			return nil, err
		}
		return nil, &PlatformIOError{Message: "read failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return b, nil
}

// FlushSegment deliberately does NOT take mutexFor(segmentName): that
// mutex also guards AppendToSegment, and group commit relies on new
// appends being able to proceed (and register with the GroupCommitter)
// *while* a physical fsync is in flight. Sharing the lock here would
// serialize every writer behind each fsync's full duration, silently
// defeating batching - found by benchmarking the Kotlin side, where a
// slower physical fsync exposed it clearly (see Phase 1 notes in
// docs/benchmarks/phase0-baseline.md). Safe because SegmentByteStore
// implementations must tolerate Flush running concurrently with writes
// to the same segment (true of os.File.Sync and Go's O_APPEND writes).
func (f *FileBackedPlatformIO) FlushSegment(segmentName string) error {
	if err := ValidateSegmentName(segmentName); err != nil {
		return err
	}
	if err := f.store.Flush(segmentName, f.config.FsyncOnFlush); err != nil {
		return &PlatformIOError{Message: "flush failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return nil
}

func (f *FileBackedPlatformIO) SealSegment(segmentName string) error {
	if err := ValidateSegmentName(segmentName); err != nil {
		return err
	}
	mu := f.mutexFor(segmentName)
	mu.Lock()
	defer mu.Unlock()
	f.globalMu.Lock()
	if _, ok := f.sealedSegments[segmentName]; ok {
		f.globalMu.Unlock()
		return nil
	}
	f.globalMu.Unlock()
	if err := f.store.Flush(segmentName, f.config.FsyncOnFlush); err != nil {
		return &PlatformIOError{Message: "seal flush failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	if err := f.store.MarkSealed(segmentName); err != nil {
		return &PlatformIOError{Message: "seal failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	f.globalMu.Lock()
	f.sealedSegments[segmentName] = struct{}{}
	f.globalMu.Unlock()
	return nil
}

func (f *FileBackedPlatformIO) ListSegments(namespaceID string) ([]string, error) {
	prefix := SegmentNameBuilder.NamespacePrefix(namespaceID)
	names, err := f.store.List(prefix)
	if err != nil {
		return nil, err
	}
	// sorted copy
	out := append([]string(nil), names...)
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func (f *FileBackedPlatformIO) DeleteSegment(segmentName string) error {
	if err := ValidateSegmentName(segmentName); err != nil {
		return err
	}
	mu := f.mutexFor(segmentName)
	mu.Lock()
	defer mu.Unlock()
	f.globalMu.Lock()
	delete(f.sealedSegments, segmentName)
	delete(f.segmentMu, segmentName)
	f.globalMu.Unlock()
	return f.store.Delete(segmentName)
}

func (f *FileBackedPlatformIO) AvailableBytes() (int64, error) {
	return f.store.AvailableBytes()
}

func (f *FileBackedPlatformIO) ReadSnapshot(key string) ([]byte, error) {
	return f.store.ReadSnapshot(key)
}

func (f *FileBackedPlatformIO) WriteSnapshot(key string, data []byte) error {
	return f.store.WriteSnapshot(key, data)
}

func (f *FileBackedPlatformIO) DeleteSnapshot(key string) error {
	return f.store.DeleteSnapshot(key)
}

var _ storage.PlatformIOShim = (*FileBackedPlatformIO)(nil)

// FileBackedPlatformIOFactory opens file-backed shims (skeleton: requires SegmentByteStore injection).
type FileBackedPlatformIOFactory struct {
	NewStore func(config PlatformIOConfig) (SegmentByteStore, error)
}

// Open creates a FileBackedPlatformIO when NewStore is configured.
func (f *FileBackedPlatformIOFactory) Open(config PlatformIOConfig) (storage.PlatformIOShim, error) {
	if f.NewStore == nil {
		return nil, &PlatformIOError{Message: "file-backed I/O not wired: set NewStore on factory"}
	}
	store, err := f.NewStore(config)
	if err != nil {
		return nil, err
	}
	return NewFileBackedPlatformIO(config, store), nil
}
