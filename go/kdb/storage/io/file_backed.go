package io

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
)

// FileBackedPlatformIO shares segment logic; platform code supplies SegmentByteStore.
type FileBackedPlatformIO struct {
	config PlatformIOConfig
	store  SegmentByteStore

	globalMu sync.Mutex
	// segmentMu holds one lock per segment. Appends, seals, restores and deletes take it
	// exclusively; reads (and size/stamp queries) share it. A read must not see an append
	// half-applied - a positional read racing a write can return the front of a frame without
	// its tail - but two reads never disturb each other: OSByteStore reads with pread on a shared
	// handle (ReadAt), which needs no lock and keeps no file position. Reads used to take the
	// lock exclusively too, so concurrent cold reads of one segment queued behind each other for
	// the length of every disk read.
	segmentMu      map[string]*sync.RWMutex
	sealedSegments map[string]struct{}
	// generations names each segment's current incarnation (storage.SegmentGenerationer). Assigned
	// on first ask and forgotten when the segment is deleted, restored or its namespace released,
	// so whatever carries the name afterwards is asked about under a new number.
	generations map[string]uint64
}

// HasArchive reports whether the store beneath this shim keeps a copy of what it deletes, which
// is what decides whether reclaiming a segment is eviction or destruction. Forwarded rather than
// answered here: this shim adds locking, not replication, and only the store below knows what
// sinks it has.
func (f *FileBackedPlatformIO) HasArchive() bool {
	type archiveAware interface{ HasArchive() bool }
	if a, ok := f.store.(archiveAware); ok {
		return a.HasArchive()
	}
	return false
}

// RestoreSegment fetches a segment back from the archive beneath this shim and writes it to the
// primary. Forwarded for the same reason HasArchive is: the store below owns the sinks.
//
// Takes this shim's per-segment lock, because a restore writes the segment file and anything
// reading it concurrently must not see a half-written one.
func (f *FileBackedPlatformIO) RestoreSegment(segmentName string) error {
	type archiveAware interface{ RestoreSegment(string) error }
	store, ok := f.store.(archiveAware)
	if !ok {
		return fmt.Errorf("this storage has no archive to restore %s from", segmentName)
	}
	mu := f.mutexFor(segmentName)
	mu.Lock()
	defer mu.Unlock()
	if err := store.RestoreSegment(segmentName); err != nil {
		return err
	}
	// A restored segment is sealed by definition - it was sealed before it was evicted, and
	// nothing appends to it again. Recording that keeps a later writer from treating it as the
	// open segment and appending into history.
	f.globalMu.Lock()
	f.sealedSegments[segmentName] = struct{}{}
	delete(f.generations, segmentName)
	f.globalMu.Unlock()
	return nil
}

// NewFileBackedPlatformIO wraps a SegmentByteStore with shared locking.
func NewFileBackedPlatformIO(config PlatformIOConfig, store SegmentByteStore) *FileBackedPlatformIO {
	return &FileBackedPlatformIO{
		config:         config,
		store:          store,
		segmentMu:      make(map[string]*sync.RWMutex),
		sealedSegments: make(map[string]struct{}),
		generations:    make(map[string]uint64),
	}
}

func (f *FileBackedPlatformIO) mutexFor(segmentName string) *sync.RWMutex {
	f.globalMu.Lock()
	defer f.globalMu.Unlock()
	m, ok := f.segmentMu[segmentName]
	if !ok {
		m = &sync.RWMutex{}
		f.segmentMu[segmentName] = m
	}
	return m
}

// MaxAppendBytes is the largest single append this shim will accept. Writers that build a
// payload whose size grows with the data - an SSTable's index, most obviously - must split it
// into pieces no larger than this rather than assume one call is enough.
//
// Reported through an optional interface rather than added to storage.PlatformIOShim, because a
// shim with no ceiling (the in-memory one) has nothing to say here and should not be made to
// answer.
func (f *FileBackedPlatformIO) MaxAppendBytes() int {
	if f.config.MaxAppendBytes > 0 {
		return f.config.MaxAppendBytes
	}
	return DefaultPlatformIOConfig().MaxAppendBytes
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
	mu.RLock()
	defer mu.RUnlock()
	b, err := f.store.Read(segmentName, offset, length)
	if err != nil {
		if _, ok := err.(*PlatformIOError); ok {
			return nil, err
		}
		return nil, &PlatformIOError{Message: "read failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return b, nil
}

// ReadFromSegmentInto implements storage.SegmentIntoReader under the same shared lock as
// ReadFromSegment. A store with no positional read of its own is served by Read and a copy.
func (f *FileBackedPlatformIO) ReadFromSegmentInto(segmentName string, offset int64, dst []byte) (int, error) {
	if err := ValidateSegmentName(segmentName); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, &PlatformIOError{Message: "negative offset or length", SegmentName: segmentName}
	}
	mu := f.mutexFor(segmentName)
	mu.RLock()
	defer mu.RUnlock()
	n, err := readIntoStore(f.store, segmentName, offset, dst)
	if err != nil {
		if _, ok := err.(*PlatformIOError); ok {
			return 0, err
		}
		return 0, &PlatformIOError{Message: "read failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return n, nil
}

var _ storage.SegmentIntoReader = (*FileBackedPlatformIO)(nil)

// readIntoStore reads into dst through store's positional read when it has one, and through Read
// and a copy when it does not.
func readIntoStore(store SegmentByteStore, segmentName string, offset int64, dst []byte) (int, error) {
	if r, ok := store.(segmentStoreIntoReader); ok {
		return r.ReadInto(segmentName, offset, dst)
	}
	b, err := store.Read(segmentName, offset, len(dst))
	if err != nil {
		return 0, err
	}
	return copy(dst, b), nil
}

// SegmentSize implements storage.SegmentSizer by asking the store beneath, under the same shared
// per-segment lock ReadFromSegment takes so it cannot observe an append half-applied.
func (f *FileBackedPlatformIO) SegmentSize(segmentName string) (int64, error) {
	if err := ValidateSegmentName(segmentName); err != nil {
		return 0, err
	}
	sz, ok := f.store.(segmentStoreSizer)
	if !ok {
		return 0, storage.ErrSegmentSizeUnknown
	}
	mu := f.mutexFor(segmentName)
	mu.RLock()
	defer mu.RUnlock()
	n, err := sz.Size(segmentName)
	if err != nil {
		if errors.Is(err, storage.ErrSegmentSizeUnknown) {
			return 0, err
		}
		if _, ok := err.(*PlatformIOError); ok {
			return 0, err
		}
		return 0, &PlatformIOError{Message: "size failed: " + err.Error(), SegmentName: segmentName, Cause: err}
	}
	return n, nil
}

var _ storage.SegmentSizer = (*FileBackedPlatformIO)(nil)

// SegmentStamp implements storage.SegmentStamper by asking the store beneath, under the same
// per-segment lock as SegmentSize. ok is false when the store cannot vouch for the segment.
func (f *FileBackedPlatformIO) SegmentStamp(segmentName string) (storage.SegmentStamp, bool, error) {
	if err := ValidateSegmentName(segmentName); err != nil {
		return storage.SegmentStamp{}, false, err
	}
	st, has := f.store.(segmentStoreStamper)
	if !has {
		return storage.SegmentStamp{}, false, nil
	}
	mu := f.mutexFor(segmentName)
	mu.RLock()
	defer mu.RUnlock()
	size, mod, ok, err := st.Stamp(segmentName)
	if err != nil || !ok {
		return storage.SegmentStamp{}, false, err
	}
	return storage.SegmentStamp{Size: size, ModTimeNanos: mod}, true, nil
}

var _ storage.SegmentStamper = (*FileBackedPlatformIO)(nil)

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
	delete(f.generations, segmentName)
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

// SegmentGeneration implements storage.SegmentGenerationer.
func (f *FileBackedPlatformIO) SegmentGeneration(segmentName string) (uint64, bool) {
	f.globalMu.Lock()
	defer f.globalMu.Unlock()
	g, ok := f.generations[segmentName]
	if !ok {
		g = storage.NextSegmentGeneration()
		f.generations[segmentName] = g
	}
	return g, true
}

var _ storage.SegmentGenerationer = (*FileBackedPlatformIO)(nil)

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

// ReleaseHandles forgets the per-segment state kept for segments under prefix and releases the
// store's handles for them - for a namespace that has closed (see HandleReleaser).
func (f *FileBackedPlatformIO) ReleaseHandles(prefix string) error {
	f.globalMu.Lock()
	for name := range f.segmentMu {
		if strings.HasPrefix(name, prefix) {
			delete(f.segmentMu, name)
		}
	}
	for name := range f.sealedSegments {
		if strings.HasPrefix(name, prefix) {
			delete(f.sealedSegments, name)
		}
	}
	// A namespace that closed may be changed by another process before it opens again, so it
	// comes back under new generations and nothing cached about it before is asked for again.
	for name := range f.generations {
		if strings.HasPrefix(name, prefix) {
			delete(f.generations, name)
		}
	}
	f.globalMu.Unlock()
	if r, ok := f.store.(HandleReleaser); ok {
		return r.ReleaseHandles(prefix)
	}
	return nil
}
