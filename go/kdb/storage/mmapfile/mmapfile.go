// Package mmapfile maps immutable files read-only so their bytes live outside the Go heap.
//
// Memory-mapped bytes are not scanned by the garbage collector and are paged by the OS, so a
// structure that is large, immutable and searched by position (a commit graph, an SSTable index)
// costs the GC nothing here, where as a map or a slice of pointers it costs a scan every cycle.
//
// What this package guarantees, and what it does not:
//
//   - Read-only. Nothing in KDB writes through a mapping; durability and ordering stay on
//     write + fsync.
//   - A Mapping is reference counted. The file is unmapped only when the owner has called Close
//     AND every Region taken from it has been Released. Unmapping under a live reader is a
//     segfault, not a recoverable panic, so no code path unmaps on the thread that replaces a
//     file; it drops the owner reference and the last reader's Release does the unmap.
//   - A Region's bytes must not be retained past Release. CopyAt exists so callers can take a
//     heap copy and let go of the mapping.
//   - A truncated or failing file raises SIGBUS on first touch. CopyAt converts that into an
//     error; Bytes does not, so a caller that indexes Bytes directly must treat the file as
//     trusted (it was written and fsynced by this process and is never truncated in place).
//
// A mapping is always a cache of something else. Callers fall back to the heap path on any error,
// including ErrUnsupported on platforms without mmap.
package mmapfile

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// mappedBytes is the total size of every live mapping in the process. Mapped pages are page
// cache, not Go heap, so GOMEMLIMIT does not see them. This is the whole-file figure; see
// ResidentBytes for how much of it is in memory right now.
var mappedBytes atomic.Int64

// MappedBytes reports the bytes currently mapped by this process through this package: the
// address space the mappings span, whether or not their pages are in memory.
func MappedBytes() int64 { return mappedBytes.Load() }

// live is every mapping that has not been unmapped yet, so ResidentBytes can walk them.
var (
	liveMu sync.Mutex
	live   = map[*Mapping]struct{}{}
)

func register(m *Mapping) {
	liveMu.Lock()
	live[m] = struct{}{}
	liveMu.Unlock()
}

func unregister(m *Mapping) {
	liveMu.Lock()
	delete(live, m)
	liveMu.Unlock()
}

// residentCacheTTL is the shortest time a ResidentBytes result is reused. The memory guard
// samples every 200ms; residency moves on the scale of the kernel's reclaim, not of one sample,
// and a fresh count walks every page of every mapping.
const residentCacheTTL = time.Second

// residentCostFactor stretches the cache interval to this many times the last count's cost, so
// counting never takes more than about 2% of a core. Measured over a 4 GiB mapping: about 5ms on
// linux/arm64 (the 1s floor applies), about 85ms on darwin/arm64, whose mincore costs ~300ns a
// page (the interval becomes ~4s).
const residentCostFactor = 50

var resident struct {
	mu    sync.Mutex
	at    time.Time
	ttl   time.Duration
	bytes int64
	vec   []byte // mincore output buffer, reused across calls
}

// ResidentBytes reports how many bytes of the live mappings are in memory now: the pages the OS
// would have to give up, or keep, rather than the address space they span. Clean mapped pages
// are reclaimable page cache, so a large mapping read only in part costs only that part. A figure
// for reporting: the memory guard does not charge mapped pages at all, since every KDB mapping is
// read-only and so always clean and reclaimable (server.currentMemoryUsageBytes).
//
// Counted with mincore(2), which reports residency in the page cache, so a page counts whether
// this process or another brought it in. Where mincore is unavailable the full mapped size is
// returned instead. The result is cached for a second, or longer where counting is slow (see
// residentCostFactor), and is never reported above MappedBytes.
func ResidentBytes() int64 {
	resident.mu.Lock()
	defer resident.mu.Unlock()
	if now := time.Now(); resident.at.IsZero() || now.Sub(resident.at) >= resident.ttl {
		resident.bytes = residentBytesNow()
		done := time.Now()
		resident.at = done
		resident.ttl = max(residentCacheTTL, residentCostFactor*done.Sub(now))
	}
	return min(resident.bytes, MappedBytes())
}

// ResidentBytesUncached is ResidentBytes counted now, bypassing its cache: for measurement
// harnesses that sample residency at a known moment. It costs a full mincore pass every call, so
// nothing on a hot path should use it.
func ResidentBytesUncached() int64 {
	resident.mu.Lock()
	defer resident.mu.Unlock()
	return min(residentBytesNow(), MappedBytes())
}

// residentBytesNow counts resident bytes across every live mapping, uncached. The caller holds
// resident.mu, which owns the mincore buffer.
func residentBytesNow() int64 {
	liveMu.Lock()
	ms := make([]*Mapping, 0, len(live))
	for m := range live {
		ms = append(ms, m)
	}
	liveMu.Unlock()

	var total int64
	for _, m := range ms {
		// Pin the mapping the way a Region does, so a concurrent Close + Release cannot unmap it
		// under mincore; the last unpin unmaps instead. Holding m.mu across the count would stall
		// readers' Acquire for its duration.
		m.mu.Lock()
		if m.done || len(m.data) == 0 {
			m.mu.Unlock()
			continue
		}
		m.refs++
		data := m.data
		m.mu.Unlock()

		total += residentOf(data, &resident.vec)

		m.mu.Lock()
		m.refs--
		_ = m.maybeUnmapLocked()
		m.mu.Unlock()
	}
	return total
}

// ErrUnsupported is returned by Open on platforms where file mapping is not available.
var ErrUnsupported = errors.New("mmapfile: not supported on this platform")

// ErrClosed is returned by Acquire once the owner has closed the mapping.
var ErrClosed = errors.New("mmapfile: mapping closed")

// Mapping is a read-only mapped file.
type Mapping struct {
	mu     sync.Mutex
	data   []byte
	unmap  func() error
	refs   int // outstanding Regions
	closed bool
	done   bool
}

// Size is the length of the mapped file in bytes.
func (m *Mapping) Size() int { return len(m.data) }

// Acquire takes a reference for a reader. The caller must Release it. Acquire fails with
// ErrClosed once the owner has closed the mapping, which is how a reader that raced a rebuild
// learns to reopen instead of touching freed pages.
func (m *Mapping) Acquire() (*Region, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	m.refs++
	return &Region{m: m}, nil
}

// Close drops the owner's reference. The file is unmapped now if no reader holds a Region, and
// otherwise when the last one is released. Closing twice is a no-op.
func (m *Mapping) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	err := m.maybeUnmapLocked()
	m.mu.Unlock()
	return err
}

func (m *Mapping) maybeUnmapLocked() error {
	if m.done || !m.closed || m.refs > 0 {
		return nil
	}
	m.done = true
	data := m.data
	m.data = nil
	mappedBytes.Add(-int64(len(data)))
	unregister(m)
	if m.unmap != nil && data != nil {
		return m.unmap()
	}
	return nil
}

// Region is one reader's hold on a Mapping.
type Region struct {
	m        *Mapping
	released bool
}

// Bytes is the mapped file. Valid until Release; never retain it past that.
func (r *Region) Bytes() []byte { return r.m.data }

// CopyAt returns a heap copy of n bytes at off, converting a fault on the mapped pages
// (truncated file, I/O error) into an error instead of crashing the process.
func (r *Region) CopyAt(off, n int) (out []byte, err error) {
	data := r.m.data
	if off < 0 || n < 0 || off > len(data) || n > len(data)-off {
		return nil, fmt.Errorf("mmapfile: range [%d,%d) outside %d-byte mapping", off, off+n, len(data))
	}
	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)
	defer func() {
		if rec := recover(); rec != nil {
			out, err = nil, fmt.Errorf("mmapfile: fault reading mapped file: %v", rec)
		}
	}()
	out = make([]byte, n)
	copy(out, data[off:off+n])
	return out, nil
}

// Guard runs fn over the mapped bytes, converting a fault on the mapped pages into an error the
// way CopyAt does, without copying. fn must not retain the slice.
func (r *Region) Guard(fn func([]byte)) (err error) {
	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("mmapfile: fault reading mapped file: %v", rec)
		}
	}()
	fn(r.m.data)
	return nil
}

// Release returns the reference. Safe to call more than once.
func (r *Region) Release() {
	if r.released {
		return
	}
	r.released = true
	m := r.m
	m.mu.Lock()
	m.refs--
	_ = m.maybeUnmapLocked()
	m.mu.Unlock()
}
