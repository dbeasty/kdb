package sstable

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// How LsmBlobStore uses sidecars (sidecar.go).
//
// Each table gets at most one sidecarSlot, made on the table's first probe and kept until the
// table is retired. A slot holds the table's opened sidecar, or nil when the table has none that
// can be used - the shim keeps no local files, the platform cannot map, the file is damaged and
// could not be rebuilt - in which case the table is served from its footer index through
// tableIndexCache, exactly as before sidecars existed. A table with a sidecar never has its
// footer parsed into the heap.
//
// Lifetime is the part that has to be right: unmapping a sidecar under a reader is a segfault,
// not an error. A sidecar is closed only when its table is retired - deleted after a compaction
// swapped it out, or the store closed - and both happen only once no lookup holds the table
// (pin/release). A lookup searches a sidecar only for a table it has pinned, so no lookup can be
// inside one when it closes, and the mapping needs no per-probe reference count.
type sidecarSlot struct {
	loaded atomic.Bool // sc is set; written once, before loaded
	done   chan struct{}
	sc     *sidecar
	// bad is set when a search faulted (the file was truncated or failed under the mapping). The
	// table falls back to its footer from then on; the mapping stays until the table is retired,
	// since a concurrent lookup may still be inside it.
	bad atomic.Bool
}

func (s *LsmBlobStore) sidecarsOn() bool { return s.sidecarPaths != nil }

// SetSidecarBuild says whether the store may write a sidecar for a table that lacks one (or has
// one it cannot use). A read-only opener turns it off: it may use the sidecars the writer left,
// but must not write into a directory another process owns.
func (s *LsmBlobStore) SetSidecarBuild(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sidecarBuild = on
}

// sidecarFor returns h's sidecar, opening (and, where allowed, building) it on the first probe,
// or nil when h is served from its footer. The caller must hold h pinned.
func (s *LsmBlobStore) sidecarFor(h Handle) *sidecar {
	if !s.sidecarsOn() {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	slot, ok := s.sidecars[h.SegmentName]
	if !ok {
		slot = &sidecarSlot{done: make(chan struct{})}
		s.sidecars[h.SegmentName] = slot
		build := s.sidecarBuild
		s.mu.Unlock()
		// Outside the lock: opening verifies the whole file, and building parses the footer.
		// Lookups of the same table wait on done rather than doing either twice.
		slot.sc = s.loadSidecar(h, build)
		slot.loaded.Store(true)
		close(slot.done)
	} else {
		s.mu.Unlock()
		if !slot.loaded.Load() {
			<-slot.done
		}
	}
	if slot.sc == nil || slot.bad.Load() {
		return nil
	}
	return slot.sc
}

// loadSidecar opens h's sidecar, or builds it from the footer when it is missing or unusable and
// build is set. nil when the table must be served from its footer.
func (s *LsmBlobStore) loadSidecar(h Handle, build bool) *sidecar {
	path, ok := sidecarPath(s.sidecarPaths, h.SegmentName)
	if !ok {
		return nil
	}
	r := NewDefaultReader(s.io, h)
	size, fileHash, err := r.identity()
	if err != nil {
		return nil
	}
	if sc, err := openSidecar(path, size, fileHash); err == nil {
		return sc
	}
	if !build {
		return nil
	}
	// Missing, damaged, from another format version, or for some other table: rebuild it from the
	// footer, which is what it is derived from. One pass, once per table ever - the result
	// persists - and the transient cost is 64 B per entry plus one footer chunk, not the footer
	// text and a map of it.
	entries, err := r.indexEntries(size)
	if err != nil {
		return nil
	}
	if err := writeSidecarFile(path, entries, size, fileHash); err != nil {
		return nil
	}
	sc, err := openSidecar(path, size, fileHash)
	if err != nil {
		return nil
	}
	return sc
}

// probe reports what table h says about key, from its sidecar when it has one and its footer
// index otherwise. The caller must hold h pinned.
func (s *LsmBlobStore) probe(h Handle, key *codec.Hash) (BlockHandle, bool, error) {
	if sc := s.sidecarFor(h); sc != nil {
		bh, ok, err := sc.find(key)
		if err == nil {
			return bh, ok, nil
		}
		s.markSidecarBad(h.SegmentName)
	}
	index, err := s.indexFor(h)
	if err != nil {
		return BlockHandle{}, false, err
	}
	bh, ok := index[*key]
	return bh, ok, nil
}

func (s *LsmBlobStore) markSidecarBad(segment string) {
	s.mu.Lock()
	slot := s.sidecars[segment]
	s.mu.Unlock()
	if slot != nil {
		slot.bad.Store(true)
	}
}

// takeSidecarLocked removes segment's slot, for a table being retired. Callers hold s.mu and must
// pass the result to closeSidecars once they have released it.
func (s *LsmBlobStore) takeSidecarLocked(segment string) *sidecarSlot {
	slot, ok := s.sidecars[segment]
	if !ok {
		return nil
	}
	delete(s.sidecars, segment)
	return slot
}

// closeSidecars unmaps the sidecars of retired tables. No lookup can be searching one (see
// sidecarSlot); a slot still loading cannot be retired either, since its loader holds the table
// pinned.
func closeSidecars(slots []*sidecarSlot) {
	for _, slot := range slots {
		if slot != nil && slot.sc != nil {
			slot.sc.close()
		}
	}
}

// removeSidecarFile deletes the sidecar of a table that has been deleted. A failure leaves a stray
// file, which RemoveOrphanSidecars clears on a later open.
func (s *LsmBlobStore) removeSidecarFile(segment string) {
	if path, ok := sidecarPath(s.sidecarPaths, segment); ok {
		_ = os.Remove(path)
	}
}

// SidecarMappedBytes reports the bytes of sidecar files this store has mapped: their whole size,
// not how much is in memory. They are page cache, not heap: the OS pages them in on use and may
// drop them under pressure. Off a cgroup the memory guard charges only their resident pages
// (mmapfile.ResidentBytes, process-wide, alongside the commit graph's and the tree's mappings).
func (s *LsmBlobStore) SidecarMappedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, slot := range s.sidecars {
		if slot.loaded.Load() && slot.sc != nil {
			n += slot.sc.bytes
		}
	}
	return n
}

// Close unmaps every sidecar the store has open. A table some lookup still holds keeps its
// sidecar until that lookup releases it. Lookups after Close are still answered, from the
// footers. Closing twice is a no-op.
func (s *LsmBlobStore) Close() error {
	s.mu.Lock()
	s.closed = true
	var slots []*sidecarSlot
	for name := range s.sidecars {
		if s.readers[name] == 0 {
			slots = append(slots, s.takeSidecarLocked(name))
		}
	}
	s.mu.Unlock()
	closeSidecars(slots)
	return nil
}

// RemoveOrphanSidecars deletes sidecar files with no table beside them, and temporary files a
// crash left mid-write. Only for the process that owns the namespace's files, once its tables
// are discovered: a read-only opener must not, since the writer's in-flight files look exactly
// like orphans.
func (s *LsmBlobStore) RemoveOrphanSidecars() {
	if !s.sidecarsOn() {
		return
	}
	prefix := storio.SegmentNameBuilder.NamespacePrefix(s.namespaceID)
	root, ok := s.sidecarPaths.LocalSegmentPath(prefix + "sstidx")
	if !ok {
		return
	}
	nsRoot, ok := s.sidecarPaths.LocalSegmentPath(strings.TrimSuffix(prefix, "/"))
	if !ok {
		return
	}
	s.mu.Lock()
	known := make(map[string]struct{}, len(s.tables))
	for _, t := range s.tables {
		known[t.SegmentName] = struct{}{}
	}
	s.mu.Unlock()
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), sidecarTempMarker) {
			_ = os.Remove(p)
			return nil
		}
		rel, err := filepath.Rel(nsRoot, p)
		if err != nil {
			return nil
		}
		table, ok := sidecarTableName(prefix + filepath.ToSlash(rel))
		if !ok {
			return nil
		}
		if _, ok := known[table]; !ok {
			_ = os.Remove(p)
		}
		return nil
	})
}

// sidecarState is the store's sidecar bookkeeping, embedded in LsmBlobStore; all fields but
// sidecarPaths are guarded by its mu.
type sidecarState struct {
	// sidecarPaths resolves sidecar names to local files; nil when this store uses no sidecars.
	sidecarPaths storage.LocalPathResolver
	sidecarBuild bool
	sidecars     map[string]*sidecarSlot
	closed       bool
}
