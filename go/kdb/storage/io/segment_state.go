package io

import (
	"sort"
	"strings"
	"sync"
)

// This file holds the per-segment state FileBackedPlatformIO keeps, in forms whose size does not
// grow with the number of segments a process touches. It used to keep a lock, a sealed flag and a
// generation for every segment name it had ever seen, pruned only on delete or namespace release:
// ~300 bytes a segment, so ~31 MB at 100k segments, under stores that are otherwise bounded
// (docs/kdb-memory-cassandra-plan.md: data larger than RAM, memory a bounded cache).

// segmentLockStripes is how many locks the segments share. Two segments on one stripe only contend
// when one of them is being appended to, sealed, restored, renamed or deleted - reads share a
// stripe - so a few thousand is ample for the handful of segments written at once.
const segmentLockStripes = 1024

// paddedRWMutex keeps each stripe on its own cache line, so reads taking neighbouring stripes do
// not bounce one line between cores (RLock writes the mutex's reader count).
type paddedRWMutex struct {
	sync.RWMutex
	_ [64 - 24]byte
}

// segmentLocks is a fixed array of locks indexed by a hash of the segment name. A name maps to the
// same stripe for the life of the process, so there is nothing to create on first use or forget on
// delete - and so no window, as there was when DeleteSegment dropped a name's lock while holding
// it, in which two callers can hold two different locks for one segment.
type segmentLocks [segmentLockStripes]paddedRWMutex

func segmentStripe(segmentName string) int {
	// FNV-1a, inline so hashing a name allocates nothing.
	h := uint64(14695981039346656037)
	for i := 0; i < len(segmentName); i++ {
		h ^= uint64(segmentName[i])
		h *= 1099511628211
	}
	return int(h % segmentLockStripes)
}

func (l *segmentLocks) forName(segmentName string) *sync.RWMutex {
	return &l[segmentStripe(segmentName)].RWMutex
}

// lockPair locks the stripes of two names exclusively, in stripe order so two callers locking the
// same pair cannot deadlock, and once when both names share a stripe. It returns the unlock.
func (l *segmentLocks) lockPair(a, b string) func() {
	i, j := segmentStripe(a), segmentStripe(b)
	if i == j {
		l[i].Lock()
		return l[i].Unlock
	}
	if j < i {
		i, j = j, i
	}
	l[i].Lock()
	l[j].Lock()
	return func() {
		l[j].Unlock()
		l[i].Unlock()
	}
}

const (
	// defaultSealedMemory is how many recently sealed segment names the shim remembers by name.
	defaultSealedMemory = 4096
	// defaultGenerationMemory is how many segments' generations the shim remembers.
	defaultGenerationMemory = 16384
)

// sealedSegments answers "was this segment sealed (or restored) in this process", which is what
// lets AppendToSegment refuse to write into history and RenameSegment refuse to move a segment
// replicas already copied. Not guarded itself: FileBackedPlatformIO.globalMu guards it.
//
// Two parts, both bounded:
//
//   - Restored sequenced segments (delta segments named by DeltaSequencedFileName, which is all
//     RestoreFromArchive brings back) are held exactly, as runs of sequence numbers per directory.
//     A restore brings back a contiguous range, so a range of any length is one run; runs only
//     multiply with holes punched by deleting single restored segments again. These stay sealed
//     until deleted, renamed over or their namespace released, however many segments are sealed
//     after them.
//   - Every other seal is remembered by name, the most recent `memory` of them, oldest forgotten
//     first. A name forgotten here is in the state every segment is in after a restart - the shim
//     no longer refuses appends to it, and its writer, which does not reopen a segment it sealed
//     (delta.Factory.OpenWriter always starts a new one, an SSTable is written once), is what keeps
//     it closed. The window this guards - a writer appending to the segment it just sealed, or to
//     one sealed a moment ago by another - is far shorter than thousands of seals.
type sealedSegments struct {
	memory int
	names  map[string]uint64 // name -> the stamp of its entry in ring
	ring   []sealedEntry     // FIFO of what names holds; entries whose stamp no longer matches are stale
	head   int
	stamp  uint64

	restored map[string]seqRuns // directory (name up to and including its last '/') -> runs
}

type sealedEntry struct {
	name  string
	stamp uint64
}

func newSealedSegments(memory int) *sealedSegments {
	return &sealedSegments{
		memory:   memory,
		names:    make(map[string]uint64),
		restored: make(map[string]seqRuns),
	}
}

// sequencedName splits a segment name into its directory and its sequence number, ok false when
// the file name is not a sequenced one.
func sequencedName(segmentName string) (dir string, seq int64, ok bool) {
	i := strings.LastIndexByte(segmentName, '/')
	if i < 0 {
		return "", 0, false
	}
	seq, ok = ParseDeltaSequencedFileName(segmentName[i+1:])
	return segmentName[:i+1], seq, ok
}

func (s *sealedSegments) has(segmentName string) bool {
	if _, ok := s.names[segmentName]; ok {
		return true
	}
	if dir, seq, ok := sequencedName(segmentName); ok {
		return s.restored[dir].contains(seq)
	}
	return false
}

// seal remembers segmentName as sealed, forgetting the oldest remembered seal if that is full.
func (s *sealedSegments) seal(segmentName string) {
	if _, ok := s.names[segmentName]; ok {
		return
	}
	if s.ring == nil {
		s.ring = make([]sealedEntry, s.memory)
	}
	// The slot about to be reused holds the oldest entry; forget its name unless the name has
	// since been forgotten and sealed again under a newer entry.
	if old := s.ring[s.head]; old.name != "" && s.names[old.name] == old.stamp {
		delete(s.names, old.name)
	}
	s.stamp++
	s.ring[s.head] = sealedEntry{name: segmentName, stamp: s.stamp}
	s.head = (s.head + 1) % len(s.ring)
	s.names[segmentName] = s.stamp
}

// restore remembers segmentName as restored, and so sealed.
func (s *sealedSegments) restore(segmentName string) {
	dir, seq, ok := sequencedName(segmentName)
	if !ok {
		s.seal(segmentName)
		return
	}
	s.restored[dir] = s.restored[dir].add(seq)
}

// forget drops whatever is known about segmentName: it was deleted, or something else now holds
// its name.
func (s *sealedSegments) forget(segmentName string) {
	// The ring entry goes stale rather than being found and cleared; it is skipped when reused.
	delete(s.names, segmentName)
	if dir, seq, ok := sequencedName(segmentName); ok {
		if runs, had := s.restored[dir]; had {
			if runs = runs.remove(seq); len(runs) == 0 {
				delete(s.restored, dir)
			} else {
				s.restored[dir] = runs
			}
		}
	}
}

// forgetPrefix drops everything known about segments whose names start with prefix.
func (s *sealedSegments) forgetPrefix(prefix string) {
	for name := range s.names {
		if strings.HasPrefix(name, prefix) {
			delete(s.names, name)
		}
	}
	for dir := range s.restored {
		if strings.HasPrefix(dir, prefix) {
			delete(s.restored, dir)
		}
	}
}

// seqRuns is a sorted list of disjoint, non-adjacent inclusive runs of sequence numbers.
type seqRuns []seqRun

type seqRun struct{ lo, hi int64 }

// at returns the index of the first run ending at or after seq.
func (rs seqRuns) at(seq int64) int {
	return sort.Search(len(rs), func(i int) bool { return rs[i].hi >= seq })
}

func (rs seqRuns) contains(seq int64) bool {
	i := rs.at(seq)
	return i < len(rs) && rs[i].lo <= seq
}

func (rs seqRuns) add(seq int64) seqRuns {
	i := rs.at(seq - 1) // the first run that contains seq or ends just before it
	if i < len(rs) && rs[i].lo <= seq+1 {
		switch {
		case seq < rs[i].lo:
			rs[i].lo = seq // no run before i ends at seq-1, or i would be it
		case seq > rs[i].hi:
			rs[i].hi = seq
			if i+1 < len(rs) && rs[i+1].lo == seq+1 {
				rs[i].hi = rs[i+1].hi
				rs = append(rs[:i+1], rs[i+2:]...)
			}
		}
		return rs
	}
	rs = append(rs, seqRun{})
	copy(rs[i+1:], rs[i:])
	rs[i] = seqRun{seq, seq}
	return rs
}

func (rs seqRuns) remove(seq int64) seqRuns {
	i := rs.at(seq)
	if i == len(rs) || rs[i].lo > seq {
		return rs
	}
	r := rs[i]
	switch {
	case r.lo == r.hi:
		return append(rs[:i], rs[i+1:]...)
	case seq == r.lo:
		rs[i].lo++
	case seq == r.hi:
		rs[i].hi--
	default:
		rs = append(rs, seqRun{})
		copy(rs[i+1:], rs[i:])
		rs[i] = seqRun{r.lo, seq - 1}
		rs[i+1] = seqRun{seq + 1, r.hi}
	}
	return rs
}

// segmentGenerations remembers the generation (storage.SegmentGenerationer) of at most `memory`
// segments. Forgetting one is always safe: the next ask hands out a new generation, which nothing
// has cached anything under, so the cost is a cache miss, never a stale hit. When full, an
// arbitrary entry is forgotten - Go's map iteration starts at a random slot - which is cheap and,
// for a frame cache much smaller than the segments it caches, about as good as recency.
// Not guarded itself: FileBackedPlatformIO.globalMu guards it.
type segmentGenerations struct {
	memory int
	gens   map[string]uint64
}

func newSegmentGenerations(memory int) *segmentGenerations {
	return &segmentGenerations{memory: memory, gens: make(map[string]uint64)}
}

func (g *segmentGenerations) get(segmentName string, next func() uint64) uint64 {
	if gen, ok := g.gens[segmentName]; ok {
		return gen
	}
	if len(g.gens) >= g.memory {
		for name := range g.gens {
			delete(g.gens, name)
			break
		}
	}
	gen := next()
	g.gens[segmentName] = gen
	return gen
}

func (g *segmentGenerations) forget(segmentName string) { delete(g.gens, segmentName) }

func (g *segmentGenerations) forgetPrefix(prefix string) {
	for name := range g.gens {
		if strings.HasPrefix(name, prefix) {
			delete(g.gens, name)
		}
	}
}
