package sstable

import (
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Which runs a merge takes: size-tiered, after RocksDB's universal compaction.
//
// A merge used to take every table in the store whenever it held DefaultCompactionTrigger runs.
// A store that only grows therefore rewrote all of its data every three flushes, and the bytes a
// store wrote to disk grew with the square of its size: N flushes of s bytes wrote about N*N*s/6
// in merges. A merge now takes a window of runs:
//
//   - Runs are taken newest first, and a window is always a sequence of consecutive runs, so its
//     output can take the window's place in the search order - newer than every run older than
//     it, older than every run newer - and a tombstone it keeps shadows exactly what it did.
//   - Full merge, when everything newer than the oldest run adds up to at least the oldest run
//     (fullMergeRatio): the only merge that may drop tombstones, and the one that drops what the
//     keep predicate refuses from the oldest run. It bounds what superseded and duplicated data can
//     cost to about the size of the live data.
//   - Otherwise a window of at least trigger similar sizes: from a run, each next older run joins
//     while it is no larger than the window so far. Tiers form the way a binary counter's digits
//     do, so a byte is rewritten about log(N) times rather than on every merge.
//   - Otherwise nothing, until the store holds more than mergeMaxRuns runs; then the cheapest
//     window that brings it back to that.
//
// A merge that takes only some runs keeps its tombstones and leaves older runs alone; the keep
// predicate still applies to what it does take, which is safe for the reason dropping is safe at
// all: nothing can ask for a refused key again, so a copy left in an older run is only disk.

// fullMergeRatio is how large, relative to the oldest run, everything newer must have grown before
// a merge takes the whole store. One: the store may hold up to about twice what one full merge
// would leave.
const fullMergeRatio = 1.0

// tieredMerges turns the window selection off, for tests that compare against full merges.
var tieredMerges = true

// mergeRun is one run in the search order: its tables, newest first as the search order has them,
// and their bytes on disk.
type mergeRun struct {
	tables []Handle
	bytes  int64
}

// runsNewestFirst groups tables, given newest first, into runs in the same order. A run's tables
// are contiguous in the search order - one merge wrote them, at one level, and they are registered
// together - so a run is placed where its first table is.
func runsNewestFirst(order []Handle) []mergeRun {
	var runs []mergeRun
	at := make(map[string]int)
	for _, h := range order {
		id := runOf(h.SegmentName)
		i, ok := at[id]
		if !ok {
			i = len(runs)
			at[id] = i
			runs = append(runs, mergeRun{})
		}
		runs[i].tables = append(runs[i].tables, h)
	}
	return runs
}

// sizeRuns fills in each run's bytes, and reports false when the shim cannot say.
func sizeRuns(io storage.PlatformIOShim, runs []mergeRun) bool {
	sizer, ok := io.(storage.SegmentSizer)
	if !ok {
		return false
	}
	for i := range runs {
		runs[i].bytes = 0
		for _, h := range runs[i].tables {
			n, err := sizer.SegmentSize(h.SegmentName)
			if err != nil {
				return false
			}
			runs[i].bytes += n
		}
	}
	return true
}

// mergeMaxRuns is how many runs a store may hold before a merge is forced whatever their sizes:
// what bounds the runs a lookup searches. Tiers need room - a store of N flushes settles into
// about log(N) of them - and a cap at the trigger itself left none: with only three runs allowed,
// every flush forced a merge into a larger run, and the bytes written grew linearly again (380x
// the data at 10,000 flushes, against 6x with this cap).
func mergeMaxRuns(trigger int) int { return 3 * trigger }

// pickMerge chooses which runs the next merge takes: runs[lo:hi] (runs are newest first), and
// whether that is all of them. hi is 0 when no merge is due.
//
// trigger is both the fewest runs worth merging and the narrowest window of similar sizes that
// justifies a merge - DefaultCompactionTrigger's four, as size-tiered compaction's minimum
// threshold. Simulated growth from N equal flushes, bytes written in merges over bytes flushed:
//
//	flushes          100     1,000    10,000
//	full merges      17x      167x    1,667x
//	tiered          2.8x      4.1x      6.2x   (at most 12 runs)
func pickMerge(runs []mergeRun, sized bool, trigger int) (lo, hi int, full bool) {
	lo, hi, _ = pickWindow(runs, sized, trigger)
	hi = pastAgeless(runs, hi)
	return lo, hi, hi > 0 && lo == 0 && hi == len(runs)
}

// pastAgeless widens a window ending at hi over every older ageless run at its oldest run's level,
// when that oldest run is ageless itself. The window's output takes its oldest input's age and
// level (newRunWriter), here zero; beside another ageless run at that level the two would tie, and
// a tie sorts by name - at random - when the store reopens. Swallowing them leaves the output the
// only ageless run at its level. Ageless runs are a level's oldest (DiscoverTables registers them
// first and every table this build writes has an age), so they are all at the window's old end.
func pastAgeless(runs []mergeRun, hi int) int {
	if hi == 0 || hi == len(runs) || !runs[hi-1].ageless() {
		return hi
	}
	level := runs[hi-1].tables[0].Level
	for hi < len(runs) && runs[hi].ageless() && runs[hi].tables[0].Level == level {
		hi++
	}
	return hi
}

// ageless reports a run named without an age: Kotlin's tables, and this package's before ages.
func (r mergeRun) ageless() bool {
	return len(r.tables) > 0 && ageOf(r.tables[0].SegmentName) == noAge
}

func pickWindow(runs []mergeRun, sized bool, trigger int) (lo, hi int, full bool) {
	m := len(runs)
	if m < trigger {
		return 0, 0, false
	}
	if !sized || !tieredMerges {
		return 0, m, true
	}
	var younger int64
	for _, r := range runs[:m-1] {
		younger += r.bytes
	}
	if float64(younger) >= fullMergeRatio*float64(runs[m-1].bytes) {
		return 0, m, true
	}
	// A window of at least trigger similar sizes, trying each run in turn as its newest member:
	// windows form at the newest end and, as tiers fill, further back.
	for lo = 0; lo < m-1; lo++ {
		acc := runs[lo].bytes
		hi = lo + 1
		for hi < m && runs[hi].bytes <= acc {
			acc += runs[hi].bytes
			hi++
		}
		if hi-lo >= trigger {
			return lo, hi, lo == 0 && hi == m
		}
	}
	// Too many runs of dissimilar sizes: the cheapest window that brings the store back to the
	// cap.
	limit := mergeMaxRuns(trigger)
	if m <= limit {
		return 0, 0, false
	}
	width := m - limit + 1
	best, bestBytes := 0, int64(-1)
	for start := 0; start+width <= m; start++ {
		var b int64
		for _, r := range runs[start : start+width] {
			b += r.bytes
		}
		if bestBytes < 0 || b < bestBytes {
			best, bestBytes = start, b
		}
	}
	return best, best + width, width == m
}

// A table's file name starts with its age: a sequence number in 16 hex digits, then a random UUID
// (and, for a merge's table, its sequence in the run). Names sort by age, and that is what
// DiscoverTables relies on to register a level's tables in the order they were written - the order
// a lookup searches them in - because it registers them in that order (byAge).
//
// A flushed table's age is the next number in its store's Sequence (sequence.go): one past the
// highest age the namespace holds. No clock is involved, so no clock's resolution or steps can
// make two tables tie or sort backwards. A merge's tables take their oldest input's age, so they
// sort where that input did: the place in the search order the merge gave them (see
// LsmBlobStore.CompactContextUpTo). Without it a window's output at level 0, and every flush
// beside it, came back after a restart in an arbitrary order - harmless for values, which every
// table holds identically, but not for a tombstone.
//
// Ages were once wall-clock nanoseconds. Those names still sort correctly: a sequence continues
// from the highest age on disk, so a store written by such a build carries on above it.
//
// Tables named before ages (a bare UUID) have none; they count as the oldest, which is what they
// are, being older than anything this build writes. Kotlin builds before its own sequence names
// wrote that way too. Their order among themselves is their names', the same on every open; a
// merge that takes one takes every older one beside it (pastAgeless), so its output never ties
// with one.

// ageOf is the age part of a table's name, all zeros for a name without one.
func ageOf(segmentName string) string {
	base := segmentName[strings.LastIndexByte(segmentName, '/')+1:]
	if len(base) > 17 && base[16] == '-' && isLowerHex(base[:16]) {
		return base[:16]
	}
	return noAge
}

// noAge is ageOf a name without an age.
const noAge = "0000000000000000"

// byAge sorts table names oldest first: by age, and ageless names by name. A listing's order is
// not it - a bare UUID can sort anywhere among names that start with an age.
func byAge(names []string) {
	sort.SliceStable(names, func(i, j int) bool {
		ai, aj := ageOf(names[i]), ageOf(names[j])
		if ai != aj {
			return ai < aj
		}
		return names[i] < names[j]
	})
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// tableFileID names a table of the given age: the age, then a random UUID so names never collide.
func tableFileID(age string) (string, error) {
	uuid, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	return age + "-" + uuid.String(), nil
}
