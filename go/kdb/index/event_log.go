package index

import (
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// bucketCacheEntries caps how many distinct bucket sets are memoized at once. Lookups
// normally target one cutoff (the current head), so a handful of slots covers the common
// "head plus a few historical reads" pattern without letting the memo grow without bound.
const bucketCacheEntries = 8

// bucketCacheKey identifies everything a replay's result depends on: which commit it was cut
// off at, how many events the log held, and what shape the commit graph was in. Any change to
// one of those produces a different key, so a stale bucket set can never be served - there is
// no explicit invalidation to forget to call.
type bucketCacheKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

// eventLog is the chronological put/delete log both index implementations replay, together
// with a memo of the bucket sets already derived from it.
//
// Replay used to run on *every* Lookup and Range: rebuilding the whole map from the full
// event log, sorting a freshly built combined event slice, and asking the DAG IsAncestor per
// event - which walks the descendant's entire ancestor closure from scratch each time. A
// lookup against an index with n events and h commits of history therefore cost O(n log n +
// n*h) and allocated the whole bucket map, per call, even when nothing had changed since the
// previous identical lookup. Now a replay happens once per (cutoff, log state, DAG shape),
// walks the two already-sorted event slices by merge instead of sorting a copy of them, and
// resolves ancestry against one closure computed once.
type eventLog struct {
	dag *dag.InMemoryCommitDag

	mu         sync.Mutex
	puts       []putEvent
	deletes    []deleteEvent
	seqCounter int64

	cache      map[bucketCacheKey]map[Key]map[codec.UUID]struct{}
	cacheOrder []bucketCacheKey
}

func newEventLog(d *dag.InMemoryCommitDag) *eventLog {
	return &eventLog{dag: d}
}

func (l *eventLog) put(entry Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.appendPutLocked(entry)
}

func (l *eventLog) delete(docID codec.UUID, atCommit codec.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.appendDeleteLocked(docID, atCommit)
}

func (l *eventLog) bulkLoad(entries []Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLocked()
	for _, e := range entries {
		l.appendPutLocked(e)
	}
}

func (l *eventLog) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLocked()
}

func (l *eventLog) snapshotBytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return []byte(strings.Join(snapshotLines(l.puts, l.deletes), "\n"))
}

func (l *eventLog) restoreSnapshotBytes(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLocked()
	return restoreSnapshotLines(
		strings.Split(string(data), "\n"),
		l.appendPutLocked,
		l.appendDeleteLocked,
	)
}

func (l *eventLog) hasCommit(hash codec.Hash) bool {
	return l.dag.HasCommit(hash)
}

// cutoff resolves an optional as-of commit to the commit a replay should stop at, defaulting
// to the DAG head.
func (l *eventLog) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return l.dag.Head()
}

// buckets returns the key → doc-id map visible at cutoffHash. The returned map is shared with
// the memo and with other callers: treat it as read-only. Use bucketsCopy to hand it out.
func (l *eventLog) buckets(cutoffHash codec.Hash) map[Key]map[codec.UUID]struct{} {
	version := l.dag.AncestryVersion()
	l.mu.Lock()
	defer l.mu.Unlock()
	key := bucketCacheKey{cutoff: cutoffHash, seq: l.seqCounter, ancestryVersion: version}
	if cached, ok := l.cache[key]; ok {
		return cached
	}
	built := l.replayLocked(cutoffHash)
	if l.cache == nil {
		l.cache = make(map[bucketCacheKey]map[Key]map[codec.UUID]struct{}, bucketCacheEntries)
	}
	l.cache[key] = built
	l.cacheOrder = append(l.cacheOrder, key)
	if len(l.cacheOrder) > bucketCacheEntries {
		delete(l.cache, l.cacheOrder[0])
		l.cacheOrder = l.cacheOrder[1:]
	}
	return built
}

// bucketsCopy returns a deep copy safe for a caller to mutate.
func (l *eventLog) bucketsCopy(cutoffHash codec.Hash) map[Key]map[codec.UUID]struct{} {
	shared := l.buckets(cutoffHash)
	out := make(map[Key]map[codec.UUID]struct{}, len(shared))
	for k, ids := range shared {
		copied := make(map[codec.UUID]struct{}, len(ids))
		for id := range ids {
			copied[id] = struct{}{}
		}
		out[k] = copied
	}
	return out
}

// replayLocked rebuilds the bucket map at cutoffHash by merging the two event slices in
// sequence order. Both are appended to in increasing seq, so they are already sorted.
func (l *eventLog) replayLocked(cutoffHash codec.Hash) map[Key]map[codec.UUID]struct{} {
	ancestors := l.dag.AncestorSet(cutoffHash)
	buckets := make(map[Key]map[codec.UUID]struct{})
	p, d := 0, 0
	for p < len(l.puts) || d < len(l.deletes) {
		if d >= len(l.deletes) || (p < len(l.puts) && l.puts[p].seq < l.deletes[d].seq) {
			entry := l.puts[p].entry
			p++
			if _, ok := ancestors[entry.CommitHash]; !ok {
				continue
			}
			ids := buckets[entry.Key]
			if ids == nil {
				ids = make(map[codec.UUID]struct{})
				buckets[entry.Key] = ids
			}
			ids[entry.DocID] = struct{}{}
			continue
		}
		evt := l.deletes[d]
		d++
		if _, ok := ancestors[evt.atCommit]; !ok {
			continue
		}
		pruneDoc(buckets, evt.docID)
	}
	return buckets
}

func (l *eventLog) appendPutLocked(entry Entry) {
	l.seqCounter++
	l.puts = append(l.puts, putEvent{seq: l.seqCounter, entry: entry})
}

func (l *eventLog) appendDeleteLocked(docID codec.UUID, atCommit codec.Hash) {
	l.seqCounter++
	l.deletes = append(l.deletes, deleteEvent{seq: l.seqCounter, docID: docID, atCommit: atCommit})
}

func (l *eventLog) clearLocked() {
	l.puts = nil
	l.deletes = nil
	l.seqCounter = 0
	l.cache = nil
	l.cacheOrder = nil
}

// lookup returns the doc ids filed under key at cutoffHash.
func (l *eventLog) lookup(key Key, cutoffHash codec.Hash) []codec.UUID {
	buckets := l.buckets(cutoffHash)
	ids, ok := buckets[key]
	if !ok {
		return nil
	}
	out := make([]codec.UUID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	return out
}

// rangeScan returns the doc ids whose keys fall within [from, to] at cutoffHash, in key
// order, capped at limit.
func (l *eventLog) rangeScan(from, to Key, cutoffHash codec.Hash, limit int, ascending bool) []codec.UUID {
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	buckets := l.buckets(cutoffHash)
	keys := make([]Key, 0, len(buckets))
	for k := range buckets {
		if from != nil && CompareKeys(k, from) < 0 {
			continue
		}
		if to != nil && CompareKeys(k, to) > 0 {
			continue
		}
		keys = append(keys, k)
	}
	sortKeys(keys, ascending)
	seen := make(map[codec.UUID]struct{})
	var out []codec.UUID
	for _, k := range keys {
		for id := range buckets[k] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

func sortKeys(keys []Key, ascending bool) {
	sort.Slice(keys, func(i, j int) bool {
		c := CompareKeys(keys[i], keys[j])
		if !ascending {
			return c > 0
		}
		return c < 0
	})
}
