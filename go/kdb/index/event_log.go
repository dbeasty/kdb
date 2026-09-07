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

// bucket is one key's document set. Buckets are stored by KeyString rather than by Key
// because CompositeKey and VectorKey contain slices and are not hashable.
type bucket struct {
	key Key
	ids map[codec.UUID]struct{}
}

type bucketMap map[string]*bucket

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

	cache      map[bucketCacheKey]bucketMap
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
func (l *eventLog) buckets(cutoffHash codec.Hash) bucketMap {
	version := l.dag.AncestryVersion()
	l.mu.Lock()
	defer l.mu.Unlock()
	key := bucketCacheKey{cutoff: cutoffHash, seq: l.seqCounter, ancestryVersion: version}
	if cached, ok := l.cache[key]; ok {
		return cached
	}
	built := l.replayLocked(cutoffHash)
	if l.cache == nil {
		l.cache = make(map[bucketCacheKey]bucketMap, bucketCacheEntries)
	}
	l.cache[key] = built
	l.cacheOrder = append(l.cacheOrder, key)
	if len(l.cacheOrder) > bucketCacheEntries {
		delete(l.cache, l.cacheOrder[0])
		l.cacheOrder = l.cacheOrder[1:]
	}
	return built
}

// KeyBucket is one key together with the documents filed under it.
type KeyBucket struct {
	Key    Key
	DocIDs []codec.UUID
}

// bucketsCopy returns the buckets at cutoffHash as a caller-owned, key-ordered slice.
func (l *eventLog) bucketsCopy(cutoffHash codec.Hash) []KeyBucket {
	shared := l.buckets(cutoffHash)
	out := make([]KeyBucket, 0, len(shared))
	for _, b := range shared {
		out = append(out, KeyBucket{Key: b.key, DocIDs: sortedBucketIDs(b.ids)})
	}
	sort.Slice(out, func(i, j int) bool { return CompareKeys(out[i].Key, out[j].Key) < 0 })
	return out
}

// replayLocked rebuilds the bucket map at cutoffHash by merging the two event slices in
// sequence order. Both are appended to in increasing seq, so they are already sorted.
func (l *eventLog) replayLocked(cutoffHash codec.Hash) bucketMap {
	ancestors := l.dag.AncestorSet(cutoffHash)
	buckets := make(bucketMap)
	p, d := 0, 0
	for p < len(l.puts) || d < len(l.deletes) {
		if d >= len(l.deletes) || (p < len(l.puts) && l.puts[p].seq < l.deletes[d].seq) {
			entry := l.puts[p].entry
			p++
			if _, ok := ancestors[entry.CommitHash]; !ok {
				continue
			}
			ks := KeyString(entry.Key)
			b := buckets[ks]
			if b == nil {
				b = &bucket{key: entry.Key, ids: make(map[codec.UUID]struct{})}
				buckets[ks] = b
			}
			b.ids[entry.DocID] = struct{}{}
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

func pruneDoc(buckets bucketMap, docID codec.UUID) {
	var dead []string
	for ks, b := range buckets {
		delete(b.ids, docID)
		if len(b.ids) == 0 {
			dead = append(dead, ks)
		}
	}
	for _, ks := range dead {
		delete(buckets, ks)
	}
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
	b, ok := buckets[KeyString(key)]
	if !ok {
		return nil
	}
	// Stable order, for the same reason rangeScan needs one: a bucket is a map, and a caller
	// that compares two lookups - or hands the result straight on as query rows - should not see
	// the order change between identical calls.
	return sortedBucketIDs(b.ids)
}

// rangeScan returns the doc ids whose keys fall within [from, to] at cutoffHash, in key
// order, capped at limit.
func (l *eventLog) rangeScan(from, to Key, cutoffHash codec.Hash, limit int, ascending bool) []codec.UUID {
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	buckets := l.buckets(cutoffHash)
	matched := make([]*bucket, 0, len(buckets))
	for _, b := range buckets {
		if from != nil && CompareKeys(b.key, from) < 0 {
			continue
		}
		if to != nil && CompareKeys(b.key, to) > 0 {
			continue
		}
		matched = append(matched, b)
	}
	sort.Slice(matched, func(i, j int) bool {
		c := CompareKeys(matched[i].key, matched[j].key)
		if !ascending {
			return c > 0
		}
		return c < 0
	})
	seen := make(map[codec.UUID]struct{})
	var out []codec.UUID
	for _, b := range matched {
		// A bucket is a map, so ranging it directly gave a different order on every call. That
		// is invisible without a limit - the same set comes back, just shuffled - but with one
		// it changes *which* documents are returned, so a limited range query answered
		// differently each time it ran, and paging through an index could show a document twice
		// or never. Sorted by id: an arbitrary order, but the same arbitrary order every time.
		for _, id := range sortedBucketIDs(b.ids) {
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

// sortedBucketIDs returns a bucket's document ids in a stable order.
func sortedBucketIDs(bucket map[codec.UUID]struct{}) []codec.UUID {
	ids := make([]codec.UUID, 0, len(bucket))
	for id := range bucket {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].MSB != ids[j].MSB {
			return uint64(ids[i].MSB) < uint64(ids[j].MSB)
		}
		return uint64(ids[i].LSB) < uint64(ids[j].LSB)
	})
	return ids
}
