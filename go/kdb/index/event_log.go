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
	// sorted memoizes the ids in stable order. A low-selectivity key (a collection tag shared by
	// most documents) is looked up on every query that mentions it, and sorting a hundred
	// thousand ids each time cost more than the query. Dropped whenever the bucket changes.
	sorted []codec.UUID
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

	// head is the bucket state at the commit most recently read at, advanced in place as commits
	// are appended. Without it the first read after every write replays the whole log - and
	// materializes the whole ancestor closure - so a filtered read costs O(documents) however
	// selective the index is. With it a read after a write costs O(that write).
	head *headState
	// firstSeq is the sequence number of the first event logged for each commit. Advancing the
	// head state is only exact when every event of the commits it adds is newer than the state
	// (see advanceHeadLocked), and this is what answers that in O(1).
	firstSeq map[codec.Hash]int64
}

// headState is a mutable replay result. Everything in it is guarded by eventLog.mu.
type headState struct {
	cutoff    codec.Hash
	seq       int64  // seqCounter this state covers
	epoch     uint64 // dag.AncestryEpoch it was built under
	puts      int    // events consumed so far, per slice
	deletes   int
	buckets   bucketMap
	docKeys   map[codec.UUID][]string // bucket keys each document is filed under
	ancestors map[codec.Hash]struct{}
}

// maxAdvanceSteps bounds how many commits one advance will walk back through before giving up
// and replaying. A reader that fell far behind is better served by one replay than a long walk.
const maxAdvanceSteps = 256

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

// bucketsLocked returns the key → doc-id map visible at cutoffHash. Callers hold l.mu and must
// finish with the map before releasing it: the head state's map is mutated in place by the next
// write, so unlike the memoized replays it cannot be handed out.
func (l *eventLog) bucketsLocked(cutoffHash codec.Hash) bucketMap {
	if m, ok := l.headBucketsLocked(cutoffHash); ok {
		return m
	}
	version := l.dag.AncestryVersion()
	key := bucketCacheKey{cutoff: cutoffHash, seq: l.seqCounter, ancestryVersion: version}
	if cached, ok := l.cache[key]; ok {
		return cached
	}
	built, state := l.replayLocked(cutoffHash)
	// Only a read at the current head seeds the incremental state: it is the one that the next
	// write will move on from, whereas a historical read is a one-off that would thrash it.
	if head, err := l.dag.Head(); err == nil && head == cutoffHash {
		l.head = state
		return state.buckets
	}
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

// headBucketsLocked serves cutoff from the incremental state when it can: as is when nothing
// changed, or after advancing it over the commits appended since. ok is false when a replay is
// needed instead.
func (l *eventLog) headBucketsLocked(cutoff codec.Hash) (bucketMap, bool) {
	h := l.head
	if h == nil || h.epoch != l.dag.AncestryEpoch() {
		return nil, false
	}
	if h.cutoff == cutoff && h.seq == l.seqCounter {
		return h.buckets, true
	}
	if !l.advanceHeadLocked(cutoff) {
		return nil, false
	}
	return h.buckets, true
}

// advanceHeadLocked moves the head state forward to cutoff by applying only the events logged
// since it was built. It is exact - the same buckets a full replay at cutoff produces - only
// when all of these hold, and returns false (leaving the state untouched or discarded) otherwise:
//
//   - cutoff reaches the state's cutoff by a chain of single-parent commits, so the new
//     ancestor closure is the old one plus that chain and nothing else;
//   - none of the chain's own events pre-dates the state, since a replay would have applied
//     those at their earlier position in sequence order, not after everything already applied;
//   - the DAG epoch is unchanged, so no resident commit has gained or lost ancestors.
//
// Events newer than the state that belong to commits already in the closure (a backfill, or an
// index rebuilt late) are applied too, in sequence order, exactly as a replay would.
func (l *eventLog) advanceHeadLocked(cutoff codec.Hash) bool {
	h := l.head
	var chain []codec.Hash
	cur := cutoff
	for cur != h.cutoff {
		if len(chain) >= maxAdvanceSteps {
			l.head = nil
			return false
		}
		c, ok := l.dag.GetCommit(cur)
		if !ok || len(c.ParentHashes) != 1 {
			l.head = nil
			return false
		}
		chain = append(chain, cur)
		cur = c.ParentHashes[0]
	}
	for _, c := range chain {
		if first, seen := l.firstSeq[c]; seen && first <= h.seq {
			l.head = nil
			return false
		}
	}
	for _, c := range chain {
		h.ancestors[c] = struct{}{}
	}
	p, d := h.puts, h.deletes
	for p < len(l.puts) || d < len(l.deletes) {
		if d >= len(l.deletes) || (p < len(l.puts) && l.puts[p].seq < l.deletes[d].seq) {
			h.applyPut(l.puts[p].entry)
			p++
			continue
		}
		h.applyDelete(l.deletes[d])
		d++
	}
	h.puts, h.deletes = p, d
	h.cutoff, h.seq = cutoff, l.seqCounter
	return true
}

func (h *headState) applyPut(entry Entry) {
	if _, ok := h.ancestors[entry.CommitHash]; !ok {
		return
	}
	ks := KeyString(entry.Key)
	b := h.buckets[ks]
	if b == nil {
		b = &bucket{key: entry.Key, ids: make(map[codec.UUID]struct{})}
		h.buckets[ks] = b
	}
	if _, filed := b.ids[entry.DocID]; !filed {
		b.sorted = nil
		b.ids[entry.DocID] = struct{}{}
		h.docKeys[entry.DocID] = append(h.docKeys[entry.DocID], ks)
	}
}

func (h *headState) applyDelete(evt deleteEvent) {
	if _, ok := h.ancestors[evt.atCommit]; !ok {
		return
	}
	for _, ks := range h.docKeys[evt.docID] {
		b := h.buckets[ks]
		if b == nil {
			continue
		}
		delete(b.ids, evt.docID)
		b.sorted = nil
		if len(b.ids) == 0 {
			delete(h.buckets, ks)
		}
	}
	delete(h.docKeys, evt.docID)
}

// KeyBucket is one key together with the documents filed under it.
type KeyBucket struct {
	Key    Key
	DocIDs []codec.UUID
}

// bucketsCopy returns the buckets at cutoffHash as a caller-owned, key-ordered slice.
func (l *eventLog) bucketsCopy(cutoffHash codec.Hash) []KeyBucket {
	l.mu.Lock()
	defer l.mu.Unlock()
	shared := l.bucketsLocked(cutoffHash)
	out := make([]KeyBucket, 0, len(shared))
	for _, b := range shared {
		out = append(out, KeyBucket{Key: b.key, DocIDs: sortedBucketIDs(b.ids)})
	}
	sort.Slice(out, func(i, j int) bool { return CompareKeys(out[i].Key, out[j].Key) < 0 })
	return out
}

// replayLocked rebuilds the bucket state at cutoffHash by merging the two event slices in
// sequence order. Both are appended to in increasing seq, so they are already sorted. The
// returned state is what advanceHeadLocked continues from.
func (l *eventLog) replayLocked(cutoffHash codec.Hash) (bucketMap, *headState) {
	h := &headState{
		cutoff:    cutoffHash,
		seq:       l.seqCounter,
		epoch:     l.dag.AncestryEpoch(),
		buckets:   make(bucketMap),
		docKeys:   make(map[codec.UUID][]string),
		ancestors: l.dag.AncestorSet(cutoffHash),
	}
	p, d := 0, 0
	for p < len(l.puts) || d < len(l.deletes) {
		if d >= len(l.deletes) || (p < len(l.puts) && l.puts[p].seq < l.deletes[d].seq) {
			h.applyPut(l.puts[p].entry)
			p++
			continue
		}
		h.applyDelete(l.deletes[d])
		d++
	}
	h.puts, h.deletes = p, d
	return h.buckets, h
}

func (l *eventLog) appendPutLocked(entry Entry) {
	l.seqCounter++
	l.noteFirstLocked(entry.CommitHash)
	l.puts = append(l.puts, putEvent{seq: l.seqCounter, entry: entry})
}

func (l *eventLog) appendDeleteLocked(docID codec.UUID, atCommit codec.Hash) {
	l.seqCounter++
	l.noteFirstLocked(atCommit)
	l.deletes = append(l.deletes, deleteEvent{seq: l.seqCounter, docID: docID, atCommit: atCommit})
}

func (l *eventLog) noteFirstLocked(commit codec.Hash) {
	if l.firstSeq == nil {
		l.firstSeq = make(map[codec.Hash]int64)
	}
	if _, seen := l.firstSeq[commit]; !seen {
		l.firstSeq[commit] = l.seqCounter
	}
}

func (l *eventLog) clearLocked() {
	l.head = nil
	l.firstSeq = nil
	l.puts = nil
	l.deletes = nil
	l.seqCounter = 0
	l.cache = nil
	l.cacheOrder = nil
}

// lookup returns the doc ids filed under key at cutoffHash.
func (l *eventLog) lookup(key Key, cutoffHash codec.Hash) []codec.UUID {
	l.mu.Lock()
	defer l.mu.Unlock()
	buckets := l.bucketsLocked(cutoffHash)
	b, ok := buckets[KeyString(key)]
	if !ok {
		return nil
	}
	// Stable order, for the same reason rangeScan needs one: a bucket is a map, and a caller
	// that compares two lookups - or hands the result straight on as query rows - should not see
	// the order change between identical calls.
	return b.sortedIDs()
}

// lookupCount is the number of doc ids filed under key at cutoffHash.
func (l *eventLog) lookupCount(key Key, cutoffHash codec.Hash) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.bucketsLocked(cutoffHash)[KeyString(key)]; ok {
		return len(b.ids)
	}
	return 0
}

// sortedIDs returns a caller-owned copy of the bucket's ids in stable order.
func (b *bucket) sortedIDs() []codec.UUID {
	if b.sorted == nil || len(b.sorted) != len(b.ids) {
		b.sorted = sortedBucketIDs(b.ids)
	}
	return append([]codec.UUID(nil), b.sorted...)
}

// rangeScan returns the doc ids whose keys fall within [from, to] at cutoffHash, in key
// order, capped at limit.
func (l *eventLog) rangeScan(from, to Key, cutoffHash codec.Hash, limit int, ascending bool) []codec.UUID {
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	buckets := l.bucketsLocked(cutoffHash)
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
