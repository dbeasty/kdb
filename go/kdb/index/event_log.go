package index

import (
	"errors"
	"slices"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// The keyed (HASH and BTREE) index: a head state plus a bounded window of history.
//
// What a read at commit X must return is defined by replay: take every put and delete ever
// logged, in the order they were logged, and apply the ones whose commit is X or an ancestor of
// X. The log used to be exactly that - every event ever written, kept forever, plus the full
// ancestor closure of the head and one map entry per commit - so an index's memory grew with its
// write history rather than with its data.
//
// Now the authoritative structure is the head state: the replay result at the head cutoff,
// advanced in place as events arrive. Each event, once applied, records its effect on the head
// state (which keys it actually filed or unfiled its document under), and only the most recent
// MemoryOptions.HistoryEvents events are kept. A read at an older commit X is answered from the
// head state by undoing the retained events X must not see, newest first. That is exact when
// those events are a suffix of the applied ones, which they are for any X the head has moved
// through (see planLocked). A read the window cannot answer exactly returns
// ErrHistoryUnavailable, and the caller answers it from the documents instead (the server's
// index provider makes the SQL executor scan). While nothing has been dropped the log is still
// complete and any cutoff can be replayed, exactly as before.

// DefaultHistoryEvents is how many recent events a keyed index keeps for reads at a commit other
// than the head when MemoryOptions.HistoryEvents is zero. At 88 bytes an event that is under
// 3 MB per index, whatever its write history.
const DefaultHistoryEvents = 1 << 15

// MemoryOptions tunes a keyed (HASH or BTREE) MemoryStore.
type MemoryOptions struct {
	// HistoryEvents is how many of the most recent index events are kept for as-of reads. 0 means
	// DefaultHistoryEvents; a negative value keeps every event forever (the unbounded behaviour
	// this replaced, for tests and comparisons).
	HistoryEvents int
}

func (o MemoryOptions) maxEvents() int {
	switch {
	case o.HistoryEvents == 0:
		return DefaultHistoryEvents
	case o.HistoryEvents < 0:
		return 0
	default:
		return o.HistoryEvents
	}
}

// ErrHistoryUnavailable reports that an index no longer holds the history a read at an older
// commit needs (or, after an unusual change to the commit graph, its head state). The answer is
// still defined - it is whatever the documents at that commit say - and callers fall back to
// reading them. Match it with errors.Is.
var ErrHistoryUnavailable = errors.New("index: history at that commit is not held by the index")

// maxAdvanceSteps bounds the commit walks the log does without a closure: following the head
// forward along a chain of single-parent commits, and walking back from a historical cutoff to a
// commit the head passed through.
const maxAdvanceSteps = 256

// replayCacheEntries caps how many replayed historical states are memoized while the log is
// complete.
const replayCacheEntries = 8

const (
	evNew     uint8 = iota // logged, not yet processed into the head state
	evApplied              // applied to the head state; effect recorded
	evSkipped              // its commit was not an ancestor of the head when processed
)

// logEvent is one put or delete. Pointer-free: the key is an ordinal into the keyTable, and a
// delete's effect (the keys it unfiled) is a range of the shared effects arena.
type logEvent struct {
	seq    int64
	commit codec.Hash
	doc    codec.UUID
	key    uint32 // put: the key ordinal
	del    bool
	state  uint8
	added  bool // put: the document was not already filed under the key
	// cexact says cstep is exact: the step at which the event's commit became an ancestor of the
	// head (it is an ancestor of that step's cutoff and not of the one before). Otherwise cstep is
	// only an upper bound - a late event for a commit the head passed before the retained steps.
	cexact bool
	step   uint32 // the step (head cutoff) it was applied under; never decreases along the log
	cstep  uint32
	effN   uint32 // delete: how many keys it unfiled
	effOff uint64 // delete: absolute offset of those keys in the effects arena
}

type replayKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

// eventLog is the event window and head state both keyed index implementations share.
type eventLog struct {
	dag       *dag.InMemoryCommitDag
	maxEvents int // 0: never drop

	mu      sync.Mutex
	keys    *keyTable
	seq     int64
	events  []logEvent
	eff     []uint32 // effects arena: the keys each applied delete unfiled
	effBase uint64   // absolute offset of eff[0]
	// dropped is set once any event has been dropped (or the log was filled from a scan, which
	// has no history): from then on only the window can answer historical reads.
	dropped bool
	// pending holds the commit of every event the head skipped because its commit was not an
	// ancestor of the head. If such a commit later becomes one, replay would apply the event at
	// its original position - which the incremental state cannot do - so moving the head over it
	// resets the state. Empty in a server, where every event is logged under the new head.
	pending map[codec.Hash]struct{}

	head *headState
	// steps are the cutoffs the head state has been advanced to, oldest first, each a descendant
	// of the one before: step number stepBase+i is steps[i]. stepIdx maps a cutoff to its number.
	steps    []codec.Hash
	stepBase uint32
	stepIdx  map[codec.Hash]uint32
	// broken is set when the head state can neither be advanced nor replayed (the log is no
	// longer complete); every read then reports ErrHistoryUnavailable until a Clear or rebuild.
	broken bool

	replays     map[replayKey]*postingState
	replayOrder []replayKey
	scratch     []byte
}

// headState is the replay result at cutoff, maintained in place. Guarded by eventLog.mu.
type headState struct {
	cutoff codec.Hash
	epoch  uint64 // dag.AncestryEpoch it was built under
	pos    int    // events[:pos] have been processed
	st     *postingState
	// fresh is the latest step's new commits: ancestors of cutoff that were not ancestors of
	// the previous cutoff. Usually just cutoff.
	fresh map[codec.Hash]struct{}
	// inCutoff answers ancestry for older commits (a late event for a commit the head passed
	// long ago). Built on first use, dropped once the step's events are processed.
	inCutoff func(codec.Hash) bool
}

func newEventLog(d *dag.InMemoryCommitDag, opts MemoryOptions) *eventLog {
	l := &eventLog{dag: d, maxEvents: opts.maxEvents()}
	l.clearLocked()
	return l
}

func (l *eventLog) clearLocked() {
	l.keys = newKeyTable()
	l.seq = 0
	l.events = nil
	l.eff = nil
	l.effBase = 0
	l.dropped = false
	l.pending = make(map[codec.Hash]struct{})
	l.head = nil
	l.steps = nil
	l.stepBase = 0
	l.stepIdx = make(map[codec.Hash]uint32)
	l.broken = false
	l.replays = nil
	l.replayOrder = nil
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

func (l *eventLog) hasCommit(hash codec.Hash) bool {
	return l.dag.HasCommit(hash)
}

func (l *eventLog) appendPutLocked(entry Entry) {
	if l.broken {
		// Nothing can be answered until a rebuild, which clears the log first.
		return
	}
	l.seq++
	l.scratch = appendKeyEnc(l.scratch[:0], entry.Key)
	ord := l.keys.intern(l.scratch)
	l.events = append(l.events, logEvent{seq: l.seq, commit: entry.CommitHash, doc: entry.DocID, key: ord})
	l.afterAppendLocked()
}

func (l *eventLog) appendDeleteLocked(docID codec.UUID, atCommit codec.Hash) {
	if l.broken {
		return
	}
	l.seq++
	l.events = append(l.events, logEvent{seq: l.seq, commit: atCommit, doc: docID, del: true})
	l.afterAppendLocked()
}

// afterAppendLocked keeps the unprocessed tail bounded: only processed events can be dropped,
// so a namespace taking writes and no reads advances the head itself every so often.
func (l *eventLog) afterAppendLocked() {
	if l.maxEvents <= 0 {
		return
	}
	every := l.maxEvents / 4
	if every < 1 {
		every = 1
	}
	processed := 0
	if l.head != nil {
		processed = l.head.pos
	}
	if len(l.events)-processed < every {
		return
	}
	if h, err := l.dag.Head(); err == nil {
		l.advanceLocked(h)
	}
}

// ---------------------------------------------------------------------------------------------
// Head state
// ---------------------------------------------------------------------------------------------

// advanceLocked brings the head state to target and processes every logged event into it. It
// returns false when the state could not be brought there (and is now broken).
func (l *eventLog) advanceLocked(target codec.Hash) bool {
	if l.broken {
		return false
	}
	h := l.head
	if h == nil || h.epoch != l.dag.AncestryEpoch() {
		return l.resetHeadLocked(target)
	}
	if h.cutoff != target {
		fresh, ok := l.newAncestorsLocked(h.cutoff, target)
		if !ok {
			return l.resetHeadLocked(target)
		}
		for c := range fresh {
			if _, p := l.pending[c]; p {
				return l.resetHeadLocked(target)
			}
		}
		h.cutoff, h.fresh, h.inCutoff = target, fresh, nil
		l.pushStepLocked(target)
	}
	if h.pos < len(l.events) {
		l.processLocked()
	}
	l.truncateLocked()
	return true
}

// newAncestorsLocked returns the ancestors of to (itself included) that are not ancestors of
// from, or false when to does not descend from from. The common case - to reached from from by
// a short chain of single-parent commits - is a walk of that chain; anything else (a merge, a
// long gap) takes the two closures, once.
func (l *eventLog) newAncestorsLocked(from, to codec.Hash) (map[codec.Hash]struct{}, bool) {
	fresh := make(map[codec.Hash]struct{}, 1)
	for cur := to; cur != from; {
		if len(fresh) >= maxAdvanceSteps {
			return l.newAncestorsByClosure(from, to)
		}
		c, ok := l.dag.GetCommit(cur)
		if !ok || len(c.ParentHashes) != 1 {
			return l.newAncestorsByClosure(from, to)
		}
		fresh[cur] = struct{}{}
		cur = c.ParentHashes[0]
	}
	return fresh, true
}

func (l *eventLog) newAncestorsByClosure(from, to codec.Hash) (map[codec.Hash]struct{}, bool) {
	toAnc := l.dag.AncestorSet(to)
	if _, ok := toAnc[from]; !ok {
		return nil, false
	}
	for c := range l.dag.AncestorSet(from) {
		delete(toAnc, c)
	}
	return toAnc, true
}

func (l *eventLog) pushStepLocked(cutoff codec.Hash) {
	if _, seen := l.stepIdx[cutoff]; seen {
		// A chain of descendants cannot come back to a cutoff it passed; only a reset (which
		// clears the steps) moves the head backwards.
		return
	}
	l.stepIdx[cutoff] = l.stepBase + uint32(len(l.steps))
	l.steps = append(l.steps, cutoff)
}

func (l *eventLog) curStep() uint32 { return l.stepBase + uint32(len(l.steps)) - 1 }

// processLocked applies events[head.pos:] to the head state in log order.
func (l *eventLog) processLocked() {
	h := l.head
	step := l.curStep()
	for i := h.pos; i < len(l.events); i++ {
		e := &l.events[i]
		in, cstep, exact := l.inHeadLocked(e.commit)
		if !in {
			e.state = evSkipped
			l.pending[e.commit] = struct{}{}
			continue
		}
		e.state, e.step, e.cstep, e.cexact = evApplied, step, cstep, exact
		l.applyLocked(h.st, e)
	}
	h.pos = len(l.events)
	h.inCutoff = nil
}

// applyLocked applies an event to st and records its effect on the event.
func (l *eventLog) applyLocked(st *postingState, e *logEvent) {
	if e.del {
		start := len(l.eff)
		l.eff = st.removeDoc(e.doc, l.eff)
		e.effOff = l.effBase + uint64(start)
		e.effN = uint32(len(l.eff) - start)
		return
	}
	e.added = st.add(e.doc, e.key)
}

// effects returns the keys an applied delete unfiled.
func (l *eventLog) effects(e *logEvent) []uint32 {
	off := int(e.effOff - l.effBase)
	return l.eff[off : off+int(e.effN)]
}

// inHeadLocked reports whether commit is the head cutoff or one of its ancestors, and if so the
// step at which it became one (see logEvent.cstep).
func (l *eventLog) inHeadLocked(commit codec.Hash) (in bool, cstep uint32, exact bool) {
	h := l.head
	cur := l.curStep()
	if _, ok := h.fresh[commit]; ok {
		return true, cur, true
	}
	if s, ok := l.stepIdx[commit]; ok {
		// A step cutoff became an ancestor at its own step: it descends from every earlier one.
		return true, s, true
	}
	if h.inCutoff == nil {
		h.inCutoff = l.dag.AncestorTest(h.cutoff)
	}
	if !h.inCutoff(commit) {
		return false, 0, false
	}
	if h.fresh == nil || cur == l.stepBase {
		// The first step after a reset or restore: everything is at or before it.
		return true, cur, true
	}
	// Not new at this step, so an ancestor of the previous step's cutoff.
	return true, cur - 1, false
}

// resetHeadLocked rebuilds the head state at target by replaying the whole log, which is only
// possible while the log is complete. Otherwise the state is broken until a rebuild.
func (l *eventLog) resetHeadLocked(target codec.Hash) bool {
	l.head = nil
	l.steps, l.stepBase = nil, 0
	l.stepIdx = make(map[codec.Hash]uint32)
	if l.dropped {
		l.broken = true
		l.events, l.eff, l.effBase = nil, nil, 0
		l.pending = make(map[codec.Hash]struct{})
		return false
	}
	anc := l.dag.AncestorSet(target)
	h := &headState{cutoff: target, epoch: l.dag.AncestryEpoch(), st: newPostingState()}
	l.head = h
	l.pushStepLocked(target)
	l.pending = make(map[codec.Hash]struct{})
	l.eff, l.effBase = l.eff[:0], 0
	for i := range l.events {
		e := &l.events[i]
		if _, in := anc[e.commit]; !in {
			e.state = evSkipped
			l.pending[e.commit] = struct{}{}
			continue
		}
		e.state, e.step, e.cstep, e.cexact = evApplied, 0, 0, true
		l.applyLocked(h.st, e)
	}
	h.pos = len(l.events)
	l.truncateLocked()
	return true
}

// truncateLocked drops the oldest processed events beyond the window (down to three quarters
// of it, so the copying is amortized), with the effects only they referred to, and trims the
// step list the same way.
func (l *eventLog) truncateLocked() {
	if l.maxEvents <= 0 || l.head == nil {
		return
	}
	h := l.head
	keepFrom := l.stepBase
	if len(l.events) > l.maxEvents {
		drop := len(l.events) - l.maxEvents*3/4
		if drop > h.pos {
			drop = h.pos
		}
		if drop > 0 {
			for i := 0; i < drop; i++ {
				// Every retained step must be at least as new as every dropped applied event: a
				// historical read that stops at a retained step counts on every older effect
				// being one it sees.
				if e := &l.events[i]; e.state == evApplied && e.step > keepFrom {
					keepFrom = e.step
				}
			}
			l.dropped = true
			firstEff := l.effBase + uint64(len(l.eff))
			for i := drop; i < len(l.events); i++ {
				if e := &l.events[i]; e.del && e.state == evApplied {
					firstEff = e.effOff
					break
				}
			}
			cut := int(firstEff - l.effBase)
			l.eff = l.eff[:copy(l.eff, l.eff[cut:])]
			l.effBase = firstEff
			l.events = l.events[:copy(l.events, l.events[drop:])]
			h.pos -= drop
			l.replays, l.replayOrder = nil, nil
		}
	}
	if n := uint32(len(l.steps)); n > uint32(l.maxEvents) {
		if b := l.stepBase + n - uint32(l.maxEvents)*3/4; b > keepFrom {
			keepFrom = b
		}
	}
	if cur := l.curStep(); keepFrom > cur {
		keepFrom = cur
	}
	if k := int(keepFrom - l.stepBase); k > 0 {
		for _, c := range l.steps[:k] {
			delete(l.stepIdx, c)
		}
		l.steps = l.steps[:copy(l.steps, l.steps[k:])]
		l.stepBase = keepFrom
	}
	l.maybeCompactKeysLocked()
}

// maybeCompactKeysLocked rebuilds the key table without the keys that are empty at head and
// named by no retained event, once those are the majority: keys that come and go (a timestamp
// field, rewritten forever) must not accumulate.
func (l *eventLog) maybeCompactKeysLocked() {
	h := l.head
	n := l.keys.len()
	if n < 1024 || n < 2*h.st.live+1024 {
		return
	}
	used := make([]bool, n)
	for i := range l.events {
		if e := &l.events[i]; !e.del {
			used[e.key] = true
		}
	}
	for _, k := range l.eff {
		used[k] = true
	}
	keep := func(ord uint32) bool { return used[ord] || h.st.count(ord) > 0 }
	kept := 0
	for i := 0; i < n; i++ {
		if keep(uint32(i)) {
			kept++
		}
	}
	if n-kept < n/2 {
		return
	}
	remap := l.keys.compact(keep)
	for i := range l.events {
		if e := &l.events[i]; !e.del {
			e.key = remap[e.key]
		}
	}
	for i, k := range l.eff {
		l.eff[i] = remap[k]
	}
	h.st.remap(remap, l.keys.len())
	l.replays, l.replayOrder = nil, nil
}

// ---------------------------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------------------------

// keyedView is how a read at one cutoff is answered: st's buckets, with the effects of undo
// (event indices, newest first) reversed. undo is only ever non-empty with st the head state.
type keyedView struct {
	st   *postingState
	undo []int
}

// viewLocked resolves the cutoff a read is answered at: at, or the DAG head when at is nil. The
// head is read under the lock, so a head read is never turned into a historical one by commits
// that land while it waits.
func (l *eventLog) viewLocked(at *codec.Hash) (keyedView, error) {
	head, err := l.dag.Head()
	if err != nil {
		return keyedView{}, err
	}
	l.advanceLocked(head)
	cutoff := head
	if at != nil {
		cutoff = *at
	}
	if l.broken {
		return keyedView{}, ErrHistoryUnavailable
	}
	if h := l.head; h != nil {
		if h.pos < len(l.events) {
			l.processLocked()
			l.truncateLocked()
		}
		if h.cutoff == cutoff {
			return keyedView{st: h.st}, nil
		}
		if undo, ok := l.planLocked(cutoff); ok {
			return keyedView{st: h.st, undo: undo}, nil
		}
	}
	if !l.dropped {
		return keyedView{st: l.replayLocked(cutoff)}, nil
	}
	return keyedView{}, ErrHistoryUnavailable
}

// planLocked works out which applied events a read at x must not see, and reports whether
// undoing exactly those from the head state gives the replay result at x. It does when they are
// a suffix of the applied events: then undoing their effects, newest first, restores the state
// from before they were applied, which is what a replay at x builds.
//
// x is placed relative to the steps by walking back from it (at most maxAdvanceSteps commits)
// until every path reaches a retained step cutoff. If the newest step reached is j, an event
// whose commit became an ancestor of the head at a step <= j (cstep) is visible at x: its commit
// is an ancestor of step j's cutoff, which is an ancestor of x. One whose commit became an
// ancestor later is not an ancestor of step j's cutoff, so it is visible exactly when the walk
// visited its commit. An event whose cstep is only an upper bound (a late event for a commit the
// head passed before the retained steps) and is above j cannot be placed without a closure, and
// fails the plan. If the walk reaches no step at all it has seen x's whole ancestry, which places
// every event - but only while nothing has been dropped.
func (l *eventLog) planLocked(x codec.Hash) ([]int, bool) {
	jmax, stopped := int64(-1), false
	var visited map[codec.Hash]struct{}
	if j, ok := l.stepIdx[x]; ok {
		jmax, stopped = int64(j), true
	} else {
		visited = make(map[codec.Hash]struct{})
		queue := []codec.Hash{x}
		for len(queue) > 0 {
			c := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if j, ok := l.stepIdx[c]; ok {
				if int64(j) > jmax {
					jmax = int64(j)
				}
				stopped = true
				continue
			}
			if _, seen := visited[c]; seen {
				continue
			}
			visited[c] = struct{}{}
			if len(visited) > maxAdvanceSteps {
				return nil, false
			}
			if commit, ok := l.dag.GetCommit(c); ok {
				queue = append(queue, commit.ParentHashes...)
			}
		}
		// An event the head skipped, under a commit x sees, would be applied by a replay at x.
		for c := range visited {
			if _, p := l.pending[c]; p {
				return nil, false
			}
		}
	}
	if !stopped && l.dropped {
		return nil, false
	}
	var undo []int
	sawVisible := false
	for i := len(l.events) - 1; i >= 0; i-- {
		e := &l.events[i]
		if e.state != evApplied {
			continue
		}
		if stopped && int64(e.step) <= jmax {
			break
		}
		var visible bool
		switch {
		case !stopped:
			_, visible = visited[e.commit]
		case int64(e.cstep) <= jmax:
			visible = true
		case e.cexact:
			_, visible = visited[e.commit]
		default:
			return nil, false
		}
		if visible {
			sawVisible = true
			continue
		}
		if sawVisible {
			return nil, false
		}
		undo = append(undo, i)
	}
	return undo, true
}

// replayLocked builds the state at cutoff from the whole (complete) log, memoized.
func (l *eventLog) replayLocked(cutoff codec.Hash) *postingState {
	key := replayKey{cutoff: cutoff, seq: l.seq, ancestryVersion: l.dag.AncestryVersion()}
	if st, ok := l.replays[key]; ok {
		return st
	}
	anc := l.dag.AncestorSet(cutoff)
	st := newPostingState()
	var removed []uint32
	for i := range l.events {
		e := &l.events[i]
		if _, in := anc[e.commit]; !in {
			continue
		}
		if e.del {
			removed = st.removeDoc(e.doc, removed[:0])
		} else {
			st.add(e.doc, e.key)
		}
	}
	if l.replays == nil {
		l.replays = make(map[replayKey]*postingState, replayCacheEntries)
	}
	l.replays[key] = st
	l.replayOrder = append(l.replayOrder, key)
	if len(l.replayOrder) > replayCacheEntries {
		delete(l.replays, l.replayOrder[0])
		l.replayOrder = l.replayOrder[1:]
	}
	return st
}

// overlay is a view's undo, per key: for each (key, document) it touches, whether the document
// is filed there at the view's cutoff. only, when set, limits it to some keys.
func (l *eventLog) overlay(v keyedView, only func(ord uint32) bool) map[uint32]map[codec.UUID]bool {
	if len(v.undo) == 0 {
		return nil
	}
	out := make(map[uint32]map[codec.UUID]bool)
	set := func(ord uint32, doc codec.UUID, filed bool) {
		if only != nil && !only(ord) {
			return
		}
		m := out[ord]
		if m == nil {
			m = make(map[codec.UUID]bool)
			out[ord] = m
		}
		// Undo runs newest first, so the last write wins: the undo of the oldest event.
		m[doc] = filed
	}
	for _, i := range v.undo {
		e := &l.events[i]
		if !e.del {
			if e.added {
				set(e.key, e.doc, false)
			}
			continue
		}
		for _, k := range l.effects(e) {
			set(k, e.doc, true)
		}
	}
	return out
}

// applyOverlay returns base adjusted by delta, in UUID order. base is not modified.
func applyOverlay(base []codec.UUID, delta map[codec.UUID]bool) []codec.UUID {
	out := make([]codec.UUID, 0, len(base)+len(delta))
	for _, id := range base {
		if filed, ok := delta[id]; ok && !filed {
			continue
		}
		out = append(out, id)
	}
	for id, filed := range delta {
		if !filed {
			continue
		}
		if _, ok := slices.BinarySearchFunc(base, id, uuidCmp); ok {
			continue
		}
		out = append(out, id)
	}
	slices.SortFunc(out, uuidCmp)
	return out
}

func (l *eventLog) findKeyLocked(key Key) (uint32, bool) {
	l.scratch = appendKeyEnc(l.scratch[:0], key)
	return l.keys.find(l.scratch)
}

// lookup returns the doc ids filed under key at cutoff, in UUID order: stable, so a caller
// comparing two lookups, or handing one straight on as query rows, sees the same order each time.
func (l *eventLog) lookup(key Key, at *codec.Hash) ([]codec.UUID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.viewLocked(at)
	if err != nil {
		return nil, err
	}
	ord, ok := l.findKeyLocked(key)
	if !ok {
		return nil, nil
	}
	base := v.st.ids(ord)
	var out []codec.UUID
	if d := l.overlay(v, func(o uint32) bool { return o == ord })[ord]; d != nil {
		out = applyOverlay(base, d)
	} else {
		out = slices.Clone(base)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// lookupCount is len(lookup(key, cutoff)) without building the list.
func (l *eventLog) lookupCount(key Key, at *codec.Hash) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.viewLocked(at)
	if err != nil {
		return 0, err
	}
	ord, ok := l.findKeyLocked(key)
	if !ok {
		return 0, nil
	}
	n := v.st.count(ord)
	for doc, filed := range l.overlay(v, func(o uint32) bool { return o == ord })[ord] {
		in := v.st.contains(ord, doc)
		switch {
		case filed && !in:
			n++
		case !filed && in:
			n--
		}
	}
	return n, nil
}

// rangeScan returns the doc ids whose keys fall within [from, to] at cutoff (a nil bound is
// open), in key order and UUID order within a key, each id once, capped at limit.
//
// The order is what makes a limited range repeatable: an arbitrary order that changed between
// calls would change *which* documents a limit returns, so paging through an index could show a
// document twice or never.
func (l *eventLog) rangeScan(from, to Key, at *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.viewLocked(at)
	if err != nil {
		return nil, err
	}
	order := l.keys.order()
	lo, hi := 0, len(order)
	if from != nil {
		lo = l.keys.lowerBound(order, appendKeyEnc(nil, from))
	}
	if to != nil {
		hi = l.keys.upperBound(order, appendKeyEnc(nil, to))
	}
	if lo >= hi {
		return nil, nil
	}
	ov := l.overlay(v, nil)
	// A document filed under several keys in range is returned once, at its first key. Only a
	// multikey document can be, so a scalar index skips the bookkeeping.
	var seen map[codec.UUID]struct{}
	if len(v.st.docMany) > 0 || ov != nil {
		seen = make(map[codec.UUID]struct{})
	}
	var out []codec.UUID
	emit := func(ord uint32) bool {
		ids := v.st.ids(ord)
		if d := ov[ord]; d != nil {
			ids = applyOverlay(ids, d)
		}
		for _, id := range ids {
			if seen != nil {
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
			}
			out = append(out, id)
			if len(out) >= limit {
				return false
			}
		}
		return true
	}
	if ascending {
		for i := lo; i < hi; i++ {
			if !emit(order[i]) {
				break
			}
		}
	} else {
		for i := hi - 1; i >= lo; i-- {
			if !emit(order[i]) {
				break
			}
		}
	}
	return out, nil
}

// KeyBucket is one key together with the documents filed under it.
type KeyBucket struct {
	Key    Key
	DocIDs []codec.UUID
}

// bucketsCopy returns the buckets at cutoff as a caller-owned, key-ordered slice.
func (l *eventLog) bucketsCopy(at *codec.Hash) ([]KeyBucket, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.viewLocked(at)
	if err != nil {
		return nil, err
	}
	ov := l.overlay(v, nil)
	out := []KeyBucket{}
	for _, ord := range l.keys.order() {
		ids := v.st.ids(ord)
		if d := ov[ord]; d != nil {
			ids = applyOverlay(ids, d)
		} else {
			ids = slices.Clone(ids)
		}
		if len(ids) == 0 {
			continue
		}
		out = append(out, KeyBucket{Key: l.keys.key(ord), DocIDs: ids})
	}
	return out, nil
}

// needsRebuild brings the head state to the DAG head and reports whether it is broken.
func (l *eventLog) needsRebuild() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if head, err := l.dag.Head(); err == nil {
		l.advanceLocked(head)
	}
	return l.broken
}

// forgetHistory marks everything logged so far as describing only the head: the log was filled
// from a scan of the documents at head, so its events carry no history, and a read at an older
// commit must not replay them as if they did.
func (l *eventLog) forgetHistory() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if head, err := l.dag.Head(); err == nil {
		l.advanceLocked(head)
	}
	if l.broken || l.head == nil {
		return
	}
	l.dropped = true
	l.replays, l.replayOrder = nil, nil
}

// eventLogStats reports the log's size, for tests and diagnostics.
type eventLogStats struct {
	Events  int
	Steps   int
	Keys    int
	Live    int
	Dropped bool
	Broken  bool
}

func (l *eventLog) stats() eventLogStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := eventLogStats{Events: len(l.events), Steps: len(l.steps), Keys: l.keys.len(), Dropped: l.dropped, Broken: l.broken}
	if l.head != nil {
		s.Live = l.head.st.live
	}
	return s
}
