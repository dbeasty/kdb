package server

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
)

// DefaultPremergeRetries is how many times an Upsert whose premerge went stale goes back to redo
// it off the write gate before the gate merges it itself. Cassandra's AtomicBTreePartition does
// the same with its compare-and-swap: rebuild off the lock and retry, then, after repeated
// contention, take the lock and apply in place - here the merge under the gate (see Upsert).
//
// One retry. Merging onto head, a retry waited behind the same queue as the first attempt, and on
// zipfian-hot documents two thirds of retries went stale again. Built on the write queued ahead
// (see premerge), under 1% of premerges are stale at update@32 and about 70% of their retries are
// current; a second retry would save little and cost each such write another trip through the
// queue.
const DefaultPremergeRetries = 1

// errStalePremerge is how Upsert's work at the gate front hands the gate back: the document its
// premerge was built from is no longer the one at head, and the merge is to be redone off the gate.
// Never returned to a caller.
var errStalePremerge = errors.New("kdb server: upsert premerge is stale")

// premergeStats counts what became of each Upsert's premerge at the gate front.
type premergeStats struct {
	// current: the premerge was still current and was installed as built.
	current atomic.Int64
	// retried: it was stale and went back to be redone off the gate.
	retried atomic.Int64
	// mergedAtGate: it was stale with no retry left, so the gate merged the patch itself.
	mergedAtGate atomic.Int64
	// none: no premerge could be built (no head yet, a body that does not merge, storage that
	// cannot say which version a tree holds); the gate did all the work.
	none atomic.Int64
}

// PremergeOutcomes is premergeStats as plain numbers, for /metrics and tests.
type PremergeOutcomes struct {
	Current, Retried, MergedAtGate, None int64
}

// PremergeOutcomes reports what became of this runtime's Upsert premerges so far.
func (s *KdbServerRuntime) PremergeOutcomes() PremergeOutcomes {
	return PremergeOutcomes{
		Current:      s.premerges.current.Load(),
		Retried:      s.premerges.retried.Load(),
		MergedAtGate: s.premerges.mergedAtGate.Load(),
		None:         s.premerges.none.Load(),
	}
}

// premergeRetryLimit is DefaultPremergeRetries unless a test set another (SetPremergeRetriesForTest).
func (s *KdbServerRuntime) premergeRetryLimit() int {
	if n := s.premergeRetries.Load(); n != nil {
		return int(*n)
	}
	return DefaultPremergeRetries
}

// SetPremergeRetriesForTest overrides DefaultPremergeRetries for this runtime. Tests only.
func (s *KdbServerRuntime) SetPremergeRetriesForTest(n int) {
	v := int32(n)
	s.premergeRetries.Store(&v)
}

// SetAfterPremergeHookForTest installs a hook run after each of an Upsert's premerges and before it
// queues for the write gate, with the attempt number (0 for the first). A test uses it to write
// the same document in between, which makes the premerge stale. Tests only.
func (s *KdbServerRuntime) SetAfterPremergeHookForTest(hook func(attempt int)) {
	if hook == nil {
		s.afterPremerge.Store(nil)
		return
	}
	s.afterPremerge.Store(&hook)
}

func (s *KdbServerRuntime) runAfterPremergeHook(attempt int) {
	if h := s.afterPremerge.Load(); h != nil {
		(*h)(attempt)
	}
}

// inFlightPremerges holds, for each document, the newest premerge whose Upsert has not finished
// yet - the version the document will have once the writes queued for it commit, which is what the
// next Upsert of it merges onto (see premerge). Sharded: every Upsert passes through it twice.
type inFlightPremerges struct {
	shards [16]inFlightShard
}

type inFlightShard struct {
	mu sync.Mutex
	m  map[codec.UUID]*inFlightPremerge
}

// inFlightPremerge is one Upsert's premerge while that Upsert is unfinished. Its fields other
// than pre are guarded by the shard's mutex.
type inFlightPremerge struct {
	pre *premergedUpsert
	// builtOn is the in-flight premerge pre was merged onto. Cut once this one commits - from then
	// on the version at head is this one's - and kept if it does not, so the writes built on it can
	// tell they are built on a version that will never exist.
	builtOn   *inFlightPremerge
	finished  bool
	committed bool
}

// maxChainWalk bounds how far back sound looks; past it a writer merges onto head.
const maxChainWalk = 8

// failedAncestor is the first premerge on p's chain - p itself included - that finished without
// committing, or nil when the chain is sound back to a committed write or to head.
func failedAncestor(p *inFlightPremerge) *inFlightPremerge {
	for x, n := p, 0; x != nil && n < maxChainWalk; x, n = x.builtOn, n+1 {
		if x.finished {
			if x.committed {
				return nil
			}
			return x
		}
	}
	return nil
}

// sound is the newest unfinished premerge at or behind p whose chain is sound: a write whose result
// can still become the document's version. A write built - however indirectly - on one that went
// stale or failed is skipped, and so is that one; what it was built on is tried next. A write
// that already committed is no base either - writes after it may have committed too - so reaching
// one means building on head.
func sound(p *inFlightPremerge) *inFlightPremerge {
	for n := 0; p != nil && n < maxChainWalk; n++ {
		if p.finished && p.committed {
			return nil
		}
		bad := failedAncestor(p)
		if bad == nil {
			return p
		}
		p = bad.builtOn
	}
	return nil
}

func (f *inFlightPremerges) shard(id codec.UUID) *inFlightShard {
	return &f.shards[uint64(id.LSB)%uint64(len(f.shards))]
}

// newest is the newest premerge of id a write can build on (see sound), or nil: build on head.
func (f *inFlightPremerges) newest(id codec.UUID) *inFlightPremerge {
	sh := f.shard(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sound(sh.m[id])
}

// push records pre, merged onto builtOn (nil: onto the version at head), as the newest premerge of
// id.
func (f *inFlightPremerges) push(id codec.UUID, pre *premergedUpsert, builtOn *inFlightPremerge) *inFlightPremerge {
	sh := f.shard(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if builtOn != nil && builtOn.finished && builtOn.committed {
		builtOn = nil
	}
	p := &inFlightPremerge{pre: pre, builtOn: builtOn}
	if sh.m == nil {
		sh.m = make(map[codec.UUID]*inFlightPremerge)
	}
	sh.m[id] = p
	return p
}

// done marks p's Upsert finished: committed, or stale or failed - in which case nothing built on
// p is current any more, which newest then knows. The map entry goes once p and everything it
// can point back to is no use to a new write.
func (f *inFlightPremerges) done(id codec.UUID, p *inFlightPremerge, committed bool) {
	sh := f.shard(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	p.finished, p.committed = true, committed
	if committed {
		// Unlinked, so a long run of writes to one document never holds the ones already finished.
		p.builtOn = nil
	}
	if sh.m[id] != p {
		return
	}
	if next := sound(p); next != nil {
		sh.m[id] = next
		return
	}
	delete(sh.m, id)
}
