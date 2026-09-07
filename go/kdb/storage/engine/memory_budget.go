package engine

import (
	"github.com/limidus/kdb/go/kdb/storage"
)

// coldLoadGrowthFactor is how much more than it currently holds a namespace asks for when it has
// missed reads since the last time it was measured. A miss means the live working set did not
// fit, and resident bytes alone cannot say that - a namespace pinned at its ceiling reports the
// same number whether the ceiling is exactly right or far too small. Deliberately a flat bump
// rather than a function of the miss count: a busy namespace should climb over several passes,
// not win the whole pool on one unlucky tick.
const coldLoadGrowthFactor = 0.5

// memtableFlushBytes is the threshold maybeFlushMemtable actually uses: the arbiter's most
// recent share if there is one, and otherwise whatever the open-time config resolved to.
func (e *ServerEngine) memtableFlushBytes() int64 {
	if v := e.memtableFlushOverride.Load(); v > 0 {
		return v
	}
	return storage.ResolvedMemtableFlushBytes(e.config)
}

// SetMemoryBudgetBytes re-cuts this namespace's hot tier to budgetBytes, splitting it across the
// three things the engine holds by the same fractions the open path uses, so an arbitrated
// namespace and a standalone one are proportioned identically and only the total differs.
//
// The version store is the one piece this will not touch without a cold loader installed. Its
// budget is what lets it drop versions, and dropping a version that nothing can re-read is data
// loss, not eviction - which is exactly why SetColdLoader installs the loader and the budget
// together. An engine with no delta log behind it (TargetInMemory, TargetGPU) therefore keeps an
// unbounded version store no matter what the arbiter says, and only its trees and memtable move.
//
// Safe to call while the namespace is serving traffic. Shrinking evicts synchronously inside the
// stores' own locks, which is why the arbiter calls this on a slow timer and only when the share
// has moved enough to be worth it.
func (e *ServerEngine) SetMemoryBudgetBytes(budgetBytes int64) {
	if e == nil || budgetBytes <= 0 {
		return
	}
	// A sub-budget the caller set explicitly is a pin, not a default, and re-deriving it from a
	// share would be the same defect the Resolved* helpers already avoid: an explicit setting
	// quietly replaced by one computed from MemoryBudgetBytes. Honouring it at open and then
	// overwriting it on the arbiter's first pass is worse than never honouring it, because it
	// looks correct for as long as anyone watches.
	//
	// Caught by TestExplicitRetentionBudgetsAreHonoured, which has guarded exactly this since
	// before there was an arbiter to break it.
	if e.config.DocumentCacheBytes <= 0 && e.coldLoader.Load() != nil {
		e.docsByHash.SetBudget(int64(float64(budgetBytes) * storage.DefaultDocumentCacheFraction))
	}
	if e.config.HistoryTreeCacheBytes <= 0 && e.treesByHash != nil {
		e.treesByHash.SetBudget(int64(float64(budgetBytes) * storage.DefaultHistoryTreeFraction))
	}
	if e.config.MemtableFlushBytes <= 0 {
		e.memtableFlushOverride.Store(int64(float64(budgetBytes) * storage.DefaultMemtableFraction))
	}
}

// MemoryDemandBytes is what this namespace would like to hold: what it holds now, plus a bump
// when it has missed reads since the last call.
//
// Reporting resident bytes alone would be a feedback loop with the wrong sign - a namespace
// squeezed down to its floor holds little, would therefore report little demand, and would be
// kept at its floor forever. The cold-load bump is what breaks that: a namespace that is
// re-reading versions from the delta log asks for more than it has, every pass, until it stops
// missing.
//
// Not on any hot path; it walks the stores' shards. Called once per namespace per rebalance.
func (e *ServerEngine) MemoryDemandBytes() int64 {
	if e == nil {
		return 0
	}
	var resident int64
	if e.docsByHash != nil {
		resident += e.docsByHash.ResidentBytes()
	}
	if e.treesByHash != nil {
		resident += e.treesByHash.ResidentBytes()
	}
	if e.memTable != nil {
		resident += e.memTable.SizeBytes()
	}
	now := e.coldLoads.Load()
	missed := now > e.lastColdLoads.Load()
	e.lastColdLoads.Store(now)
	if missed {
		return resident + int64(float64(resident)*coldLoadGrowthFactor)
	}
	return resident
}
