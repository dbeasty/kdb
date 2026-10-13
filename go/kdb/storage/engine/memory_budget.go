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
	if e.blobStore != nil {
		e.blobStore.SetIndexCacheBudget(int64(float64(budgetBytes) * storage.DefaultSSTableIndexFraction))
		e.blobStore.SetBlockCacheBudget(int64(float64(budgetBytes) * storage.DefaultBlockCacheFraction))
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
// "What it holds" is the stores a share governs, at their real footprint: the version store
// (arena, dead bytes awaiting compaction, slot index and placeholder pins - the pinned live
// bodies were always in here), the evictable part of the tree store, both memtable
// generations, the SSTable block cache, and the parsed SSTable indexes, which
// SetMemoryBudgetBytes bounds but which were left out of the demand until now. The live trie, pinned trees and the commit graph are
// reported by MemoryBreakdown but kept out of the demand: no share can shrink them, so counting
// them would only move pool between namespaces without changing what either can evict.
//
// Not on any hot path; it takes each store's locks once. Called once per namespace per
// rebalance.
func (e *ServerEngine) MemoryDemandBytes() int64 {
	if e == nil {
		return 0
	}
	b := e.MemoryBreakdown()
	resident := b.DocumentBodies + b.DocumentIndex + b.HistoryTrees + b.Memtable + b.SSTableIndexes + b.SSTableBlockCache
	now := e.coldLoads.Load()
	missed := now > e.lastColdLoads.Load()
	e.lastColdLoads.Store(now)
	if missed {
		return resident + int64(float64(resident)*coldLoadGrowthFactor)
	}
	return resident
}

// MemoryBreakdown is what a namespace's storage engine holds in memory, by structure. Every
// figure is maintained by its store or derived from lengths, so producing it walks nothing.
type MemoryBreakdown struct {
	// DocumentBodies is the version store's arena: live bodies, bodies evicted or superseded
	// but not yet compacted away, and the unfilled tail of each shard's current chunk.
	DocumentBodies int64
	// DocumentBodiesLive is the part of DocumentBodies still referenced.
	DocumentBodiesLive int64
	// DocumentIndex is the version store's records and indexes, including the placeholder pins
	// for live versions not yet loaded.
	DocumentIndex int64
	// DocumentVersions and DocumentPlaceholders count resident versions and pinned-but-unloaded
	// ones.
	DocumentVersions, DocumentPlaceholders int64
	// LiveTree is the memory of the slab holding the live tree's trie (document.SlabBytes): the
	// live tree, the history records it shares that slab with, and free slots awaiting reuse.
	// Held outside every budget: it is the namespace's current state.
	LiveTree int64
	// HistoryTrees is the tree store's evictable charge: what the history budget governs.
	HistoryTrees int64
	// PinnedTrees is the tree store's charge for trees pinned by in-flight writers.
	PinnedTrees int64
	// Memtable is both memtable generations' footprint.
	Memtable int64
	// SSTableIndexes is the parsed SSTable footer indexes held by the blob store.
	SSTableIndexes int64
	// SSTableBlockCache is the decoded SSTable blocks the blob store's cache holds.
	SSTableBlockCache int64
}

// Total sums every byte figure (not the counts, and not DocumentBodiesLive, which is part of
// DocumentBodies).
func (b MemoryBreakdown) Total() int64 {
	return b.DocumentBodies + b.DocumentIndex + b.LiveTree + b.HistoryTrees + b.PinnedTrees + b.Memtable + b.SSTableIndexes +
		b.SSTableBlockCache
}

// MemoryBreakdown reports what this engine holds, by structure. See the type.
func (e *ServerEngine) MemoryBreakdown() MemoryBreakdown {
	var b MemoryBreakdown
	if e == nil {
		return b
	}
	if e.docsByHash != nil {
		st := e.docsByHash.Stats()
		b.DocumentBodies = st.ArenaBytes
		b.DocumentBodiesLive = st.BodyLiveBytes
		b.DocumentIndex = st.IndexBytes
		b.DocumentVersions = st.Versions
		b.DocumentPlaceholders = st.Placeholders
	}
	if s := e.latestTree.Load(); s != nil {
		if b.LiveTree = s.tree.SlabBytes(); b.LiveTree == 0 {
			b.LiveTree = treeBytesPerDoc * int64(s.tree.Size())
		}
	}
	if e.treesByHash != nil {
		// Evictable rather than resident: see boundedTreeStore.EvictableBytes.
		b.HistoryTrees = e.treesByHash.EvictableBytes()
		b.PinnedTrees = e.treesByHash.PinnedBytes()
	}
	if e.memTable != nil {
		b.Memtable = e.memTable.FootprintBytes()
	}
	if e.blobStore != nil {
		b.SSTableIndexes = e.blobStore.IndexCacheBytes()
		b.SSTableBlockCache = e.blobStore.BlockCacheBytes()
	}
	return b
}
