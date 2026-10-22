package storage

import "fmt"

// DefaultHotTierBytes is used when neither an absolute nor a percentage
// hot-tier budget is configured. Deliberately small (per the reengineering
// plan's Phase 5: "small by default, configurable how much we use, or
// just constrained by availability") so a namespace doesn't quietly claim
// a large chunk of host memory unless someone opts in.
const DefaultHotTierBytes int64 = 128 << 20 // 128MiB

// HotTierMemoryConfig configures how much memory the storage engine's
// hot tier (block cache, memtable) is allowed to use. At most one of
// FixedBytes / PercentOfAvailable should be set; if both are zero,
// DefaultHotTierBytes applies.
type HotTierMemoryConfig struct {
	// FixedBytes, if > 0, is used directly as an absolute ceiling.
	FixedBytes int64
	// PercentOfAvailable, if > 0 and FixedBytes == 0, sizes the hot tier
	// as this percentage (0-100) of total system memory as reported by
	// the platform (see system_memory_*.go). Falls back to
	// DefaultHotTierBytes if the platform's memory can't be determined.
	PercentOfAvailable float64
}

// ResolveHotTierBytes computes the effective hot-tier byte budget for
// cfg. Safe to call with a zero-value HotTierMemoryConfig (returns
// DefaultHotTierBytes).
func ResolveHotTierBytes(cfg HotTierMemoryConfig) int64 {
	if cfg.FixedBytes > 0 {
		return cfg.FixedBytes
	}
	if cfg.PercentOfAvailable > 0 {
		total, err := totalSystemMemoryBytes()
		if err == nil && total > 0 {
			pct := cfg.PercentOfAvailable
			if pct > 100 {
				pct = 100
			}
			budget := int64(float64(total) * pct / 100)
			if budget > 0 {
				return budget
			}
		}
	}
	return DefaultHotTierBytes
}

// ValidateHotTierMemoryConfig reports a descriptive error for
// out-of-range values rather than silently clamping, so misconfiguration
// is caught at startup instead of producing a surprising budget.
func ValidateHotTierMemoryConfig(cfg HotTierMemoryConfig) error {
	if cfg.FixedBytes < 0 {
		return fmt.Errorf("hot tier FixedBytes must be >= 0, got %d", cfg.FixedBytes)
	}
	if cfg.PercentOfAvailable < 0 || cfg.PercentOfAvailable > 100 {
		return fmt.Errorf("hot tier PercentOfAvailable must be in [0, 100], got %v", cfg.PercentOfAvailable)
	}
	return nil
}

// TotalSystemMemoryBytes reports the host's total physical memory, or an error if this platform
// has no implementation (see system_memory_*.go). Exported so callers outside this package -
// notably server.DetectMemoryBudgetBytes, which needs a sane default memory budget on a host
// with no cgroup limit to read - do not have to duplicate the per-platform detection.
func TotalSystemMemoryBytes() (int64, error) { return totalSystemMemoryBytes() }

// The Default*Fraction shares below split one namespace's hot-tier budget between the structures
// that hold memory, and they sum to exactly 1.0 (TestDefaultBudgetFractionsSumToOne): every cache
// filling to its share at once still fits the budget. They summed to 1.75 before, so a namespace
// under full pressure could hold 75% more than it was given. The split favours what serves the
// latest data - live documents and the write path's memtable - over history (commit operations,
// historical trees), which can be read back from the log.

// defaultShare is fraction of DefaultHotTierBytes: the fallback when no budget resolves.
func defaultShare(fraction float64) int64 { return int64(float64(DefaultHotTierBytes) * fraction) }

// DefaultDocumentCacheFraction is the share of the hot-tier budget the
// in-memory document version store may hold. The rest of the budget is
// already spoken for by the block cache and the memtable, and the version
// store is the one of the three whose miss path is merely slower rather
// than incorrect - so it is the one that gives ground first.
const DefaultDocumentCacheFraction = 0.30

// ResolvedDocumentCacheBytes is how many bytes of document versions the
// storage engine may hold resident before it starts evicting the oldest
// and re-reading them from the delta log on demand.
//
// Versions reachable from the current tree are pinned and do not answer to
// this budget (see shardedDocByHashStore), so a namespace whose live data
// exceeds it stays correct and stays fast - it simply holds more than this
// number. What the budget actually bounds is how much *history* is kept in
// memory, which before it was bounded was everything, forever.
func ResolvedDocumentCacheBytes(cfg StorageEngineConfig) int64 {
	if cfg.DocumentCacheBytes > 0 {
		return cfg.DocumentCacheBytes
	}
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultDocumentCacheFraction)
	if budget < 0 {
		return 0
	}
	return budget
}

// DefaultCommitOpsFraction is the share of the hot-tier budget the commit
// DAG may hold in commit operations. Smaller than the document version
// store's share because the two hold the same bytes for different readers
// - the version store answers ordinary document reads, while these answer
// history walks, which are rarer and already tolerate I/O.
const DefaultCommitOpsFraction = 0.10

// ResolvedCommitOpsBytes is how many bytes of commit operations the DAG
// may keep resident before it starts dropping the oldest and re-reading
// them from the delta log on demand. See dag.SetOperationsLoader.
func ResolvedCommitOpsBytes(cfg StorageEngineConfig) int64 {
	if cfg.CommitOpsBytes > 0 {
		return cfg.CommitOpsBytes
	}
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultCommitOpsFraction)
	if budget < 0 {
		return 0
	}
	return budget
}

// DefaultMemtableFraction is the share of the hot-tier budget the
// in-memory blob generation may reach before being flushed to an SSTable.
const DefaultMemtableFraction = 0.20

// ResolvedMemtableFlushBytes is how large the memtable may grow before it
// is written out.
//
// It needs a bound at all because the memtable has none of its own: it
// grows on every Put and shrinks only when something calls Flush. Under
// the objects history strategy every document version written goes through
// it, so without this a writing session held every version it had written
// in memory until close - the exact shape of unbounded retention this
// tier's budgets exist to prevent.
func ResolvedMemtableFlushBytes(cfg StorageEngineConfig) int64 {
	if cfg.MemtableFlushBytes > 0 {
		return cfg.MemtableFlushBytes
	}
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultMemtableFraction)
	if budget <= 0 {
		return defaultShare(DefaultMemtableFraction)
	}
	return budget
}

// DefaultSSTableIndexFraction is the share of the hot-tier budget the SSTable store may spend
// on parsed table indexes. Small, because an index costs about 128 bytes per key and compaction
// keeps the table count low; the cache keeps the most recently used index even over budget, so
// a namespace whose single table is larger than this still never re-parses it per probe.
const DefaultSSTableIndexFraction = 0.08

// ResolvedSSTableIndexCacheBytes is how many bytes of parsed SSTable footer indexes a namespace
// keeps resident (see sstable.LsmBlobStore.SetIndexCacheBudget).
func ResolvedSSTableIndexCacheBytes(cfg StorageEngineConfig) int64 {
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultSSTableIndexFraction)
	if budget <= 0 {
		return defaultShare(DefaultSSTableIndexFraction)
	}
	return budget
}

// DefaultBlockCacheFraction is the share of the hot-tier budget the SSTable block cache may hold
// in decoded blocks (see sstable.BlockCache). It was a quarter of the budget, reserved for years
// while nothing read the cache; since the cache serves lookups it gets a share in the 1.0 split.
const DefaultBlockCacheFraction = 0.15

// ResolvedBlockCacheBytes is how many bytes of decoded SSTable blocks a namespace keeps resident.
func ResolvedBlockCacheBytes(cfg StorageEngineConfig) int64 {
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultBlockCacheFraction)
	if budget <= 0 {
		return defaultShare(DefaultBlockCacheFraction)
	}
	return budget
}

// DefaultHistoryTreeFraction is the share of the hot-tier budget that
// historical document trees may hold. It gave half of its tenth to the
// secondary indexes' as-of history (DefaultIndexHistoryFraction).
const DefaultHistoryTreeFraction = 0.05

// DefaultIndexHistoryFraction is the share of the hot-tier budget the namespace's secondary
// indexes may hold together for reads at commits behind the head (index.Registry.SetHistoryBudget).
// Each index used to bound only its own window, about 6.5 MB, so qtask's 51 indexes held about
// 330 MB outside the budget.
const DefaultIndexHistoryFraction = 0.05

// ResolvedHistoryTreeBytes is how many bytes of historical document trees
// stay resident before the least recently used are evicted and obtained
// again on demand.
//
// The *current* tree is not subject to this: the engine serves it from an
// atomic snapshot outside the store, so cache pressure can never slow a
// normal read or evict what the write path is building on. What this
// bounds is history, which before it was bounded was every tree the
// namespace had ever produced - about 8.6KB per commit whatever the size
// of the documents, because the document trie is fixed-depth 32 and
// successive versions share none of the nodes on the path they change.
func ResolvedHistoryTreeBytes(cfg StorageEngineConfig) int64 {
	if cfg.HistoryTreeCacheBytes > 0 {
		return cfg.HistoryTreeCacheBytes
	}
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultHistoryTreeFraction)
	if budget <= 0 {
		return defaultShare(DefaultHistoryTreeFraction)
	}
	return budget
}

// DefaultDeltaFrameCacheFraction is the share of the hot-tier budget a namespace may spend on
// decompressed delta frames (delta.FrameCache): Cassandra's ChunkCache, sitting beside the SSTable
// block cache (DefaultBlockCacheFraction) as the cache of the other on-disk structure reads go to.
// About half the block cache's share, because a frame read on a miss costs one pread and one
// decompress rather than an index probe as well. Like the other shares it is a cap, not a
// reservation: the cache holds only frames cold reads have actually asked for.
const DefaultDeltaFrameCacheFraction = 0.07

// ResolvedDeltaFrameCacheBytes is how many bytes of decompressed delta frames a namespace keeps
// for cold reads. Zero means no cache.
func ResolvedDeltaFrameCacheBytes(cfg StorageEngineConfig) int64 {
	if cfg.DeltaFrameCacheBytes < 0 {
		return 0
	}
	if cfg.DeltaFrameCacheBytes > 0 {
		return cfg.DeltaFrameCacheBytes
	}
	budget := int64(float64(cfg.ResolvedGlobalMemoryBudgetBytes()) * DefaultDeltaFrameCacheFraction)
	if budget <= 0 {
		return defaultShare(DefaultDeltaFrameCacheFraction)
	}
	return budget
}
