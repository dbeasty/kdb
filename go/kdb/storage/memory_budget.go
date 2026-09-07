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

// DefaultDocumentCacheFraction is the share of the hot-tier budget the
// in-memory document version store may hold. The rest of the budget is
// already spoken for by the block cache and the memtable, and the version
// store is the one of the three whose miss path is merely slower rather
// than incorrect - so it is the one that gives ground first.
const DefaultDocumentCacheFraction = 0.5

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
const DefaultCommitOpsFraction = 0.25

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
const DefaultMemtableFraction = 0.25

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
		return DefaultHotTierBytes / 4
	}
	return budget
}

// DefaultHistoryTreeFraction is the share of the hot-tier budget that
// historical document trees may hold.
const DefaultHistoryTreeFraction = 0.25

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
		return DefaultHotTierBytes / 4
	}
	return budget
}
