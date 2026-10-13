package storage

import (
	"math"
	"testing"
)

// The structures that hold a namespace's memory each get a share of its hot-tier budget; together
// the shares must be the budget, not more. Add a new share here when you add one.
func TestDefaultBudgetFractionsSumToOne(t *testing.T) {
	shares := map[string]float64{
		"document cache":    DefaultDocumentCacheFraction,
		"commit operations": DefaultCommitOpsFraction,
		"memtable":          DefaultMemtableFraction,
		"sstable index":     DefaultSSTableIndexFraction,
		"block cache":       DefaultBlockCacheFraction,
		"history trees":     DefaultHistoryTreeFraction,
		"delta frames":      DefaultDeltaFrameCacheFraction,
	}
	sum := 0.0
	for name, f := range shares {
		if f <= 0 || f >= 1 {
			t.Errorf("%s: share %v is not a fraction", name, f)
		}
		sum += f
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("budget shares sum to %v; a namespace whose caches all fill would hold more than its budget", sum)
	}
}

// With no budget resolved, each structure falls back to its share of the default hot tier, so the
// fallbacks fit the default budget too.
func TestFallbackSharesFitTheDefaultHotTier(t *testing.T) {
	cfg := StorageEngineConfig{}
	total := ResolvedMemtableFlushBytes(cfg) + ResolvedSSTableIndexCacheBytes(cfg) + ResolvedBlockCacheBytes(cfg) +
		ResolvedHistoryTreeBytes(cfg) + ResolvedDeltaFrameCacheBytes(cfg)
	if total > DefaultHotTierBytes {
		t.Fatalf("fallback shares total %d, over the %d default hot tier", total, DefaultHotTierBytes)
	}
}
