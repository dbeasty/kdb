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
