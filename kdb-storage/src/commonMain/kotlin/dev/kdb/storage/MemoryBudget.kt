package dev.kdb.storage

/**
 * Used when neither an absolute nor a percentage hot-tier budget is
 * configured. Deliberately small (per the reengineering plan's Phase 5:
 * "small by default, configurable how much we use, or just constrained
 * by availability") so a namespace doesn't quietly claim a large chunk
 * of host memory unless someone opts in. Mirrors Go's
 * storage.DefaultHotTierBytes.
 */
public const val DEFAULT_HOT_TIER_BYTES: Long = 128L * 1024 * 1024 // 128MiB

/**
 * Configures how much memory the storage engine's hot tier (block
 * cache, memtable) is allowed to use. At most one of [fixedBytes] /
 * [percentOfAvailable] should be set; if both are zero,
 * [DEFAULT_HOT_TIER_BYTES] applies.
 */
public data class HotTierMemoryConfig(
    /** If > 0, used directly as an absolute ceiling. */
    val fixedBytes: Long = 0,
    /**
     * If > 0 and [fixedBytes] == 0, sizes the hot tier as this
     * percentage (0-100) of total system memory as reported by the
     * platform (see [totalSystemMemoryBytes]). Falls back to
     * [DEFAULT_HOT_TIER_BYTES] if the platform's memory can't be
     * determined (e.g. browser/JS targets).
     */
    val percentOfAvailable: Double = 0.0,
)

/** Platform-specific total physical memory in bytes, or null if not determinable on this target. */
public expect fun totalSystemMemoryBytes(): Long?

/** Computes the effective hot-tier byte budget for [config]. Safe to call with a default-constructed config. */
public fun resolveHotTierBytes(config: HotTierMemoryConfig): Long {
    if (config.fixedBytes > 0) return config.fixedBytes
    if (config.percentOfAvailable > 0) {
        val total = totalSystemMemoryBytes()
        if (total != null && total > 0) {
            val pct = config.percentOfAvailable.coerceAtMost(100.0)
            val budget = (total * pct / 100).toLong()
            if (budget > 0) return budget
        }
    }
    return DEFAULT_HOT_TIER_BYTES
}

/**
 * Validates [config], returning a descriptive error message rather than
 * silently clamping, so misconfiguration is caught at startup instead of
 * producing a surprising budget. Returns null when valid.
 */
public fun validateHotTierMemoryConfig(config: HotTierMemoryConfig): String? {
    if (config.fixedBytes < 0) {
        return "hot tier fixedBytes must be >= 0, got ${config.fixedBytes}"
    }
    if (config.percentOfAvailable < 0 || config.percentOfAvailable > 100) {
        return "hot tier percentOfAvailable must be in [0, 100], got ${config.percentOfAvailable}"
    }
    return null
}
