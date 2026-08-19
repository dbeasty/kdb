package dev.kdb.storage

/**
 * Selects how strongly a namespace's writes are synced to disk before
 * writeBlob returns. See docs/benchmarks/phase0-baseline.md Phase 4 for
 * the throughput/durability tradeoff this exists to make explicit and
 * per-namespace rather than a single global default. Mirrors Go's
 * storage.Durability.
 */
public enum class Durability {
    /** fsyncs (via group commit) before every write acknowledgement. Default. */
    SYNC,

    /**
     * Acknowledges writes once appended to the WAL in memory, syncing on
     * a background timer instead of per-write. A crash can lose up to
     * one sync interval of acknowledged writes.
     */
    ASYNC,

    /**
     * Never syncs the WAL to disk; durability is whatever periodic
     * checkpointing the caller layers on top. Intended for namespaces
     * that treat data as reconstructable/ephemeral.
     */
    MEMORY_ONLY,
}

/**
 * Layer 4 shim instance bundled with runtime config ([Component 9] §G).
 * Production wiring injects concrete [PlatformIoShim] implementations per platform.
 */
public data class StorageEngineConfig(
    val pageTargetSizeBytes: Long = 8L * 1024 * 1024,
    val pageMaxSizeBytes: Long = 16L * 1024 * 1024,
    /**
     * If > 0, used directly as the hot-tier byte budget (block cache +
     * memtable sizing). If <= 0 (the default), resolved from
     * [hotTierMemory] instead (see [resolveHotTierBytes]) - small by
     * default, configurable via an absolute value or a percentage of
     * total system memory. Set this field directly only when you want
     * to bypass that resolution entirely.
     */
    val globalMemoryBudgetBytes: Long = 0,
    /** Configures the hot-tier budget when [globalMemoryBudgetBytes] is left at zero. */
    val hotTierMemory: HotTierMemoryConfig = HotTierMemoryConfig(),
    val compressionCodec: CompressionCodec = CompressionCodec.ZSTD,
    val defaultIndexRetention: IndexRetention = IndexRetention.EVICTABLE,
    val ioShim: PlatformIoShim,
    val debugSidecar: DebugSidecarConfig? = null,
    /** Zero value SYNC preserves prior behavior. */
    val durability: Durability = Durability.SYNC,
    /** Background fsync period used when [durability] is ASYNC. Null uses a built-in default. */
    val asyncSyncIntervalMillis: Long? = null,
) {
    /** [globalMemoryBudgetBytes] if set, otherwise resolved from [hotTierMemory]. */
    public fun resolvedGlobalMemoryBudgetBytes(): Long =
        if (globalMemoryBudgetBytes > 0) globalMemoryBudgetBytes else resolveHotTierBytes(hotTierMemory)
}
