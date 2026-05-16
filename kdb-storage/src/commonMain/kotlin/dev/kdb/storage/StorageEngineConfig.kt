package dev.kdb.storage

/**
 * Layer 4 shim instance bundled with runtime config ([Component 9] §G).
 * Production wiring injects concrete [PlatformIoShim] implementations per platform.
 */
public data class StorageEngineConfig(
    val pageTargetSizeBytes: Long = 8L * 1024 * 1024,
    val pageMaxSizeBytes: Long = 16L * 1024 * 1024,
    val globalMemoryBudgetBytes: Long,
    val compressionCodec: CompressionCodec = CompressionCodec.ZSTD,
    val defaultIndexRetention: IndexRetention = IndexRetention.EVICTABLE,
    val ioShim: PlatformIoShim,
    val debugSidecar: DebugSidecarConfig? = null,
)
