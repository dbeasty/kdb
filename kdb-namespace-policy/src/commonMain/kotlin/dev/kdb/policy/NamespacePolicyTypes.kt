package dev.kdb.policy

import dev.kdb.schema.KdbSchema
import dev.kdb.storage.IndexRetention
import dev.kdb.transaction.ConflictPolicy

public enum class NamespaceMode {
    MUTABLE,
    APPEND_ONLY,
}

public enum class HistoryMode {
    FULL,
    NONE,
}

public enum class SquashMode {
    AUTO,
    NEVER,
}

public enum class RetainStrategy {
    FULL_HISTORY,
    DAILY_SNAPSHOTS,
    TAGGED_ONLY,
}

public enum class StorageKind {
    LOCAL,
    LOCAL_FS,
    OBJECT_STORE,
    ARCHIVE,
}

public data class RetainRule(
    val olderThanMillis: Long,
    val strategy: RetainStrategy,
)

public data class CompactionPolicy(
    val keepTagged: Boolean = true,
    val keepBranchPoints: Boolean = true,
    val squashAfter: SquashMode = SquashMode.AUTO,
    val retainGranularity: List<RetainRule> = defaultRetainGranularity(),
)

public data class TierBand(
    val maxAgeMillis: Long,
    val storageKind: StorageKind = StorageKind.LOCAL,
)

public data class IceTierBand(
    val storageKind: StorageKind = StorageKind.ARCHIVE,
)

public data class TierPolicy(
    val hot: TierBand = TierBand(maxAgeMillis = 7L * 24 * 3600 * 1000),
    val warm: TierBand = TierBand(maxAgeMillis = 90L * 24 * 3600 * 1000),
    val cold: TierBand = TierBand(maxAgeMillis = 365L * 24 * 3600 * 1000),
    val ice: IceTierBand = IceTierBand(),
)

public data class GpuPromotionPolicyRef(
    val minSegmentAgeMillis: Long,
    val minSegmentSizeBytes: Long,
    val maxChangeRatePerMinute: Double,
)

public data class VectorIndexPolicy(
    val hnswM: Int = 16,
    val hnswEfConstruction: Int = 200,
    val defaultDimensions: Int = 128,
)

/**
 * Document expiry (Layer 16 §9.5, Component 72). A document is expired when the value at
 * `$.<fieldPath>` is a timestamp `<= now - graceMillis`: an RFC 3339 string or a number of epoch
 * milliseconds; any other value means "never expires". Head reads hide expired documents between
 * sweeps; the server's sweeper deletes them every [sweepIntervalMillis].
 */
public data class DocumentExpiryPolicy(
    val fieldPath: String,
    val graceMillis: Long = 0,
    val sweepIntervalMillis: Long = 60_000,
)

public data class NamespacePolicy(
    val namespaceId: String,
    val schema: KdbSchema?,
    val mode: NamespaceMode,
    val history: HistoryMode,
    val conflict: ConflictPolicy,
    val compaction: CompactionPolicy,
    val tiers: TierPolicy = TierPolicy(),
    val indexRetentionDefault: IndexRetention = IndexRetention.EVICTABLE,
    val gpuPromotion: GpuPromotionPolicyRef? = null,
    val vectorIndex: VectorIndexPolicy = VectorIndexPolicy(),
    val revision: Long = 1L,
    val documentExpiry: DocumentExpiryPolicy? = null,
)

public fun defaultRetainGranularity(): List<RetainRule> =
    listOf(
        RetainRule(7L * 24 * 3600 * 1000, RetainStrategy.FULL_HISTORY),
        RetainRule(30L * 24 * 3600 * 1000, RetainStrategy.DAILY_SNAPSHOTS),
        RetainRule(365L * 24 * 3600 * 1000, RetainStrategy.TAGGED_ONLY),
    )
