package dev.kdb.storage

public data class GpuPromotionPolicy(
    val strategy: GpuPromotionStrategy,
    val minSegmentAgeMillis: Long = 5 * 60 * 1000L,
    val minSegmentSizeBytes: Long = 64L * 1024 * 1024,
    val maxChangeRatePerMinute: Int = 100,
)

public enum class GpuPromotionStrategy {
    PROMOTE_ON_QUERY,
    PROMOTE_EAGERLY,
    NEVER,
}
