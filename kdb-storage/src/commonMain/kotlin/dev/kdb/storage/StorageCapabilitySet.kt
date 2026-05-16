package dev.kdb.storage

/** Declares what a storage engine implementation can and cannot do. */
public data class StorageCapabilitySet(
    val persistsDeltaLog: Boolean,
    val persistsAcrossReload: Boolean,
    val supportsGpuBulkRead: Boolean,
    val supportsDirectDeltaIngest: Boolean,
    val maxEnlistments: Int?,
    val indexRetentionDefault: IndexRetention,
) {
    public companion object {
        /** Reference capabilities for volatile in-memory adapters. */
        public val MEMORY: StorageCapabilitySet =
            StorageCapabilitySet(
                persistsDeltaLog = false,
                persistsAcrossReload = false,
                supportsGpuBulkRead = false,
                supportsDirectDeltaIngest = false,
                maxEnlistments = null,
                indexRetentionDefault = IndexRetention.EVICTABLE,
            )
    }
}
