package dev.kdb.storage

import dev.kdb.codec.KdbUuid

/** Optional debug JSONL sidecar configuration ([master §12.4]). */
public data class DebugSidecarConfig(
    val enabled: Boolean = false,
    val directory: String,
    val logDelta: Boolean = true,
    val logWire: Boolean = false,
)

/** Non-authoritative hook invoked after a delta record is appended. */
public fun interface DeltaDebugHook {
    public suspend fun onAppend(
        record: DeltaRecord,
        segmentId: KdbUuid,
        offset: Long,
    )
}

public object NoOpDeltaDebugHook : DeltaDebugHook {
    override suspend fun onAppend(
        record: DeltaRecord,
        segmentId: KdbUuid,
        offset: Long,
    ) {
    }
}
