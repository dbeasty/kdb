package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.peersync.PeerSession
import dev.kdb.peersync.PeerSyncResult
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import kotlin.math.min

/** Backoff schedule for stream WebSocket reconnect attempts. */
public object StreamReconnectPolicy {
    public const val INITIAL_BACKOFF_MS: Long = 500
    public const val MAX_BACKOFF_MS: Long = 30_000
    public const val MAX_ATTEMPTS: Int = 12

    public fun backoffMs(attempt: Int): Long {
        if (attempt <= 0) return INITIAL_BACKOFF_MS
        val scaled = INITIAL_BACKOFF_MS * (1L shl attempt.coerceAtMost(6))
        return min(scaled, MAX_BACKOFF_MS)
    }

    public fun shouldRetry(attempt: Int): Boolean = attempt < MAX_ATTEMPTS
}

public data class StreamRecoveryResult(
    val appliedCommits: Int,
    val pushedCommits: Int,
    val finalHead: KdbHash,
)

/** Catch-up via peer sync when stream subscribe is down or missed deltas. */
public suspend fun recoverInboundViaPeerSync(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    schema: KdbSchema = runtime.schema,
): StreamRecoveryResult {
    val result = syncEmbeddedWithPeer(runtime, session, namespaceId, schema)
    return StreamRecoveryResult(
        appliedCommits = result.appliedCommits,
        pushedCommits = result.pushedCommits,
        finalHead = result.finalHead,
    )
}

/** Pull and materialize after a stream [DeltaCommit] notification. */
public suspend fun applyRemoteStreamDelta(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    commitHash: KdbHash,
    schema: KdbSchema = runtime.schema,
): Boolean {
    if (runtime.dag.hasCommit(commitHash)) {
        return false
    }
    session.pullMissing()
    materializeCommitHistory(runtime, namespaceId, schema)
    if (!schema.isNone) {
        syncEmbedSchema(runtime, namespaceId, schema)
    }
    return true
}

public fun streamRecoveryStartedJson(reason: String?): String {
    val escaped = reason?.replace("\"", "'") ?: "stream unavailable"
    return """{"type":"SyncFallback","reason":"$escaped"}"""
}

public fun streamRecoveryCompletedJson(result: StreamRecoveryResult): String =
    """{"type":"SyncRecovered","appliedCommits":${result.appliedCommits},"pushedCommits":${result.pushedCommits},"head":"${result.finalHead.toHex()}"}"""

public fun streamRecoveryFailedJson(error: Throwable): String {
    val msg = error.message?.replace("\"", "'") ?: "unknown"
    return """{"type":"SyncFallbackFailed","message":"$msg"}"""
}

public fun streamReconnectingJson(attempt: Int, delayMs: Long): String =
    """{"type":"Reconnecting","attempt":$attempt,"delayMs":$delayMs}"""
