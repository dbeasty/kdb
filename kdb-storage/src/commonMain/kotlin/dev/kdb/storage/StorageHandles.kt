package dev.kdb.storage

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

public data class IndexPinViolationEvent(
    val namespaceId: String,
    val enlistmentId: KdbUuid,
    val currentPressureBytes: Long,
    val pinnedIndexSizeBytes: Long,
)

public interface EvictableStorageAdapter : StorageAdapter {

    public suspend fun evictDocuments(enlistmentId: KdbUuid)

    public suspend fun evictIndex(enlistmentId: KdbUuid)

    public suspend fun rebuildDocuments(
        enlistmentId: KdbUuid,
        fromDeltaLog: DeltaSegmentReader,
    )

    public suspend fun rebuildIndex(
        enlistmentId: KdbUuid,
        fromDocuments: StorageAdapter,
    )

    public fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState
}

/** Reference-counted handle to a realized store ([Layer 4b]); interface only ([Component 9]). */
public interface RealizedStoreHandle : AutoCloseable {

    val namespaceId: String
    val commitHash: KdbHash
    val enlistmentId: KdbUuid

    val isReady: Boolean

    public suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy = RebuildBlockingPolicy.WAIT)

    val storage: StorageAdapter

    public override fun close()

    public fun release() {
        close()
    }

    public fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit)
}

/** Browser enlistment handle ([Layer 4b]); interface contract only ([Component 9]). */
public interface EnlistmentHandle : RealizedStoreHandle {

    val branchRef: String

    val pushState: EnlistmentPushState

    public suspend fun push(): PushResult

    public suspend fun fetchMissing()

    public suspend fun resolveAndPush(): PushResult

    val snapshotAnchorHash: KdbHash?

    public suspend fun writeSnapshot()

    public suspend fun restoreSnapshot(): SnapshotRestoreResult
}

public sealed class PushResult {
    public data object Success : PushResult()

    public data class Rejected(val missingDeltaHashes: List<KdbHash>) : PushResult()
}

public sealed class SnapshotRestoreResult {
    public data class Restored(val anchorHash: KdbHash) : SnapshotRestoreResult()

    public data class Failed(val reason: SnapshotFailureReason) : SnapshotRestoreResult()

    /** Snapshot anchor compacted server-side ([Component 9 §4]). */
    public data object AnchorCompactedAway : SnapshotRestoreResult()
}
