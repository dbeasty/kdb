package dev.kdb.storage.manager.enlistment

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.storage.*

public interface EnlistmentManager {
    public suspend fun openBrowserEnlistment(enlistmentId: KdbUuid): EnlistmentHandle
}

public class DefaultEnlistmentManager(
    private val backing: StorageAdapter,
) : EnlistmentManager {
    override suspend fun openBrowserEnlistment(enlistmentId: KdbUuid): EnlistmentHandle =
        SimpleEnlistmentHandle(enlistmentId, backing)
}

public class SimpleEnlistmentHandle(
    override val enlistmentId: KdbUuid,
    override val storage: StorageAdapter,
) : EnlistmentHandle {
    override val namespaceId: String = "default"
    override val commitHash: KdbHash = KdbHash.fromHex("0".repeat(64))
    override val isReady: Boolean = true
    override val branchRef: String = "main"
    override val pushState: EnlistmentPushState = EnlistmentPushState.IDLE
    override val snapshotAnchorHash: KdbHash? = null

    override suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy) {}

    override fun close() {}

    override fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit) {}

    override suspend fun push(): PushResult = PushResult.Success

    override suspend fun fetchMissing() {}

    override suspend fun resolveAndPush(): PushResult = PushResult.Success

    override suspend fun writeSnapshot() {}

    override suspend fun restoreSnapshot(): SnapshotRestoreResult =
        SnapshotRestoreResult.Failed(SnapshotFailureReason.INTEGRITY_CHECK_FAILED)
}
