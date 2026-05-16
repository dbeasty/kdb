package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.document.CommitStub
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.policy.StorageKind

public data class TierCycleResult(
    val segmentsMoved: Int,
    val archivesStarted: Int,
    val errors: List<TierJobError>,
)

public data class TierJobError(
    val message: String,
    val retryable: Boolean,
)

public data class ArchiveRequest(
    val namespaceId: String,
    val commitHash: KdbHash,
    val tag: String? = null,
    val targetBackendId: String = "default-ice",
)

public data class ArchiveResult(
    val bundleLocation: String,
    val stub: CommitStub,
    val bundleHash: KdbHash,
)

public data class RestoreRequest(
    val archiveLocation: String,
    val intoNamespaceId: String,
    val verifyBundle: Boolean = true,
)

public data class RestoreResult(
    val namespaceId: String,
    val headCommit: KdbHash,
    val documentsImported: Int,
)

public data class SegmentMoveRequest(
    val namespaceId: String,
    val segmentId: dev.kdb.codec.KdbUuid,
    val toTier: dev.kdb.storage.manager.tier.SegmentTier,
)

public data class SegmentMoveResult(
    val bytesMoved: Long,
    val sourcePath: String?,
    val destPath: String?,
)

public data class IceBundleArtifact(
    val location: String,
    val contentHash: KdbHash,
    val sizeBytes: Long,
)

public class TierJobSkippedException(
    val namespaceId: String,
    val reason: String,
) : KdbException("tier job skipped: $reason") {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class BundleIntegrityException(
    val expected: KdbHash,
    val actual: KdbHash,
) : KdbException("ice bundle hash mismatch") {
    override val code: KdbErrorCode get() = KdbErrorCode.ARCHIVE_RESTORE
}

public interface TierBackend {
    public val id: String
    public val storageKind: StorageKind
    public suspend fun put(key: String, bytes: ByteArray): String
    public suspend fun get(location: String): ByteArray
    public suspend fun delete(location: String): Boolean
    public suspend fun exists(location: String): Boolean
}

public interface TierBackendRegistry {
    public fun get(backendId: String): TierBackend
    public fun register(backendId: String, backend: TierBackend)
}
