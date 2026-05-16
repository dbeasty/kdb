package dev.kdb.storage

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class DocumentNotFoundException(
    message: String,
    val namespaceId: String,
    val docId: KdbUuid,
    val atCommit: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class StorageAdapterException(
    message: String,
    val namespaceId: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class DeltaSegmentSealedException(
    message: String,
    val segmentId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class SnapshotIntegrityException(
    message: String,
    val key: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class EnlistmentNotFoundException(
    message: String,
    val enlistmentId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}
