package dev.kdb.index

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class IndexNotFoundException(
    message: String,
    val namespaceId: String,
    val fieldName: String,
    val type: IndexType,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public class IndexTypeMismatchException(
    message: String,
    val fieldName: String,
    val expectedType: IndexType,
    val actualType: IndexType,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public class IndexRebuildException(
    message: String,
    val namespaceId: String,
    val indexId: dev.kdb.codec.KdbUuid,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public class UniqueIndexViolationException(
    message: String,
    val namespaceId: String,
    val fieldName: String,
    val key: IndexKey,
    val existingDocId: dev.kdb.codec.KdbUuid,
    val incomingDocId: dev.kdb.codec.KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}
