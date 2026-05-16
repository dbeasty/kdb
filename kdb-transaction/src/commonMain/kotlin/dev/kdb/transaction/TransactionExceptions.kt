package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class TransactionBaseNotFoundException(
    message: String,
    val transactionId: KdbUuid,
    val missingHash: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class TransactionSchemaException(
    message: String,
    val transactionId: KdbUuid,
    val violations: List<OperationViolation>,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public class MergeBaseNotFoundException(
    message: String,
    val primaryHead: KdbHash,
    val mergedHead: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}
