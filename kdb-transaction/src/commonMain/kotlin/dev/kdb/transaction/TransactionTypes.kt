package dev.kdb.transaction

import dev.kdb.codec.*
import dev.kdb.document.*

public enum class ConflictPolicy {
    APPEND_ONLY,
    LAST_WRITE,
    STRICT,
    CUSTOM,
}

public fun interface ConflictResolver {
    suspend fun resolve(conflict: DocumentConflict): KdbDocument?
}

public data class DocumentConflict(
    val docId: KdbUuid,
    val operationType: dev.kdb.error.ConflictOperationType,
    val existingDoc: KdbDocument?,
    val incomingDoc: KdbDocument?,
    val baseDoc: KdbDocument?,
)

public sealed class TransactionResult {
    data class Success(
        val commit: KdbCommit,
        val newTreeHash: KdbHash,
    ) : TransactionResult()

    data class Conflict(
        val report: dev.kdb.error.ConflictReport,
        val conflictingOps: List<OperationConflict>,
    ) : TransactionResult()

    data class SchemaError(
        val violations: List<OperationViolation>,
    ) : TransactionResult()
}

public data class OperationConflict(
    val opIndex: Int,
    val op: KdbOp,
    val type: dev.kdb.error.ConflictOperationType,
    val existingDoc: KdbDocument?,
    val incomingDoc: KdbDocument?,
    val baseDoc: KdbDocument?,
)

public data class OperationViolation(
    val opIndex: Int,
    val op: KdbOp,
    val violations: List<dev.kdb.error.FieldViolation>,
)

public sealed class DocWriteOutcome {
    data class Written(
        val newDoc: KdbDocument,
        val contentHash: KdbHash,
    ) : DocWriteOutcome()

    data class Deleted(
        val docId: KdbUuid,
    ) : DocWriteOutcome()

    data class Conflicted(
        val conflict: OperationConflict,
    ) : DocWriteOutcome()

    data class SchemaRejected(
        val violation: OperationViolation,
    ) : DocWriteOutcome()
}
