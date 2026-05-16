package dev.kdb.error

public data class FieldViolation(
    val fieldName: String,
    val violationType: ViolationType,
    val detail: String,
)

public enum class ViolationType {
    REQUIRED_FIELD_MISSING,
    TYPE_MISMATCH,
    UNIQUE_CONSTRAINT,
    ENUM_VALUE_NOT_DECLARED,
    CUSTOM_CONSTRAINT,
}

public data class ConflictReport(
    val transactionId: String,
    val baseHash: String,
    val targetHash: String,
    val conflicts: List<ConflictItem>,
)

public data class ConflictItem(
    val documentId: String,
    val operationType: ConflictOperationType,
    val localDoc: String?,
    val incomingDoc: String?,
)

public enum class ConflictOperationType {
    CONCURRENT_WRITE,
    WRITE_DELETE,
    DELETE_WRITE,
    SCHEMA_INCOMPATIBLE,
}
