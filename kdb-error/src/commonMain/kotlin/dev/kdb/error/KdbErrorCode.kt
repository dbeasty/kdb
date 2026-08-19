package dev.kdb.error

/**
 * Stable machine-readable error codes. Numeric values must not change once published.
 */
public enum class KdbErrorCode(public val numericCode: Int) {
    /** Layer 0 typed codec decode (binary or JSON boundary). Same numeric legacy as BSON decode. */
    KDB_DECODE_ERROR(1001),

    /** Layer 0 typed codec encode (binary or JSON boundary). Same numeric legacy as BSON encode. */
    KDB_ENCODE_ERROR(1002),

    /** Layer 0 invalid or inconsistent schema registry / type bindings. */
    KDB_SCHEMA_ERROR(1005),

    JSON_PATH_ERROR(2001),

    SCHEMA_VIOLATION(3001),
    SCHEMA_MIGRATION_FAILED(3002),

    VERSION_NOT_FOUND(3101),
    ICE_STORAGE(3102),
    COMPACTION_BOUNDARY(3103),

    CONFLICT(4001),
    DOCUMENT_LOCKED(4002),
    TRANSACTION_ABORTED(4003),

    STORAGE_TIER_ERROR(4101),
    DATA_DIRECTORY_LOCKED(4102),
    NAMESPACE_NOT_FOUND(4201),

    INDEX_CORRUPTION(5001),

    UNSUPPORTED_PROTOCOL_VERSION(6001),
    ENCODING_NEGOTIATION_FAILURE(6002),

    ARCHIVE_RESTORE(7001),

    TRANSPORT_ERROR(6101),
    COMPUTE_UNAVAILABLE(6201),
    COMPUTE_DISPATCH_ERROR(6202),

    AUTHENTICATION_FAILED(6301),
    AUTHORIZATION_FAILED(6302),

    /** Layer 11 Component 32: no procedure registered under that namespace/name. */
    SCRIPT_NOT_FOUND(8001),

    /** Layer 11 Component 32: restricted-JS source failed to compile/parse. */
    SCRIPT_COMPILE_ERROR(8002),

    /** Layer 11 Component 32: procedure exceeded its wall-clock budget and was interrupted. */
    SCRIPT_TIMEOUT(8003),

    /** Layer 11 Component 32: procedure exceeded a call-count/log-size/heap budget. */
    SCRIPT_RESOURCE_LIMIT(8004),

    /** Layer 11 Component 32: uncaught exception thrown by the script body itself. */
    SCRIPT_RUNTIME_ERROR(8005),
}
