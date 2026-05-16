package dev.kdb.error

/**
 * Stable machine-readable error codes. Numeric values must not change once published.
 */
public enum class KdbErrorCode(public val numericCode: Int) {
    BSON_DECODE_ERROR(1001),
    BSON_ENCODE_ERROR(1002),

    JSON_PATH_ERROR(2001),

    SCHEMA_VIOLATION(3001),
    SCHEMA_MIGRATION_FAILED(3002),

    VERSION_NOT_FOUND(3101),
    ICE_STORAGE(3102),
    COMPACTION_BOUNDARY(3103),

    CONFLICT(4001),

    STORAGE_TIER_ERROR(4101),
    NAMESPACE_NOT_FOUND(4201),

    INDEX_CORRUPTION(5001),

    UNSUPPORTED_PROTOCOL_VERSION(6001),
    ENCODING_NEGOTIATION_FAILURE(6002),

    ARCHIVE_RESTORE(7001),
}
