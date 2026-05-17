package dev.kdb.error

public abstract class KdbException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    public abstract val code: KdbErrorCode
}

// ---------- Layer 0: typed codec ------------------------------

public class KdbDecodeException(
    message: String,
    public val offset: Int = -1,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class KdbEncodeException(message: String, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_ENCODE_ERROR
}

public class KdbSchemaException(message: String, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_SCHEMA_ERROR
}

@Deprecated("Use KdbDecodeException", ReplaceWith("KdbDecodeException", "dev.kdb.error.KdbDecodeException"))
public typealias BsonDecodeException = KdbDecodeException

@Deprecated("Use KdbEncodeException", ReplaceWith("KdbEncodeException", "dev.kdb.error.KdbEncodeException"))
public typealias BsonEncodeException = KdbEncodeException

// ---------- Layer 1: JSON path ------------------------------

public class JsonPathException(
    message: String,
    public val path: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.JSON_PATH_ERROR
}

// ---------- Layer 2: schema ------------------------------

public class SchemaViolationException(
    message: String,
    public val violations: List<FieldViolation>,
) : KdbException(message) {
    init {
        require(violations.isNotEmpty()) { "violations must be non-empty" }
    }

    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public class SchemaMigrationException(
    message: String,
    public val namespaceName: String,
    public val failedStep: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_MIGRATION_FAILED
}

// ---------- Layer 2: DAG ------------------------------

public class VersionNotFoundException(
    message: String,
    public val namespaceName: String,
    public val reference: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class IceStorageException(
    message: String,
    public val namespaceName: String,
    public val commitHash: String,
    public val archiveLocation: String?,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.ICE_STORAGE
}

public class CompactionBoundaryException(
    message: String,
    public val namespaceName: String,
    public val requestedBaseHash: String,
    public val compactionBoundaryHash: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}

// ---------- Layer 3: write path ------------------------------

public class ConflictException(message: String, public val report: ConflictReport) : KdbException(message) {
    init {
        require(report.conflicts.isNotEmpty()) { "report.conflicts must be non-empty" }
    }

    override val code: KdbErrorCode get() = KdbErrorCode.CONFLICT
}

public class DocumentLockedException(
    message: String,
    public val namespaceId: String,
    public val docId: String,
    public val holderSessionId: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.DOCUMENT_LOCKED
}

// ---------- Layer 3: storage ------------------------------

public class StorageTierException(
    message: String,
    public val tier: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class DataDirectoryLockedException(
    message: String,
    public val dataRoot: String,
    public val holderPid: Long? = null,
    public val holderLabel: String? = null,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.DATA_DIRECTORY_LOCKED
}

public class NamespaceNotFoundException(
    message: String,
    public val namespaceName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}

// ---------- Layer 4: index ------------------------------

public class IndexCorruptionException(
    message: String,
    public val namespaceName: String,
    public val indexName: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

// ---------- Layer 6: wire ------------------------------

public class UnsupportedProtocolVersionException(
    message: String,
    public val peerRequiredVersion: Int,
    public val localMaxVersion: Int,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.UNSUPPORTED_PROTOCOL_VERSION
}

public class EncodingNegotiationFailureException(
    message: String,
    public val localEncodings: List<String>,
    public val peerEncodings: List<String>,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.ENCODING_NEGOTIATION_FAILURE
}

// ---------- Layer 7: archive ------------------------------

public class ArchiveRestoreException(
    message: String,
    public val archiveLocation: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.ARCHIVE_RESTORE
}
