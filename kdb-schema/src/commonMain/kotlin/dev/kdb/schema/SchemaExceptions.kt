package dev.kdb.schema

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class SchemaDecodeException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class SchemaMigrationConflictException(
    message: String,
    public val step: MigrationStep,
    public val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_MIGRATION_FAILED
}
