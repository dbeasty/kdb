package dev.kdb.sql

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class SqlParseException(
    message: String,
    public val sql: String,
    public val offset: Int,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public class SqlPlanningException(
    message: String,
    public val sql: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public class VirtualViewExistsException(
    message: String,
    public val viewName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public class VirtualViewNotFoundException(
    message: String,
    public val viewName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}
