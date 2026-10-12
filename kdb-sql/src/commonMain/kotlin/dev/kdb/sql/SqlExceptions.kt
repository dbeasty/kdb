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

/**
 * A SELECT produced more rows than [limit] (`QueryContext.maxRows`). An error rather than a cut-off
 * result on purpose: a caller handed the first N rows of an answer cannot tell it is incomplete.
 * Add a LIMIT or a more selective predicate. Mirrors Go's `sql.ResultRowLimitError`.
 */
public class SqlResultLimitException(
    public val limit: Int,
) : KdbException("kdb sql: result has more than $limit rows - add a LIMIT or a more selective predicate") {
    override val code: KdbErrorCode get() = KdbErrorCode.RESOURCE_EXHAUSTED
}

/**
 * A scan examined more rows than its budget (`QueryContext.rowBudget`). The query is too expensive
 * as written; narrowing it is the fix, retrying unchanged is not. Mirrors Go's
 * `sql.ScanRowBudgetExceededError`.
 */
public class SqlRowBudgetException(
    public val budget: Int,
) : KdbException(
        "kdb sql: scan examined more than its budget of $budget rows - narrow the query (add a more selective predicate, or an indexed one)",
    ) {
    override val code: KdbErrorCode get() = KdbErrorCode.RESOURCE_EXHAUSTED
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
