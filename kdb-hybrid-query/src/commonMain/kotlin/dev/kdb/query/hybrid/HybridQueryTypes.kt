package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitRef
import dev.kdb.document.KdbOp
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.ExplainResult
import dev.kdb.transaction.DocumentLockManager
import dev.kdb.sql.QueryResult
import dev.kdb.sql.SqlParameter

public enum class ReadConsistency {
    SNAPSHOT,
    READ_COMMITTED,
    READ_YOUR_WRITES,
}

public sealed class VersionClause {
    public data class AtTag(val tag: String) : VersionClause()
    public data class AtCommit(val hex: String) : VersionClause()
    public data class AtTime(val iso8601: String) : VersionClause()
}

public data class HybridQueryRequest(
    val namespaceId: String,
    val schema: KdbSchema,
    val version: VersionClause? = null,
    val parameters: List<SqlParameter> = emptyList(),
    val maxRows: Int = 10_000,
    val readConsistency: ReadConsistency = ReadConsistency.READ_COMMITTED,
    val readPin: KdbHash? = null,
    val sessionCheckout: CheckoutHandle? = null,
    val writeSessionId: String? = null,
    val documentLocks: DocumentLockManager? = null,
    /** When true, DML ops are passed to [bufferOps] instead of committing immediately. */
    val deferCommit: Boolean = false,
    /** Commit hash reported for deferred DML (typically the transaction base). */
    val transactionBase: KdbHash? = null,
    val bufferOps: (suspend (List<KdbOp>) -> Unit)? = null,
)

public data class HybridQueryResult(
    val result: QueryResult,
    val resolvedCommit: KdbHash,
    val readOnly: Boolean,
)

public data class CheckoutHandle(
    val namespaceId: String,
    val commitHash: KdbHash,
    val readOnly: Boolean = true,
)

public interface PreparedHybridQuery {
    public val parameterCount: Int
    public suspend fun execute(
        bindings: List<SqlParameter>,
        request: HybridQueryRequest,
    ): HybridQueryResult
}

public data class ParsedHybridStatement(
    val sql: String,
    val version: VersionClause?,
)
