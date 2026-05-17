package dev.kdb.jdbc.session

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbOp
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.SqlStatement
import dev.kdb.transaction.TransactionBuilder
import java.sql.SQLException
import kotlinx.coroutines.runBlocking

/**
 * Per-connection JDBC transaction state for embedded memory/file runtimes.
 * Mirrors server [dev.kdb.server.KdbSession] buffering without wire sessions.
 */
internal class EmbeddedSqlSession(
    private val namespaceId: String,
    private val dag: CommitDag,
    private val schema: () -> KdbSchema,
) {
    var autoCommit: Boolean = true
        private set

    private var baseVersion: KdbHash? = null

    private var pending: TransactionBuilder? = null

    private fun head(): KdbHash = runBlocking { dag.head() }

    private fun base(): KdbHash = baseVersion ?: head().also { baseVersion = it }

    fun transactionBase(): KdbHash? = if (!autoCommit) base() else null

    fun setAutoCommit(value: Boolean) {
        if (value && !autoCommit && pending != null) {
            throw SQLException("commit or rollback before enabling autoCommit")
        }
        autoCommit = value
        if (value) {
            pending = null
            baseVersion = head()
        }
    }

    fun begin() {
        if (pending != null) {
            throw SQLException("transaction already in progress")
        }
        autoCommit = false
        baseVersion = head()
    }

    fun rollback() {
        pending = null
        if (!autoCommit) {
            baseVersion = head()
        }
    }

    suspend fun appendOps(operations: List<KdbOp>) {
        if (operations.isEmpty()) return
        val builder = pendingBuilder()
        for (op in operations) {
            when (op) {
                is KdbOp.Write -> builder.write(op.docId, op.patch)
                is KdbOp.Delete -> builder.delete(op.docId)
                else ->
                    throw SQLException(
                        "operation ${op::class.simpleName} not supported in SQL transaction",
                    )
            }
        }
    }

    suspend fun commit(runtime: EmbeddedKdbRuntime): KdbHash {
        val tx =
            pending?.build()
                ?: if (!autoCommit) {
                    return head()
                } else {
                    throw SQLException("no pending transaction to commit")
                }
        pending = null
        val commit =
            commitViaEngine(
                runtime = runtime,
                namespaceId = namespaceId,
                transaction = tx,
                schema = schema(),
            )
        baseVersion = commit.hash
        return commit.hash
    }

    private suspend fun pendingBuilder(): TransactionBuilder {
        if (pending == null) {
            pending =
                TransactionBuilder(
                    namespaceId = namespaceId,
                    baseVersion = base(),
                    authorNodeId = KdbUuid.random(),
                    schema = schema(),
                )
        }
        return pending!!
    }

    fun handleTransactionControl(stmt: SqlStatement): TransactionControlResult =
        when (stmt) {
            SqlStatement.BeginTransaction -> {
                begin()
                TransactionControlResult(base())
            }
            SqlStatement.Commit ->
                TransactionControlResult(base(), needsCommit = true)
            SqlStatement.Rollback -> {
                rollback()
                TransactionControlResult(head())
            }
            else -> throw IllegalArgumentException("not transaction control: $stmt")
        }
}

internal data class TransactionControlResult(
    val resolvedCommit: KdbHash,
    val needsCommit: Boolean = false,
)
