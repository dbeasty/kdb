package dev.kdb.jdbc

import dev.kdb.query.hybrid.HybridQueryResult
import dev.kdb.sql.SqlParameter
import kotlinx.coroutines.runBlocking

/** Shared surface for embedded and remote JDBC connections. */
public interface KdbSqlConnection {
    fun checkOpen()

    fun checkWritable()

    fun namespaceForTable(table: String): String

    fun <T> blocking(block: suspend () -> T): T

    suspend fun executeHybrid(
        sql: String,
        parameters: List<SqlParameter>,
    ): HybridQueryResult
}

internal fun KdbSqlConnection.runBlockingQuery(block: suspend () -> Unit) {
    blocking { block() }
}
