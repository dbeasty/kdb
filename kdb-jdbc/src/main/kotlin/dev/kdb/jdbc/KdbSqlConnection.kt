package dev.kdb.jdbc

import dev.kdb.query.hybrid.HybridQueryResult
import dev.kdb.sql.SqlParameter
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.sql.SQLFeatureNotSupportedException

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

    /**
     * [executeHybrid] with a prepared statement's bindings, which may include binary values
     * (setBytes, setBinaryStream, setBlob). Only a network connection to a server that agreed the
     * `attachments` wire feature can send those; everywhere else they are refused, never dropped.
     */
    suspend fun executeHybridBound(
        sql: String,
        bindings: List<KdbBinding>,
    ): HybridQueryResult =
        executeHybrid(
            sql,
            bindings.map {
                (it as? KdbBinding.Plain)?.parameter ?: throw SQLFeatureNotSupportedException(ATTACHMENTS_NEED_NETWORK)
            },
        )
}

/** Attachment SQL (go/kdb/sql/attachments.go): WITH / SET / DROP / RENAME ATTACHMENT, the ATTACHMENT*()
 * functions, the _attachments column and STREAM ?. */
internal val ATTACHMENT_SQL: Regex =
    Regex("""\b(ATTACHMENT|ATTACHMENT_COUNT|ATTACHMENT_SIZE|ATTACHMENT_MIME|_attachments)\b|\bSTREAM\s*\?""", RegexOption.IGNORE_CASE)

internal const val ATTACHMENTS_NEED_NETWORK: String =
    "binary parameters and attachments need a network connection (jdbc:kdb://host:port/...) to a KDB server " +
        "that supports the 'attachments' wire feature; the embedded engine does not support them yet"

/** One prepared-statement parameter. */
public sealed class KdbBinding {
    /** An ordinary SQL value. */
    public data class Plain(val parameter: SqlParameter) : KdbBinding()

    /** Bytes sent inline with the statement (`{"$bytes": base64}` on the wire). */
    public class Bytes(public val value: ByteArray) : KdbBinding()

    /** Bytes uploaded to the namespace's attachment store before the statement runs, then bound as
     * an attachment reference - what `STREAM ?` requires, and what keeps large values out of the
     * statement frame. [length] is -1 when unknown; otherwise exactly that many bytes are read. */
    public class Stream(public val input: InputStream, public val length: Long = -1) : KdbBinding()

    /** An already uploaded blob (`{"$attachment": {"hash": hex, "size": n}}` on the wire). */
    public data class AttachmentRef(val hashHex: String, val size: Long) : KdbBinding()
}

internal fun KdbSqlConnection.runBlockingQuery(block: suspend () -> Unit) {
    blocking { block() }
}
