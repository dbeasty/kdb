package dev.kdb.jdbc.remote

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.jdbc.KdbDatabaseMetaData
import dev.kdb.jdbc.KdbJdbcUrl
import dev.kdb.jdbc.transportConnectOptions
import dev.kdb.jdbc.AttachmentHost
import dev.kdb.jdbc.KdbAttachments
import dev.kdb.jdbc.KdbBinding
import dev.kdb.jdbc.KdbPreparedStatement
import dev.kdb.jdbc.KdbSqlConnection
import dev.kdb.jdbc.KdbStatement
import dev.kdb.sql.encodeSqlParameters
import dev.kdb.query.hybrid.HybridQueryResult
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.sql.ColumnSource
import dev.kdb.sql.QueryResult
import dev.kdb.sql.ResultColumn
import dev.kdb.sql.QueryRow
import dev.kdb.sql.SqlCell
import dev.kdb.sql.SqlParameter
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.SUPPORTED_WIRE_FEATURES
import dev.kdb.wire.SqlCellKind
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENTS
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENT_PROOFS
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireCapabilitySet
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import dev.kdb.wire.negotiateWireFeatures
import dev.kdb.wire.withWireFeatures
import dev.kdb.transaction.TransactionBuilder
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning
import java.sql.Savepoint
import java.util.Properties
import java.util.concurrent.Executor

public class KdbRemoteConnection(
    private val url: KdbJdbcUrl,
) : Connection,
    KdbSqlConnection {
    private val wire = defaultWireCodec()
    private val transport = defaultWebSocketWireTransport()
    private val socket =
        runBlocking {
            transport.connect(url.networkWebSocketUri(), url.transportConnectOptions())
        }
    private val rpc = WireRpcClient(wire, socket)

    /** The wire features the server agreed in the handshake. */
    @Volatile
    private var agreedFeatures: List<String> = emptyList()

    /** Whether the server agreed [WIRE_FEATURE_ATTACHMENTS]: it serves the attachment messages and
     * takes `{"$bytes": ..}` / `{"$attachment": ..}` parameters. Only the Go server does. */
    public val attachmentsSupported: Boolean get() = WIRE_FEATURE_ATTACHMENTS in agreedFeatures

    /** Whether the server agreed [WIRE_FEATURE_ATTACHMENT_PROOFS] as well: it serves verified ranged
     * reads ([KdbAttachments.openRange]). */
    public val attachmentProofsSupported: Boolean
        get() = attachmentsSupported && WIRE_FEATURE_ATTACHMENT_PROOFS in agreedFeatures

    private val authorNodeId = KdbUuid.random()

    private val attachmentHost =
        object : AttachmentHost {
            override val namespace: String get() = url.namespaceId
            override val sessionId: String get() = this@KdbRemoteConnection.sessionId
            override val attachmentsAgreed: Boolean get() = attachmentsSupported
            override val attachmentProofsAgreed: Boolean get() = attachmentProofsSupported
            override val inTransaction: Boolean get() = !autoCommit
            override val authorNodeId: KdbUuid get() = this@KdbRemoteConnection.authorNodeId

            override fun checkWritable() = this@KdbRemoteConnection.checkWritable()

            override suspend fun request(message: WireMessage): WireMessage {
                checkOpen()
                return rpc.request(message)
            }
        }

    private val attachments = KdbAttachments(attachmentHost)

    /** The attachment API (also `unwrap(KdbAttachments::class.java)`); its calls fail with
     * SQLFeatureNotSupportedException unless [attachmentsSupported]. */
    public fun attachments(): KdbAttachments = attachments
    private var closed = false
    private var autoCommit = true

    /** Go dialect only: the manual transaction has DML buffered on the server session. */
    private var bufferedWrites = false
    private var readOnly = url.readOnly
    private var sessionId: String = ""
    private var readConsistency = ReadConsistency.READ_COMMITTED

    init {
        runBlocking { handshakeAndBegin() }
    }

    private suspend fun handshakeAndBegin() {
        val hs =
            rpc.request(
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    HandshakePayload(
                        nodeId = "jdbc-client",
                        namespaces = listOf(url.namespaceId),
                        localHeads = emptyMap(),
                        capabilities = WireCapabilitySet(),
                        clientMode = WireClientMode.SQL_CLIENT,
                        wireFeatures = SUPPORTED_WIRE_FEATURES,
                    ),
                ),
            ) as WireMessage.HandshakeAck
        if (!hs.response.accepted) {
            throw SQLException("handshake rejected: ${hs.response.rejectionReason}")
        }
        // Binary frames only once the server has acked them; an older server acks none.
        val features = negotiateWireFeatures(hs.response.wireFeatures)
        agreedFeatures = features
        if (features.isNotEmpty()) rpc.useCodec(wire.withWireFeatures(features))
        val begin =
            rpc.request(
                WireMessage.SessionBegin(
                    WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    namespace = url.namespaceId,
                    sessionId = null,
                    readConsistency = readConsistency.name,
                    baseVersionHex = null,
                ),
            ) as WireMessage.SessionBeginAck
        sessionId = begin.sessionId
    }

    override suspend fun executeHybrid(
        sql: String,
        parameters: List<SqlParameter>,
    ): HybridQueryResult = executeHybridBound(sql, parameters.map { KdbBinding.Plain(it) })

    override suspend fun executeHybridBound(
        sql: String,
        bindings: List<KdbBinding>,
    ): HybridQueryResult {
        checkOpen()
        val parametersJson =
            if (attachmentsSupported) {
                // A server that agreed attachments takes the plain JSON parameter forms (Go's
                // decodeParametersJSON), binary ones included. Streams are uploaded first.
                val resolved =
                    bindings.map { b ->
                        if (b is KdbBinding.Stream) {
                            val blob = attachments.uploadSuspend(b.input, b.length, null)
                            KdbBinding.AttachmentRef(blob.hashHex, blob.size)
                        } else {
                            b
                        }
                    }
                encodePlainParameters(resolved)
            } else {
                encodeSqlParameters(
                    bindings.map {
                        (it as? KdbBinding.Plain)?.parameter
                            ?: throw SQLFeatureNotSupportedException(KdbAttachments.ATTACHMENTS_NOT_AGREED)
                    },
                )
            }
        val reply =
            rpc.request(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    namespace = url.namespaceId,
                    sessionId = sessionId,
                    sql = sql,
                    parametersJson = parametersJson,
                ),
            )
        return when (reply) {
            is WireMessage.SqlResult -> {
                if (reply.error != null) {
                    throw SQLException(reply.error)
                }
                val bufferedDml = attachmentsSupported && !reply.readOnly && !isTransactionControl(sql)
                if (bufferedDml && !autoCommit) bufferedWrites = true
                val resolvedCommitHex =
                    if (bufferedDml && autoCommit) commitBufferedDml() else reply.resolvedCommitHex
                val binary = binaryColumns(reply.columns.size, reply.rows, reply.cellKinds)
                val columns =
                    reply.columns.mapIndexed { i, name ->
                        ResultColumn(name, if (binary[i]) "VARBINARY" else "VARCHAR", ColumnSource.EXPRESSION)
                    }
                val rows = typedRows(reply.rows, reply.cellKinds)
                HybridQueryResult(
                    result =
                        QueryResult(
                            columns = columns,
                            rows = rows,
                            rowsAffected = reply.rowsAffected,
                            generatedIds = reply.generatedIds,
                        ),
                    resolvedCommit = KdbHash.fromHex(resolvedCommitHex),
                    readOnly = reply.readOnly,
                )
            }
            is WireMessage.ConflictReport ->
                throw SQLException("transaction conflict", "40001")
            else -> throw SQLException("unexpected wire reply: ${reply.header.messageType}")
        }
    }

    /**
     * The Go server buffers a session's DML until TX_COMMIT (its own client commits after every
     * Exec); the Kotlin server commits autocommit DML itself. Without this, an autocommit INSERT
     * against Go reported its row count and was never committed. Keyed on [attachmentsSupported],
     * the same Go-dialect switch the parameter encoding uses: only the Go server agrees it.
     */
    private suspend fun commitBufferedDml(): String {
        val reply =
            rpc.request(
                WireMessage.TxCommit(
                    WireHeader(WireMessageType.TX_COMMIT, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    namespace = url.namespaceId,
                    sessionId = sessionId,
                    transactionBytes = ByteArray(0),
                ),
            )
        return when (reply) {
            is WireMessage.ConflictReport -> throw SQLException("transaction conflict", "40001")
            is WireMessage.SqlResult -> {
                if (reply.error != null) throw SQLException(reply.error)
                reply.resolvedCommitHex
            }
            else -> throw SQLException("unexpected commit reply: ${reply.header.messageType}")
        }
    }

    private fun isTransactionControl(sql: String): Boolean {
        val word = sql.trimStart().takeWhile { it.isLetter() }.uppercase()
        return word == "BEGIN" || word == "START" || word == "COMMIT" || word == "ROLLBACK" || word == "END"
    }

    override fun namespaceForTable(table: String): String = url.namespaceForTable(table)

    override fun <T> blocking(block: suspend () -> T): T {
        checkOpen()
        return runBlocking { block() }
    }

    override fun createStatement(): java.sql.Statement = KdbStatement(this)

    override fun prepareStatement(sql: String): java.sql.PreparedStatement = KdbPreparedStatement(this, sql)

    override fun close() {
        closed = true
        rpc.close()
        runBlocking { socket.close() }
    }

    override fun isClosed(): Boolean = closed

    override fun setReadOnly(readOnly: Boolean) {
        this.readOnly = readOnly
    }

    override fun isReadOnly(): Boolean = readOnly

    override fun setAutoCommit(autoCommit: Boolean) {
        if (this.autoCommit == autoCommit) return
        blocking {
            if (autoCommit && !this.autoCommit) {
                execTransactionControl("COMMIT")
            } else if (!autoCommit && this.autoCommit) {
                execTransactionControl("BEGIN")
            }
        }
        this.autoCommit = autoCommit
    }

    override fun getAutoCommit(): Boolean = autoCommit

    override fun commit() {
        blocking { execTransactionControl("COMMIT") }
    }

    override fun rollback() {
        blocking { execTransactionControl("ROLLBACK") }
    }

    private suspend fun execTransactionControl(sql: String) {
        // The Go server refuses a COMMIT with nothing buffered ("no pending transaction to
        // commit") and keeps no autocommit mode of its own, so a manual transaction that only read
        // (or was rolled back) ends without one - otherwise conn.commit() after a SELECT failed.
        if (sql != "BEGIN") {
            val hadWrites = bufferedWrites
            bufferedWrites = false
            if (sql == "COMMIT" && attachmentsSupported && !hadWrites) return
        }
        val reply =
            rpc.request(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    namespace = url.namespaceId,
                    sessionId = sessionId,
                    sql = sql,
                    parametersJson = null,
                ),
            )
        when (reply) {
            is WireMessage.ConflictReport -> throw SQLException("transaction conflict", "40001")
            is WireMessage.SqlResult -> if (reply.error != null) throw SQLException(reply.error)
            else -> throw SQLException("unexpected transaction reply: ${reply.header.messageType}")
        }
    }

    /**
     * The reply's text cells as typed SqlCells. With the server's cell kinds (WireMessage.SqlResult
     * .cellKinds) each cell gets its SQL type - getObject returns a Long, a Double, null; without
     * them (an older server) every cell is a string, and "null" the only NULL, as before.
     */
    private fun typedRows(rows: List<List<String>>, kinds: String?): List<QueryRow> {
        var k = 0
        return rows.map { row ->
            QueryRow(
                row.map { cell ->
                    val kind = if (kinds != null && k < kinds.length) kinds[k] - '0' else -1
                    k++
                    when (kind) {
                        -1 -> if (cell == "null") SqlCell.Null else SqlCell.StringVal(cell)
                        SqlCellKind.NULL -> SqlCell.Null
                        SqlCellKind.LONG -> cell.toLongOrNull()?.let { SqlCell.LongVal(it) } ?: SqlCell.StringVal(cell)
                        SqlCellKind.DOUBLE -> cell.toDoubleOrNull()?.let { SqlCell.DoubleVal(it) } ?: SqlCell.StringVal(cell)
                        SqlCellKind.BOOL -> SqlCell.BoolVal(cell == "1" || cell == "true")
                        SqlCellKind.JSON -> SqlCell.JsonVal(cell)
                        // The base64 text stays a string; its VARBINARY column makes getBytes decode it.
                        else -> SqlCell.StringVal(cell)
                    }
                },
            )
        }
    }

    /** Which columns hold binary cells (CellKindBytes): the ResultSet reads those as VARBINARY. */
    private fun binaryColumns(
        width: Int,
        rows: List<List<String>>,
        kinds: String?,
    ): BooleanArray {
        val out = BooleanArray(width)
        if (kinds == null) return out
        var k = 0
        for (row in rows) {
            for (c in row.indices) {
                if (k < kinds.length && kinds[k] - '0' == SqlCellKind.BYTES && c < width) out[c] = true
                k++
            }
        }
        return out
    }

    override fun setTransactionIsolation(level: Int) {
        readConsistency =
            when (level) {
                Connection.TRANSACTION_SERIALIZABLE,
                Connection.TRANSACTION_REPEATABLE_READ,
                -> ReadConsistency.SNAPSHOT
                else -> ReadConsistency.READ_COMMITTED
            }
    }

    override fun getTransactionIsolation(): Int =
        when (readConsistency) {
            ReadConsistency.SNAPSHOT -> Connection.TRANSACTION_REPEATABLE_READ
            else -> Connection.TRANSACTION_READ_COMMITTED
        }

    override fun checkOpen() {
        if (closed) throw SQLException("Connection is closed")
    }

    override fun checkWritable() {
        if (readOnly) throw SQLException("Connection is read-only")
    }

    override fun getMetaData(): java.sql.DatabaseMetaData = KdbDatabaseMetaData.forRemote(this, url)
    override fun getCatalog(): String = url.catalog
    override fun setCatalog(catalog: String) {}
    override fun getSchema(): String? = url.namespaceId.substringAfterLast('/')
    override fun setSchema(schema: String?) {}
    override fun getTypeMap(): MutableMap<String, Class<*>> = mutableMapOf()
    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {}
    override fun isValid(timeout: Int): Boolean = !closed
    override fun getWarnings(): SQLWarning? = null
    override fun clearWarnings() {}
    override fun nativeSQL(sql: String): String = sql
    override fun <T> unwrap(iface: Class<T>): T =
        when {
            iface.isInstance(this) -> iface.cast(this)
            iface == KdbAttachments::class.java -> iface.cast(attachments)
            else -> throw SQLException("not a wrapper for ${iface.name}")
        }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this) || iface == KdbAttachments::class.java
    override fun abort(executor: Executor?) { close() }
    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {}
    override fun getNetworkTimeout(): Int = 0
    override fun prepareCall(sql: String) = throw SQLFeatureNotSupportedException()
    override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int) = throw SQLFeatureNotSupportedException()
    override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) =
        throw SQLFeatureNotSupportedException()
    override fun setClientInfo(name: String?, value: String?) {}
    override fun setClientInfo(info: Properties?) {}
    override fun getClientInfo(name: String?): String? = null
    override fun getClientInfo(): Properties = Properties()
    override fun setHoldability(holdability: Int) {}
    override fun getHoldability(): Int = java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT
    override fun setSavepoint(): Savepoint = throw SQLFeatureNotSupportedException()
    override fun setSavepoint(name: String?): Savepoint = throw SQLFeatureNotSupportedException()
    override fun rollback(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()
    override fun releaseSavepoint(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()
    override fun createClob() = throw SQLFeatureNotSupportedException()
    override fun createBlob() = throw SQLFeatureNotSupportedException()
    override fun createNClob() = throw SQLFeatureNotSupportedException()
    override fun createSQLXML() = throw SQLFeatureNotSupportedException()
    override fun createArrayOf(typeName: String, elements: Array<Any?>?) = throw SQLFeatureNotSupportedException()
    override fun createStruct(typeName: String, attributes: Array<Any?>?) = throw SQLFeatureNotSupportedException()
    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int) = prepareStatement(sql)
    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) =
        prepareStatement(sql)
    override fun prepareStatement(sql: String, autoGeneratedKeys: Int) = prepareStatement(sql)
    override fun prepareStatement(sql: String, columnIndexes: IntArray) = prepareStatement(sql)
    override fun prepareStatement(sql: String, columnNames: Array<String>) = prepareStatement(sql)
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int) = createStatement()
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) = createStatement()
}
