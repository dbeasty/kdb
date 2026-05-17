package dev.kdb.jdbc.remote

import dev.kdb.codec.KdbHash
import dev.kdb.jdbc.KdbDatabaseMetaData
import dev.kdb.jdbc.KdbJdbcUrl
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
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireCapabilitySet
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
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
            transport.connect(url.networkWebSocketUri())
        }
    private val rpc = WireRpcClient(wire, socket)
    private var closed = false
    private var autoCommit = true
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
                    ),
                ),
            ) as WireMessage.HandshakeAck
        if (!hs.response.accepted) {
            throw SQLException("handshake rejected: ${hs.response.rejectionReason}")
        }
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
    ): HybridQueryResult {
        checkOpen()
        val reply =
            rpc.request(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 0, 0),
                    namespace = url.namespaceId,
                    sessionId = sessionId,
                    sql = sql,
                    parametersJson = encodeSqlParameters(parameters),
                ),
            )
        return when (reply) {
            is WireMessage.SqlResult -> {
                if (reply.error != null) {
                    throw SQLException(reply.error)
                }
                val columns =
                    reply.columns.map {
                        ResultColumn(it, "VARCHAR", ColumnSource.EXPRESSION)
                    }
                val rows =
                    reply.rows.map { row ->
                        QueryRow(
                            row.map { cell ->
                                if (cell == "null") SqlCell.Null else SqlCell.StringVal(cell)
                            },
                        )
                    }
                HybridQueryResult(
                    result =
                        QueryResult(
                            columns = columns,
                            rows = rows,
                            rowsAffected = reply.rowsAffected,
                            generatedIds = reply.generatedIds,
                        ),
                    resolvedCommit = KdbHash.fromHex(reply.resolvedCommitHex),
                    readOnly = reply.readOnly,
                )
            }
            is WireMessage.ConflictReport ->
                throw SQLException("transaction conflict", "40001")
            else -> throw SQLException("unexpected wire reply: ${reply.header.messageType}")
        }
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
    override fun <T> unwrap(iface: Class<T>): T = throw SQLFeatureNotSupportedException()
    override fun isWrapperFor(iface: Class<*>): Boolean = false
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
