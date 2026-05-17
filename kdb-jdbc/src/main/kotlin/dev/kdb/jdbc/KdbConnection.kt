package dev.kdb.jdbc

import dev.kdb.embed.syncEmbedSchema
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.query.hybrid.HybridQueryResult
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.sql.QueryResult
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isDdlStatement
import dev.kdb.sql.isDmlStatement
import dev.kdb.sql.isTransactionControlStatement
import dev.kdb.jdbc.memory.MemoryRuntimeLease
import dev.kdb.jdbc.session.EmbeddedSqlSession
import dev.kdb.sql.SqlParameter
import kotlinx.coroutines.runBlocking
import java.sql.Array
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.SQLClientInfoException
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.Properties
import java.util.concurrent.Executor

public class KdbConnection(
    private val runtime: EmbeddedKdbRuntime,
    private val url: KdbJdbcUrl,
    private val memoryLease: MemoryRuntimeLease? = null,
) : Connection,
    KdbSqlConnection {
    private var closed = false
    private var schema: KdbSchema = memoryLease?.currentSchema() ?: runtime.schema
    private var currentNamespace: String = url.namespaceId
    private var readOnly = url.readOnly

    private val sqlSession =
        EmbeddedSqlSession(
            namespaceId = url.namespaceId,
            dag = runtime.dag,
            schema = { effectiveSchema() },
        )

    internal val embedded: EmbeddedKdbRuntime get() = runtime

    internal fun applyQuerySchema(schema: KdbSchema) {
        if (schema.isNone) return
        this.schema = schema
        memoryLease?.publishSchema(schema)
            ?: blocking {
                syncEmbedSchema(runtime, url.namespaceId, schema)
                runtime.indexManager.writer.rebuildAll(
                    runtime.dag.head(),
                    runtime.dag,
                    runtime.indexManager.registryFor(url.namespaceId),
                    runtime.storage,
                    schema,
                )
            }
    }

    /** First schema on an empty shared memory database (syncs index registry). */
    internal fun registerSchemaBlocking(schema: KdbSchema) {
        if (schema.isNone) return
        this.schema = schema
        memoryLease?.registerSchemaBlocking(schema)
            ?: blocking {
                syncEmbedSchema(runtime, url.namespaceId, schema)
                runtime.indexManager.writer.rebuildAll(
                    runtime.dag.head(),
                    runtime.dag,
                    runtime.indexManager.registryFor(url.namespaceId),
                    runtime.storage,
                    schema,
                )
            }
    }

    override fun namespaceForTable(table: String): String = url.namespaceForTable(table)

    override fun <T> blocking(block: suspend () -> T): T {
        checkOpen()
        val lease = memoryLease
        return if (lease != null) {
            lease.withAccess {
                runBlocking { block() }
            }
        } else {
            runBlocking { block() }
        }
    }

    override suspend fun executeHybrid(
        sql: String,
        parameters: List<SqlParameter>,
    ): HybridQueryResult {
        val namespaceId = resolveNamespace(sql)
        val stmt = defaultSqlParser().parse(sql.trim())
        if (isTransactionControlStatement(stmt)) {
            return executeTransactionControl(namespaceId, stmt)
        }
        if (!sqlSession.autoCommit && isDdlStatement(stmt)) {
            throw SQLException("DDL not allowed inside a transaction")
        }
        val deferCommit = !sqlSession.autoCommit
        val hybridResult =
            runtime.hybrid.execute(
                sql,
                HybridQueryRequest(
                    namespaceId = namespaceId,
                    schema = effectiveSchema(),
                    parameters = parameters,
                    readConsistency = ReadConsistency.READ_COMMITTED,
                    deferCommit = deferCommit,
                    transactionBase = sqlSession.transactionBase(),
                    bufferOps =
                        if (deferCommit) {
                            { ops -> sqlSession.appendOps(ops) }
                        } else {
                            null
                        },
                ),
            )
        val applied = hybridResult.appliedSchema ?: hybridResult.result.appliedSchema
        if (applied != null) {
            registerSchemaBlocking(applied)
        }
        return hybridResult
    }

    private suspend fun executeTransactionControl(
        namespaceId: String,
        stmt: dev.kdb.sql.SqlStatement,
    ): HybridQueryResult {
        val control = sqlSession.handleTransactionControl(stmt)
        val resolved =
            if (control.needsCommit) {
                sqlSession.commit(runtime)
            } else {
                control.resolvedCommit
            }
        return HybridQueryResult(
            result = QueryResult(emptyList(), emptyList(), rowsAffected = 0),
            resolvedCommit = resolved,
            readOnly = false,
        )
    }

    internal fun effectiveKdbSchema(): KdbSchema = effectiveSchema()

    private fun effectiveSchema(): KdbSchema {
        val leased = memoryLease?.currentSchema()
        if (leased != null && !leased.isNone) return leased
        return schema
    }

    private fun resolveNamespace(sql: String): String {
        val from = Regex("""FROM\s+([A-Za-z0-9_/]+)""", RegexOption.IGNORE_CASE).find(sql)
        if (from != null) {
            val ref = from.groupValues[1]
            return if (ref.contains('/')) ref else url.namespaceForTable(ref)
        }
        return currentNamespace
    }

    override fun createStatement(): Statement = KdbStatement(this)

    override fun prepareStatement(sql: String): PreparedStatement = KdbPreparedStatement(this, sql)

    override fun getMetaData(): DatabaseMetaData = KdbDatabaseMetaData.forEmbedded(this)

    override fun close() {
        if (closed) return
        val lease = memoryLease
        closed = true
        lease?.release()
    }

    override fun isClosed(): Boolean = closed

    override fun setReadOnly(readOnly: Boolean) {
        this.readOnly = readOnly
    }

    override fun isReadOnly(): Boolean = readOnly

    override fun setCatalog(catalog: String) {
        require(catalog == runtime.catalog) { "catalog must be ${runtime.catalog}" }
    }

    override fun getCatalog(): String = runtime.catalog

    override fun setAutoCommit(autoCommit: Boolean) {
        if (autoCommit) {
            sqlSession.setAutoCommit(true)
        } else if (sqlSession.autoCommit) {
            sqlSession.begin()
        }
    }

    override fun getAutoCommit(): Boolean = sqlSession.autoCommit

    override fun commit() {
        checkOpen()
        if (sqlSession.autoCommit) return
        blocking {
            sqlSession.commit(runtime)
        }
    }

    override fun rollback() {
        checkOpen()
        sqlSession.rollback()
    }

    override fun checkOpen() {
        if (closed) throw SQLException("Connection is closed")
    }

    override fun checkWritable() {
        if (readOnly) throw SQLException("Connection is read-only")
    }

    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): PreparedStatement = prepareStatement(sql)

    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): PreparedStatement = prepareStatement(sql)

    override fun prepareStatement(
        sql: String,
        autoGeneratedKeys: Int,
    ): PreparedStatement = prepareStatement(sql)

    override fun prepareStatement(
        sql: String,
        columnIndexes: IntArray,
    ): PreparedStatement = prepareStatement(sql)

    override fun prepareStatement(
        sql: String,
        columnNames: kotlin.Array<String>,
    ): PreparedStatement = prepareStatement(sql)

    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): Statement = createStatement()

    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): Statement = createStatement()

    override fun nativeSQL(sql: String): String = sql

    override fun setTransactionIsolation(level: Int) {}

    override fun getTransactionIsolation(): Int = Connection.TRANSACTION_READ_COMMITTED

    override fun getWarnings(): SQLWarning? = null

    override fun clearWarnings() {}

    override fun setSchema(schema: String?) {
        if (schema != null) currentNamespace = url.namespaceForTable(schema)
    }

    override fun getSchema(): String? = currentNamespace.substringAfterLast('/')

    override fun getTypeMap(): MutableMap<String, Class<*>> = mutableMapOf()

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {}

    override fun abort(executor: Executor?) {
        close()
    }

    override fun setNetworkTimeout(
        executor: Executor?,
        milliseconds: Int,
    ) {}

    override fun getNetworkTimeout(): Int = 0

    override fun <T> unwrap(iface: Class<T>): T = throw SQLFeatureNotSupportedException()

    override fun isWrapperFor(iface: Class<*>): Boolean = false

    override fun prepareCall(sql: String): CallableStatement = unsupported()

    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): CallableStatement = unsupported()

    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): CallableStatement = unsupported()

    override fun setClientInfo(
        name: String?,
        value: String?,
    ) {}

    override fun setClientInfo(info: Properties?) {}

    override fun getClientInfo(name: String?): String? = null

    override fun getClientInfo(): Properties = Properties()

    override fun setHoldability(holdability: Int) {}

    override fun getHoldability(): Int = java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun setSavepoint(): Savepoint = unsupported()

    override fun setSavepoint(name: String?): Savepoint = unsupported()

    override fun rollback(savepoint: Savepoint?) = unsupported()

    override fun releaseSavepoint(savepoint: Savepoint?) = unsupported()

    override fun createClob(): Clob = unsupported()

    override fun createBlob(): Blob = unsupported()

    override fun createNClob(): NClob = unsupported()

    override fun createSQLXML(): SQLXML = unsupported()

    override fun isValid(timeout: Int): Boolean = !closed

    override fun createArrayOf(
        typeName: String,
        elements: kotlin.Array<Any?>?,
    ): java.sql.Array = unsupported()

    override fun createStruct(
        typeName: String,
        attributes: kotlin.Array<Any?>?,
    ): Struct = unsupported()

    private fun unsupported(): Nothing = throw SQLFeatureNotSupportedException()
}
