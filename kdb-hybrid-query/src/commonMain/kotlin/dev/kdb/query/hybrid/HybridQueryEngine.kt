package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitRef
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictException
import dev.kdb.index.IndexManager
import dev.kdb.policy.HistoryMode
import dev.kdb.policy.NamespacePolicyRegistry
import dev.kdb.schema.isNone
import dev.kdb.sql.ExplainResult
import dev.kdb.sql.QueryContext
import dev.kdb.sql.QueryResult
import dev.kdb.sql.SqlEngine
import dev.kdb.sql.SqlParameter
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isDmlStatement
import dev.kdb.sql.statementParameterCount
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionAbortedException
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine

public interface HybridQueryEngine {
    public suspend fun execute(
        sql: String,
        request: HybridQueryRequest,
    ): HybridQueryResult

    public suspend fun explain(
        sql: String,
        request: HybridQueryRequest,
    ): ExplainResult

    public fun prepare(
        sql: String,
        request: HybridQueryRequest,
    ): PreparedHybridQuery

    public suspend fun checkout(
        namespaceId: String,
        ref: CommitRef,
    ): CheckoutHandle

    public suspend fun resetCheckout(namespaceId: String)
}

public fun hybridQueryEngine(
    sql: SqlEngine,
    dag: CommitDag,
    policyRegistry: NamespacePolicyRegistry,
    indexManager: IndexManager,
    storage: StorageAdapter,
    parser: HybridSqlParser = hybridSqlParser(),
    versionResolver: VersionResolver = defaultVersionResolver(),
    checkoutStore: CheckoutStore = CheckoutStore(),
): HybridQueryEngine =
    DefaultHybridQueryEngine(
        sql,
        dag,
        policyRegistry,
        indexManager,
        storage,
        parser,
        versionResolver,
        checkoutStore,
    )

internal class DefaultHybridQueryEngine(
    private val sqlEngine: SqlEngine,
    private val dag: CommitDag,
    private val policyRegistry: NamespacePolicyRegistry,
    private val indexManager: IndexManager,
    private val storage: StorageAdapter,
    private val parser: HybridSqlParser,
    private val versionResolver: VersionResolver,
    private val checkoutStore: CheckoutStore,
) : HybridQueryEngine {

    override suspend fun execute(
        sql: String,
        request: HybridQueryRequest,
    ): HybridQueryResult {
        val parsed = parser.parseWithVersion(sql)
        ensureNamespace(request.namespaceId)
        val policy = policyRegistry.get(request.namespaceId)
        val version = request.version ?: parsed.version
        if (version != null && policy.history == HistoryMode.NONE) {
            throw HistoryDisabledException(request.namespaceId)
        }
        val resolved = resolveReadCommit(request, version)
        val readOnly = version != null || request.sessionCheckout != null || checkoutStore.get(request.namespaceId) != null
        val stmt = defaultSqlParser().parse(parsed.sql)
        if (readOnly && isDmlStatement(stmt)) {
            throw ReadOnlyCheckoutException(request.namespaceId, resolved)
        }
        val ctx =
            QueryContext(
                namespaceId = request.namespaceId,
                schema = request.schema,
                atCommit = if (resolved == dag.head()) null else resolved,
                parameters = request.parameters,
                maxRows = request.maxRows,
            )
        if (isDmlStatement(stmt)) {
            val dml = sqlEngine.executeDml(parsed.sql, ctx.copy(atCommit = null))
            if (request.deferCommit) {
                val buffer = request.bufferOps
                    ?: throw IllegalStateException("deferCommit requires bufferOps")
                buffer(dml.operations)
                val base = request.transactionBase ?: dag.head()
                val result =
                    QueryResult(
                        emptyList(),
                        emptyList(),
                        rowsAffected = dml.rowsAffected,
                        generatedIds = dml.generatedIds,
                    )
                return HybridQueryResult(result, base, readOnly = false)
            }
            val commitHash = commitDml(dml.operations, request)
            val result =
                QueryResult(
                    emptyList(),
                    emptyList(),
                    rowsAffected = dml.rowsAffected,
                    generatedIds = dml.generatedIds,
                )
            return HybridQueryResult(result, commitHash, readOnly = false)
        }
        val result = sqlEngine.execute(parsed.sql, ctx)
        return HybridQueryResult(result, resolved, readOnly, appliedSchema = result.appliedSchema)
    }

    override suspend fun explain(
        sql: String,
        request: HybridQueryRequest,
    ): ExplainResult {
        val parsed = parser.parseWithVersion(sql)
        val policy = policyRegistry.get(request.namespaceId)
        val version = request.version ?: parsed.version
        if (version != null && policy.history == HistoryMode.NONE) {
            throw HistoryDisabledException(request.namespaceId)
        }
        val resolved = resolveReadCommit(request, version)
        val ctx =
            QueryContext(
                namespaceId = request.namespaceId,
                schema = request.schema,
                atCommit = if (resolved == dag.head()) null else resolved,
                parameters = request.parameters,
                maxRows = request.maxRows,
            )
        return sqlEngine.explain(parsed.sql, ctx)
    }

    private suspend fun commitDml(
        operations: List<dev.kdb.document.KdbOp>,
        request: HybridQueryRequest,
    ): KdbHash {
        if (operations.isEmpty()) {
            return dag.head()
        }
        val policy = policyRegistry.get(request.namespaceId)
        val txEngine: TransactionEngine = transactionEngine(policy.conflict)
        val parent = dag.head()
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = parent,
                operations = operations,
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        val locks = request.documentLocks
        val sessionId = request.writeSessionId
        if (locks != null && sessionId != null) {
            locks.acquireAllForTransaction(request.namespaceId, sessionId, tx)
        }
        return try {
            when (val result = txEngine.commit(tx, dag, storage, request.schema)) {
                is TransactionResult.Success -> {
                    if (!request.schema.isNone) {
                        indexManager.writer.applyCommit(
                            result.commit,
                            indexManager.registryFor(request.namespaceId),
                            storage,
                            request.schema,
                        )
                    }
                    result.commit.hash
                }
                is TransactionResult.Conflict ->
                    throw ConflictException(
                        "transaction conflict: ${result.report.conflicts.size} operation(s)",
                        result.report,
                    )
                is TransactionResult.SchemaError ->
                    throw dev.kdb.sql.SqlPlanningException(
                        "schema rejection: ${result.violations.size} violation(s)",
                        "",
                    )
                is TransactionResult.Aborted ->
                    throw TransactionAbortedException(
                        "transaction aborted: ${result.cause.message ?: result.cause.toString()}",
                        result.cause,
                    )
            }
        } finally {
            if (locks != null && sessionId != null) {
                locks.releaseAll(sessionId)
            }
        }
    }

    private suspend fun resolveReadCommit(
        request: HybridQueryRequest,
        version: VersionClause?,
    ): KdbHash {
        val checkout = request.sessionCheckout ?: checkoutStore.get(request.namespaceId)
        return when {
            version != null || checkout != null ->
                versionResolver.resolve(dag, version, checkout)
            request.readConsistency == ReadConsistency.SNAPSHOT && request.readPin != null ->
                request.readPin
            else -> dag.head()
        }
    }

    override fun prepare(
        sql: String,
        request: HybridQueryRequest,
    ): PreparedHybridQuery {
        val parsed = parser.parseWithVersion(sql)
        val paramCount = statementParameterCount(defaultSqlParser().parse(parsed.sql))
        return DefaultPreparedHybridQuery(this, sql, request, paramCount)
    }

    override suspend fun checkout(
        namespaceId: String,
        ref: CommitRef,
    ): CheckoutHandle {
        ensureNamespace(namespaceId)
        return checkoutStore.checkout(dag, namespaceId, ref)
    }

    override suspend fun resetCheckout(namespaceId: String) {
        checkoutStore.reset(namespaceId)
    }

    private fun ensureNamespace(namespaceId: String) {
        require(namespaceId == dag.namespaceId) {
            "namespaceId $namespaceId does not match DAG ${dag.namespaceId}"
        }
    }
}

private class DefaultPreparedHybridQuery(
    private val engine: DefaultHybridQueryEngine,
    private val sql: String,
    private val request: HybridQueryRequest,
    override val parameterCount: Int,
) : PreparedHybridQuery {
    override suspend fun execute(
        bindings: List<SqlParameter>,
        request: HybridQueryRequest,
    ): HybridQueryResult =
        engine.execute(sql, request.copy(parameters = bindings))
}
