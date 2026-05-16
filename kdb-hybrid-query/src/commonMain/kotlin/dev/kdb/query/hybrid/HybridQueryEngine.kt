package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitRef
import dev.kdb.policy.HistoryMode
import dev.kdb.policy.NamespacePolicyRegistry
import dev.kdb.sql.ExplainResult
import dev.kdb.sql.QueryContext
import dev.kdb.sql.SqlEngine
import dev.kdb.sql.SqlParameter

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
    parser: HybridSqlParser = hybridSqlParser(),
    versionResolver: VersionResolver = defaultVersionResolver(),
    checkoutStore: CheckoutStore = CheckoutStore(),
): HybridQueryEngine =
    DefaultHybridQueryEngine(sql, dag, policyRegistry, parser, versionResolver, checkoutStore)

internal class DefaultHybridQueryEngine(
    private val sqlEngine: SqlEngine,
    private val dag: CommitDag,
    private val policyRegistry: NamespacePolicyRegistry,
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
        val checkout = checkoutStore.get(request.namespaceId)
        val resolved =
            versionResolver.resolve(dag, version, checkout)
        val readOnly = version != null || checkout != null
        if (readOnly && isDml(parsed.sql)) {
            throw ReadOnlyCheckoutException(request.namespaceId, resolved)
        }
        if (isDml(parsed.sql)) {
            throw HybridDmlNotSupportedException("DML via HybridQueryEngine is not implemented in v1")
        }
        val ctx =
            QueryContext(
                namespaceId = request.namespaceId,
                schema = request.schema,
                atCommit = if (resolved == dag.head()) null else resolved,
                parameters = request.parameters,
                maxRows = request.maxRows,
            )
        val result = sqlEngine.execute(parsed.sql, ctx)
        return HybridQueryResult(result, resolved, readOnly)
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
        val checkout = checkoutStore.get(request.namespaceId)
        val resolved = versionResolver.resolve(dag, version, checkout)
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

    override fun prepare(
        sql: String,
        request: HybridQueryRequest,
    ): PreparedHybridQuery = DefaultPreparedHybridQuery(this, sql, request)

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

    private fun isDml(sql: String): Boolean {
        val u = sql.trimStart().uppercase()
        return u.startsWith("UPDATE") || u.startsWith("INSERT") || u.startsWith("DELETE")
    }
}

private class DefaultPreparedHybridQuery(
    private val engine: DefaultHybridQueryEngine,
    private val sql: String,
    private val request: HybridQueryRequest,
) : PreparedHybridQuery {
    override val parameterCount: Int = 0

    override suspend fun execute(
        bindings: List<SqlParameter>,
        request: HybridQueryRequest,
    ): HybridQueryResult =
        engine.execute(sql, request.copy(parameters = bindings))
}
