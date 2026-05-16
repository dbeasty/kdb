package dev.kdb.sql

import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexManager
import dev.kdb.sql.view.DefaultVirtualViewEngine
import dev.kdb.sql.view.VirtualViewEngine
import dev.kdb.sql.view.VirtualViewRegistry
import dev.kdb.sql.view.virtualViewRegistry
import dev.kdb.storage.StorageAdapter

public interface SqlEngine {
    public suspend fun execute(
        sql: String,
        context: QueryContext,
    ): QueryResult

    public suspend fun explain(
        sql: String,
        context: QueryContext,
    ): ExplainResult

    public fun prepare(
        sql: String,
        context: QueryContext,
    ): PreparedQuery
}

public fun sqlEngine(
    indexManager: IndexManager,
    storage: StorageAdapter,
    dag: CommitDag,
    viewRegistry: VirtualViewRegistry = virtualViewRegistry(),
    parser: SqlParser = defaultSqlParser(),
    planner: QueryPlanner = DefaultQueryPlanner(),
    viewEngine: VirtualViewEngine = DefaultVirtualViewEngine(parser),
): SqlEngine =
    DefaultSqlEngine(indexManager, storage, dag, viewRegistry, parser, planner, viewEngine)

internal class DefaultSqlEngine(
    private val indexManager: IndexManager,
    private val storage: StorageAdapter,
    private val dag: CommitDag,
    private val viewRegistry: VirtualViewRegistry,
    private val parser: SqlParser,
    private val planner: QueryPlanner,
    private val viewEngine: VirtualViewEngine,
) : SqlEngine {

    private val executor = SqlExecutor(indexManager, storage, dag)

    override suspend fun execute(
        sql: String,
        context: QueryContext,
    ): QueryResult {
        val stmt = parser.parse(sql)
        when (stmt) {
            is SqlStatement.CreateVirtualView -> {
                viewEngine.executeCreateView(sql, context, storage, parser)
                return QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.DropVirtualView -> {
                viewEngine.executeDropView(stmt.name, context, viewRegistry)
                return QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.Select -> {
                val query = resolveView(stmt.query, context)
                val plan = planner.plan(SqlStatement.Select(query), context)
                return executor.executeSelect(query, plan, context)
            }
        }
    }

    override suspend fun explain(
        sql: String,
        context: QueryContext,
    ): ExplainResult {
        val stmt = parser.parse(sql) as SqlStatement.Select
        val query = resolveView(stmt.query, context)
        val plan = planner.plan(SqlStatement.Select(query), context)
        return ExplainResult(plan, estimatedRows = null)
    }

    override fun prepare(
        sql: String,
        context: QueryContext,
    ): PreparedQuery = DefaultPreparedQuery(this, sql, context)

    private suspend fun resolveView(
        query: SelectQuery,
        context: QueryContext,
    ): SelectQuery {
        val resolved =
            viewEngine.resolveTableRef(
                query.from,
                context.namespaceId,
                viewRegistry,
            )
        if (resolved.rewrittenQuery == null) return query
        return resolved.rewrittenQuery.copy(
            projections = query.projections,
            where = query.where,
            orderBy = query.orderBy,
            limit = query.limit,
            offset = query.offset,
        )
    }
}

private class DefaultPreparedQuery(
    private val engine: DefaultSqlEngine,
    private val sql: String,
    private val context: QueryContext,
) : PreparedQuery {
    override val parameterCount: Int = 0

    override suspend fun execute(
        bindings: List<SqlParameter>,
        context: QueryContext,
    ): QueryResult = engine.execute(sql, context.copy(parameters = bindings))
}
