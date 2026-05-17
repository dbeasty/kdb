package dev.kdb.sql

import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.sql.view.DefaultVirtualViewEngine
import dev.kdb.sql.view.VirtualViewEngine
import dev.kdb.sql.view.VirtualViewRegistry
import dev.kdb.sql.view.virtualViewRegistry
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isDmlStatement
import dev.kdb.storage.StorageAdapter

public interface SqlEngine {
    public suspend fun execute(
        sql: String,
        context: QueryContext,
    ): QueryResult

    public suspend fun executeDml(
        sql: String,
        context: QueryContext,
    ): DmlResult

    public suspend fun explain(
        sql: String,
        context: QueryContext,
    ): ExplainResult

    public fun prepare(
        sql: String,
        context: QueryContext,
    ): PreparedQuery
}

public data class DmlResult(
    val operations: List<KdbOp>,
    val rowsAffected: Int,
)

public fun sqlEngine(
    indexManager: IndexManager,
    storage: StorageAdapter,
    dag: CommitDag,
    viewRegistry: VirtualViewRegistry = virtualViewRegistry(),
    parser: SqlParser = defaultSqlParser(),
    planner: QueryPlanner = DefaultQueryPlanner(),
    viewEngine: VirtualViewEngine = DefaultVirtualViewEngine(parser),
    indexStoreFactory: IndexStoreFactory = compositeIndexStoreFactory(dag, storage),
): SqlEngine =
    DefaultSqlEngine(
        indexManager,
        storage,
        dag,
        viewRegistry,
        parser,
        planner,
        viewEngine,
        indexStoreFactory,
    )

internal class DefaultSqlEngine(
    private val indexManager: IndexManager,
    private val storage: StorageAdapter,
    private val dag: CommitDag,
    private val viewRegistry: VirtualViewRegistry,
    private val parser: SqlParser,
    private val planner: QueryPlanner,
    private val viewEngine: VirtualViewEngine,
    private val indexStoreFactory: IndexStoreFactory,
) : SqlEngine {

    private val executor = SqlExecutor(indexManager, storage, dag)
    private val dmlExecutor = DmlExecutor(executor, storage, dag, planner)

    override suspend fun execute(
        sql: String,
        context: QueryContext,
    ): QueryResult {
        val stmt = parser.parse(sql)
        return when (stmt) {
            is SqlStatement.CreateVirtualView -> {
                viewEngine.executeCreateView(sql, context, storage, viewRegistry, parser)
                QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.DropVirtualView -> {
                viewEngine.executeDropView(stmt.name, context, viewRegistry)
                QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.CreateIndex -> {
                executeCreateIndex(stmt.ddl, context)
                QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.DropIndex -> {
                executeDropIndex(stmt.ddl, context)
                QueryResult(emptyList(), emptyList(), rowsAffected = 0)
            }
            is SqlStatement.Select -> {
                val query = resolveView(stmt.query, context)
                val plan = planner.plan(SqlStatement.Select(query), context)
                executor.executeSelect(query, plan, context)
            }
            is SqlStatement.Update,
            is SqlStatement.Insert,
            is SqlStatement.Delete,
            -> throw SqlPlanningException("DML must be executed via executeDml", sql)
        }
    }

    override suspend fun executeDml(
        sql: String,
        context: QueryContext,
    ): DmlResult {
        val stmt = parser.parse(sql)
        val ops =
            when (stmt) {
                is SqlStatement.Update -> dmlExecutor.executeUpdate(stmt.update, context)
                is SqlStatement.Insert -> dmlExecutor.executeInsert(stmt.insert, context)
                is SqlStatement.Delete -> dmlExecutor.executeDelete(stmt.delete, context)
                else -> throw SqlPlanningException("not a DML statement", sql)
            }
        return DmlResult(ops, ops.size)
    }

    override suspend fun explain(
        sql: String,
        context: QueryContext,
    ): ExplainResult {
        val stmt = parser.parse(sql)
        val plan =
            when (stmt) {
                is SqlStatement.Select -> {
                    val query = resolveView(stmt.query, context)
                    planner.plan(SqlStatement.Select(query), context)
                }
                else -> PhysicalPlan.FullTableScan("ddl: ${stmt::class.simpleName}")
            }
        return ExplainResult(plan, estimatedRows = null)
    }

    override fun prepare(
        sql: String,
        context: QueryContext,
    ): PreparedQuery = DefaultPreparedQuery(this, sql, context, statementParameterCount(parser.parse(sql)))

    private suspend fun executeCreateIndex(
        ddl: CreateIndexStatement,
        context: QueryContext,
    ) {
        if (ddl.fields.size != 1) {
            throw SqlPlanningException("v1 CREATE INDEX supports single field only", ddl.indexName)
        }
        val fieldName = ddl.fields.single()
        val field =
            context.schema.fieldsByName[fieldName]
                ?: throw SqlPlanningException("unknown schema field: $fieldName", ddl.indexName)
        val head = dag.head()
        val descriptor =
            dev.kdb.index.IndexDescriptor(
                indexId = dev.kdb.codec.KdbUuid.random(),
                namespaceId = context.namespaceId,
                fieldName = fieldName,
                fields = ddl.fields,
                type = ddl.type,
                unique = ddl.unique,
                schemaVersion = context.schema.version,
                createdAtHash = head,
            )
        val registry = indexManager.registryFor(context.namespaceId)
        registry.registerSqlIndex(
            descriptor,
            indexStoreFactory,
            dag,
            storage,
            context.schema,
            ddl.indexName,
            rebuild = true,
        )
    }

    private suspend fun executeDropIndex(
        ddl: DropIndexStatement,
        context: QueryContext,
    ) {
        val registry = indexManager.registryFor(context.namespaceId)
        if (!registry.dropSqlIndex(context.namespaceId, ddl.indexName)) {
            throw SqlPlanningException("index not found: ${ddl.indexName}", ddl.indexName)
        }
    }

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
    override val parameterCount: Int,
) : PreparedQuery {
    override suspend fun execute(
        bindings: List<SqlParameter>,
        context: QueryContext,
    ): QueryResult {
        if (bindings.size != parameterCount) {
            throw SqlPlanningException("expected $parameterCount parameters, got ${bindings.size}", sql)
        }
        val ctx = context.copy(parameters = bindings)
        val stmt = defaultSqlParser().parse(sql)
        return if (isDmlStatement(stmt)) {
            val dml = engine.executeDml(sql, ctx)
            QueryResult(emptyList(), emptyList(), rowsAffected = dml.rowsAffected)
        } else {
            engine.execute(sql, ctx)
        }
    }
}
