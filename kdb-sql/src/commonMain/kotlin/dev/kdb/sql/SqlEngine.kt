package dev.kdb.sql

import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.schema.isNone
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
    val generatedIds: List<String> = emptyList(),
)

public data class DdlResult(
    val schema: dev.kdb.schema.KdbSchema,
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
    namespaceDags: Map<String, CommitDag> = emptyMap(),
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
        namespaceDags,
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
    private val namespaceDags: Map<String, CommitDag> = emptyMap(),
) : SqlEngine {

    private val executor = SqlExecutor(indexManager, storage, dag, namespaceDags)
    private val dmlExecutor = DmlExecutor(executor, storage, dag, planner)
    private val ddlExecutor = DdlExecutor(indexManager, dag, storage, indexStoreFactory)

    /**
     * Gives the planner the namespace's live index descriptors (Layer 16 §9.3) unless the caller
     * supplied a catalog. A namespace that is not bound to the index manager planning-wise simply
     * has no indexes.
     */
    private fun withCatalog(context: QueryContext): QueryContext {
        if (context.indexCatalog != null) return context
        val catalog = runCatching { registryIndexCatalog(indexManager.registryFor(context.namespaceId)) }.getOrNull()
        return context.copy(indexCatalog = catalog)
    }

    override suspend fun execute(
        sql: String,
        rawContext: QueryContext,
    ): QueryResult {
        val context = withCatalog(rawContext)
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
            is SqlStatement.CreateTable -> {
                val schema = ddlExecutor.executeCreateTable(stmt.ddl, context)
                QueryResult(emptyList(), emptyList(), appliedSchema = schema)
            }
            is SqlStatement.AlterTableAddColumn -> {
                val schema = ddlExecutor.executeAlterTableAddColumn(stmt.ddl, context)
                QueryResult(emptyList(), emptyList(), appliedSchema = schema)
            }
            is SqlStatement.DropTable -> {
                val schema = ddlExecutor.executeDropTable(stmt.table, context)
                QueryResult(emptyList(), emptyList(), appliedSchema = schema)
            }
            is SqlStatement.Update,
            is SqlStatement.Insert,
            is SqlStatement.Delete,
            -> throw SqlPlanningException("DML must be executed via executeDml", sql)
            is SqlStatement.BeginTransaction,
            is SqlStatement.Commit,
            is SqlStatement.Rollback,
            -> throw SqlPlanningException("transaction control must be handled by the session host", sql)
            is SqlStatement.CreateRole,
            is SqlStatement.DropRole,
            is SqlStatement.Grant,
            is SqlStatement.Revoke,
            is SqlStatement.CreateUser,
            is SqlStatement.DropUser,
            -> throw SqlPlanningException("RBAC admin statements must be handled by the session host", sql)
        }
    }

    override suspend fun executeDml(
        sql: String,
        rawContext: QueryContext,
    ): DmlResult {
        val context = withCatalog(rawContext)
        val stmt = parser.parse(sql)
        val ops =
            when (stmt) {
                is SqlStatement.Update -> dmlExecutor.executeUpdate(stmt.update, context)
                is SqlStatement.Insert -> dmlExecutor.executeInsert(stmt.insert, context)
                is SqlStatement.Delete -> dmlExecutor.executeDelete(stmt.delete, context)
                else -> throw SqlPlanningException("not a DML statement", sql)
            }
        val generatedIds =
            ops.filterIsInstance<dev.kdb.document.KdbOp.Write>().map { it.docId.toString() }
        return DmlResult(ops, ops.size, generatedIds)
    }

    override suspend fun explain(
        sql: String,
        rawContext: QueryContext,
    ): ExplainResult {
        val context = withCatalog(rawContext)
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

    /**
     * `CREATE [UNIQUE] INDEX` (Layer 16 §9.2). HASH/BTREE need one declared schema field;
     * FULLTEXT takes one or more JSON paths, VECTOR one, and both are allowed on schemaless
     * namespaces. The descriptor's [dev.kdb.index.IndexDescriptor.options] carry `index_name`,
     * FULLTEXT `weights` (`f1=3,f2=1`), and the VECTOR `WITH` options so the store factory can
     * configure the index.
     */
    private suspend fun executeCreateIndex(
        ddl: CreateIndexStatement,
        context: QueryContext,
    ) {
        val sql = ddl.indexName
        if (ddl.fields.isEmpty()) throw SqlPlanningException("CREATE INDEX needs at least one field", sql)
        val options = LinkedHashMap<String, String>()
        options[INDEX_OPTION_NAME] = ddl.indexName
        when (ddl.type) {
            IndexType.HASH, IndexType.BTREE -> {
                if (ddl.fields.size != 1) {
                    throw SqlPlanningException("${ddl.type} indexes support a single field", sql)
                }
                if (ddl.options.isNotEmpty()) throw SqlPlanningException("${ddl.type} indexes take no WITH options", sql)
                val fieldName = ddl.fields.single()
                if (context.schema.isNone || fieldName !in context.schema.fieldsByName) {
                    throw SqlPlanningException("unknown schema field: $fieldName", sql)
                }
            }
            IndexType.FULLTEXT -> {
                if (ddl.options.isNotEmpty()) throw SqlPlanningException("FULLTEXT indexes take no WITH options", sql)
                ddl.fields.forEach { requireRootField(it, context, sql) }
                options[INDEX_OPTION_WEIGHTS] = ddl.fields.joinToString(",") { "$it=${ddl.weights[it] ?: 1}" }
            }
            IndexType.VECTOR -> {
                if (ddl.fields.size != 1) throw SqlPlanningException("VECTOR indexes support a single field", sql)
                requireRootField(ddl.fields.single(), context, sql)
                val allowed = setOf("dimensions", "metric", "m", "ef_construction", "ef_search")
                for ((k, v) in ddl.options) {
                    if (k !in allowed) throw SqlPlanningException("unknown VECTOR index option: $k", sql)
                    if (k == "metric") {
                        if (v.lowercase() !in setOf("cosine", "l2", "inner_product")) {
                            throw SqlPlanningException("unknown vector metric: $v", sql)
                        }
                        options[k] = v.lowercase()
                    } else {
                        val n = v.toIntOrNull()
                        if (n == null || n <= 0) throw SqlPlanningException("VECTOR option $k must be a positive integer", sql)
                        options[k] = n.toString()
                    }
                }
                if ("dimensions" !in options) throw SqlPlanningException("VECTOR index requires WITH (dimensions = n)", sql)
            }
        }
        if (ddl.unique && ddl.type != IndexType.HASH && ddl.type != IndexType.BTREE) {
            throw SqlPlanningException("UNIQUE is only valid for HASH/BTREE indexes", sql)
        }
        val head = dag.head()
        val descriptor =
            dev.kdb.index.IndexDescriptor(
                indexId = dev.kdb.codec.KdbUuid.random(),
                namespaceId = context.namespaceId,
                fieldName = ddl.fields.first(),
                fields = ddl.fields,
                type = ddl.type,
                unique = ddl.unique,
                schemaVersion = context.schema.version,
                createdAtHash = head,
                options = options,
            )
        val registry = indexManager.registryFor(context.namespaceId)
        try {
            registry.registerSqlIndex(
                descriptor,
                indexStoreFactory,
                dag,
                storage,
                context.schema,
                ddl.indexName,
                rebuild = true,
            )
        } catch (e: IllegalArgumentException) {
            throw SqlPlanningException(e.message ?: "cannot create index", sql)
        }
    }

    /** With a declared schema the root of an indexed JSON path must be a schema field (Rule 1). */
    private fun requireRootField(
        path: String,
        context: QueryContext,
        sql: String,
    ) {
        if (context.schema.isNone) return
        val root = path.substringBefore('.')
        if (root !in context.schema.fieldsByName) throw SqlPlanningException("unknown schema field: $root", sql)
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
