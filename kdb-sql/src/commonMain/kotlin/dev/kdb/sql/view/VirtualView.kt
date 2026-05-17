package dev.kdb.sql.view

import dev.kdb.codec.KdbHash
import dev.kdb.sql.SelectQuery
import dev.kdb.sql.SqlExpr
import dev.kdb.sql.SqlPlanningException
import dev.kdb.sql.SqlStatement
import dev.kdb.sql.TableRef
import dev.kdb.sql.VirtualViewExistsException
import dev.kdb.sql.VirtualViewNotFoundException
import dev.kdb.sql.QueryContext
import dev.kdb.sql.SqlParser
import dev.kdb.sql.defaultSqlParser
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public sealed class VirtualColumnSource {
    public data class SchemaField(val fieldName: String) : VirtualColumnSource()
    public data class Expression(val sqlExpr: SqlExpr) : VirtualColumnSource()
    public data object KdbId : VirtualColumnSource()
    public data object DocJson : VirtualColumnSource()
}

public data class VirtualColumn(
    val name: String,
    val sqlType: String,
    val source: VirtualColumnSource,
)

public data class VirtualViewDefinition(
    val viewName: String,
    val namespaceId: String,
    val baseTable: String,
    val query: SelectQuery,
    val columns: List<VirtualColumn>,
    val createdAtCommit: KdbHash,
    val schemaVersion: Int,
)

public data class ResolvedTable(
    val baseTable: String,
    val rewrittenQuery: SelectQuery?,
    val columnMap: Map<String, VirtualColumn>,
)

public interface VirtualViewRegistry {
    public suspend fun list(namespaceId: String): List<VirtualViewDefinition>
    public suspend fun get(namespaceId: String, viewName: String): VirtualViewDefinition?
    public suspend fun put(definition: VirtualViewDefinition)
    public suspend fun drop(namespaceId: String, viewName: String): Boolean
}

public interface VirtualViewEngine {
    public suspend fun resolveTableRef(
        ref: TableRef,
        namespaceId: String,
        registry: VirtualViewRegistry,
    ): ResolvedTable

    public suspend fun executeCreateView(
        sql: String,
        context: QueryContext,
        storage: StorageAdapter,
        registry: VirtualViewRegistry,
        parser: SqlParser = defaultSqlParser(),
    )

    public suspend fun executeDropView(
        viewName: String,
        context: QueryContext,
        registry: VirtualViewRegistry,
    ): Boolean
}

public class InMemoryVirtualViewRegistry : VirtualViewRegistry {
    private val mutex = Mutex()
    private val views = mutableMapOf<Pair<String, String>, VirtualViewDefinition>()

    override suspend fun list(namespaceId: String): List<VirtualViewDefinition> =
        mutex.withLock { views.filterKeys { it.first == namespaceId }.values.toList() }

    override suspend fun get(
        namespaceId: String,
        viewName: String,
    ): VirtualViewDefinition? = mutex.withLock { views[namespaceId to viewName] }

    override suspend fun put(definition: VirtualViewDefinition) {
        mutex.withLock { views[definition.namespaceId to definition.viewName] = definition }
    }

    override suspend fun drop(
        namespaceId: String,
        viewName: String,
    ): Boolean = mutex.withLock { views.remove(namespaceId to viewName) != null }
}

public class DefaultVirtualViewEngine(
    private val parser: SqlParser = defaultSqlParser(),
) : VirtualViewEngine {

    override suspend fun resolveTableRef(
        ref: TableRef,
        namespaceId: String,
        registry: VirtualViewRegistry,
    ): ResolvedTable {
        val def = registry.get(namespaceId, ref.name) ?: return ResolvedTable(ref.name, null, emptyMap())
        if (def.query.from.name != def.baseTable) {
            throw SqlPlanningException("nested virtual views not supported", ref.name)
        }
        val cols =
            def.columns.associateBy { it.name }
        return ResolvedTable(def.baseTable, def.query, cols)
    }

    override suspend fun executeCreateView(
        sql: String,
        context: QueryContext,
        storage: StorageAdapter,
        registry: VirtualViewRegistry,
        parser: SqlParser,
    ) {
        val stmt = parser.parse(sql)
        val create =
            stmt as? SqlStatement.CreateVirtualView
                ?: throw SqlPlanningException("not a CREATE VIRTUAL VIEW", sql)
        if (registry.get(context.namespaceId, create.name) != null) {
            throw VirtualViewExistsException("view ${create.name} exists", create.name)
        }
        val columns = inferColumns(create.query)
        val def =
            VirtualViewDefinition(
                viewName = create.name,
                namespaceId = context.namespaceId,
                baseTable = create.query.from.name,
                query = create.query,
                columns = columns,
                createdAtCommit = context.atCommit ?: dev.kdb.codec.KdbHash.fromBytes(ByteArray(32)),
                schemaVersion = context.schema.version,
            )
        registry.put(def)
    }

    override suspend fun executeDropView(
        viewName: String,
        context: QueryContext,
        registry: VirtualViewRegistry,
    ): Boolean {
        if (!registry.drop(context.namespaceId, viewName)) {
            throw VirtualViewNotFoundException("view $viewName not found", viewName)
        }
        return true
    }

    private fun inferColumns(query: SelectQuery): List<VirtualColumn> =
        query.projections.mapNotNull { proj ->
            when (proj) {
                is dev.kdb.sql.SelectProjection.Column ->
                    VirtualColumn(
                        proj.alias ?: proj.name,
                        "VARCHAR",
                        VirtualColumnSource.SchemaField(proj.name),
                    )
                is dev.kdb.sql.SelectProjection.Expression ->
                    VirtualColumn(
                        proj.alias ?: "expr",
                        "JSON",
                        VirtualColumnSource.Expression(proj.expr),
                    )
                is dev.kdb.sql.SelectProjection.Star -> null
            }
        }

}

public fun virtualViewRegistry(): VirtualViewRegistry = InMemoryVirtualViewRegistry()

public fun virtualViewEngine(parser: SqlParser = defaultSqlParser()): VirtualViewEngine =
    DefaultVirtualViewEngine(parser)

private const val META_PREFIX = "kdb/virtual-views/"

public fun storageBackedVirtualViewRegistry(storage: StorageAdapter): VirtualViewRegistry =
    StorageVirtualViewRegistry(storage)

internal class StorageVirtualViewRegistry(
    private val storage: StorageAdapter,
) : VirtualViewRegistry {
    private val inner = InMemoryVirtualViewRegistry()
    private val loaded = mutableSetOf<String>()

    private suspend fun ensureLoaded(namespaceId: String) {
        if (namespaceId in loaded) return
        // v1: in-memory only until namespace meta op exists
        loaded += namespaceId
    }

    override suspend fun list(namespaceId: String): List<VirtualViewDefinition> {
        ensureLoaded(namespaceId)
        return inner.list(namespaceId)
    }

    override suspend fun get(
        namespaceId: String,
        viewName: String,
    ): VirtualViewDefinition? {
        ensureLoaded(namespaceId)
        return inner.get(namespaceId, viewName)
    }

    override suspend fun put(definition: VirtualViewDefinition) {
        inner.put(definition)
    }

    override suspend fun drop(
        namespaceId: String,
        viewName: String,
    ): Boolean = inner.drop(namespaceId, viewName)
}
