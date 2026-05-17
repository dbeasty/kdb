package dev.kdb.sql

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.index.IndexManager
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter

internal class SqlExecutor(
    private val indexManager: IndexManager,
    private val storage: StorageAdapter,
    private val dag: CommitDag,
) {
    suspend fun executeSelect(
        query: SelectQuery,
        plan: PhysicalPlan,
        context: QueryContext,
    ): QueryResult {
        val docIds = resolveDocIds(plan, context)
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        var pairs =
            docIds.mapNotNull { id ->
                storage.getDocument(context.namespaceId, id, treeHash)?.let { id to it }
            }
        if (query.orderBy.isNotEmpty()) {
            pairs = sortPairs(pairs, query.orderBy, context)
        }
        var rows = pairs.map { (_, doc) -> QueryRow(projectRow(query.projections, doc, context)) }
        if (query.distinct) {
            rows = rows.distinctBy { it.values }
        }
        return QueryResult(columns = columnsFor(query, context.schema), rows = rows)
    }

    suspend fun resolveDocIdsForWhere(
        where: SqlExpr?,
        schema: KdbSchema,
        context: QueryContext,
        planner: QueryPlanner = DefaultQueryPlanner(),
    ): List<KdbUuid> {
        val plan = planner.plan(SqlStatement.Select(SelectQuery(false, listOf(SelectProjection.Star()), TableRef("t", null), where, emptyList(), null, 0)), context)
        return resolveDocIds(plan, context)
    }

    private fun sortPairs(
        pairs: List<Pair<KdbUuid, KdbDocument>>,
        orderBy: List<OrderItem>,
        context: QueryContext,
    ): List<Pair<KdbUuid, KdbDocument>> =
        pairs.sortedWith { (_, docA), (_, docB) ->
            for (item in orderBy) {
                val cmp =
                    when (val expr = item.expr) {
                        is SqlExpr.Similarity ->
                            throw SqlPlanningException(
                                "similarity ordering requires embedding (not yet available)",
                                "",
                            )
                        else -> {
                            val ca = SqlPredicate.evalCell(expr, docA, context.schema, context.parameters)
                            val cb = SqlPredicate.evalCell(expr, docB, context.schema, context.parameters)
                            SqlPredicate.compareCells(ca, cb)
                        }
                    }
                if (cmp != 0) return@sortedWith if (item.ascending) cmp else -cmp
            }
            0
        }

    private suspend fun resolveDocIds(
        plan: PhysicalPlan,
        context: QueryContext,
    ): List<KdbUuid> =
        when (plan) {
            is PhysicalPlan.Limit -> {
                val inner = resolveDocIds(plan.input, context)
                inner.drop(plan.offset).take(plan.limit).take(context.maxRows)
            }
            is PhysicalPlan.Sort -> resolveDocIds(plan.input, context)
            is PhysicalPlan.Project -> resolveDocIds(plan.input, context)
            is PhysicalPlan.Filter -> {
                val ids = resolveDocIds(plan.input, context)
                filterIds(ids, plan.predicate, context)
            }
            is PhysicalPlan.IndexScan -> indexScan(plan, context)
            is PhysicalPlan.FullTableScan -> fullScan(context)
        }

    private suspend fun indexScan(
        plan: PhysicalPlan.IndexScan,
        context: QueryContext,
    ): List<KdbUuid> {
        val registry = indexManager.registryFor(context.namespaceId)
        val reader = indexManager.reader
        return when (val spec = plan.lookup) {
            is IndexLookupSpec.Exact ->
                reader.lookupExact(registry, plan.fieldName, spec.key, context.atCommit)

            is IndexLookupSpec.Range ->
                reader.lookupRange(
                    registry,
                    plan.fieldName,
                    spec.from,
                    spec.to,
                    context.atCommit,
                    limit = context.maxRows,
                )

            is IndexLookupSpec.FullText ->
                reader.lookupFullText(
                    registry,
                    plan.fieldName,
                    spec.query,
                    context.atCommit,
                    context.maxRows,
                )

            is IndexLookupSpec.VectorAnn ->
                reader.lookupVector(
                    registry,
                    plan.fieldName,
                    spec.queryVector,
                    spec.k,
                    context.atCommit,
                ).map { it.docId }
        }
    }

    private suspend fun fullScan(context: QueryContext): List<KdbUuid> {
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        val ids = mutableListOf<KdbUuid>()
        storage.scanDocuments(context.namespaceId, treeHash, batchSize = 256) { batch ->
            ids += batch.map { it.id }
        }
        return ids.take(context.maxRows)
    }

    private suspend fun filterIds(
        ids: List<KdbUuid>,
        predicate: SqlExpr,
        context: QueryContext,
    ): List<KdbUuid> {
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        return ids.filter { id ->
            val doc = storage.getDocument(context.namespaceId, id, treeHash) ?: return@filter false
            SqlPredicate.eval(predicate, doc, context.schema, context.parameters)
        }
    }

    private fun projectRow(
        projections: List<SelectProjection>,
        doc: KdbDocument,
        context: QueryContext,
    ): List<SqlCell> {
        val schema = context.schema
        if (projections.any { it is SelectProjection.Star }) {
            val cols = mutableListOf<SqlCell>()
            cols += SqlCell.StringVal(doc.id.toString())
            for ((_, field) in schema.fieldsByName) {
                cols += SqlPredicate.cellForColumn(field.name, doc, schema) ?: SqlCell.Null
            }
            cols += SqlCell.JsonVal(doc.json)
            return cols
        }
        return projections.map { proj ->
            when (proj) {
                is SelectProjection.Column ->
                    SqlPredicate.cellForColumn(proj.name, doc, schema) ?: SqlCell.Null
                is SelectProjection.Expression ->
                    SqlPredicate.evalCell(proj.expr, doc, schema, context.parameters) ?: SqlCell.Null
                is SelectProjection.Star -> SqlCell.Null
            }
        }
    }

    private fun columnsFor(
        query: SelectQuery,
        schema: KdbSchema,
    ): List<ResultColumn> {
        if (query.projections.any { it is SelectProjection.Star }) {
            val cols = mutableListOf<ResultColumn>()
            cols += ResultColumn("kdb_id", "VARCHAR", ColumnSource.KDB_ID)
            for ((_, f) in schema.fieldsByName) {
                cols += ResultColumn(f.name, f.type.sqlTypeName(), ColumnSource.SCHEMA_FIELD)
            }
            cols += ResultColumn("_doc", "JSON", ColumnSource.DOC_JSON)
            return cols
        }
        return query.projections.map { proj ->
            when (proj) {
                is SelectProjection.Column ->
                    ResultColumn(
                        proj.alias ?: proj.name,
                        schema.fieldsByName[proj.name]?.type?.sqlTypeName() ?: "VARCHAR",
                        ColumnSource.SCHEMA_FIELD,
                    )
                is SelectProjection.Expression ->
                    ResultColumn(proj.alias ?: "expr", "JSON", ColumnSource.EXPRESSION)
                is SelectProjection.Star ->
                    ResultColumn("_doc", "JSON", ColumnSource.DOC_JSON)
            }
        }
    }
}
