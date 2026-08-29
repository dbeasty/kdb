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
    private val namespaceDags: Map<String, CommitDag> = emptyMap(),
) {
    suspend fun executeSelect(
        query: SelectQuery,
        plan: PhysicalPlan,
        context: QueryContext,
    ): QueryResult {
        if (query.joins.isNotEmpty()) {
            return executeJoinSelect(query, context)
        }
        if (query.groupBy.isNotEmpty() || SqlAggregates.queryHasAggregates(query)) {
            return executeAggregateSelect(query, plan, context)
        }
        // ORDER BY has to run before LIMIT/OFFSET: "the first three rows in sorted order", not
        // "three arbitrary rows, sorted among themselves". The planner puts Limit outermost and
        // resolveDocIds applies it while resolving ids - before any document has been read, let
        // alone sorted - so an ordered query answered from whichever rows the scan reached
        // first. Strip the limit here and apply it once the rows are in order. Go's
        // ExecuteSelect does the same, and had the same bug.
        val deferLimit = query.orderBy.isNotEmpty()
        val stripped = if (deferLimit) stripLimit(plan) else null
        val docIds = resolveDocIds(stripped?.inner ?: plan, context)
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        var pairs =
            docIds.mapNotNull { id ->
                storage.getDocument(context.namespaceId, id, treeHash)?.let { id to it }
            }
        if (query.orderBy.isNotEmpty()) {
            pairs = sortPairs(pairs, query.orderBy, context)
        }
        if (stripped != null) {
            pairs = pairs.drop(stripped.offset).take(stripped.limit)
        }
        var rows = pairs.map { (_, doc) -> QueryRow(projectRow(query.projections, doc, context)) }
        if (query.distinct) {
            rows = rows.distinctBy { it.values }
        }
        return QueryResult(columns = columnsFor(query, context.schema), rows = rows)
    }

    private suspend fun executeAggregateSelect(
        query: SelectQuery,
        plan: PhysicalPlan,
        context: QueryContext,
    ): QueryResult {
        // An aggregate consumes every matching row and produces one (or one per group); LIMIT
        // bounds that output, not the input. Leaving the planner's Limit in place made it
        // truncate the rows being aggregated, so `SELECT COUNT(*) FROM t LIMIT 1` answered 1
        // however many rows the table held.
        val docIds = resolveDocIds(stripLimit(plan)?.inner ?: plan, context)
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        val docs =
            docIds.mapNotNull { id ->
                storage.getDocument(context.namespaceId, id, treeHash)
            }
        val rows =
            if (query.groupBy.isEmpty()) {
                listOf(QueryRow(projectAggregateRow(query.projections, docs, context.schema, context.parameters)))
            } else {
                docs
                    .groupBy { doc -> groupKey(query.groupBy, doc, context.schema, context.parameters) }
                    .map { (_, groupDocs) ->
                        QueryRow(projectAggregateRow(query.projections, groupDocs, context.schema, context.parameters))
                    }
            }
        return QueryResult(columns = columnsFor(query, context.schema), rows = rows)
    }

    private suspend fun executeJoinSelect(
        query: SelectQuery,
        context: QueryContext,
    ): QueryResult {
        val leftAlias = query.from.alias ?: query.from.name
        val leftBinding = resolveBinding(query.from.name, context)
        val leftNs = leftBinding.namespaceId
        val leftSchema = leftBinding.schema
        val leftDag = dagFor(leftNs)
        val leftCtx = context.copy(namespaceId = leftNs, schema = leftSchema)
        val leftPlan =
            DefaultQueryPlanner().plan(
                SqlStatement.Select(
                    SelectQuery(
                        false,
                        listOf(SelectProjection.Star()),
                        query.from,
                        emptyList(),
                        query.where,
                        emptyList(),
                        emptyList(),
                        null,
                        0,
                    ),
                ),
                leftCtx,
            )
        val leftIds = resolveDocIds(leftPlan, leftCtx, leftDag)
        val join = query.joins.single()
        val rightAlias = join.table.alias ?: join.table.name
        val rightBinding = resolveBinding(join.table.name, context)
        val rightNs = rightBinding.namespaceId
        val rightSchema = rightBinding.schema
        val rightDag = dagFor(rightNs)
        val rightCtx = context.copy(namespaceId = rightNs, schema = rightSchema)
        val rightPlan = PhysicalPlan.FullTableScan("join probe")
        val rightIds = resolveDocIds(rightPlan, rightCtx, rightDag)
        val schemas = mapOf(leftAlias to leftSchema, rightAlias to rightSchema)
        val rows = mutableListOf<QueryRow>()
        val leftAt = context.atCommit ?: leftDag.head()
        val rightAt = context.atCommit ?: rightDag.head()
        val leftTree = leftDag.getCommitOrThrow(leftAt).documentTreeHash
        val rightTree = rightDag.getCommitOrThrow(rightAt).documentTreeHash
        for (leftId in leftIds) {
            val leftDoc = storage.getDocument(leftNs, leftId, leftTree) ?: continue
            for (rightId in rightIds) {
                val rightDoc = storage.getDocument(rightNs, rightId, rightTree) ?: continue
                val joined = mapOf(leftAlias to leftDoc, rightAlias to rightDoc)
                if (!SqlPredicate.evalJoin(join.on, joined, schemas, context.parameters)) continue
                rows +=
                    QueryRow(
                        projectJoinRow(query.projections, joined, schemas, context.parameters),
                    )
            }
        }
        return QueryResult(columns = columnsFor(query, leftSchema), rows = rows.take(context.maxRows))
    }

    suspend fun resolveDocIdsForWhere(
        where: SqlExpr?,
        schema: KdbSchema,
        context: QueryContext,
        planner: QueryPlanner = DefaultQueryPlanner(),
    ): List<KdbUuid> {
        val plan =
            planner.plan(
                SqlStatement.Select(
                    SelectQuery(
                        false,
                        listOf(SelectProjection.Star()),
                        TableRef("t", null),
                        emptyList(),
                        where,
                        emptyList(),
                        emptyList(),
                        null,
                        0,
                    ),
                ),
                context,
            )
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

    /** The limit and offset peeled off a plan by [stripLimit], with the plan underneath them. */
    private data class StrippedLimit(val inner: PhysicalPlan, val limit: Int, val offset: Int)

    /**
     * Peels the planner's outermost [PhysicalPlan.Limit] off a plan so the caller can apply it at
     * the right point instead. Returns null when there is no Limit to peel.
     */
    private fun stripLimit(plan: PhysicalPlan): StrippedLimit? =
        (plan as? PhysicalPlan.Limit)?.let { StrippedLimit(it.input, it.limit, it.offset) }

    private suspend fun resolveDocIds(
        plan: PhysicalPlan,
        context: QueryContext,
        commitDag: CommitDag = dag,
    ): List<KdbUuid> =
        when (plan) {
            is PhysicalPlan.Limit -> {
                val inner = resolveDocIds(plan.input, context, commitDag)
                inner.drop(plan.offset).take(plan.limit).take(context.maxRows)
            }
            is PhysicalPlan.Sort -> resolveDocIds(plan.input, context, commitDag)
            is PhysicalPlan.Project -> resolveDocIds(plan.input, context, commitDag)
            is PhysicalPlan.Filter -> {
                val ids = resolveDocIds(plan.input, context, commitDag)
                filterIds(ids, plan.predicate, context, commitDag)
            }
            is PhysicalPlan.IndexScan -> indexScan(plan, context)
            is PhysicalPlan.InListScan -> inListScan(plan, context)
            is PhysicalPlan.FullTableScan -> fullScan(context, commitDag)
        }

    private suspend fun inListScan(
        plan: PhysicalPlan.InListScan,
        context: QueryContext,
    ): List<KdbUuid> {
        val registry = indexManager.registryFor(context.namespaceId)
        val reader = indexManager.reader
        return plan.keys
            .flatMap { key ->
                reader.lookupExact(registry, plan.fieldName, key, context.atCommit)
            }
            .distinct()
            .take(context.maxRows)
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

    private suspend fun fullScan(
        context: QueryContext,
        commitDag: CommitDag,
    ): List<KdbUuid> {
        val atCommit = context.atCommit ?: commitDag.head()
        val treeHash = commitDag.getCommitOrThrow(atCommit).documentTreeHash
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
        commitDag: CommitDag,
    ): List<KdbUuid> {
        val atCommit = context.atCommit ?: commitDag.head()
        val treeHash = commitDag.getCommitOrThrow(atCommit).documentTreeHash
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
                    when (val expr = proj.expr) {
                        is SqlExpr.FunctionCall ->
                            SqlAggregates.evalAggregate(expr, listOf(doc), schema, context.parameters)
                        else ->
                            SqlPredicate.evalCell(expr, doc, schema, context.parameters) ?: SqlCell.Null
                    }
                is SelectProjection.Star -> SqlCell.Null
            }
        }
    }

    private fun projectAggregateRow(
        projections: List<SelectProjection>,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): List<SqlCell> =
        projections.map { proj ->
            when (proj) {
                is SelectProjection.Expression ->
                    when (val expr = proj.expr) {
                        is SqlExpr.FunctionCall ->
                            SqlAggregates.evalAggregate(expr, docs, schema, parameters)
                        else -> SqlCell.Null
                    }
                is SelectProjection.Column -> SqlCell.Null
                is SelectProjection.Star -> SqlCell.Null
            }
        }

    private fun projectJoinRow(
        projections: List<SelectProjection>,
        joined: Map<String, KdbDocument>,
        schemas: Map<String, KdbSchema>,
        parameters: List<SqlParameter>,
    ): List<SqlCell> =
        projections.map { proj ->
            when (proj) {
                is SelectProjection.Column -> {
                    val alias = proj.name.substringBefore('.', proj.name)
                    val col = proj.name.substringAfter('.', proj.name)
                    val doc = joined[alias] ?: joined.values.first()
                    val schema = schemas[alias] ?: schemas.values.first()
                    SqlPredicate.cellForColumn(col, doc, schema) ?: SqlCell.Null
                }
                is SelectProjection.Expression ->
                    when (val expr = proj.expr) {
                        is SqlExpr.QualifiedColumn -> {
                            val doc = joined[expr.qualifier] ?: return@map SqlCell.Null
                            val schema = schemas[expr.qualifier] ?: return@map SqlCell.Null
                            SqlPredicate.cellForColumn(expr.name, doc, schema) ?: SqlCell.Null
                        }
                        is SqlExpr.FunctionCall ->
                            SqlAggregates.evalAggregate(expr, joined.values.toList(), schemas.values.first(), parameters, joined)
                        else -> SqlCell.Null
                    }
                is SelectProjection.Star -> SqlCell.Null
            }
        }

    private fun groupKey(
        exprs: List<SqlExpr>,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): List<SqlCell?> = exprs.map { SqlPredicate.evalCell(it, doc, schema, parameters) }

    private fun resolveBinding(
        tableName: String,
        context: QueryContext,
    ): NamespaceBinding =
        context.namespacesByTable[tableName]
            ?: NamespaceBinding(context.namespaceId, context.schema)

    private fun dagFor(namespaceId: String): CommitDag = namespaceDags[namespaceId] ?: dag

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
