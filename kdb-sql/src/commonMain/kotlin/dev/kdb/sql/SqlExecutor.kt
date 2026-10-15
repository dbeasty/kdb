package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexRegistry
import dev.kdb.index.RankedResult
import dev.kdb.index.fusion.FusionArm
import dev.kdb.index.fusion.fuseRankings
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter

internal class SqlExecutor(
    private val indexManager: IndexManager,
    private val storage: StorageAdapter,
    private val dag: CommitDag,
    private val namespaceDags: Map<String, CommitDag> = emptyMap(),
) {
    /**
     * Non-aggregate SELECT pipeline (Layer 16 §3): resolve ids → materialize → sort → project →
     * distinct (first occurrence wins) → offset/limit. The planner's outermost Limit is only
     * applied while resolving ids when neither ORDER BY nor DISTINCT is present.
     */
    suspend fun executeSelect(
        query: SelectQuery,
        plan: PhysicalPlan,
        context: QueryContext,
    ): QueryResult {
        if (query.joins.isNotEmpty()) {
            return executeJoinSelect(query, context)
        }
        val env = buildEnv(query, context)
        if (query.groupBy.isNotEmpty() || SqlAggregates.queryHasAggregates(query)) {
            return executeAggregateSelect(query, plan, context, env)
        }
        val deferLimit = query.orderBy.isNotEmpty() || query.distinct
        val stripped = if (deferLimit) stripLimit(plan) else null
        val docIds = resolveDocIds(stripped?.inner ?: plan, context, env)
        val topK = if (query.orderBy.isNotEmpty() && !query.distinct && stripped != null) topKBound(stripped) else null
        var pairs =
            if (topK != null) {
                topKPairs(docIds, context, query.orderBy, env, topK)
            } else {
                materialize(docIds, context, dag)
            }
        if (query.orderBy.isNotEmpty() && topK == null) {
            pairs = sortPairs(pairs, query.orderBy, env)
        }
        var rows = pairs.map { (_, doc) -> QueryRow(projectRow(query.projections, doc, env)) }
        if (query.distinct) {
            rows = rows.distinctBy { it.values }
        }
        if (stripped != null) {
            rows = rows.drop(stripped.offset).take(stripped.limit)
        }
        return QueryResult(columns = columnsFor(query, context.schema), rows = boundRows(rows, context.maxRows))
    }

    private suspend fun materialize(
        docIds: List<KdbUuid>,
        context: QueryContext,
        commitDag: CommitDag,
    ): List<Pair<KdbUuid, KdbDocument>> {
        val atCommit = context.atCommit ?: commitDag.head()
        val treeHash = commitDag.getCommitOrThrow(atCommit).documentTreeHash
        return docIds.mapNotNull { id ->
            storage.getDocument(context.namespaceId, id, treeHash)?.let { id to it }
        }
    }

    // ---------------------------------------------------------------- scoring (Layer 16 §9.1)

    /**
     * Builds the evaluation environment for one statement, running every `MATCH`/`SIMILARITY`/
     * `FUSE` it contains against the index reader up front. Rankings are fetched to the §9.1
     * depth when the query is score-ordered with a LIMIT, else every hit.
     */
    private suspend fun buildEnv(
        query: SelectQuery,
        context: QueryContext,
    ): EvalEnv {
        val base = EvalEnv(context.schema, context.parameters, tableQualifiersOf(query.from), projectionAliases(query))
        val scoreExprs = collectScoreExprs(query)
        if (scoreExprs.isEmpty()) return base
        val catalog =
            context.indexCatalog
                ?: runCatching { registryIndexCatalog(indexManager.registryFor(context.namespaceId)) }.getOrNull()
        val depth = if (scoreOrderExpr(query) != null) scoreCandidateDepth(query.limit, query.offset) else Int.MAX_VALUE
        val registry = indexManager.registryFor(context.namespaceId)
        val leafCache = HashMap<SqlExpr, List<RankedResult>>()
        val scores = LinkedHashMap<SqlExpr, Map<KdbUuid, Float>>()
        for (expr in scoreExprs) {
            val ranking = rank(expr, catalog, registry, context, depth, leafCache)
            val table = LinkedHashMap<KdbUuid, Float>(ranking.size)
            for (r in ranking) if (r.docId !in table) table[r.docId] = r.score
            scores[expr] = table
        }
        return base.withScores(scores)
    }

    private suspend fun rank(
        expr: SqlExpr,
        catalog: SqlIndexCatalog?,
        registry: IndexRegistry,
        context: QueryContext,
        depth: Int,
        leafCache: MutableMap<SqlExpr, List<RankedResult>>,
    ): List<RankedResult> {
        if (expr is SqlExpr.Fuse) {
            val arms =
                expr.arms.map { arm ->
                    FusionArm(rank(arm, catalog, registry, context, depth, leafCache))
                }
            return fuseRankings(arms, fusionModeOf(expr.mode, ""), limit = depth)
        }
        leafCache[expr]?.let { return it }
        val arm = resolveArms(expr, catalog, context.parameters).single()
        val results =
            when (arm) {
                is ResolvedArm.Text ->
                    indexManager.reader.lookupFullText(registry, arm.descriptor.fieldName, arm.query, context.atCommit, depth)
                is ResolvedArm.Vector ->
                    indexManager.reader.lookupVector(
                        registry,
                        arm.descriptor.fieldName,
                        arm.vector,
                        depth,
                        context.atCommit,
                    )
            }
        leafCache[expr] = results
        return results
    }

    // ---------------------------------------------------------------- aggregates (Layer 16 §5)

    private suspend fun executeAggregateSelect(
        query: SelectQuery,
        plan: PhysicalPlan,
        context: QueryContext,
        env: EvalEnv,
    ): QueryResult {
        // An aggregate consumes every matching row and produces one (or one per group); LIMIT
        // bounds that output, not the input.
        val stripped = stripLimit(plan)
        val docIds = resolveDocIds(stripped?.inner ?: plan, context, env)
        val docs = materialize(docIds, context, dag).map { it.second }
        val groups: List<Pair<List<SqlCell>, List<KdbDocument>>> =
            if (query.groupBy.isEmpty()) {
                listOf(emptyList<SqlCell>() to docs)
            } else {
                val byKey = LinkedHashMap<List<SqlCell>, MutableList<KdbDocument>>()
                for (doc in docs) {
                    val key = query.groupBy.map { SqlPredicate.evalCell(resolveAlias(it, env), doc, env) }
                    byKey.getOrPut(key) { mutableListOf() } += doc
                }
                byKey.entries.map { it.key to it.value.toList() }
            }
        var rows =
            groups.map { (key, groupDocs) ->
                AggregateRow(key, groupDocs, projectAggregateRow(query.projections, groupDocs, env))
            }
        rows =
            if (query.orderBy.isNotEmpty()) {
                sortAggregateRows(rows, query, env)
            } else {
                // Deterministic group order without ORDER BY: ascending group key, total comparator.
                rows.sortedWith { a, b -> compareCellLists(a.key, b.key) }
            }
        var out = rows.map { QueryRow(it.cells) }
        if (query.distinct) out = out.distinctBy { it.values }
        if (stripped != null) out = out.drop(stripped.offset).take(stripped.limit)
        return QueryResult(columns = columnsFor(query, context.schema), rows = boundRows(out, context.maxRows))
    }

    /**
     * `maxRows` bounds what a query may return, never what it may look at: the id lists above are
     * not cut, because a residual filter still runs over them and cutting first dropped every
     * match past the cut. A result larger than the bound is an error, not a prefix - a caller
     * handed the first N rows of an answer cannot tell it is incomplete.
     */
    private fun boundRows(
        rows: List<QueryRow>,
        maxRows: Int,
    ): List<QueryRow> {
        if (rows.size > maxRows) throw SqlResultLimitException(maxRows)
        return rows
    }

    private class AggregateRow(val key: List<SqlCell>, val docs: List<KdbDocument>, val cells: List<SqlCell>)

    private fun compareCellLists(
        a: List<SqlCell>,
        b: List<SqlCell>,
    ): Int {
        for (i in 0 until minOf(a.size, b.size)) {
            val c = SqlPredicate.compareTotal(a[i], b[i])
            if (c != 0) return c
        }
        return a.size.compareTo(b.size)
    }

    /**
     * ORDER BY over aggregate output: an item names a projection (by alias or by the same
     * expression), else a group-key expression, else it is evaluated against the group's first
     * document.
     */
    private fun sortAggregateRows(
        rows: List<AggregateRow>,
        query: SelectQuery,
        env: EvalEnv,
    ): List<AggregateRow> {
        fun cellOf(
            row: AggregateRow,
            item: OrderItem,
        ): SqlCell {
            val expr = item.expr
            if (expr is SqlExpr.ColumnRef) {
                val idx =
                    query.projections.indexOfFirst {
                        (it is SelectProjection.Expression && it.alias == expr.name) ||
                            (it is SelectProjection.Column && (it.alias ?: it.name) == expr.name)
                    }
                if (idx >= 0) return row.cells[idx]
            }
            val projIdx = query.projections.indexOfFirst { it is SelectProjection.Expression && it.expr == expr }
            if (projIdx >= 0) return row.cells[projIdx]
            val keyIdx = query.groupBy.indexOf(expr)
            if (keyIdx >= 0) return row.key[keyIdx]
            if (expr is SqlExpr.FunctionCall && SqlAggregates.containsAggregate(expr)) {
                return SqlAggregates.evalAggregate(expr, row.docs, env)
            }
            val first = row.docs.firstOrNull() ?: return SqlCell.Null
            return SqlPredicate.evalCell(expr, first, env)
        }
        return rows.sortedWith { a, b ->
            for (item in query.orderBy) {
                val cmp = SqlPredicate.compareTotal(cellOf(a, item), cellOf(b, item))
                if (cmp != 0) return@sortedWith if (item.ascending) cmp else -cmp
            }
            0
        }
    }

    private fun projectAggregateRow(
        projections: List<SelectProjection>,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): List<SqlCell> =
        projections.map { proj ->
            when (proj) {
                is SelectProjection.Expression ->
                    when (val expr = proj.expr) {
                        is SqlExpr.FunctionCall ->
                            if (SqlAggregates.isAggregateFunction(expr.name)) {
                                SqlAggregates.evalAggregate(expr, docs, env)
                            } else {
                                docs.firstOrNull()?.let { SqlPredicate.evalCell(expr, it, env) } ?: SqlCell.Null
                            }
                        else -> docs.firstOrNull()?.let { SqlPredicate.evalCell(expr, it, env) } ?: SqlCell.Null
                    }
                // Group-key columns project the group's value (Layer 16 §5).
                is SelectProjection.Column ->
                    docs.firstOrNull()?.let { SqlPredicate.cellForColumn(proj.name, it, env) } ?: SqlCell.Null
                is SelectProjection.Star -> SqlCell.Null
            }
        }

    // ---------------------------------------------------------------- joins

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
        val leftQuery =
            SelectQuery(false, listOf(SelectProjection.Star()), query.from, emptyList(), query.where, emptyList(), emptyList(), null, 0)
        val leftPlan = DefaultQueryPlanner().plan(SqlStatement.Select(leftQuery), leftCtx)
        val leftEnv = EvalEnv(leftSchema, context.parameters, tableQualifiersOf(query.from))
        val leftIds = resolveDocIds(leftPlan, leftCtx, leftEnv, leftDag)
        val join = query.joins.single()
        val rightAlias = join.table.alias ?: join.table.name
        val rightBinding = resolveBinding(join.table.name, context)
        val rightNs = rightBinding.namespaceId
        val rightSchema = rightBinding.schema
        val rightDag = dagFor(rightNs)
        val rightCtx = context.copy(namespaceId = rightNs, schema = rightSchema)
        val rightEnv = EvalEnv(rightSchema, context.parameters, tableQualifiersOf(join.table))
        val rightIds = resolveDocIds(PhysicalPlan.FullTableScan("join probe"), rightCtx, rightEnv, rightDag)
        val envs = mapOf(leftAlias to leftEnv, rightAlias to rightEnv)
        val rows = mutableListOf<QueryRow>()
        val leftDocs = materialize(leftIds, leftCtx, leftDag)
        val rightDocs = materialize(rightIds, rightCtx, rightDag)
        for ((_, leftDoc) in leftDocs) {
            for ((_, rightDoc) in rightDocs) {
                val joined = mapOf(leftAlias to leftDoc, rightAlias to rightDoc)
                if (!SqlPredicate.evalJoin(join.on, joined, envs)) continue
                rows += QueryRow(projectJoinRow(query.projections, joined, envs))
            }
        }
        return QueryResult(columns = columnsFor(query, leftSchema), rows = boundRows(rows, context.maxRows))
    }

    private fun projectJoinRow(
        projections: List<SelectProjection>,
        joined: Map<String, KdbDocument>,
        envs: Map<String, EvalEnv>,
    ): List<SqlCell> =
        projections.map { proj ->
            when (proj) {
                is SelectProjection.Column -> SqlPredicate.evalJoinCell(SqlExpr.ColumnRef(proj.name), joined, envs)
                is SelectProjection.Expression -> SqlPredicate.evalJoinCell(proj.expr, joined, envs)
                is SelectProjection.Star -> SqlCell.Null
            }
        }

    // ---------------------------------------------------------------- DML support

    suspend fun resolveDocIdsForWhere(
        where: SqlExpr?,
        table: TableRef,
        context: QueryContext,
        planner: QueryPlanner = DefaultQueryPlanner(),
    ): List<KdbUuid> {
        val query = SelectQuery(false, listOf(SelectProjection.Star()), table, emptyList(), where, emptyList(), emptyList(), null, 0)
        val plan = planner.plan(SqlStatement.Select(query), context)
        val env = buildEnv(query, context)
        return resolveDocIds(plan, context, env)
    }

    // ---------------------------------------------------------------- sorting / projection

    private fun resolveAlias(
        expr: SqlExpr,
        env: EvalEnv,
    ): SqlExpr = if (expr is SqlExpr.ColumnRef) env.aliases[expr.name] ?: expr else expr

    private fun sortPairs(
        pairs: List<Pair<KdbUuid, KdbDocument>>,
        orderBy: List<OrderItem>,
        env: EvalEnv,
    ): List<Pair<KdbUuid, KdbDocument>> {
        val resolved = orderBy.map { resolveAlias(it.expr, env) to it.ascending }
        return pairs.sortedWith { (_, docA), (_, docB) ->
            for ((expr, ascending) in resolved) {
                val ca = SqlPredicate.evalCell(expr, docA, env)
                val cb = SqlPredicate.evalCell(expr, docB, env)
                val cmp = SqlPredicate.compareTotal(ca, cb)
                if (cmp != 0) return@sortedWith if (ascending) cmp else -cmp
            }
            0
        }
    }

    /** LIMIT + OFFSET when a bounded top-k can serve it, else null (the ordinary sort runs). */
    private fun topKBound(stripped: StrippedLimit): Int? {
        val (_, limit, offset) = stripped
        if (limit < 0 || offset < 0 || limit > SqlTopK.MAX_K || offset > SqlTopK.MAX_K - limit) return null
        return limit + offset
    }

    /**
     * ORDER BY ... LIMIT without materializing every candidate (Go `topKDocs`): documents are read
     * one at a time and only the best [k] under the ORDER BY are held, each with its sort keys
     * evaluated once. Returned in final order - exactly the stable full sort's first [k] rows.
     */
    private suspend fun topKPairs(
        docIds: List<KdbUuid>,
        context: QueryContext,
        orderBy: List<OrderItem>,
        env: EvalEnv,
        k: Int,
    ): List<Pair<KdbUuid, KdbDocument>> {
        if (k == 0) return emptyList()
        val resolved = orderBy.map { resolveAlias(it.expr, env) }
        val top =
            SqlTopK<Pair<Pair<KdbUuid, KdbDocument>, List<SqlCell>>>(k) { (_, ka), (_, kb) ->
                for (i in orderBy.indices) {
                    val cmp = SqlPredicate.compareTotal(ka[i], kb[i])
                    if (cmp != 0) return@SqlTopK if (orderBy[i].ascending) cmp else -cmp
                }
                0
            }
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        for (id in docIds) {
            val doc = storage.getDocument(context.namespaceId, id, treeHash) ?: continue
            top.offer((id to doc) to resolved.map { SqlPredicate.evalCell(it, doc, env) })
        }
        return top.sorted().map { it.first }
    }

    private fun projectRow(
        projections: List<SelectProjection>,
        doc: KdbDocument,
        env: EvalEnv,
    ): List<SqlCell> {
        val schema = env.schema
        if (projections.any { it is SelectProjection.Star }) {
            val cols = mutableListOf<SqlCell>()
            cols += SqlCell.StringVal(doc.id.toString())
            for ((_, field) in schema.fieldsByName) {
                cols += SqlPredicate.cellForColumn(field.name, doc, env)
            }
            cols += SqlCell.JsonVal(doc.json)
            return cols
        }
        return projections.map { proj ->
            when (proj) {
                is SelectProjection.Column -> SqlPredicate.cellForColumn(proj.name, doc, env)
                is SelectProjection.Expression -> SqlPredicate.evalCell(proj.expr, doc, env)
                is SelectProjection.Star -> SqlCell.Null
            }
        }
    }

    /** The limit and offset peeled off a plan by [stripLimit], with the plan underneath them. */
    private data class StrippedLimit(val inner: PhysicalPlan, val limit: Int, val offset: Int)

    private fun stripLimit(plan: PhysicalPlan): StrippedLimit? =
        (plan as? PhysicalPlan.Limit)?.let { StrippedLimit(it.input, it.limit, it.offset) }

    // ---------------------------------------------------------------- id resolution

    private suspend fun resolveDocIds(
        plan: PhysicalPlan,
        context: QueryContext,
        env: EvalEnv,
        commitDag: CommitDag = dag,
    ): List<KdbUuid> =
        when (plan) {
            is PhysicalPlan.Limit -> {
                val inner = resolveDocIds(plan.input, context, env, commitDag)
                inner.drop(plan.offset).take(plan.limit)
            }
            is PhysicalPlan.Sort -> resolveDocIds(plan.input, context, env, commitDag)
            is PhysicalPlan.Project -> resolveDocIds(plan.input, context, env, commitDag)
            is PhysicalPlan.Filter -> {
                val ids = resolveDocIds(plan.input, context, env, commitDag)
                filterIds(ids, plan.predicate, context, env, commitDag)
            }
            is PhysicalPlan.IndexScan -> indexScan(plan, context)
            is PhysicalPlan.InListScan -> inListScan(plan, context)
            is PhysicalPlan.ScoredScan ->
                (env.scores[plan.scoreExpr] ?: throw SqlPlanningException("ranking missing for ${plan.scoreExpr}", ""))
                    .keys.toList()
            is PhysicalPlan.FullTableScan -> fullScan(context, commitDag)
            is PhysicalPlan.KeyLookup -> plan.ids
            is PhysicalPlan.UnionScan -> plan.arms.flatMap { resolveDocIds(it, context, env, commitDag) }.distinct()
        }

    private suspend fun inListScan(
        plan: PhysicalPlan.InListScan,
        context: QueryContext,
    ): List<KdbUuid> {
        val registry = indexManager.registryFor(context.namespaceId)
        val reader = indexManager.reader
        return plan.keys
            .flatMap { key -> reader.lookupExact(registry, plan.fieldName, key, context.atCommit) }
            .distinct()
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
                reader.lookupRange(registry, plan.fieldName, spec.from, spec.to, context.atCommit)
            is IndexLookupSpec.FullText ->
                reader.lookupFullText(registry, plan.fieldName, spec.query, context.atCommit).map { it.docId }
            is IndexLookupSpec.VectorAnn ->
                reader.lookupVector(registry, plan.fieldName, spec.queryVector, spec.k, context.atCommit).map { it.docId }
        }
    }

    private suspend fun fullScan(
        context: QueryContext,
        commitDag: CommitDag,
    ): List<KdbUuid> {
        val atCommit = context.atCommit ?: commitDag.head()
        val treeHash = commitDag.getCommitOrThrow(atCommit).documentTreeHash
        val ids = mutableListOf<KdbUuid>()
        var examined = 0
        storage.scanDocuments(context.namespaceId, treeHash, batchSize = 256) { batch ->
            for (d in batch) {
                examined++
                if (context.rowBudget > 0 && examined > context.rowBudget) throw SqlRowBudgetException(context.rowBudget)
                ids += d.id
            }
        }
        return ids
    }

    private suspend fun filterIds(
        ids: List<KdbUuid>,
        predicate: SqlExpr,
        context: QueryContext,
        env: EvalEnv,
        commitDag: CommitDag,
    ): List<KdbUuid> {
        val atCommit = context.atCommit ?: commitDag.head()
        val treeHash = commitDag.getCommitOrThrow(atCommit).documentTreeHash
        var examined = 0
        return ids.filter { id ->
            examined++
            if (context.rowBudget > 0 && examined > context.rowBudget) throw SqlRowBudgetException(context.rowBudget)
            val doc = storage.getDocument(context.namespaceId, id, treeHash) ?: return@filter false
            SqlPredicate.eval(predicate, doc, env)
        }
    }

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
                        when (proj.name) {
                            "_doc" -> "JSON"
                            else -> schema.fieldsByName[proj.name]?.type?.sqlTypeName() ?: "VARCHAR"
                        },
                        when (proj.name) {
                            "kdb_id" -> ColumnSource.KDB_ID
                            "_doc" -> ColumnSource.DOC_JSON
                            else -> ColumnSource.SCHEMA_FIELD
                        },
                    )
                is SelectProjection.Expression ->
                    ResultColumn(proj.alias ?: "expr", expressionSqlType(proj.expr), ColumnSource.EXPRESSION)
                is SelectProjection.Star ->
                    ResultColumn("_doc", "JSON", ColumnSource.DOC_JSON)
            }
        }
    }

    /** Score columns are DOUBLE (Layer 16 §9.1); everything else keeps the historical JSON label. */
    private fun expressionSqlType(expr: SqlExpr): String =
        when {
            isScoreExpr(expr) -> "DOUBLE"
            expr is SqlExpr.FunctionCall && expr.name.lowercase() == "array_length" -> "BIGINT"
            expr is SqlExpr.FunctionCall && expr.name.lowercase().startsWith("array_contains") -> "BOOLEAN"
            expr is SqlExpr.FunctionCall && expr.name.lowercase() == "exists" -> "BOOLEAN"
            expr is SqlExpr.FunctionCall && expr.name.lowercase() == "count" -> "BIGINT"
            else -> "JSON"
        }
}
