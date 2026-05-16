package dev.kdb.sql

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.index.IndexManager
import dev.kdb.json.JsonValue
import dev.kdb.json.KdbJsonFunctionRegistry
import dev.kdb.json.kdbJsonGet
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
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
        val docs =
            docIds.mapNotNull { id ->
                storage.getDocument(context.namespaceId, id, treeHash)
            }
        val rows =
            docs.map { doc ->
                QueryRow(projectRow(query.projections, doc, context))
            }
        val limited = rows.drop(query.offset ?: 0).take(query.limit ?: context.maxRows).take(context.maxRows)
        return QueryResult(columns = columnsFor(query, context.schema), rows = limited)
    }

    private suspend fun resolveDocIds(
        plan: PhysicalPlan,
        context: QueryContext,
    ): List<KdbUuid> =
        when (plan) {
            is PhysicalPlan.Limit -> {
                val inner = resolveDocIds(plan.input, context)
                inner.drop(plan.offset).take(plan.limit)
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
            evalPredicate(predicate, doc, context)
        }
    }

    private fun evalPredicate(
        expr: SqlExpr,
        doc: KdbDocument,
        context: QueryContext,
    ): Boolean =
        when (expr) {
            is SqlExpr.Binary -> {
                when (expr.op) {
                    BinaryOp.AND -> evalPredicate(expr.left, doc, context) && evalPredicate(expr.right, doc, context)
                    BinaryOp.OR -> evalPredicate(expr.left, doc, context) || evalPredicate(expr.right, doc, context)
                    BinaryOp.EQ -> compareCell(evalExpr(expr.left, doc, context), evalExpr(expr.right, doc, context)) == 0
                    else -> false
                }
            }
            else -> false
        }

    private fun compareCell(
        a: SqlCell?,
        b: SqlCell?,
    ): Int {
        if (a is SqlCell.Null && b is SqlCell.Null) return 0
        if (a is SqlCell.StringVal && b is SqlCell.StringVal) return a.value.compareTo(b.value)
        return -1
    }

    private fun evalExpr(
        expr: SqlExpr,
        doc: KdbDocument,
        context: QueryContext,
    ): SqlCell? =
        when (expr) {
            is SqlExpr.Literal -> expr.cell
            is SqlExpr.ColumnRef -> cellForColumn(expr.name, doc, context.schema)
            is SqlExpr.FunctionCall -> evalFunction(expr, doc)
            else -> null
        }

    private fun evalFunction(
        call: SqlExpr.FunctionCall,
        doc: KdbDocument,
    ): SqlCell? {
        val desc = KdbJsonFunctionRegistry.get(call.name) ?: return null
        val args =
            call.args.map { arg ->
                when (arg) {
                    is SqlExpr.Literal ->
                        when (val c = arg.cell) {
                            is SqlCell.StringVal -> JsonValue.JString(c.value)
                            is SqlCell.LongVal -> JsonValue.JInt(c.value)
                            is SqlCell.DoubleVal -> JsonValue.JNumber(c.value)
                            is SqlCell.BoolVal -> JsonValue.JBool(c.value)
                            else -> null
                        }
                    is SqlExpr.ColumnRef ->
                        if (arg.name == "_doc") {
                            JsonValue.JString(doc.json)
                        } else {
                            null
                        }
                    else -> null
                }
            }
        val result = desc.evaluate(args)
        return result?.let { SqlCell.StringVal(it.toJsonString()) }
    }

    private fun cellForColumn(
        name: String,
        doc: KdbDocument,
        schema: KdbSchema,
    ): SqlCell? =
        when (name) {
            "kdb_id" -> SqlCell.StringVal(doc.id.toString())
            "_doc" -> SqlCell.JsonVal(doc.json)
            else -> {
                val field = schema.fieldsByName[name] ?: return null
                val raw = kdbJsonGet(doc.json, "$.$name")
                jsonToCell(raw, field)
            }
        }

    private fun jsonToCell(
        raw: JsonValue?,
        field: SchemaField,
    ): SqlCell? =
        when (raw) {
            null, JsonValue.JNull -> SqlCell.Null
            is JsonValue.JString -> SqlCell.StringVal(raw.value)
            is JsonValue.JInt -> SqlCell.LongVal(raw.value)
            is JsonValue.JNumber -> SqlCell.DoubleVal(raw.value)
            is JsonValue.JBool -> SqlCell.BoolVal(raw.value)
            else -> SqlCell.JsonVal(raw.toJsonString())
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
                cols += cellForColumn(field.name, doc, schema) ?: SqlCell.Null
            }
            cols += SqlCell.JsonVal(doc.json)
            return cols
        }
        return projections.map { proj ->
            when (proj) {
                is SelectProjection.Column ->
                    cellForColumn(proj.name, doc, schema) ?: SqlCell.Null
                is SelectProjection.Expression ->
                    evalExpr(proj.expr, doc, context) ?: SqlCell.Null
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
