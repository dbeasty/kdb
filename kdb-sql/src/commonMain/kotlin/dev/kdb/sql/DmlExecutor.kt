package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.error.KdbResult
import dev.kdb.error.SchemaViolationException
import dev.kdb.json.JsonValue
import dev.kdb.json.KdbJsonFunctionRegistry
import dev.kdb.json.kdbJsonGet
import dev.kdb.json.kdbJsonSet
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaEngine
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter

internal class DmlExecutor(
    private val sqlExecutor: SqlExecutor,
    private val storage: StorageAdapter,
    private val dag: CommitDag,
    private val planner: QueryPlanner = DefaultQueryPlanner(),
) {
    suspend fun executeUpdate(
        update: UpdateStatement,
        context: QueryContext,
    ): List<KdbOp> {
        val targetIds = resolveTargetIds(update.where, context)
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        val ops = mutableListOf<KdbOp>()
        for (id in targetIds) {
            val doc = storage.getDocument(context.namespaceId, id, treeHash) ?: continue
            var json = doc.json
            for (assignment in update.assignments) {
                json =
                    when (assignment.column) {
                        "_doc" -> evalDocAssignment(assignment.expr, doc, context, json)
                        else -> patchSchemaField(json, assignment, context)
                    }
            }
            validateJson(id, json, context.schema)
            ops += KdbOp.Write(id, json)
        }
        return ops
    }

    suspend fun executeInsert(
        insert: InsertStatement,
        context: QueryContext,
    ): List<KdbOp> {
        val id = KdbUuid.random()
        val fields = insert.columns
        val values = insert.values
        if (fields.size != values.size) {
            throw SqlPlanningException("column count does not match value count", "")
        }
        var json = "{}"
        for (i in fields.indices) {
            val cell = evalValueExpr(values[i], null, context) ?: SqlCell.Null
            json = kdbJsonSet(json, "$.${fields[i]}", cellToJsonValue(cell))
        }
        validateJson(id, json, context.schema)
        return listOf(KdbOp.Write(id, json))
    }

    suspend fun executeDelete(
        delete: DeleteStatement,
        context: QueryContext,
    ): List<KdbOp> {
        val targetIds = resolveTargetIds(delete.where, context)
        return targetIds.map { KdbOp.Delete(it) }
    }

    private suspend fun resolveTargetIds(
        where: SqlExpr?,
        context: QueryContext,
    ): List<KdbUuid> = sqlExecutor.resolveDocIdsForWhere(where, context.schema, context, planner)

    private fun validateJson(
        id: KdbUuid,
        json: String,
        schema: KdbSchema,
    ) {
        if (schema.isNone) return
        when (val r = SchemaEngine.validate(KdbDocument(id, json), schema)) {
            is KdbResult.Success -> Unit
            is KdbResult.Failure -> {
                val ex = r.exception
                if (ex is SchemaViolationException) {
                    throw SqlPlanningException(
                        "schema violation: ${ex.violations.firstOrNull()?.detail ?: ex.message}",
                        "",
                    )
                }
                throw SqlPlanningException(ex.message ?: "schema validation failed", "")
            }
        }
    }

    private fun evalDocAssignment(
        expr: SqlExpr,
        doc: KdbDocument,
        context: QueryContext,
        currentJson: String,
    ): String =
        when (expr) {
            is SqlExpr.FunctionCall -> evalFunction(expr, doc, context) ?: currentJson
            is SqlExpr.Literal ->
                when (val cell = expr.cell) {
                    is SqlCell.JsonVal -> cell.json
                    is SqlCell.StringVal -> cell.value
                    else -> currentJson
                }
            else -> currentJson
        }

    private fun patchSchemaField(
        json: String,
        assignment: Assignment,
        context: QueryContext,
    ): String {
        val cell = evalValueExpr(assignment.expr, null, context) ?: SqlCell.Null
        return kdbJsonSet(json, "$.${assignment.column}", cellToJsonValue(cell))
    }

    private fun evalValueExpr(
        expr: SqlExpr,
        doc: KdbDocument?,
        context: QueryContext,
    ): SqlCell? =
        when (expr) {
            is SqlExpr.Literal -> expr.cell
            is SqlExpr.Parameter ->
                SqlPredicate.evalCell(
                    expr,
                    doc ?: KdbDocument(KdbUuid.random(), "{}"),
                    context.schema,
                    context.parameters,
                )
            is SqlExpr.FunctionCall ->
                doc?.let { evalFunction(expr, it, context) }?.let { SqlCell.JsonVal(it) }
            is SqlExpr.ColumnRef -> doc?.let { SqlPredicate.cellForColumn(expr.name, it, context.schema) }
            else -> null
        }

    private fun evalFunction(
        call: SqlExpr.FunctionCall,
        doc: KdbDocument,
        context: QueryContext,
    ): String? {
        val desc = KdbJsonFunctionRegistry.get(call.name) ?: return null
        val jsonArgs =
            call.args.map { arg ->
                when (arg) {
                    is SqlExpr.Literal -> cellToJsonValue(arg.cell)
                    is SqlExpr.ColumnRef ->
                        if (arg.name == "_doc") {
                            JsonValue.JString(doc.json)
                        } else {
                            kdbJsonGet(doc.json, "$.${arg.name}")
                        }
                    is SqlExpr.Parameter -> {
                        val cell =
                            SqlPredicate.evalCell(arg, doc, context.schema, context.parameters)
                        cellToJsonValue(cell ?: SqlCell.Null)
                    }
                    else -> null
                }
            }
        if (jsonArgs.any { it == null }) return null
        return desc.evaluate(jsonArgs.filterNotNull())?.toJsonString()
    }

    private fun cellToJsonValue(cell: SqlCell): JsonValue =
        when (cell) {
            SqlCell.Null -> JsonValue.JNull
            is SqlCell.StringVal -> JsonValue.JString(cell.value)
            is SqlCell.LongVal -> JsonValue.JInt(cell.value)
            is SqlCell.DoubleVal -> JsonValue.JNumber(cell.value)
            is SqlCell.BoolVal -> JsonValue.JBool(cell.value)
            is SqlCell.JsonVal -> JsonValue.JString(cell.json)
        }
}
