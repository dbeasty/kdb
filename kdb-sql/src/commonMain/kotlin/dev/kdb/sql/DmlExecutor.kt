package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.error.KdbResult
import dev.kdb.error.SchemaViolationException
import dev.kdb.json.JsonValue
import dev.kdb.json.kdbJsonSet
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaEngine
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter

/**
 * `UPDATE` / `INSERT` / `DELETE` → [KdbOp]s (Layer 16 §5). Assignment expressions are evaluated
 * against the pre-update document; `SET _doc = …` replaces the whole document, any other target
 * is a JSON-path set (`SET meta.reviewed = true`). The result is validated against the schema.
 */
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
        val env = EvalEnv(context.schema, context.parameters, tableQualifiersOf(update.table))
        for (assignment in update.assignments) {
            val target = env.resolvePath(assignment.column)
            if (target != SqlPredicate.DOC) {
                if (target == SqlPredicate.KDB_ID) throw SqlPlanningException("cannot assign kdb_id", "")
                SqlColumnValidation.checkColumn(assignment.column, env, allowAlias = false)
            }
            SqlColumnValidation.checkExpr(assignment.expr, env, allowAlias = false)
        }
        val targetIds = sqlExecutor.resolveDocIdsForWhere(update.where, update.table, context, planner)
        val atCommit = context.atCommit ?: dag.head()
        val treeHash = dag.getCommitOrThrow(atCommit).documentTreeHash
        val ops = mutableListOf<KdbOp>()
        for (id in targetIds) {
            val doc = storage.getDocument(context.namespaceId, id, treeHash) ?: continue
            var json = doc.json
            for (assignment in update.assignments) {
                val target = env.resolvePath(assignment.column)
                json =
                    if (target == SqlPredicate.DOC) {
                        evalDocAssignment(assignment.expr, doc, env)
                    } else {
                        val cell = SqlPredicate.evalCell(assignment.expr, doc, env)
                        setPath(json, target, SqlPaths.toJson(cell))
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
        val fields = insert.columns
        val env = EvalEnv(context.schema, context.parameters, tableQualifiersOf(insert.table))
        for (f in fields) {
            // kdb_id is minted by the engine and cannot be assigned. _doc CAN be: it supplies the
            // whole body, which is how a schemaless namespace inserts a document and what the Go
            // executor does with the same statement - rejecting it here would diverge the trees.
            if (env.resolvePath(f) == SqlPredicate.KDB_ID) {
                throw SqlPlanningException("cannot insert into reserved column $f", "")
            }
            if (env.resolvePath(f) != SqlPredicate.DOC) {
                SqlColumnValidation.checkColumn(f, env, allowAlias = false)
            }
        }
        val ops = mutableListOf<KdbOp>()
        for (values in insert.rows) {
            if (fields.size != values.size) {
                throw SqlPlanningException("column count does not match value count", "")
            }
            val id = KdbUuid.random()
            val blank = KdbDocument(id, "{}")
            var json = "{}"
            for (i in fields.indices) {
                val target = env.resolvePath(fields[i])
                json =
                    if (target == SqlPredicate.DOC) {
                        evalDocAssignment(values[i], blank, env)
                    } else {
                        val cell = SqlPredicate.evalCell(values[i], blank, env)
                        setPath(json, target, SqlPaths.toJson(cell))
                    }
            }
            validateJson(id, json, context.schema)
            ops += KdbOp.Write(id, json)
        }
        return ops
    }

    suspend fun executeDelete(
        delete: DeleteStatement,
        context: QueryContext,
    ): List<KdbOp> {
        val targetIds = sqlExecutor.resolveDocIdsForWhere(delete.where, delete.table, context, planner)
        return targetIds.map { KdbOp.Delete(it) }
    }

    private fun setPath(
        json: String,
        path: String,
        value: JsonValue,
    ): String =
        try {
            kdbJsonSet(json, "$.$path", value)
        } catch (e: SqlPlanningException) {
            throw e
        } catch (e: Exception) {
            throw SqlPlanningException("cannot set $path: ${e.message}", "")
        }

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

    /** `SET _doc = '<json>' | ? | kdb_json_set(_doc, …)` - the value must be a JSON object text. */
    private fun evalDocAssignment(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): String {
        val text =
            when (val cell = SqlPredicate.evalCell(expr, doc, env)) {
                is SqlCell.JsonVal -> cell.json
                is SqlCell.StringVal -> cell.value
                else -> throw SqlPlanningException("SET _doc requires a JSON document value", "")
            }
        val parsed =
            try {
                JsonValue.fromJsonString(text)
            } catch (e: Exception) {
                throw SqlPlanningException("SET _doc value is not valid JSON: ${e.message}", "")
            }
        if (parsed !is JsonValue.JObject) throw SqlPlanningException("SET _doc value must be a JSON object", "")
        return text
    }
}
