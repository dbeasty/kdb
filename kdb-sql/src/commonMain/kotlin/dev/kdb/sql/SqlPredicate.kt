package dev.kdb.sql

import dev.kdb.document.KdbDocument
import dev.kdb.json.JsonValue
import dev.kdb.json.KdbJsonFunctionRegistry
import dev.kdb.json.kdbJsonGet
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField

internal object SqlPredicate {
    fun eval(
        expr: SqlExpr,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean =
        when (expr) {
            is SqlExpr.Binary -> evalBinary(expr, doc, schema, parameters)
            is SqlExpr.Unary -> evalUnary(expr, doc, schema, parameters)
            is SqlExpr.Match -> evalMatch(expr, doc, schema, parameters)
            is SqlExpr.Between -> evalBetween(expr, doc, schema, parameters)
            is SqlExpr.InList -> evalInList(expr, doc, schema, parameters)
            else -> false
        }

    fun evalJoin(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        schemas: Map<String, KdbSchema>,
        parameters: List<SqlParameter>,
    ): Boolean = evalWithJoin(expr, joinedDocs, schemas, parameters)

    private fun evalWithJoin(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        schemas: Map<String, KdbSchema>,
        parameters: List<SqlParameter>,
    ): Boolean =
        when (expr) {
            is SqlExpr.Binary ->
                when (expr.op) {
                    BinaryOp.AND ->
                        evalWithJoin(expr.left, joinedDocs, schemas, parameters) &&
                            evalWithJoin(expr.right, joinedDocs, schemas, parameters)
                    BinaryOp.OR ->
                        evalWithJoin(expr.left, joinedDocs, schemas, parameters) ||
                            evalWithJoin(expr.right, joinedDocs, schemas, parameters)
                    else -> {
                        val left = evalCellForContext(expr.left, joinedDocs, schemas, parameters)
                        val right = evalCellForContext(expr.right, joinedDocs, schemas, parameters)
                        compareOp(expr.op, left, right)
                    }
                }
            is SqlExpr.Unary ->
                when (expr.op) {
                    UnaryOp.NOT -> !evalWithJoin(expr.expr, joinedDocs, schemas, parameters)
                    UnaryOp.IS_NULL ->
                        evalCellForContext(expr.expr, joinedDocs, schemas, parameters) is SqlCell.Null
                }
            is SqlExpr.InList -> {
                val qualifier = expr.column.substringBefore('.', expr.column)
                val colName = expr.column.substringAfter('.', expr.column)
                val doc =
                    joinedDocs[qualifier]
                        ?: joinedDocs.values.first()
                val schema = schemas[qualifier] ?: schemas.values.first()
                evalInList(expr.copy(column = colName), doc, schema, parameters)
            }
            else -> false
        }

    private fun compareOp(
        op: BinaryOp,
        left: SqlCell?,
        right: SqlCell?,
    ): Boolean =
        when (op) {
            BinaryOp.EQ -> compareCells(left, right) == 0
            BinaryOp.NE -> compareCells(left, right) != 0
            BinaryOp.LT -> compareCells(left, right) < 0
            BinaryOp.LE -> compareCells(left, right) <= 0
            BinaryOp.GT -> compareCells(left, right) > 0
            BinaryOp.GE -> compareCells(left, right) >= 0
            BinaryOp.LIKE -> false
            BinaryOp.AND,
            BinaryOp.OR,
            -> false
        }

    fun evalCell(
        expr: SqlExpr,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): SqlCell? =
        when (expr) {
            is SqlExpr.Literal -> expr.cell
            is SqlExpr.ColumnRef -> cellForColumn(expr.name, doc, schema)
            is SqlExpr.Parameter -> parameterToCell(parameters.getOrNull(expr.index))
            is SqlExpr.FunctionCall -> evalFunction(expr, doc, schema, parameters)
            is SqlExpr.QualifiedColumn -> {
                val field = schema.fieldsByName[expr.name] ?: return null
                val raw = kdbJsonGet(doc.json, "$.${expr.name}")
                jsonToCell(raw, field)
            }
            else -> null
        }

    fun evalCellForContext(
        expr: SqlExpr,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument> = emptyMap(),
    ): SqlCell? {
        if (joinedDocs.isEmpty()) return evalCell(expr, doc, schema, parameters)
        return evalCellForContext(expr, joinedDocs, mapOf("" to schema), parameters)
    }

    fun evalCellForContext(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        schemas: Map<String, KdbSchema>,
        parameters: List<SqlParameter>,
    ): SqlCell? =
        when (expr) {
            is SqlExpr.Literal -> expr.cell
            is SqlExpr.ColumnRef -> {
                val doc = joinedDocs.values.first()
                val schema = schemas.values.first()
                cellForColumn(expr.name, doc, schema)
            }
            is SqlExpr.QualifiedColumn -> {
                val doc = joinedDocs[expr.qualifier] ?: return null
                val schema = schemas[expr.qualifier] ?: schemas.values.first()
                cellForColumn(expr.name, doc, schema)
            }
            is SqlExpr.Parameter -> parameterToCell(parameters.getOrNull(expr.index))
            is SqlExpr.FunctionCall -> {
                val doc = joinedDocs.values.first()
                val schema = schemas.values.first()
                evalFunction(expr, doc, schema, parameters)
            }
            else -> null
        }

    private fun evalBinary(
        expr: SqlExpr.Binary,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean =
        when (expr.op) {
            BinaryOp.AND ->
                eval(expr.left, doc, schema, parameters) &&
                    eval(expr.right, doc, schema, parameters)
            BinaryOp.OR ->
                eval(expr.left, doc, schema, parameters) ||
                    eval(expr.right, doc, schema, parameters)
            BinaryOp.EQ -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) == 0
            BinaryOp.NE -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) != 0
            BinaryOp.LT -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) < 0
            BinaryOp.LE -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) <= 0
            BinaryOp.GT -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) > 0
            BinaryOp.GE -> compareCells(evalCell(expr.left, doc, schema, parameters), evalCell(expr.right, doc, schema, parameters)) >= 0
            BinaryOp.LIKE -> evalLike(expr, doc, schema, parameters)
        }

    private fun evalUnary(
        expr: SqlExpr.Unary,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean =
        when (expr.op) {
            UnaryOp.NOT -> !eval(expr.expr, doc, schema, parameters)
            UnaryOp.IS_NULL -> evalCell(expr.expr, doc, schema, parameters) is SqlCell.Null
        }

    private fun evalMatch(
        expr: SqlExpr.Match,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean {
        val cell = cellForColumn(expr.column, doc, schema) ?: return false
        val text = (cell as? SqlCell.StringVal)?.value ?: return false
        return text.contains(expr.query, ignoreCase = true)
    }

    private fun evalInList(
        expr: SqlExpr.InList,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean {
        val cell = cellForColumn(expr.column, doc, schema) ?: return false
        for (valueExpr in expr.values) {
            val expected = evalCell(valueExpr, doc, schema, parameters)
            if (compareCells(cell, expected) == 0) return true
        }
        return false
    }

    private fun evalBetween(
        expr: SqlExpr.Between,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean {
        val cell = cellForColumn(expr.column, doc, schema) ?: return false
        val low = evalCell(expr.low, doc, schema, parameters)
        val high = evalCell(expr.high, doc, schema, parameters)
        return compareCells(cell, low) >= 0 && compareCells(cell, high) <= 0
    }

    private fun evalLike(
        expr: SqlExpr.Binary,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): Boolean {
        val left = evalCell(expr.left, doc, schema, parameters) as? SqlCell.StringVal ?: return false
        val right = evalCell(expr.right, doc, schema, parameters) as? SqlCell.StringVal ?: return false
        val pattern = right.value.replace("%", ".*").replace("_", ".")
        return Regex("^$pattern$", RegexOption.IGNORE_CASE).matches(left.value)
    }

    private fun evalFunction(
        call: SqlExpr.FunctionCall,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): SqlCell? {
        val desc = KdbJsonFunctionRegistry.get(call.name) ?: return null
        val jsonArgs =
            call.args.map { arg ->
                exprToJsonValue(arg, doc, schema, parameters)
            }
        if (jsonArgs.any { it == null }) return null
        val result = desc.evaluate(jsonArgs.filterNotNull())
        return result?.let { SqlCell.StringVal(it.toJsonString()) }
    }

    private fun exprToJsonValue(
        expr: SqlExpr,
        doc: KdbDocument,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
    ): JsonValue? =
        when (expr) {
            is SqlExpr.Literal -> cellToJson(expr.cell)
            is SqlExpr.ColumnRef ->
                if (expr.name == "_doc") {
                    JsonValue.JString(doc.json)
                } else {
                    kdbJsonGet(doc.json, "$.${expr.name}")
                }
            is SqlExpr.Parameter -> cellToJson(parameterToCell(parameters.getOrNull(expr.index)))
            else -> null
        }

    private fun cellToJson(cell: SqlCell?): JsonValue? =
        when (cell) {
            null, SqlCell.Null -> JsonValue.JNull
            is SqlCell.StringVal -> JsonValue.JString(cell.value)
            is SqlCell.LongVal -> JsonValue.JInt(cell.value)
            is SqlCell.DoubleVal -> JsonValue.JNumber(cell.value)
            is SqlCell.BoolVal -> JsonValue.JBool(cell.value)
            is SqlCell.JsonVal -> JsonValue.JString(cell.json)
        }

    private fun parameterToCell(param: SqlParameter?): SqlCell? =
        when (param) {
            null -> null
            is SqlParameter.StringParam -> SqlCell.StringVal(param.value)
            is SqlParameter.IntParam -> SqlCell.LongVal(param.value)
            is SqlParameter.DoubleParam -> SqlCell.DoubleVal(param.value)
            is SqlParameter.BoolParam -> SqlCell.BoolVal(param.value)
            SqlParameter.NullParam -> SqlCell.Null
        }

    fun cellForColumn(
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

    fun jsonToCell(
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

    fun compareCells(
        a: SqlCell?,
        b: SqlCell?,
    ): Int {
        if (a is SqlCell.Null && b is SqlCell.Null) return 0
        if (a is SqlCell.Null || b is SqlCell.Null) return if (a is SqlCell.Null) -1 else 1
        if (a is SqlCell.StringVal && b is SqlCell.StringVal) return a.value.compareTo(b.value)
        if (a is SqlCell.LongVal && b is SqlCell.LongVal) return a.value.compareTo(b.value)
        if (a is SqlCell.DoubleVal && b is SqlCell.DoubleVal) {
            return a.value.compareTo(b.value)
        }
        if (a is SqlCell.BoolVal && b is SqlCell.BoolVal) {
            return a.value.compareTo(b.value)
        }
        val da = a?.toComparableDouble()
        val db = b?.toComparableDouble()
        if (da != null && db != null) return da.compareTo(db)
        return -1
    }

    internal fun SqlCell.toComparableDouble(): Double? =
        when (this) {
            is SqlCell.LongVal -> value.toDouble()
            is SqlCell.DoubleVal -> value
            is SqlCell.StringVal -> value.toDoubleOrNull()
            else -> null
        }
}
