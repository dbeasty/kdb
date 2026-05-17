package dev.kdb.sql

import dev.kdb.document.KdbDocument
import dev.kdb.schema.KdbSchema

internal object SqlAggregates {
    fun isAggregateFunction(name: String): Boolean =
        name.lowercase() in AGGREGATE_NAMES

    fun queryHasAggregates(query: SelectQuery): Boolean =
        query.projections.any { proj ->
            proj is SelectProjection.Expression &&
                proj.expr is SqlExpr.FunctionCall &&
                isAggregateFunction((proj.expr as SqlExpr.FunctionCall).name)
        }

    fun evalAggregate(
        call: SqlExpr.FunctionCall,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument> = emptyMap(),
    ): SqlCell {
        val fn = call.name.lowercase()
        val arg = call.args.singleOrNull()
        return when (fn) {
            "count" -> evalCount(arg, docs, schema, parameters, joinedDocs)
            "sum" -> evalSum(arg, docs, schema, parameters, joinedDocs)
            "avg" -> evalAvg(arg, docs, schema, parameters, joinedDocs)
            "min" -> evalMin(arg, docs, schema, parameters, joinedDocs)
            "max" -> evalMax(arg, docs, schema, parameters, joinedDocs)
            else -> SqlCell.Null
        }
    }

    private fun evalCount(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument>,
    ): SqlCell {
        if (arg == null || arg is SqlExpr.ColumnRef && arg.name == "*") {
            return SqlCell.LongVal(docs.size.toLong())
        }
        val count =
            docs.count { doc ->
                val cell = SqlPredicate.evalCellForContext(arg, doc, schema, parameters, joinedDocs)
                cell != null && cell !is SqlCell.Null
            }
        return SqlCell.LongVal(count.toLong())
    }

    private fun evalSum(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument>,
    ): SqlCell {
        var sum = 0.0
        var any = false
        for (doc in docs) {
            val cell = arg?.let { SqlPredicate.evalCellForContext(it, doc, schema, parameters, joinedDocs) } ?: continue
            val n = cell.toComparableDouble() ?: continue
            sum += n
            any = true
        }
        return if (any) SqlCell.DoubleVal(sum) else SqlCell.Null
    }

    private fun evalAvg(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument>,
    ): SqlCell {
        var sum = 0.0
        var count = 0
        for (doc in docs) {
            val cell = arg?.let { SqlPredicate.evalCellForContext(it, doc, schema, parameters, joinedDocs) } ?: continue
            val n = cell.toComparableDouble() ?: continue
            sum += n
            count++
        }
        return if (count > 0) SqlCell.DoubleVal(sum / count) else SqlCell.Null
    }

    private fun evalMin(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument>,
    ): SqlCell {
        var best: SqlCell? = null
        for (doc in docs) {
            val cell = arg?.let { SqlPredicate.evalCellForContext(it, doc, schema, parameters, joinedDocs) } ?: continue
            if (cell is SqlCell.Null) continue
            if (best == null || SqlPredicate.compareCells(cell, best) < 0) best = cell
        }
        return best ?: SqlCell.Null
    }

    private fun evalMax(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        schema: KdbSchema,
        parameters: List<SqlParameter>,
        joinedDocs: Map<String, KdbDocument>,
    ): SqlCell {
        var best: SqlCell? = null
        for (doc in docs) {
            val cell = arg?.let { SqlPredicate.evalCellForContext(it, doc, schema, parameters, joinedDocs) } ?: continue
            if (cell is SqlCell.Null) continue
            if (best == null || SqlPredicate.compareCells(cell, best) > 0) best = cell
        }
        return best ?: SqlCell.Null
    }

    private fun SqlCell.toComparableDouble(): Double? =
        when (this) {
            is SqlCell.LongVal -> value.toDouble()
            is SqlCell.DoubleVal -> value
            is SqlCell.StringVal -> value.toDoubleOrNull()
            else -> null
        }

    private val AGGREGATE_NAMES = setOf("count", "sum", "avg", "min", "max")
}
