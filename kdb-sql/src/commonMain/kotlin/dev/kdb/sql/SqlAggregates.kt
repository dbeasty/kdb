package dev.kdb.sql

import dev.kdb.document.KdbDocument

/** `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` with the typing rules of Layer 16 §5. */
internal object SqlAggregates {
    fun isAggregateFunction(name: String): Boolean = name.lowercase() in AGGREGATE_NAMES

    fun queryHasAggregates(query: SelectQuery): Boolean =
        query.projections.any { proj ->
            proj is SelectProjection.Expression && containsAggregate(proj.expr)
        } || query.orderBy.any { containsAggregate(it.expr) }

    fun containsAggregate(expr: SqlExpr): Boolean =
        when (expr) {
            is SqlExpr.FunctionCall -> isAggregateFunction(expr.name) || expr.args.any { containsAggregate(it) }
            is SqlExpr.Binary -> containsAggregate(expr.left) || containsAggregate(expr.right)
            is SqlExpr.Unary -> containsAggregate(expr.expr)
            else -> false
        }

    /**
     * Evaluates one aggregate over [docs]. Zero rows → NULL except `COUNT` → 0; `SUM` over
     * integers only is a `LongVal`, any double input makes it a `DoubleVal`; `AVG` is always a
     * double; `MIN`/`MAX` ignore NULL. Non-numeric inputs are ignored by `SUM`/`AVG` (no
     * string-to-number coercion).
     */
    fun evalAggregate(
        call: SqlExpr.FunctionCall,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): SqlCell {
        val fn = call.name.lowercase()
        if (call.args.size > 1) throw SqlPlanningException("$fn takes one argument", "")
        val arg = call.args.singleOrNull()
        return when (fn) {
            "count" -> evalCount(arg, docs, env)
            "sum" -> evalSum(arg, docs, env)
            "avg" -> evalAvg(arg, docs, env)
            "min" -> evalMinMax(arg, docs, env, wantMax = false)
            "max" -> evalMinMax(arg, docs, env, wantMax = true)
            else -> throw SqlPlanningException("unknown aggregate: $fn", "")
        }
    }

    private fun cells(
        arg: SqlExpr,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): List<SqlCell> = docs.map { SqlPredicate.evalCell(arg, it, env) }

    private fun evalCount(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): SqlCell {
        if (arg == null || (arg is SqlExpr.ColumnRef && arg.name == "*")) {
            return SqlCell.LongVal(docs.size.toLong())
        }
        return SqlCell.LongVal(cells(arg, docs, env).count { it !is SqlCell.Null }.toLong())
    }

    private fun evalSum(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): SqlCell {
        arg ?: throw SqlPlanningException("sum needs an argument", "")
        var longSum = 0L
        var doubleSum = 0.0
        var any = false
        var sawDouble = false
        for (cell in cells(arg, docs, env)) {
            when (cell) {
                is SqlCell.LongVal -> {
                    longSum += cell.value
                    doubleSum += cell.value.toDouble()
                    any = true
                }
                is SqlCell.DoubleVal -> {
                    doubleSum += cell.value
                    any = true
                    sawDouble = true
                }
                else -> Unit
            }
        }
        return when {
            !any -> SqlCell.Null
            sawDouble -> SqlCell.DoubleVal(doubleSum)
            else -> SqlCell.LongVal(longSum)
        }
    }

    private fun evalAvg(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        env: EvalEnv,
    ): SqlCell {
        arg ?: throw SqlPlanningException("avg needs an argument", "")
        var sum = 0.0
        var count = 0
        for (cell in cells(arg, docs, env)) {
            when (cell) {
                is SqlCell.LongVal -> {
                    sum += cell.value.toDouble()
                    count++
                }
                is SqlCell.DoubleVal -> {
                    sum += cell.value
                    count++
                }
                else -> Unit
            }
        }
        return if (count > 0) SqlCell.DoubleVal(sum / count) else SqlCell.Null
    }

    private fun evalMinMax(
        arg: SqlExpr?,
        docs: List<KdbDocument>,
        env: EvalEnv,
        wantMax: Boolean,
    ): SqlCell {
        arg ?: throw SqlPlanningException("${if (wantMax) "max" else "min"} needs an argument", "")
        var best: SqlCell? = null
        for (cell in cells(arg, docs, env)) {
            if (cell is SqlCell.Null) continue
            if (best == null) {
                best = cell
                continue
            }
            val cmp = SqlPredicate.compareTotal(cell, best)
            if (if (wantMax) cmp > 0 else cmp < 0) best = cell
        }
        return best ?: SqlCell.Null
    }

    private val AGGREGATE_NAMES = setOf("count", "sum", "avg", "min", "max")
}
