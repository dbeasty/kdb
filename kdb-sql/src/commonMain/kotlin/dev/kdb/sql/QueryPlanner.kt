package dev.kdb.sql

import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.index.inferIndexType
import dev.kdb.json.JsonValue
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema

public interface QueryPlanner {
    public fun plan(
        statement: SqlStatement,
        context: QueryContext,
    ): PhysicalPlan
}

public class DefaultQueryPlanner : QueryPlanner {
    override fun plan(
        statement: SqlStatement,
        context: QueryContext,
    ): PhysicalPlan {
        val select =
            when (statement) {
                is SqlStatement.Select -> statement.query
                else -> throw SqlPlanningException("only SELECT is planned here", "")
            }
        validateProjections(select, context.schema)
        return planSelect(select, context.schema)
    }

    private fun validateProjections(
        query: SelectQuery,
        schema: KdbSchema,
    ) {
        for (proj in query.projections) {
            if (proj is SelectProjection.Column) {
                val name = proj.name
                if (name != "kdb_id" && name != "_doc" && name !in schema.fieldsByName) {
                    throw SqlPlanningException("unknown column: $name", "")
                }
            }
        }
    }

    private fun planSelect(
        query: SelectQuery,
        schema: KdbSchema,
    ): PhysicalPlan {
        val (scan, residual) = planWhere(query.where, schema)
        var plan: PhysicalPlan = scan
        if (residual != null) {
            plan = PhysicalPlan.Filter(residual, scan)
        }
        plan = PhysicalPlan.Project(query.projections, plan)
        if (query.orderBy.isNotEmpty()) {
            plan = PhysicalPlan.Sort(query.orderBy, plan)
        }
        val limit = query.limit ?: Int.MAX_VALUE
        plan = PhysicalPlan.Limit(limit, query.offset ?: 0, plan)
        return plan
    }

    private fun planWhere(
        expr: SqlExpr?,
        schema: KdbSchema,
    ): Pair<PhysicalPlan, SqlExpr?> {
        if (expr == null) return PhysicalPlan.FullTableScan("no predicate") to null
        val conjuncts = andConjuncts(expr)
        var scan: PhysicalPlan? = null
        var used: SqlExpr? = null
        for (c in conjuncts) {
            val candidate = findIndexScan(c, schema)
            if (candidate != null) {
                scan = candidate
                used = c
                break
            }
        }
        val base = scan ?: PhysicalPlan.FullTableScan("no index for predicate")
        val residualParts =
            conjuncts.filter { c ->
                c != used && !isSubsumedByScan(c, used, base)
            }
        val residual = recomposeAnd(residualParts)
        return base to residual
    }

    private fun isSubsumedByScan(
        conjunct: SqlExpr,
        used: SqlExpr?,
        scan: PhysicalPlan,
    ): Boolean {
        if (used == null || scan !is PhysicalPlan.IndexScan) return false
        if (conjunct == used) return true
        if (conjunct is SqlExpr.Binary && conjunct.op == BinaryOp.EQ) {
            val (col, _) = columnAndLiteral(conjunct) ?: return false
            if (col == scan.fieldName && scan.lookup is IndexLookupSpec.Exact) return true
        }
        return false
    }

    private fun findIndexScan(
        expr: SqlExpr,
        schema: KdbSchema,
    ): PhysicalPlan? =
        when (expr) {
            is SqlExpr.Match -> {
                if (expr.column !in schema.fieldsByName) {
                    null
                } else {
                    PhysicalPlan.IndexScan(
                        expr.column,
                        IndexType.FULLTEXT,
                        IndexLookupSpec.FullText(expr.query),
                    )
                }
            }
            is SqlExpr.Between -> matchBetween(expr, schema)
            is SqlExpr.Binary -> matchEquality(expr, schema) ?: matchRange(expr, schema)
            else -> null
        }

    private fun matchBetween(
        expr: SqlExpr.Between,
        schema: KdbSchema,
    ): PhysicalPlan? {
        val field = schema.fieldsByName[expr.column] ?: return null
        if (!field.indexed || inferIndexType(field.type) != IndexType.BTREE) return null
        val lowLit = expr.low as? SqlExpr.Literal ?: return null
        val highLit = expr.high as? SqlExpr.Literal ?: return null
        val from = literalToIndexKey(lowLit, field.type) ?: return null
        val to = literalToIndexKey(highLit, field.type) ?: return null
        return PhysicalPlan.IndexScan(expr.column, IndexType.BTREE, IndexLookupSpec.Range(from, to))
    }

    private fun matchEquality(
        expr: SqlExpr.Binary,
        schema: KdbSchema,
    ): PhysicalPlan? {
        if (expr.op != BinaryOp.EQ) return null
        val (col, lit) = columnAndLiteral(expr) ?: return null
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed) return null
        val key = literalToIndexKey(lit, field.type) ?: return null
        val type = inferIndexType(field.type)
        return PhysicalPlan.IndexScan(col, type, IndexLookupSpec.Exact(key))
    }

    private fun matchRange(
        expr: SqlExpr.Binary,
        schema: KdbSchema,
    ): PhysicalPlan? {
        if (expr.op !in setOf(BinaryOp.GT, BinaryOp.GE, BinaryOp.LT, BinaryOp.LE)) return null
        val (col, lit) = columnAndLiteral(expr) ?: return null
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed || inferIndexType(field.type) != IndexType.BTREE) return null
        val key = literalToIndexKey(lit, field.type) ?: return null
        val (from, to) =
            when (expr.op) {
                BinaryOp.GT, BinaryOp.GE -> key to null
                BinaryOp.LT, BinaryOp.LE -> null to key
                else -> null to null
            }
        return PhysicalPlan.IndexScan(col, IndexType.BTREE, IndexLookupSpec.Range(from, to))
    }

    private fun columnAndLiteral(expr: SqlExpr.Binary): Pair<String, SqlExpr.Literal>? {
        val leftCol = expr.left as? SqlExpr.ColumnRef
        val rightLit = expr.right as? SqlExpr.Literal
        if (leftCol != null && rightLit != null) return leftCol.name to rightLit
        val rightCol = expr.right as? SqlExpr.ColumnRef
        val leftLit = expr.left as? SqlExpr.Literal
        if (rightCol != null && leftLit != null) return rightCol.name to leftLit
        return null
    }

    private fun literalToIndexKey(
        lit: SqlExpr.Literal,
        type: KdbFieldType,
    ): IndexKey? =
        when (val cell = lit.cell) {
            is SqlCell.StringVal ->
                dev.kdb.index.indexKeyFromJsonValue(JsonValue.JString(cell.value), type)

            is SqlCell.LongVal ->
                dev.kdb.index.indexKeyFromJsonValue(JsonValue.JInt(cell.value), type)

            is SqlCell.DoubleVal ->
                dev.kdb.index.indexKeyFromJsonValue(JsonValue.JNumber(cell.value), type)

            is SqlCell.BoolVal ->
                dev.kdb.index.indexKeyFromJsonValue(JsonValue.JBool(cell.value), type)

            else -> null
        }
}
