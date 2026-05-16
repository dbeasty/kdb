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
        return planSelect(select, context.schema)
    }

    private fun planSelect(
        query: SelectQuery,
        schema: KdbSchema,
    ): PhysicalPlan {
        val scan = planWhere(query.where, schema)
        var plan: PhysicalPlan = scan
        if (query.where != null && scan !is PhysicalPlan.IndexScan) {
            plan = PhysicalPlan.Filter(query.where, scan)
        } else if (query.where != null && scan is PhysicalPlan.IndexScan) {
            val residual = residualPredicate(query.where, scan)
            if (residual != null) {
                plan = PhysicalPlan.Filter(residual, scan)
            }
        }
        plan = PhysicalPlan.Project(query.projections, plan)
        if (query.orderBy.isNotEmpty()) {
            plan = PhysicalPlan.Sort(query.orderBy, plan)
        }
        val limit = query.limit ?: Int.MAX_VALUE
        val offset = query.offset ?: 0
        plan = PhysicalPlan.Limit(limit, offset, plan)
        return plan
    }

    private fun planWhere(
        expr: SqlExpr?,
        schema: KdbSchema,
    ): PhysicalPlan {
        if (expr == null) return PhysicalPlan.FullTableScan("no predicate")
        val indexPlan = findIndexScan(expr, schema)
        return indexPlan ?: PhysicalPlan.FullTableScan("no index for predicate")
    }

    private fun findIndexScan(
        expr: SqlExpr,
        schema: KdbSchema,
    ): PhysicalPlan? =
        when (expr) {
            is SqlExpr.Match -> {
                val field = schema.fieldsByName[expr.column] ?: return null
                PhysicalPlan.IndexScan(
                    expr.column,
                    IndexType.FULLTEXT,
                    IndexLookupSpec.FullText(expr.query),
                )
            }
            is SqlExpr.Binary -> {
                if (expr.op == BinaryOp.AND) {
                    findIndexScan(expr.left, schema) ?: findIndexScan(expr.right, schema)
                } else {
                    matchEquality(expr, schema) ?: matchRange(expr, schema)
                }
            }
            else -> null
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
        val (col, _) = columnAndLiteral(expr) ?: return null
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed || inferIndexType(field.type) != IndexType.BTREE) return null
        return PhysicalPlan.IndexScan(col, IndexType.BTREE, IndexLookupSpec.Range(null, null))
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

    private fun residualPredicate(
        where: SqlExpr,
        scan: PhysicalPlan.IndexScan,
    ): SqlExpr? {
        if (where is SqlExpr.Binary && where.op == BinaryOp.EQ) {
            val (col, _) = columnAndLiteral(where) ?: return where
            if (col == scan.fieldName) return null
        }
        return where
    }
}
