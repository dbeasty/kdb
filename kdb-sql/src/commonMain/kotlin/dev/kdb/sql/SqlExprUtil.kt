package dev.kdb.sql

internal fun maxParameterIndex(expr: SqlExpr?): Int {
    if (expr == null) return -1
    return when (expr) {
        is SqlExpr.Parameter -> expr.index
        is SqlExpr.Literal,
        is SqlExpr.ColumnRef,
        is SqlExpr.Match,
        -> -1
        is SqlExpr.Between -> maxOf(maxParameterIndex(expr.low), maxParameterIndex(expr.high))
        is SqlExpr.Similarity -> -1
        is SqlExpr.Binary ->
            maxOf(maxParameterIndex(expr.left), maxParameterIndex(expr.right))
        is SqlExpr.Unary -> maxParameterIndex(expr.expr)
        is SqlExpr.FunctionCall -> expr.args.maxOfOrNull { maxParameterIndex(it) } ?: -1
    }
}

public fun statementParameterCount(stmt: SqlStatement): Int {
    val exprs =
        when (stmt) {
            is SqlStatement.Select -> {
                val q = stmt.query
                listOfNotNull(q.where) +
                    q.orderBy.map { it.expr } +
                    q.projections.mapNotNull {
                        when (it) {
                            is SelectProjection.Expression -> it.expr
                            else -> null
                        }
                    }
            }
            is SqlStatement.CreateVirtualView -> {
                val q = stmt.query
                listOfNotNull(q.where) + q.orderBy.map { it.expr }
            }
            is SqlStatement.DropVirtualView -> emptyList()
            is SqlStatement.Update -> {
                val u = stmt.update
                listOfNotNull(u.where) + u.assignments.map { it.expr }
            }
            is SqlStatement.Insert -> stmt.insert.values
            is SqlStatement.Delete -> listOfNotNull(stmt.delete.where)
            is SqlStatement.CreateIndex -> emptyList()
            is SqlStatement.DropIndex -> emptyList()
            is SqlStatement.BeginTransaction,
            is SqlStatement.Commit,
            is SqlStatement.Rollback,
            -> emptyList()
        }
    val maxIdx = exprs.maxOfOrNull { maxParameterIndex(it) } ?: -1
    return maxIdx + 1
}

/** Splits top-level AND into conjuncts (flattened one level). */
internal fun andConjuncts(expr: SqlExpr): List<SqlExpr> {
    if (expr is SqlExpr.Binary && expr.op == BinaryOp.AND) {
        return andConjuncts(expr.left) + andConjuncts(expr.right)
    }
    return listOf(expr)
}

internal fun recomposeAnd(parts: List<SqlExpr>): SqlExpr? {
    if (parts.isEmpty()) return null
    return parts.reduce { acc, e -> SqlExpr.Binary(BinaryOp.AND, acc, e) }
}

public fun isDmlStatement(stmt: SqlStatement): Boolean =
    stmt is SqlStatement.Update || stmt is SqlStatement.Insert || stmt is SqlStatement.Delete

public fun isTransactionControlStatement(stmt: SqlStatement): Boolean =
    stmt is SqlStatement.BeginTransaction ||
        stmt is SqlStatement.Commit ||
        stmt is SqlStatement.Rollback

public fun isDdlStatement(stmt: SqlStatement): Boolean =
    stmt is SqlStatement.CreateVirtualView ||
        stmt is SqlStatement.DropVirtualView ||
        stmt is SqlStatement.CreateIndex ||
        stmt is SqlStatement.DropIndex
