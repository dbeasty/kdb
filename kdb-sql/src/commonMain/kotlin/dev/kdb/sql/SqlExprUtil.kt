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
        is SqlExpr.InList -> expr.values.maxOfOrNull { maxParameterIndex(it) } ?: -1
        is SqlExpr.QualifiedColumn -> -1
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
                    q.groupBy +
                    q.joins.map { it.on } +
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
            is SqlStatement.Insert -> stmt.insert.rows.flatten()
            is SqlStatement.Delete -> listOfNotNull(stmt.delete.where)
            is SqlStatement.CreateIndex -> emptyList()
            is SqlStatement.DropIndex -> emptyList()
            is SqlStatement.BeginTransaction,
            is SqlStatement.Commit,
            is SqlStatement.Rollback,
            is SqlStatement.CreateTable,
            is SqlStatement.AlterTableAddColumn,
            is SqlStatement.DropTable,
            is SqlStatement.CreateRole,
            is SqlStatement.DropRole,
            is SqlStatement.Grant,
            is SqlStatement.Revoke,
            is SqlStatement.CreateUser,
            is SqlStatement.DropUser,
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
        stmt is SqlStatement.DropIndex ||
        stmt is SqlStatement.CreateTable ||
        stmt is SqlStatement.AlterTableAddColumn ||
        stmt is SqlStatement.DropTable

/** `CREATE ROLE`/`DROP ROLE`/`GRANT`/`REVOKE`/`CREATE USER`/`DROP USER` — handled by the wire
 * host against `UserStore`/`RoleStore` (see docs/kdb-rbac-plan.md phase 4), gated behind the
 * `admin` permission kind rather than the ordinary `write` check other DDL uses. */
public fun isAdminStatement(stmt: SqlStatement): Boolean =
    stmt is SqlStatement.CreateRole ||
        stmt is SqlStatement.DropRole ||
        stmt is SqlStatement.Grant ||
        stmt is SqlStatement.Revoke ||
        stmt is SqlStatement.CreateUser ||
        stmt is SqlStatement.DropUser
