package dev.kdb.sql

import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.index.inferIndexType
import dev.kdb.json.JsonValue
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone

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
        if (select.joins.isNotEmpty() && select.joins.any { it.type != JoinType.INNER }) {
            throw SqlPlanningException("only INNER JOIN is supported in v1", "")
        }
        if (select.groupBy.isNotEmpty() && select.projections.any { it is SelectProjection.Star }) {
            throw SqlPlanningException("SELECT * is not allowed with GROUP BY", "")
        }
        val env = EvalEnv(context.schema, context.parameters, tableQualifiersOf(select.from), projectionAliases(select))
        if (select.joins.isEmpty()) {
            SqlColumnValidation.validateSelect(select, env)
        } else {
            SqlColumnValidation.validateProjectionsOnly(select, context.schema)
        }
        // Every MATCH/SIMILARITY/FUSE must be backed by an index (Layer 16 §9.1) - resolved here
        // so the error is a planning error whether or not the query is ever executed.
        for (scoreExpr in collectScoreExprs(select)) {
            resolveArms(scoreExpr, context.indexCatalog, context.parameters)
        }
        return planSelect(select, context, env)
    }

    private fun planSelect(
        query: SelectQuery,
        context: QueryContext,
        env: EvalEnv,
    ): PhysicalPlan {
        val scoreOrder = scoreOrderExpr(query)
        val depth = if (scoreOrder != null) scoreCandidateDepth(query.limit, query.offset) else Int.MAX_VALUE
        val (scan, residual) = planWhere(query.where, context, env, scoreOrder, depth)
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

    /**
     * Index selection over the top-level AND conjuncts (Layer 16 §9.3). A score-ordered query is
     * always answered from its ranking ([PhysicalPlan.ScoredScan]); otherwise the first conjunct
     * an index can answer becomes the scan and everything else is the residual filter.
     */
    private fun planWhere(
        expr: SqlExpr?,
        context: QueryContext,
        env: EvalEnv,
        scoreOrder: SqlExpr?,
        depth: Int,
    ): Pair<PhysicalPlan, SqlExpr?> {
        val conjuncts = expr?.let { andConjuncts(it) } ?: emptyList()
        if (scoreOrder != null) {
            val scan = scoredScan(scoreOrder, context, depth)
            val residual = recomposeAnd(conjuncts.filter { it != scoreOrder })
            return scan to residual
        }
        if (expr == null) return PhysicalPlan.FullTableScan("no predicate") to null
        var scan: PhysicalPlan? = null
        var used: SqlExpr? = null
        var usedSubsumed = false
        for (c in conjuncts) {
            val candidate = findIndexScan(c, context, env) ?: continue
            scan = candidate.plan
            used = c
            usedSubsumed = candidate.subsumesConjunct
            break
        }
        val base = scan ?: PhysicalPlan.FullTableScan("no index for predicate")
        val residualParts =
            conjuncts.filter { c ->
                !(c == used && usedSubsumed) && !isSubsumedByScan(c, used, base, env)
            }
        return base to recomposeAnd(residualParts)
    }

    private fun scoredScan(
        scoreExpr: SqlExpr,
        context: QueryContext,
        depth: Int,
    ): PhysicalPlan.ScoredScan {
        val arms =
            resolveArms(scoreExpr, context.indexCatalog, context.parameters).map { arm ->
                when (arm) {
                    is ResolvedArm.Text ->
                        PhysicalPlan.IndexScan(arm.descriptor.fieldName, IndexType.FULLTEXT, IndexLookupSpec.FullText(arm.query))
                    is ResolvedArm.Vector ->
                        PhysicalPlan.IndexScan(
                            arm.descriptor.fieldName,
                            IndexType.VECTOR,
                            IndexLookupSpec.VectorAnn(arm.vector, if (depth == Int.MAX_VALUE) context.maxRows else depth),
                        )
                }
            }
        return PhysicalPlan.ScoredScan(scoreExpr, arms, depth)
    }

    private fun isSubsumedByScan(
        conjunct: SqlExpr,
        used: SqlExpr?,
        scan: PhysicalPlan,
        env: EvalEnv,
    ): Boolean {
        if (used == null || scan !is PhysicalPlan.IndexScan) return false
        if (conjunct is SqlExpr.Binary && conjunct.op == BinaryOp.EQ) {
            val (col, _) = columnAndLiteral(conjunct, env) ?: return false
            if (col == scan.fieldName && scan.lookup is IndexLookupSpec.Exact) return true
        }
        return false
    }

    /** A scan for one conjunct, plus whether it answers the conjunct exactly (else it stays residual). */
    private class ScanCandidate(val plan: PhysicalPlan, val subsumesConjunct: Boolean)

    private fun findIndexScan(
        expr: SqlExpr,
        context: QueryContext,
        env: EvalEnv,
    ): ScanCandidate? =
        when (expr) {
            is SqlExpr.Match -> ScanCandidate(scoredScan(expr, context, Int.MAX_VALUE), subsumesConjunct = true)
            is SqlExpr.Between -> matchBetween(expr, context.schema, env)
            is SqlExpr.Binary -> matchEquality(expr, context.schema, env) ?: matchRange(expr, context.schema, env)
            is SqlExpr.InList -> matchInList(expr, context.schema, env)
            else -> null
        }

    private fun matchInList(
        expr: SqlExpr.InList,
        schema: KdbSchema,
        env: EvalEnv,
    ): ScanCandidate? {
        if (expr.negated) return null
        val col = env.resolvePath(expr.column)
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed) return null
        val keys =
            expr.values.mapNotNull { value ->
                val lit = value as? SqlExpr.Literal ?: return null
                literalToIndexKey(lit, field.type)
            }
        if (keys.size != expr.values.size || keys.isEmpty()) return null
        val type = inferIndexType(field.type)
        return ScanCandidate(PhysicalPlan.InListScan(col, type, keys), subsumesConjunct = true)
    }

    private fun matchBetween(
        expr: SqlExpr.Between,
        schema: KdbSchema,
        env: EvalEnv,
    ): ScanCandidate? {
        if (expr.negated) return null
        val col = env.resolvePath(expr.column)
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed || inferIndexType(field.type) != IndexType.BTREE) return null
        val lowLit = expr.low as? SqlExpr.Literal ?: return null
        val highLit = expr.high as? SqlExpr.Literal ?: return null
        val from = literalToIndexKey(lowLit, field.type) ?: return null
        val to = literalToIndexKey(highLit, field.type) ?: return null
        return ScanCandidate(
            PhysicalPlan.IndexScan(col, IndexType.BTREE, IndexLookupSpec.Range(from, to)),
            subsumesConjunct = true,
        )
    }

    private fun matchEquality(
        expr: SqlExpr.Binary,
        schema: KdbSchema,
        env: EvalEnv,
    ): ScanCandidate? {
        if (expr.op != BinaryOp.EQ) return null
        val (col, lit) = columnAndLiteral(expr, env) ?: return null
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed) return null
        val key = literalToIndexKey(lit, field.type) ?: return null
        val type = inferIndexType(field.type)
        return ScanCandidate(PhysicalPlan.IndexScan(col, type, IndexLookupSpec.Exact(key)), subsumesConjunct = true)
    }

    /**
     * `col < | <= | > | >= literal` over a BTREE field. Stores scan inclusively, so a strict bound
     * is recorded on the spec and the conjunct is kept in the residual filter (Layer 16 §9.3).
     */
    private fun matchRange(
        expr: SqlExpr.Binary,
        schema: KdbSchema,
        env: EvalEnv,
    ): ScanCandidate? {
        if (expr.op !in setOf(BinaryOp.GT, BinaryOp.GE, BinaryOp.LT, BinaryOp.LE)) return null
        val (col, lit) = columnAndLiteral(expr, env) ?: return null
        val field = schema.fieldsByName[col] ?: return null
        if (!field.indexed || inferIndexType(field.type) != IndexType.BTREE) return null
        val key = literalToIndexKey(lit, field.type) ?: return null
        // `literal < col` reads as `col > literal`.
        val columnOnLeft = expr.left is SqlExpr.ColumnRef || expr.left is SqlExpr.QualifiedColumn
        val op =
            if (columnOnLeft) {
                expr.op
            } else {
                when (expr.op) {
                    BinaryOp.GT -> BinaryOp.LT
                    BinaryOp.GE -> BinaryOp.LE
                    BinaryOp.LT -> BinaryOp.GT
                    else -> BinaryOp.GE
                }
            }
        val spec =
            when (op) {
                BinaryOp.GT -> IndexLookupSpec.Range(key, null, fromInclusive = false)
                BinaryOp.GE -> IndexLookupSpec.Range(key, null)
                BinaryOp.LT -> IndexLookupSpec.Range(null, key, toInclusive = false)
                else -> IndexLookupSpec.Range(null, key)
            }
        val strict = op == BinaryOp.GT || op == BinaryOp.LT
        return ScanCandidate(PhysicalPlan.IndexScan(col, IndexType.BTREE, spec), subsumesConjunct = !strict)
    }

    private fun columnAndLiteral(
        expr: SqlExpr.Binary,
        env: EvalEnv,
    ): Pair<String, SqlExpr.Literal>? {
        val leftCol = columnPath(expr.left, env)
        val rightLit = expr.right as? SqlExpr.Literal
        if (leftCol != null && rightLit != null) return leftCol to rightLit
        val rightCol = columnPath(expr.right, env)
        val leftLit = expr.left as? SqlExpr.Literal
        if (rightCol != null && leftLit != null) return rightCol to leftLit
        return null
    }

    private fun columnPath(
        expr: SqlExpr,
        env: EvalEnv,
    ): String? =
        when (expr) {
            is SqlExpr.ColumnRef -> env.resolvePath(expr.name)
            is SqlExpr.QualifiedColumn -> env.resolvePath("${expr.qualifier}.${expr.name}")
            else -> null
        }

    private fun literalToIndexKey(
        lit: SqlExpr.Literal,
        type: KdbFieldType,
    ): IndexKey? {
        val json =
            when (val cell = lit.cell) {
                is SqlCell.StringVal -> JsonValue.JString(cell.value)
                is SqlCell.LongVal -> JsonValue.JInt(cell.value)
                is SqlCell.DoubleVal -> JsonValue.JNumber(cell.value)
                is SqlCell.BoolVal -> JsonValue.JBool(cell.value)
                else -> return null
            }
        return try {
            dev.kdb.index.indexKeyFromJsonValue(json, type).takeIf { it !== IndexKey.NullKey }
        } catch (_: Exception) {
            null
        }
    }
}

internal fun tableQualifiersOf(table: TableRef): List<String> = listOfNotNull(table.name, table.alias)

/**
 * Column resolution rules (Layer 16 §2). Rule 1: with a declared schema the root segment of every
 * column reference - projection, WHERE, ORDER BY, GROUP BY, function arguments - must be a schema
 * field or a reserved name (`kdb_id`, `_doc`), else `unknown column: <name>`. Rule 2: schemaless
 * namespaces resolve everything dynamically and are not checked here. Predicate shapes that can
 * never be boolean are rejected in both cases.
 */
internal object SqlColumnValidation {
    fun validateSelect(
        query: SelectQuery,
        env: EvalEnv,
    ) {
        for (proj in query.projections) {
            when (proj) {
                is SelectProjection.Column -> checkColumn(proj.name, env, allowAlias = false)
                is SelectProjection.Expression -> checkExpr(proj.expr, env, allowAlias = false)
                is SelectProjection.Star -> Unit
            }
        }
        query.where?.let {
            checkPredicateShape(it)
            checkExpr(it, env, allowAlias = false)
        }
        for (g in query.groupBy) checkExpr(g, env, allowAlias = true)
        for (o in query.orderBy) checkExpr(o.expr, env, allowAlias = true)
    }

    /** Pre-Layer-16 behaviour for JOIN queries: only bare projection columns are checked. */
    fun validateProjectionsOnly(
        query: SelectQuery,
        schema: KdbSchema,
    ) {
        if (schema.isNone) return
        for (proj in query.projections) {
            if (proj is SelectProjection.Column) {
                val root = proj.name.substringBefore('.')
                if (!SqlPredicate.isReserved(root) && root !in schema.fieldsByName) {
                    throw SqlPlanningException("unknown column: ${proj.name}", "")
                }
            }
        }
    }

    fun checkColumn(
        name: String,
        env: EvalEnv,
        allowAlias: Boolean,
    ) {
        if (env.schema.isNone) return
        if (allowAlias && name in env.aliases) return
        val path = env.resolvePath(name)
        val root = path.substringBefore('.')
        if (SqlPredicate.isReserved(root) || root in env.schema.fieldsByName) return
        throw SqlPlanningException("unknown column: $name", "")
    }

    fun checkExpr(
        expr: SqlExpr,
        env: EvalEnv,
        allowAlias: Boolean,
    ) {
        when (expr) {
            is SqlExpr.ColumnRef -> if (expr.name != "*") checkColumn(expr.name, env, allowAlias)
            is SqlExpr.QualifiedColumn -> checkColumn("${expr.qualifier}.${expr.name}", env, allowAlias)
            is SqlExpr.Binary -> {
                checkExpr(expr.left, env, allowAlias)
                checkExpr(expr.right, env, allowAlias)
            }
            is SqlExpr.Unary -> checkExpr(expr.expr, env, allowAlias)
            is SqlExpr.FunctionCall -> expr.args.forEach { checkExpr(it, env, allowAlias) }
            is SqlExpr.Between -> {
                checkColumn(expr.column, env, allowAlias = false)
                checkExpr(expr.low, env, allowAlias)
                checkExpr(expr.high, env, allowAlias)
            }
            is SqlExpr.InList -> {
                checkColumn(expr.column, env, allowAlias = false)
                expr.values.forEach { checkExpr(it, env, allowAlias) }
            }
            is SqlExpr.Fuse -> expr.arms.forEach { checkExpr(it, env, allowAlias) }
            // MATCH names an index, SIMILARITY an indexed path: both are checked against the
            // index catalog, not the schema.
            is SqlExpr.Match, is SqlExpr.Similarity, is SqlExpr.Literal,
            is SqlExpr.Parameter, is SqlExpr.VectorLiteral,
            -> Unit
        }
    }

    /** Rejects WHERE shapes that can never be boolean before any row is touched. */
    fun checkPredicateShape(expr: SqlExpr) {
        when (expr) {
            is SqlExpr.Binary ->
                if (expr.op == BinaryOp.AND || expr.op == BinaryOp.OR) {
                    checkPredicateShape(expr.left)
                    checkPredicateShape(expr.right)
                }
            is SqlExpr.Unary -> if (expr.op == UnaryOp.NOT) checkPredicateShape(expr.expr)
            is SqlExpr.Literal ->
                if (expr.cell !is SqlCell.BoolVal) throw SqlPlanningException("unsupported predicate expression: literal", "")
            is SqlExpr.Similarity, is SqlExpr.Fuse, is SqlExpr.VectorLiteral ->
                throw SqlPlanningException("unsupported predicate expression: ${expr::class.simpleName}", "")
            is SqlExpr.Match, is SqlExpr.Between, is SqlExpr.InList, is SqlExpr.ColumnRef,
            is SqlExpr.QualifiedColumn, is SqlExpr.FunctionCall, is SqlExpr.Parameter,
            -> Unit
        }
    }
}
