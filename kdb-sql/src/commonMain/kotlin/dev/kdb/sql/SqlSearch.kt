package dev.kdb.sql

import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexRegistry
import dev.kdb.index.IndexType
import dev.kdb.index.fusion.FusionMode
import kotlin.math.max
import kotlin.math.min

/**
 * The planner's view of a namespace's indexes (Layer 16 §9.3). `MATCH`/`SIMILARITY` need a
 * FULLTEXT/VECTOR index to exist at planning time; HASH/BTREE selection still goes through the
 * schema's `indexed` flags.
 */
public interface SqlIndexCatalog {
    public fun descriptors(): List<IndexDescriptor>
}

/** Catalog over a live [IndexRegistry]. */
public fun registryIndexCatalog(registry: IndexRegistry): SqlIndexCatalog =
    object : SqlIndexCatalog {
        override fun descriptors(): List<IndexDescriptor> = registry.indexes.map { it.descriptor }
    }

/** Catalog over a fixed descriptor list (tests, EXPLAIN without a runtime). */
public fun staticIndexCatalog(descriptors: List<IndexDescriptor>): SqlIndexCatalog =
    object : SqlIndexCatalog {
        override fun descriptors(): List<IndexDescriptor> = descriptors
    }

/** Option key under which `CREATE INDEX` records the SQL index name on the descriptor. */
public const val INDEX_OPTION_NAME: String = "index_name"

/** Option key for FULLTEXT field weights, encoded `field=weight,field=weight`. */
public const val INDEX_OPTION_WEIGHTS: String = "weights"

/**
 * Resolves `MATCH(x, …)`: [nameOrField] is a FULLTEXT index's SQL name or its first field
 * (Layer 16 §9.1). Null when there is no such index.
 */
public fun SqlIndexCatalog.fullTextIndexFor(nameOrField: String): IndexDescriptor? {
    val fulltext = descriptors().filter { it.type == IndexType.FULLTEXT }
    return fulltext.firstOrNull { it.options[INDEX_OPTION_NAME] == nameOrField }
        ?: fulltext.firstOrNull { it.fieldName == nameOrField }
}

/** Resolves `SIMILARITY(field, …)` to the VECTOR index on [field] (by first field or SQL name). */
public fun SqlIndexCatalog.vectorIndexFor(field: String): IndexDescriptor? {
    val vector = descriptors().filter { it.type == IndexType.VECTOR }
    return vector.firstOrNull { it.fieldName == field }
        ?: vector.firstOrNull { it.options[INDEX_OPTION_NAME] == field }
}

/**
 * Candidate depth per arm for a score-ordered `LIMIT n [OFFSET m]` query (Layer 16 §9.1):
 * `min(1000, max(50, 4·(n+m)))`; without `LIMIT`, every hit.
 */
public fun scoreCandidateDepth(
    limit: Int?,
    offset: Int?,
): Int {
    if (limit == null) return Int.MAX_VALUE
    val want = limit.toLong() + (offset ?: 0).toLong()
    return min(1000L, max(50L, 4L * want)).toInt()
}

/** True for the expression kinds that produce a ranking: `MATCH`, `SIMILARITY`, `FUSE`. */
internal fun isScoreExpr(expr: SqlExpr): Boolean =
    expr is SqlExpr.Match || expr is SqlExpr.Similarity || expr is SqlExpr.Fuse

internal fun fusionModeOf(
    mode: String,
    sql: String,
): FusionMode =
    when (mode.lowercase()) {
        "rrf" -> FusionMode.RRF
        "weighted", "weighted_sum" -> FusionMode.WEIGHTED_SUM
        else -> throw SqlPlanningException("unknown FUSE mode: $mode (expected 'rrf' or 'weighted')", sql)
    }

/**
 * One leaf ranking arm resolved against the catalog: which index answers it, and the query text
 * or vector to send. Building one throws the spec's planning errors when the index is missing.
 */
internal sealed class ResolvedArm {
    abstract val descriptor: IndexDescriptor

    data class Text(
        override val descriptor: IndexDescriptor,
        val query: String,
    ) : ResolvedArm()

    class Vector(
        override val descriptor: IndexDescriptor,
        val vector: FloatArray,
    ) : ResolvedArm()
}

/**
 * Resolves the leaf arms of a score expression in evaluation order. A `FUSE` yields one arm per
 * nested `MATCH`/`SIMILARITY`; anything else is a planning error.
 */
internal fun resolveArms(
    expr: SqlExpr,
    catalog: SqlIndexCatalog?,
    parameters: List<SqlParameter>,
    sql: String = "",
): List<ResolvedArm> =
    when (expr) {
        is SqlExpr.Match -> listOf(resolveMatchArm(expr, catalog, parameters, sql))
        is SqlExpr.Similarity -> listOf(resolveSimilarityArm(expr, catalog, parameters, sql))
        is SqlExpr.Fuse -> {
            if (expr.arms.size < 2) throw SqlPlanningException("FUSE needs at least two arms", sql)
            fusionModeOf(expr.mode, sql)
            expr.arms.flatMap { arm ->
                if (arm is SqlExpr.Fuse) throw SqlPlanningException("FUSE arms must be MATCH or SIMILARITY", sql)
                resolveArms(arm, catalog, parameters, sql)
            }
        }
        else -> throw SqlPlanningException("not a score expression: $expr", sql)
    }

private fun resolveMatchArm(
    expr: SqlExpr.Match,
    catalog: SqlIndexCatalog?,
    parameters: List<SqlParameter>,
    sql: String,
): ResolvedArm.Text {
    val descriptor =
        catalog?.fullTextIndexFor(expr.column)
            ?: throw SqlPlanningException("no FULLTEXT index for ${expr.column}", sql)
    val query =
        when (val q = expr.query) {
            is SqlExpr.Literal -> (q.cell as? SqlCell.StringVal)?.value
            is SqlExpr.Parameter -> (parameters.getOrNull(q.index) as? SqlParameter.StringParam)?.value
            else -> null
        } ?: throw SqlPlanningException("MATCH query must be a string literal or string parameter", sql)
    return ResolvedArm.Text(descriptor, query)
}

private fun resolveSimilarityArm(
    expr: SqlExpr.Similarity,
    catalog: SqlIndexCatalog?,
    parameters: List<SqlParameter>,
    sql: String,
): ResolvedArm.Vector {
    val descriptor =
        catalog?.vectorIndexFor(expr.column)
            ?: throw SqlPlanningException("no VECTOR index for ${expr.column}", sql)
    val vector =
        when (val v = expr.vector) {
            is SqlExpr.VectorLiteral -> FloatArray(v.values.size) { v.values[it].toFloat() }
            is SqlExpr.Parameter ->
                when (val p = parameters.getOrNull(v.index)) {
                    is SqlParameter.VectorParam -> p.asFloatArray()
                    null -> throw SqlPlanningException("SIMILARITY parameter ${v.index} is not bound", sql)
                    else -> throw SqlPlanningException("SIMILARITY requires a vector parameter", sql)
                }
            else -> throw SqlPlanningException("SIMILARITY vector must be a parameter or a vector literal", sql)
        }
    if (vector.isEmpty()) throw SqlPlanningException("SIMILARITY vector must not be empty", sql)
    return ResolvedArm.Vector(descriptor, vector)
}

/** Every distinct score expression a SELECT evaluates: projections, WHERE, ORDER BY (via aliases too). */
internal fun collectScoreExprs(query: SelectQuery): List<SqlExpr> {
    val out = LinkedHashSet<SqlExpr>()
    fun visit(e: SqlExpr?) {
        when (e) {
            null -> Unit
            is SqlExpr.Match, is SqlExpr.Similarity, is SqlExpr.Fuse -> out += e
            is SqlExpr.Binary -> {
                visit(e.left)
                visit(e.right)
            }
            is SqlExpr.Unary -> visit(e.expr)
            is SqlExpr.FunctionCall -> e.args.forEach { visit(it) }
            is SqlExpr.Between -> {
                visit(e.low)
                visit(e.high)
            }
            is SqlExpr.InList -> e.values.forEach { visit(it) }
            is SqlExpr.Literal, is SqlExpr.ColumnRef, is SqlExpr.Parameter,
            is SqlExpr.QualifiedColumn, is SqlExpr.VectorLiteral,
            -> Unit
        }
    }
    query.projections.forEach { if (it is SelectProjection.Expression) visit(it.expr) }
    visit(query.where)
    query.groupBy.forEach { visit(it) }
    query.orderBy.forEach { visit(it.expr) }
    return out.toList()
}

/** Projection aliases (`expr AS name`) → their expressions, for ORDER BY / GROUP BY resolution. */
internal fun projectionAliases(query: SelectQuery): Map<String, SqlExpr> {
    val out = LinkedHashMap<String, SqlExpr>()
    for (p in query.projections) {
        when (p) {
            is SelectProjection.Expression -> p.alias?.let { out[it] = p.expr }
            is SelectProjection.Column -> p.alias?.let { out[it] = SqlExpr.ColumnRef(p.name) }
            is SelectProjection.Star -> Unit
        }
    }
    return out
}

/**
 * The score expression a query is ordered by, when its first ORDER BY item is a `MATCH`/
 * `SIMILARITY`/`FUSE` directly or through a projection alias (Layer 16 §9.1 depth rule).
 */
internal fun scoreOrderExpr(query: SelectQuery): SqlExpr? {
    val first = query.orderBy.firstOrNull()?.expr ?: return null
    val resolved =
        when (first) {
            is SqlExpr.ColumnRef -> projectionAliases(query)[first.name] ?: first
            else -> first
        }
    return resolved.takeIf { isScoreExpr(it) }
}
