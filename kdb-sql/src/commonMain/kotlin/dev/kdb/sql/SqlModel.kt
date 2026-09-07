package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType

public sealed class SqlStatement {
    public data class Select(val query: SelectQuery) : SqlStatement()
    public data class CreateVirtualView(val name: String, val query: SelectQuery) : SqlStatement()
    public data class DropVirtualView(val name: String) : SqlStatement()
    public data class Update(val update: UpdateStatement) : SqlStatement()
    public data class Insert(val insert: InsertStatement) : SqlStatement()
    public data class Delete(val delete: DeleteStatement) : SqlStatement()
    public data class CreateIndex(val ddl: CreateIndexStatement) : SqlStatement()
    public data class DropIndex(val ddl: DropIndexStatement) : SqlStatement()
    public data class CreateTable(val ddl: CreateTableStatement) : SqlStatement()
    public data class AlterTableAddColumn(val ddl: AlterTableAddColumnStatement) : SqlStatement()
    public data class DropTable(val table: TableRef) : SqlStatement()
    public data object BeginTransaction : SqlStatement()
    public data object Commit : SqlStatement()
    public data object Rollback : SqlStatement()

    // RBAC admin statements (docs/kdb-rbac-plan.md phase 4). These carry only primitive fields —
    // no dependency on kdb-auth's ResourcePath — since kdb-sql has no dependency on the auth
    // modules; SqlWireHost (which does) resolves them against UserStore/RoleStore directly,
    // intercepting before the statement would otherwise reach SqlEngine/HybridQueryEngine.
    public data class CreateRole(val name: String) : SqlStatement()

    public data class DropRole(val name: String) : SqlStatement()

    public data class Grant(val grant: GrantSpec) : SqlStatement()

    public data class Revoke(val grant: GrantSpec) : SqlStatement()

    public data class CreateUser(
        val id: String,
        val password: String,
        val roles: List<String>,
    ) : SqlStatement()

    public data class DropUser(val id: String) : SqlStatement()
}

/** `<kind> ON <scope> <database>[.<collection>[.<documentId>]] TO/FROM <role>` — [scope] is one
 * of "DATABASE"/"COLLECTION"/"DOCUMENT", validated at parse time against how many of
 * [collection]/[documentId] are present. */
public data class GrantSpec(
    val kind: String,
    val database: String,
    val collection: String? = null,
    val documentId: String? = null,
    val role: String,
)

public data class UpdateStatement(
    val table: TableRef,
    val assignments: List<Assignment>,
    val where: SqlExpr?,
)

public data class Assignment(
    val column: String,
    val expr: SqlExpr,
)

public data class ColumnDefinition(
    val name: String,
    val type: dev.kdb.schema.KdbFieldType,
    val required: Boolean,
    val indexed: Boolean = true,
    /** `col type UNIQUE` (Layer 16 §9.2) — the 1-tuple case of a unique constraint. */
    val unique: Boolean = false,
)

public data class CreateTableStatement(
    val table: TableRef,
    val columns: List<ColumnDefinition>,
    /** `UNIQUE (a, b {, c})` table constraints (Layer 16 §9.2/§9.6), each an ordered field tuple. */
    val uniqueConstraints: List<List<String>> = emptyList(),
)

public data class AlterTableAddColumnStatement(
    val table: TableRef,
    val column: ColumnDefinition,
)

public data class InsertStatement(
    val table: TableRef,
    val columns: List<String>,
    val rows: List<List<SqlExpr>>,
)

public data class DeleteStatement(
    val table: TableRef,
    val where: SqlExpr?,
)

public data class CreateIndexStatement(
    val indexName: String,
    val table: String,
    /** Indexed fields as JSON paths (dotted, e.g. `steps.text`); FULLTEXT may list several. */
    val fields: List<String>,
    val type: IndexType,
    val unique: Boolean = false,
    /** `field WEIGHT n` per FULLTEXT field (Layer 16 §6.3); absent means 1. */
    val weights: Map<String, Int> = emptyMap(),
    /** `WITH (k = v, ...)` options (Layer 16 §9.2), values unquoted. */
    val options: Map<String, String> = emptyMap(),
)

public data class DropIndexStatement(
    val indexName: String,
    val table: String,
)

public enum class JoinType {
    INNER,
}

public data class JoinClause(
    val type: JoinType,
    val table: TableRef,
    val on: SqlExpr,
)

public data class SelectQuery(
    val distinct: Boolean,
    val projections: List<SelectProjection>,
    val from: TableRef,
    val joins: List<JoinClause> = emptyList(),
    val where: SqlExpr?,
    val groupBy: List<SqlExpr> = emptyList(),
    val orderBy: List<OrderItem>,
    val limit: Int?,
    val offset: Int?,
)

public sealed class SelectProjection {
    public data class Column(val name: String, val alias: String?) : SelectProjection()
    public data class Expression(val expr: SqlExpr, val alias: String?) : SelectProjection()
    public data class Star(val includeDoc: Boolean = true) : SelectProjection()
}

public data class TableRef(
    val name: String,
    val alias: String? = null,
)

public sealed class SqlExpr {
    public data class Literal(val cell: SqlCell) : SqlExpr()

    /** A column reference; [name] may be a dotted JSON path (`a.b.c`, Layer 16 §2). */
    public data class ColumnRef(val name: String) : SqlExpr()
    public data class Parameter(val index: Int) : SqlExpr()
    public data class Binary(val op: BinaryOp, val left: SqlExpr, val right: SqlExpr) : SqlExpr()
    public data class Unary(val op: UnaryOp, val expr: SqlExpr) : SqlExpr()
    public data class FunctionCall(val name: String, val args: List<SqlExpr>) : SqlExpr()

    /**
     * `MATCH(index_or_field, query)` (Layer 16 §9.1). [column] is a FULLTEXT index name or the
     * first field of one; [query] is a string literal or a parameter. As a predicate it is true
     * for hits, as a projection it is the BM25 score (`0.0` for non-hits).
     */
    public data class Match(val column: String, val query: SqlExpr) : SqlExpr() {
        public constructor(column: String, query: String) : this(column, Literal(SqlCell.StringVal(query)))
    }

    /** `[NOT] BETWEEN low AND high` (inclusive); [column] may be a dotted path. */
    public data class Between(
        val column: String,
        val low: SqlExpr,
        val high: SqlExpr,
        val negated: Boolean = false,
    ) : SqlExpr()

    /**
     * `SIMILARITY(field, vector)` (Layer 16 §9.1). [vector] is a [Parameter] bound to a
     * [SqlParameter.VectorParam] or a [VectorLiteral]. Requires a VECTOR index on [column].
     */
    public data class Similarity(val column: String, val vector: SqlExpr) : SqlExpr()

    /** `[NOT] IN (v1, v2, ...)`; [column] may be a dotted path. */
    public data class InList(
        val column: String,
        val values: List<SqlExpr>,
        val negated: Boolean = false,
    ) : SqlExpr()

    /**
     * `qualifier.name` — [qualifier] is either the FROM table / its alias (stripped during
     * resolution, Layer 16 §2) or, for JOINs, the alias of a joined table. [name] itself may be
     * dotted (`a.b.c` parses as `QualifiedColumn("a", "b.c")`).
     */
    public data class QualifiedColumn(val qualifier: String, val name: String) : SqlExpr()

    /** `[0.1, 0.2, ...]` — a vector literal for [Similarity]. */
    public data class VectorLiteral(val values: List<Double>) : SqlExpr()

    /**
     * `FUSE(arm1, arm2[, 'rrf' | 'weighted'])` (Layer 16 §9.1) — [arms] are [Match]/[Similarity]
     * calls, [mode] is `"rrf"` (default) or `"weighted"`.
     */
    public data class Fuse(val arms: List<SqlExpr>, val mode: String = "rrf") : SqlExpr()
}

public enum class BinaryOp {
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE,
    AND,
    OR,
    /** Case-sensitive pattern match (Layer 16 §4). */
    LIKE,
    NOT_LIKE,
    /** Case-insensitive pattern match. */
    ILIKE,
    NOT_ILIKE,
}

public enum class UnaryOp {
    NOT,
    IS_NULL,
}

public data class OrderItem(
    val expr: SqlExpr,
    val ascending: Boolean,
)

public sealed class SqlCell {
    public data object Null : SqlCell()
    public data class StringVal(val value: String) : SqlCell()
    public data class LongVal(val value: Long) : SqlCell()
    public data class DoubleVal(val value: Double) : SqlCell()
    public data class BoolVal(val value: Boolean) : SqlCell()
    public data class JsonVal(val json: String) : SqlCell()
}

public sealed class SqlParameter {
    public data class StringParam(val value: String) : SqlParameter()
    public data class IntParam(val value: Long) : SqlParameter()
    public data class DoubleParam(val value: Double) : SqlParameter()
    public data class BoolParam(val value: Boolean) : SqlParameter()
    public data object NullParam : SqlParameter()

    /** A vector for `SIMILARITY(field, ?)` (Layer 16 §9.1); wire tag `"v"`, value a JSON number array. */
    public class VectorParam(values: FloatArray) : SqlParameter() {
        private val values: FloatArray = values.copyOf()

        public val size: Int get() = values.size

        public fun asFloatArray(): FloatArray = values.copyOf()

        override fun equals(other: Any?): Boolean =
            this === other || (other is VectorParam && values.contentEquals(other.values))

        override fun hashCode(): Int = values.contentHashCode()

        override fun toString(): String = "VectorParam(${values.joinToString(",")})"
    }
}

public data class NamespaceBinding(
    val namespaceId: String,
    val schema: dev.kdb.schema.KdbSchema,
)

public data class QueryContext(
    val namespaceId: String,
    val schema: dev.kdb.schema.KdbSchema,
    val atCommit: dev.kdb.codec.KdbHash? = null,
    val parameters: List<SqlParameter> = emptyList(),
    val maxRows: Int = 10_000,
    /** catalog/table → namespace id (for JOIN). */
    val namespacesByTable: Map<String, NamespaceBinding> = emptyMap(),
    /**
     * Index descriptors visible to the planner (Layer 16 §9.3). [sqlEngine] fills this from the
     * namespace's [dev.kdb.index.IndexRegistry] when the caller leaves it null; a planner used on
     * its own with no catalog treats every `MATCH`/`SIMILARITY` as having no index.
     */
    val indexCatalog: SqlIndexCatalog? = null,
)

public data class ResultColumn(
    val name: String,
    val sqlType: String,
    val source: ColumnSource,
)

public enum class ColumnSource {
    SCHEMA_FIELD,
    KDB_ID,
    DOC_JSON,
    EXPRESSION,
}

public data class QueryRow(val values: List<SqlCell>)

public data class QueryResult(
    val columns: List<ResultColumn>,
    val rows: List<QueryRow>,
    val rowsAffected: Int = 0,
    /** Document ids produced by INSERT (for JDBC generated keys). */
    val generatedIds: List<String> = emptyList(),
    /** Schema after DDL (CREATE/ALTER/DROP TABLE). */
    val appliedSchema: dev.kdb.schema.KdbSchema? = null,
)

public sealed class PhysicalPlan {
    public data class IndexScan(
        val fieldName: String,
        val indexType: IndexType,
        val lookup: IndexLookupSpec,
    ) : PhysicalPlan()

    public data class FullTableScan(val reason: String) : PhysicalPlan()

    public data class Filter(val predicate: SqlExpr, val input: PhysicalPlan) : PhysicalPlan()

    public data class Project(val projections: List<SelectProjection>, val input: PhysicalPlan) : PhysicalPlan()

    public data class Sort(val orderBy: List<OrderItem>, val input: PhysicalPlan) : PhysicalPlan()

    public data class Limit(val limit: Int, val offset: Int, val input: PhysicalPlan) : PhysicalPlan()

    public data class InListScan(
        val fieldName: String,
        val indexType: IndexType,
        val keys: List<IndexKey>,
    ) : PhysicalPlan()

    /**
     * A ranked access path (Layer 16 §9.1/§9.3): the candidate set is the ranking produced by
     * [scoreExpr] — a [SqlExpr.Match], [SqlExpr.Similarity] or [SqlExpr.Fuse] — fetched to
     * [depth] results per arm ([Int.MAX_VALUE] = every hit). [arms] name the index scans behind
     * each arm, for EXPLAIN.
     */
    public data class ScoredScan(
        val scoreExpr: SqlExpr,
        val arms: List<IndexScan>,
        val depth: Int,
    ) : PhysicalPlan()
}

public sealed class IndexLookupSpec {
    public data class Exact(val key: IndexKey) : IndexLookupSpec()

    /**
     * Bounded range over a BTREE index. Strict bounds ([fromInclusive]/[toInclusive] false) are
     * re-checked by a residual filter because stores scan inclusively (Layer 16 §9.3).
     */
    public data class Range(
        val from: IndexKey?,
        val to: IndexKey?,
        val fromInclusive: Boolean = true,
        val toInclusive: Boolean = true,
    ) : IndexLookupSpec()
    public data class FullText(val query: String) : IndexLookupSpec()

    public class VectorAnn(queryVector: FloatArray, public val k: Int) : IndexLookupSpec() {
        public val queryVector: FloatArray = queryVector.copyOf()

        override fun equals(other: Any?): Boolean =
            this === other ||
                (other is VectorAnn && k == other.k && queryVector.contentEquals(other.queryVector))

        override fun hashCode(): Int = 31 * queryVector.contentHashCode() + k

        override fun toString(): String = "VectorAnn(dims=${queryVector.size}, k=$k)"
    }
}

public data class ExplainResult(
    val plan: PhysicalPlan,
    val estimatedRows: Long?,
    /** Human-readable name of the chosen access path (Layer 16 §9.3), e.g. `IndexScan(userId HASH Exact)`. */
    val accessPath: String = describeAccessPath(plan),
)

/** Names the innermost access path of [plan] so tests can assert an index was used (Layer 16 §9.3). */
public fun describeAccessPath(plan: PhysicalPlan): String =
    when (plan) {
        is PhysicalPlan.Limit -> describeAccessPath(plan.input)
        is PhysicalPlan.Sort -> describeAccessPath(plan.input)
        is PhysicalPlan.Project -> describeAccessPath(plan.input)
        is PhysicalPlan.Filter -> describeAccessPath(plan.input)
        is PhysicalPlan.FullTableScan -> "FullTableScan(${plan.reason})"
        is PhysicalPlan.InListScan -> "InListScan(${plan.fieldName} ${plan.indexType} ${plan.keys.size} keys)"
        is PhysicalPlan.IndexScan -> describeIndexScan(plan)
        is PhysicalPlan.ScoredScan ->
            "ScoredScan(depth=${if (plan.depth == Int.MAX_VALUE) "all" else plan.depth.toString()}; " +
                plan.arms.joinToString(", ") { describeIndexScan(it) } + ")"
    }

private fun describeIndexScan(scan: PhysicalPlan.IndexScan): String {
    val lookup =
        when (val l = scan.lookup) {
            is IndexLookupSpec.Exact -> "Exact"
            is IndexLookupSpec.Range ->
                "Range(" + (if (l.fromInclusive) ">=" else ">") + (l.from?.let { " lo" } ?: " -inf") +
                    ", " + (if (l.toInclusive) "<=" else "<") + (l.to?.let { " hi" } ?: " +inf") + ")"
            is IndexLookupSpec.FullText -> "FullText"
            is IndexLookupSpec.VectorAnn -> "VectorAnn(k=${l.k})"
        }
    return "IndexScan(${scan.fieldName} ${scan.indexType} $lookup)"
}

public interface PreparedQuery {
    public val parameterCount: Int
    public suspend fun execute(bindings: List<SqlParameter>, context: QueryContext): QueryResult
}
