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
    public data object BeginTransaction : SqlStatement()
    public data object Commit : SqlStatement()
    public data object Rollback : SqlStatement()
}

public data class UpdateStatement(
    val table: TableRef,
    val assignments: List<Assignment>,
    val where: SqlExpr?,
)

public data class Assignment(
    val column: String,
    val expr: SqlExpr,
)

public data class InsertStatement(
    val table: TableRef,
    val columns: List<String>,
    val values: List<SqlExpr>,
)

public data class DeleteStatement(
    val table: TableRef,
    val where: SqlExpr?,
)

public data class CreateIndexStatement(
    val indexName: String,
    val table: String,
    val fields: List<String>,
    val type: IndexType,
    val unique: Boolean = false,
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
    public data class ColumnRef(val name: String) : SqlExpr()
    public data class Parameter(val index: Int) : SqlExpr()
    public data class Binary(val op: BinaryOp, val left: SqlExpr, val right: SqlExpr) : SqlExpr()
    public data class Unary(val op: UnaryOp, val expr: SqlExpr) : SqlExpr()
    public data class FunctionCall(val name: String, val args: List<SqlExpr>) : SqlExpr()
    public data class Match(val column: String, val query: String) : SqlExpr()
    public data class Between(val column: String, val low: SqlExpr, val high: SqlExpr) : SqlExpr()
    public data class Similarity(val column: String, val query: String, val limit: Int?) : SqlExpr()
    public data class InList(val column: String, val values: List<SqlExpr>) : SqlExpr()
    public data class QualifiedColumn(val qualifier: String, val name: String) : SqlExpr()
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
    LIKE,
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
}

public sealed class IndexLookupSpec {
    public data class Exact(val key: IndexKey) : IndexLookupSpec()
    public data class Range(val from: IndexKey?, val to: IndexKey?) : IndexLookupSpec()
    public data class FullText(val query: String) : IndexLookupSpec()
    public data class VectorAnn(val queryVector: FloatArray, val k: Int) : IndexLookupSpec()
}

public data class ExplainResult(
    val plan: PhysicalPlan,
    val estimatedRows: Long?,
)

public interface PreparedQuery {
    public val parameterCount: Int
    public suspend fun execute(bindings: List<SqlParameter>, context: QueryContext): QueryResult
}
