# KDB Component Spec — Layer 5
## Component 15: SQL DSL + Query Planner
### `dev.kdb.sql`

**File:** `kdb-spec-layer5-component15-sql-dsl-query-planner.md`  
**Layer:** 5 — Index Implementations  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-sql`  
**Depends on:** Layers 0–3, Components 12–14 (index stores), Layer 1 (`kdb-json`), Layer 2 (schema), Layer 3 (`IndexReader`, `StorageAdapter`, `CommitDag`)

-----

## 1. Purpose

Provides the **shared SQL engine** used by the browser runtime, JVM embedded mode, and (via thin adapter) JDBC: parse a KDB SQL subset, plan index-aware access paths, execute against `StorageAdapter` + `IndexReader`, and assemble result rows including `kdb_id`, schema columns, and `_doc`.

This module owns parsing, AST, logical/physical planning, and row-oriented execution. It does **not** own JDBC wire types (Layer 8) or the full hybrid/versioning integration (`AT VERSION`, cross-namespace policy) — those are Layer 6 — but it exposes hooks (`QueryContext.atCommit`, `QueryContext.namespaceId`) that Layer 6 fills in.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid`, `KdbTimestamp` |
| `dev.kdb.error` | `KdbException`, `SqlParseException`, `SqlPlanningException` |
| `dev.kdb.document` | `KdbDocument` |
| `dev.kdb.json` | `KdbJsonFunctionRegistry`, `kdbJsonGet`, `JsonValue` |
| `dev.kdb.schema` | `KdbSchema`, `SchemaField`, `KdbFieldType` |
| `dev.kdb.dag` | `CommitDag` |
| `dev.kdb.index` | `IndexReader`, `IndexRegistry`, `IndexKey`, `IndexType`, `inferIndexType` |
| `dev.kdb.storage` | `StorageAdapter` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.sql

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.index.IndexManager
import dev.kdb.json.JsonValue
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter

// ── Entry points ──────────────────────────────────────────────────────────────

interface SqlEngine {
    suspend fun execute(sql: String, context: QueryContext): QueryResult
    suspend fun explain(sql: String, context: QueryContext): ExplainResult
    fun prepare(sql: String, context: QueryContext): PreparedQuery
}

fun sqlEngine(
    indexManager: IndexManager,
    storage: StorageAdapter,
    dag: CommitDag,
): SqlEngine

data class QueryContext(
    val namespaceId: String,
    val schema: KdbSchema,
    val atCommit: KdbHash? = null,       // null = HEAD; Layer 6 sets for AT VERSION
    val parameters: List<SqlParameter> = emptyList(),
    val maxRows: Int = 10_000,
)

sealed class SqlParameter {
    data class StringParam(val value: String) : SqlParameter()
    data class IntParam(val value: Long) : SqlParameter()
    data class DoubleParam(val value: Double) : SqlParameter()
    data class BoolParam(val value: Boolean) : SqlParameter()
    data class NullParam : SqlParameter()
}

// ── Results ───────────────────────────────────────────────────────────────────

data class QueryResult(
    val columns: List<ResultColumn>,
    val rows: List<QueryRow>,
    val rowsAffected: Int = 0,          // DML only
)

data class ResultColumn(
    val name: String,
    val sqlType: String,                // JDBC-style name from schema or "JSON"
    val source: ColumnSource,
)

enum class ColumnSource { SCHEMA_FIELD, KDB_ID, DOC_JSON, EXPRESSION }

data class QueryRow(val values: List<SqlCell>)

sealed class SqlCell {
    data class Null : SqlCell()
    data class StringVal(val value: String) : SqlCell()
    data class LongVal(val value: Long) : SqlCell()
    data class DoubleVal(val value: Double) : SqlCell()
    data class BoolVal(val value: Boolean) : SqlCell()
    data class JsonVal(val json: String) : SqlCell()   // _doc and JSON functions
}

data class ExplainResult(
    val plan: PhysicalPlan,
    val estimatedRows: Long?,
)

// ── Parser ────────────────────────────────────────────────────────────────────

interface SqlParser {
    fun parse(sql: String): SqlStatement
}

fun defaultSqlParser(): SqlParser

sealed class SqlStatement {
    data class Select(val query: SelectQuery) : SqlStatement()
    data class Update(val update: UpdateStatement) : SqlStatement()
    data class Insert(val insert: InsertStatement) : SqlStatement()
    data class Delete(val delete: DeleteStatement) : SqlStatement()
    data class CreateIndex(val ddl: CreateIndexStatement) : SqlStatement()
    data class DropIndex(val ddl: DropIndexStatement) : SqlStatement()
}

data class SelectQuery(
    val distinct: Boolean,
    val projections: List<SelectProjection>,
    val from: TableRef,
    val where: SqlExpr?,
    val orderBy: List<OrderItem>,
    val limit: Int?,
    val offset: Int?,
)

sealed class SelectProjection {
    data class Column(val name: String, val alias: String?) : SelectProjection()
    data class Expression(val expr: SqlExpr, val alias: String?) : SelectProjection()
    data class Star(val includeDoc: Boolean = true) : SelectProjection()
}

data class TableRef(
    val name: String,                    // namespace table or virtual view name
    val alias: String? = null,
)

// ── Expressions (subset) ──────────────────────────────────────────────────────

sealed class SqlExpr {
    data class Literal(val cell: SqlCell) : SqlExpr()
    data class ColumnRef(val name: String) : SqlExpr()
    data class Parameter(val index: Int) : SqlExpr()
    data class Binary(val op: BinaryOp, val left: SqlExpr, val right: SqlExpr) : SqlExpr()
    data class Unary(val op: UnaryOp, val expr: SqlExpr) : SqlExpr()
    data class FunctionCall(val name: String, val args: List<SqlExpr>) : SqlExpr()
    data class Match(val column: String, val query: String) : SqlExpr()           // MATCH(col, 'q')
    data class Similarity(val column: String, val text: String, val limit: Int?) : SqlExpr()
}

enum class BinaryOp { EQ, NE, LT, LE, GT, GE, AND, OR, LIKE }
enum class UnaryOp { NOT, IS_NULL }

data class OrderItem(val expr: SqlExpr, val ascending: Boolean)

// ── Planner ───────────────────────────────────────────────────────────────────

interface QueryPlanner {
    fun plan(statement: SqlStatement, context: QueryContext): PhysicalPlan
}

sealed class PhysicalPlan {
    data class IndexScan(
        val fieldName: String,
        val indexType: dev.kdb.index.IndexType,
        val lookup: IndexLookupSpec,
    ) : PhysicalPlan()

    data class FullTableScan(val reason: String) : PhysicalPlan()

    data class Filter(val predicate: SqlExpr, val input: PhysicalPlan) : PhysicalPlan()
    data class Project(val projections: List<SelectProjection>, val input: PhysicalPlan) : PhysicalPlan()
    data class Sort(val orderBy: List<OrderItem>, val input: PhysicalPlan) : PhysicalPlan()
    data class Limit(val limit: Int, val offset: Int, val input: PhysicalPlan) : PhysicalPlan()
    data class FetchDocuments(val docIds: List<KdbUuid>) : PhysicalPlan()
}

sealed class IndexLookupSpec {
    data class Exact(val key: dev.kdb.index.IndexKey) : IndexLookupSpec()
    data class Range(val from: dev.kdb.index.IndexKey?, val to: dev.kdb.index.IndexKey?) : IndexLookupSpec()
    data class FullText(val query: String) : IndexLookupSpec()
    data class VectorAnn(val queryVector: FloatArray, val k: Int) : IndexLookupSpec()
}

interface PreparedQuery {
    val parameterCount: Int
    suspend fun execute(bindings: List<SqlParameter>, context: QueryContext): QueryResult
}

// ── DML (write path delegates to Transaction Engine) ──────────────────────────

data class UpdateStatement(
    val table: TableRef,
    val assignments: List<Assignment>,
    val where: SqlExpr?,
)

data class Assignment(val column: String, val expr: SqlExpr)

data class InsertStatement(val table: TableRef, val columns: List<String>, val values: List<SqlExpr>)
data class DeleteStatement(val table: TableRef, val where: SqlExpr?)

data class CreateIndexStatement(
    val indexName: String,
    val table: String,
    val fields: List<String>,
    val type: dev.kdb.index.IndexType,
    val unique: Boolean = false,
)

data class DropIndexStatement(val indexName: String, val table: String)
```

Built-in functions resolved via `KdbJsonFunctionRegistry` for `kdb_json_*` names; `MATCH` and `similarity` lower to index plans (Components 13–14).

-----

## 4. Data Structures

### `PhysicalPlan` tree
Planner produces a tree rooted at `Limit` → `Project` → optional `Sort` → `Filter` → scan node. Scan is always `IndexScan` when predicate matches an index rule (see §9); otherwise `FullTableScan` over `StorageAdapter.scanDocuments`.

### `PreparedQuery`
Holds parsed `SqlStatement` + bound logical plan template; parameters replace `SqlExpr.Parameter` nodes at execute time.

### Column resolution
Table metadata from `KdbSchema` + virtual view registry (Component 16). Implicit columns: `kdb_id`, `_doc`, plus each `SchemaField.name`.

-----

## 5. Contracts

### `SqlEngine.execute`
- **Preconditions:** `context.namespaceId` matches `dag.namespaceId`. Schema consistent with registry indexes.
- **Postconditions:** `SELECT` returns rows ≤ `maxRows`. Each row's `_doc` column (if projected) is valid JSON from `KdbDocument.json`. Schema columns match typed extraction from document.
- **SELECT only in v1 execute path for read-heavy deliverable;** DML routes to `TransactionEngine` builder (Component 7) — planner produces `WritePlan` side effect list.

### Index selection (normative rules)
| Predicate shape | Index used |
|---|---|
| `col = ?` literal | HASH or BTREE exact via `inferIndexType` |
| `col BETWEEN ? AND ?` | BTREE range |
| `col > ?`, `<`, `ORDER BY col` | BTREE range / sort-assisted |
| `MATCH(col, 'q')` | FULLTEXT on `col` |
| `ORDER BY similarity(col, 'text')` | VECTOR ANN on `col` |
| `kdb_json_get(_doc, '$.x') = ?` | FullTableScan (no index) |

### `explain`
Returns planned `PhysicalPlan` without executing; `estimatedRows` optional (index cardinality hint or -1).

### `prepare`
Parsing errors throw `SqlParseException`. Planning errors throw `SqlPlanningException`.

### JSON functions
Evaluated per row during `Project` via `KdbJsonFunctionRegistry` — same semantics as Layer 1.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `SqlParseException` | Token/syntax error; includes `offset`. |
| `SqlPlanningException` | Unknown table/column, ambiguous reference, unsupported construct. |
| `IndexNotFoundException` | `MATCH`/`similarity` on field without index (from Component 8). |
| `QueryTimeoutException` | Row cap exceeded (optional). |

```kotlin
class SqlParseException(message: String, val sql: String, val offset: Int) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

class SqlPlanningException(message: String, val sql: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `parse_selectStar` | `SELECT * FROM users` | `SelectQuery` with `Star`. |
| 2 | `plan_hashEquality` | `WHERE userId = 'x'` + HASH index. | `IndexScan` HASH exact. |
| 3 | `plan_btreeRange` | `WHERE score BETWEEN 1 AND 10`. | `IndexScan` BTREE range. |
| 4 | `plan_matchFullText` | `WHERE MATCH(email, 'alice')`. | `IndexScan` FULLTEXT. |
| 5 | `plan_jsonPath_scan` | `WHERE kdb_json_get(_doc,'$.x') = '1'`. | `FullTableScan`. |
| 6 | `execute_returnsDoc` | `SELECT _doc FROM users WHERE userId=?` + param. | One row; JSON matches storage. |
| 7 | `execute_orderByLimit` | `ORDER BY createdAt DESC LIMIT 5`. | ≤5 rows, descending. |
| 8 | `prepare_parameterBinding` | `WHERE status = ?` with `PreparedQuery`. | Correct filtering after bind. |
| 9 | `explain_showsPlan` | Any indexed query. | `ExplainResult.plan` is `IndexScan`. |
| 10 | `parse_error_badSyntax` | `SELEC * FROM`. | `SqlParseException`. |
| 11 | `unknownColumn_planError` | `SELECT notACol FROM users`. | `SqlPlanningException`. |
| 12 | `maxRows_enforced` | Scan returning 20k docs, `maxRows=100`. | Truncation or `QueryTimeoutException`. |

-----

## 8. Non-Goals

- **Full SQL-92** — v1 subset: single-table `SELECT`, basic `WHERE`, `ORDER BY`, `LIMIT`, `INSERT`/`UPDATE`/`DELETE` with simple predicates, `CREATE INDEX` / `DROP INDEX`.
- **JOINs, subqueries, window functions, CTEs** — future.
- **`AT VERSION` / `AT COMMIT` parsing** — Layer 6 sets `QueryContext.atCommit`; parser may accept syntax stub only.
- **JDBC `ResultSet` mapping** — Layer 8.
- **Query optimisation cost model** — v1 rule-based index picker only.
- **Virtual view expansion** — Component 16 rewrites to underlying `SelectQuery` before planning.

-----

## 9. Implementation Notes

### Parser strategy
Master spec open question §15.1: **v1 ships a hand-written recursive-descent parser in `commonMain`** (~1,200 lines) covering the documented subset. JVM-only adapter may wrap JSQLParser later behind `expect/actual` — not required for v1 deliverable.

### Planner
Rule-based: walk `WHERE` AND-tree; pick first matching index rule; push remaining predicates to `Filter` above scan.

### Execution
1. Run scan plan → `List<KdbUuid>`.
2. Batch-fetch documents: `storage.getDocument(ns, id, treeHash)` where `treeHash` from `dag.getCommit(atCommit).documentTreeHash`.
3. Evaluate projections; apply `Sort`/`Limit` in memory if not satisfied by index.

### DML
`UPDATE` with schema column → validate via `SchemaEngine`, build `TransactionBuilder`. `UPDATE _doc = kdb_json_set(...)` → JSON patch path without index update for non-schema paths (master §3.5).

### Module layout
```
dev.kdb.sql.parser
dev.kdb.sql.ast
dev.kdb.sql.planner
dev.kdb.sql.exec
dev.kdb.sql.ddl
```

### KMP
Parser + planner + exec in `commonMain`. No `expect/actual` in v1.

### Registration
`sqlEngine(indexManager, storage, dag)` is the single factory used by embedded API and later JDBC.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| Parser + AST | 1,200 |
| Planner + index rules | 900 |
| Executor + document fetch | 1,100 |
| DML + DDL routing | 700 |
| `SqlEngine` facade + prepared | 400 |
| Exceptions + tests | 1,700 |
| **Total** | **~5,000** |
