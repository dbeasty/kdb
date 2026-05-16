# KDB Component Spec — Layer 6
## Component 17: Hybrid Query Engine
### `dev.kdb.query.hybrid`

**File:** `kdb-spec-layer6-component17-hybrid-query-engine.md`  
**Layer:** 6 — Hybrid Query + Policy  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-hybrid-query`  
**Depends on:** Layers 0–2, Layer 3 (`StorageAdapter`, `IndexManager`), Layer 5 (`SqlEngine`, `SqlParser`, `VirtualViewEngine`), Layer 2 (`CommitDag`, `CommitRef`)

-----

## 1. Purpose

Delivers the **hybrid document query surface** described in master spec §3: `_doc` and `kdb_json_*` alongside schema columns in one SQL execution path, with **version-aware reads** (`AT VERSION`, `AT COMMIT`, `AT TIME`) and checkout semantics wired into `QueryContext.atCommit`.

Component 15 (`:kdb-sql`) owns parsing, planning, and row execution for the SQL subset. This module is the **facade and integration layer** that resolves version references, enforces namespace history mode, applies hybrid DML rules (§3.5), and delegates planning/execution to `SqlEngine` with a fully populated `QueryContext`. It does **not** duplicate the planner or index rules.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbTimestamp`, `KdbUuid` |
| `dev.kdb.error` | `KdbException`, `VersionNotFoundException`, `SqlParseException` |
| `dev.kdb.document` | `KdbDocument` |
| `dev.kdb.json` | `KdbJsonFunctionRegistry`, `kdbJsonGet`, `kdbJsonSet`, … |
| `dev.kdb.schema` | `KdbSchema`, `SchemaEngine` (DML validation) |
| `dev.kdb.dag` | `CommitDag`, `CommitRef`, `resolveRefOrThrow` |
| `dev.kdb.index` | `IndexManager`, `IndexReader` (historical pin) |
| `dev.kdb.storage` | `StorageAdapter` |
| `dev.kdb.sql` (15) | `SqlEngine`, `SqlParser`, `QueryContext`, `QueryResult`, AST |
| `dev.kdb.sql.view` (16) | `VirtualViewRegistry`, `VirtualViewEngine` |
| `dev.kdb.policy` (18) | `NamespacePolicy`, `HistoryMode` — read-only at query time |

-----

## 3. Public Interface

```kotlin
package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitRef
import dev.kdb.index.IndexManager
import dev.kdb.policy.NamespacePolicy
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.*
import dev.kdb.sql.view.VirtualViewEngine
import dev.kdb.sql.view.VirtualViewRegistry
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionEngine

/** Primary entry for embedded / CLI / future JDBC adapter. */
interface HybridQueryEngine {
    suspend fun execute(sql: String, request: HybridQueryRequest): HybridQueryResult
    suspend fun explain(sql: String, request: HybridQueryRequest): ExplainResult
    fun prepare(sql: String, request: HybridQueryRequest): PreparedHybridQuery

    /** Kotlin API checkout — read-only view at [ref]. */
    suspend fun checkout(namespaceId: String, ref: CommitRef): CheckoutHandle
    suspend fun resetCheckout(namespaceId: String)
}

fun hybridQueryEngine(
    sql: SqlEngine,
    parser: SqlParser,
    dag: CommitDag,
    storage: StorageAdapter,
    indexManager: IndexManager,
    views: VirtualViewEngine,
    viewRegistry: VirtualViewRegistry,
    transactionEngine: TransactionEngine,
    policyProvider: suspend (String) -> NamespacePolicy,
): HybridQueryEngine

data class HybridQueryRequest(
    val namespaceId: String,
    val schema: KdbSchema,
    /** When null, use DAG HEAD (or active checkout if set). */
    val version: VersionClause? = null,
    val parameters: List<SqlParameter> = emptyList(),
    val maxRows: Int = 10_000,
)

/** Parsed from SQL suffix or supplied by API. */
sealed class VersionClause {
    data class AtTag(val tag: String) : VersionClause()
    data class AtCommit(val hex: String) : VersionClause()
    data class AtTime(val iso8601: String) : VersionClause()
}

data class HybridQueryResult(
    val result: QueryResult,
    val resolvedCommit: KdbHash,
    val readOnly: Boolean,
)

interface PreparedHybridQuery {
    val parameterCount: Int
    suspend fun execute(bindings: List<SqlParameter>, request: HybridQueryRequest): HybridQueryResult
}

/** Active checkout per namespace (in-memory on this node). */
data class CheckoutHandle(
    val namespaceId: String,
    val commitHash: KdbHash,
    val readOnly: Boolean = true,
)

interface VersionResolver {
    suspend fun resolve(
        dag: CommitDag,
        clause: VersionClause?,
        activeCheckout: CheckoutHandle?,
    ): KdbHash
}

fun defaultVersionResolver(): VersionResolver

/** Extends Component 15 parser with `AT VERSION|COMMIT|TIME` on SELECT. */
interface HybridSqlParser : SqlParser {
    fun parseWithVersion(sql: String): ParsedHybridStatement
}

data class ParsedHybridStatement(
    val statement: SqlStatement,
    val version: VersionClause?,
)

fun hybridSqlParser(base: SqlParser = defaultSqlParser()): HybridSqlParser
```

-----

## 4. Data Structures

### `ParsedHybridStatement`
Wraps `SqlStatement` after stripping trailing version clause. `SELECT … FROM t AT VERSION 'tag'` → `version = AtTag("tag")`.

### `QueryContext` population (owned by this module, consumed by Component 15)
```kotlin
QueryContext(
    namespaceId = request.namespaceId,
    schema = request.schema,
    atCommit = resolvedHash,          // non-null when not HEAD
    parameters = request.parameters,
    maxRows = request.maxRows,
)
```

### Historical index pin (internal)
`HistoricalIndexSession(commitHash)` — `IndexReader` queries pass `atCommit` so hash/btree/fulltext/vector stores filter entries with `dag.isAncestor(entryCommit, atCommit)`.

### Implicit result columns (normative, master §3.2)
Every table exposes `kdb_id`, `_doc`, and schema field columns. `SELECT *` expands to schema fields + `_doc` unless `SELECT * EXCLUDE _doc` (optional v1: always include `_doc`).

### DML classification (`WriteKind`)
| Assignment pattern | Validation | Index update |
|---|---|---|
| Schema column `SET col = expr` | `SchemaEngine` | Yes, for that field |
| `SET _doc = kdb_json_set(_doc, …)` | None on extension paths | No |
| `SET _doc = '{...}'` whole doc | Schema fields in JSON | All schema indexes |

-----

## 5. Contracts

### `HybridQueryEngine.execute`
- **Preconditions:** `request.namespaceId == dag.namespaceId`. Policy exists for namespace (Component 18). If `HistoryMode.NONE`, `version` must be null.
- **Postconditions:** `resolvedCommit` is the commit hash used for document tree + indexes. `SELECT` rows: projected `_doc` is valid JSON from storage at that commit; schema columns match typed extraction.
- **Read-only:** When `version != null` or active checkout, DML throws `ReadOnlyCheckoutException`.

### `VersionResolver.resolve`
- `null` clause + no checkout → `dag.head()`.
- `null` clause + checkout → `checkout.commitHash`.
- `AtTag` / `AtCommit` / `AtTime` → `dag.resolveRefOrThrow(CommitRef.*)`; `VersionNotFoundException` if unresolved.

### `checkout` / `resetCheckout`
- **Postconditions:** Subsequent `execute` without explicit `version` uses checkout hash until reset.
- Checkout is **node-local**; not replicated (master §6.2).

### JSON functions in SQL
Evaluated via `KdbJsonFunctionRegistry` during Component 15 `Project`/`Filter` — same semantics as Layer 1. Hybrid engine ensures `_doc` column is fetched before expressions that reference it.

### Virtual views
`VirtualViewEngine.resolveTableRef` runs before planning; expanded `SelectQuery` inherits version clause from outer query.

### `explain`
Same version resolution as `execute`; delegates to `SqlEngine.explain`.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `VersionNotFoundException` | Tag/branch/time/hash not in DAG |
| `ReadOnlyCheckoutException` | DML while `atCommit != HEAD` or checkout active |
| `HistoryDisabledException` | `HistoryMode.NONE` + version clause |
| `SqlParseException` | Malformed `AT VERSION` suffix |
| `SqlPlanningException` | Delegated from planner |
| `IceStorageException` | Resolved commit is stubbed (archived) — from DAG walk |

```kotlin
class ReadOnlyCheckoutException(
    val namespaceId: String,
    val atCommit: KdbHash,
) : KdbException("namespace $namespaceId is read-only at $atCommit")

class HistoryDisabledException(val namespaceId: String) : KdbException(
    "versioned queries are disabled for namespace $namespaceId"
)
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `selectDoc_atHead` | `SELECT _doc FROM users WHERE userId='u1'` | JSON matches storage at HEAD |
| 2 | `atVersion_tag` | `… AT VERSION 'v1'` | Rows from tagged commit |
| 3 | `atCommit_prefix` | `… AT COMMIT 'a3f9c2'` | Resolves via `lookupHashPrefix` |
| 4 | `atTime_nearest` | `… AT TIME '2024-06-01T00:00:00Z'` | Commit at or before timestamp |
| 5 | `jsonPath_filter` | `WHERE kdb_json_get(_doc,'$.x')='1'` | Full scan; correct filtering |
| 6 | `schemaAndDoc_hybrid` | `SELECT userId, _doc WHERE status='active'` | Both columns populated |
| 7 | `checkout_blocksDml` | checkout + `UPDATE …` | `ReadOnlyCheckoutException` |
| 8 | `historyNone_rejectsVersion` | policy `NONE` + `AT VERSION` | `HistoryDisabledException` |
| 9 | `virtualView_atVersion` | query through view with `AT VERSION` | Underlying plan uses pinned commit |
| 10 | `update_docJsonSet` | `UPDATE t SET _doc=kdb_json_set(_doc,'$.a',1)` | Doc patched; schema indexes unchanged |
| 11 | `update_schemaField` | `UPDATE t SET status='x'` | Index updated for `status` |
| 12 | `unknownTag` | `AT VERSION 'missing'` | `VersionNotFoundException` |

-----

## 8. Non-Goals

- **JDBC `ResultSet` / `Connection`** — Layer 8.
- **Cross-namespace queries** — v1 single namespace per request.
- **Wire encoding of version clauses** — Layer 7/8.
- **Parser for full SQL-92** — remains Component 15 scope.
- **Automatic schema migration on read** — Layer 2 / transaction path.
- **Ice restore** — Layer 7 Storage Tier Manager; this module only surfaces `IceStorageException`.

-----

## 9. Implementation Notes

### Facade pattern
`DefaultHybridQueryEngine` holds `SqlEngine`, `HybridSqlParser`, `VersionResolver`, checkout map. No fork of planner/executor.

### Parser extension
Append optional clause after `SelectQuery` parse in `HybridSqlParser`:
`AT VERSION 'id' | AT COMMIT 'hex' | AT TIME 'iso'`. Reuse `CommitRef` mapping in `VersionResolver`.

### Document fetch at version
Executor (15) already uses `context.atCommit` for `documentTreeHash`. Hybrid engine must pass resolved hash; ensure `storage.getDocument(ns, id, treeHash)` uses tree from `dag.getCommit(atCommit)`.

### Index historical reads
Pass `atCommit` into `IndexReader` lookup methods (extend Component 8 reader if needed). v1: filter post-lookup via `dag.isAncestor` if reader lacks native pin.

### Module layout
```
dev.kdb.query.hybrid
  HybridQueryEngine.kt
  VersionResolver.kt
  HybridSqlParser.kt
  CheckoutStore.kt
  DmlHybridRouter.kt
```

### KMP
`commonMain` only. No `expect/actual` in v1.

### Bootstrap
Node runtime constructs `hybridQueryEngine(sqlEngine(...), …)` once per namespace or shares engine with policy-aware `policyProvider`.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `HybridQueryEngine` facade | 350 |
| `VersionResolver` + checkout store | 280 |
| `HybridSqlParser` (`AT VERSION` suffix) | 220 |
| DML hybrid router + schema/_doc paths | 450 |
| Historical index pin helpers | 200 |
| Exceptions + tests | 500 |
| **Total** | **~2,000** |
