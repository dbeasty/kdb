# KDB Component Spec — Layer 5
## Component 16: Virtual View Engine
### `dev.kdb.sql.view`

**File:** `kdb-spec-layer5-component16-virtual-view-engine.md`  
**Layer:** 5 — Index Implementations  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-sql` (subpackage `dev.kdb.sql.view`; may split to `:kdb-sql-view` if circular deps arise)  
**Depends on:** Component 15 (`SqlParser`, `SelectQuery`, `SqlEngine`), Layer 2 (`KdbSchema`), Layer 3 (`StorageAdapter` for namespace metadata blobs)

-----

## 1. Purpose

Implements **virtual views** (master spec §3.7): named, persisted `SELECT` definitions that promote extension-field JSON paths to top-level columns for BI tools and JDBC metadata, without altering the underlying namespace schema.

`CREATE VIRTUAL VIEW … AS SELECT …` stores view metadata in namespace configuration. Queries against `users_extended` rewrite to the base table query before Component 15 plans indexes on underlying schema columns.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid`, `encodeToBytes`, `decodeFromBytes` |
| `dev.kdb.error` | `KdbException`, `SqlParseException` |
| `dev.kdb.schema` | `KdbSchema`, `SchemaField` |
| `dev.kdb.sql` (Component 15) | `SqlParser`, `SelectQuery`, `SqlStatement`, `QueryContext`, `SqlEngine` |
| `dev.kdb.storage` | `StorageAdapter` — metadata key `namespace/{id}/virtual-views` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.sql.view

import dev.kdb.codec.KdbHash
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.*
import dev.kdb.storage.StorageAdapter

interface VirtualViewRegistry {
    suspend fun list(namespaceId: String): List<VirtualViewDefinition>
    suspend fun get(namespaceId: String, viewName: String): VirtualViewDefinition?
    suspend fun put(definition: VirtualViewDefinition)
    suspend fun drop(namespaceId: String, viewName: String): Boolean
}

data class VirtualViewDefinition(
    val viewName: String,
    val namespaceId: String,
    val baseTable: String,
    val query: SelectQuery,              // parsed SELECT (validated at create time)
    val columns: List<VirtualColumn>,   // exposed JDBC column list
    val createdAtCommit: KdbHash,
    val schemaVersion: Int,              // namespace schema version at create time
)

data class VirtualColumn(
    val name: String,
    val sqlType: String,
    val source: VirtualColumnSource,
)

sealed class VirtualColumnSource {
    data class SchemaField(val fieldName: String) : VirtualColumnSource()
    data class Expression(val sqlExpr: SqlExpr) : VirtualColumnSource()  // e.g. kdb_json_get
    object KdbId : VirtualColumnSource()
    object DocJson : VirtualColumnSource()
}

interface VirtualViewEngine {
    /** Expand view table ref to base [SelectQuery] with column aliases applied. */
    fun resolveTableRef(ref: TableRef, namespaceId: String, registry: VirtualViewRegistry): ResolvedTable

    suspend fun executeCreateView(sql: String, context: QueryContext, storage: StorageAdapter): Unit
    suspend fun executeDropView(viewName: String, context: QueryContext, storage: StorageAdapter): Boolean
}

data class ResolvedTable(
    val baseTable: String,
    val rewrittenQuery: SelectQuery,
    val columnMap: Map<String, VirtualColumn>,   // outer name → source
)

fun virtualViewRegistry(storage: StorageAdapter): VirtualViewRegistry
fun virtualViewEngine(parser: SqlParser, registry: VirtualViewRegistry): VirtualViewEngine
```

`SqlEngine` (Component 15) calls `VirtualViewEngine.resolveTableRef` during planning when `TableRef.name` is not the base namespace table.

DDL surface:

```sql
CREATE VIRTUAL VIEW users_extended AS
SELECT userId, email, status,
       kdb_json_get(_doc, '$.clientField.source') AS source,
       _doc
FROM users;

DROP VIRTUAL VIEW users_extended;
```

-----

## 4. Data Structures

### Persistence (`VirtualViewCatalog`)
Layer 0 record list keyed under namespace metadata:

| Field | Type |
|---|---|
| `views` | `Array` of `VirtualViewRecord` |
| `VirtualViewRecord.viewName` | `String` |
| `VirtualViewRecord.baseTable` | `String` |
| `VirtualViewRecord.queryBytes` | `Bytes` — serialised `SelectQuery` (custom wire or SQL text) |
| `VirtualViewRecord.columns` | `Array` of column metadata |

v1 may store **canonical SQL text** plus parsed `SelectQuery` cache in memory on load.

### `ResolvedTable`
Planner sees a `SelectQuery` whose `FROM` is the base table; projections from the view definition are merged with any additional `SELECT` list from the user query (outer query).

-----

## 5. Contracts

### `executeCreateView`
- **Preconditions:** View name matches `[a-zA-Z_][a-zA-Z0-9_]*`, not colliding with base table name. Inner `SELECT` references only base table (no nested views in v1). All `kdb_json_get` paths syntactically valid.
- **Postconditions:** `registry.get` returns definition. `VirtualViewDefinition.columns` matches SELECT projections. Persisted to storage before return.

### `resolveTableRef`
- If `ref.name` is a virtual view: return `ResolvedTable` whose `rewrittenQuery` is the stored query; outer queries wrap as subquery alias (inline expansion).
- If not a view: return `ResolvedTable(baseTable = ref.name, rewrittenQuery = null, columnMap = empty)` — planner uses schema only.

### Column visibility (JDBC)
`VirtualViewDefinition.columns` drives `DatabaseMetaData.getColumns` in Layer 8; includes stable `sqlType` strings (`VARCHAR`, `JSON`, etc.).

### Immutability
`CREATE OR REPLACE VIRTUAL VIEW` optional in v1; otherwise `DROP` then `CREATE`. Schema migration does not auto-drop views; mismatched views return `SqlPlanningException` until recreated.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `VirtualViewExistsException` | `CREATE` with existing name. |
| `VirtualViewNotFoundException` | `DROP` or query unknown view. |
| `SqlParseException` | Invalid view body SQL. |
| `SqlPlanningException` | View references unknown column; nested view. |

```kotlin
class VirtualViewExistsException(message: String, val viewName: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

class VirtualViewNotFoundException(message: String, val viewName: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `createView_persists` | `CREATE VIRTUAL VIEW v AS SELECT kdb_id, _doc FROM t`. | `registry.get` non-null. |
| 2 | `queryView_rewrites` | Select from `v` where base indexed column filtered. | Planner uses base table index. |
| 3 | `jsonPath_column` | View with `kdb_json_get` alias `source`. | Result column `source` populated. |
| 4 | `dropView_removes` | `DROP VIRTUAL VIEW v`. | `get` null; query throws `VirtualViewNotFoundException`. |
| 5 | `duplicateCreate_throws` | Two CREATE same name. | `VirtualViewExistsException`. |
| 6 | `nestedView_rejected` | View body `FROM other_view`. | `SqlPlanningException`. |
| 7 | `catalog_reload` | Create view, new registry instance load from storage. | Same definition. |
| 8 | `starExpansion` | View `SELECT *` from base with schema fields. | Columns include schema + `_doc`. |
| 9 | `outerProjection_merge` | `SELECT source FROM v WHERE status='x'`. | Filter on schema field + project alias. |
| 10 | `invalidPath_parseError` | `kdb_json_get(_doc, 'not-a-path')`. | `SqlParseException` or `JsonPathException` at create. |
| 11 | `viewName_reserved` | Create view named like system table `kdb_id`. | Rejected. |
| 12 | `schemaVersion_recorded` | Create at schema v3. | `definition.schemaVersion == 3`. |

-----

## 8. Non-Goals

- **Materialised views** — always logical rewrite; no stored duplicate data.
- **Updatable views** (`INSERT INTO view`) — v1 read-only; DML targets base table only.
- **Cross-namespace views** — single namespace only.
- **View grants / security** — future policy engine (Layer 6).
- **Automatic promotion to schema field** — operational signal only (master §3.7); no auto-DDL.

-----

## 9. Implementation Notes

### Query rewriting
Inline expansion (not subquery AST node in v1): merge view `SelectQuery` into outer query's `FROM` and apply column alias map to outer `SELECT` list. Simplifies index planning.

### Storage key
`putBlob(namespaceMetaKey(namespaceId, "virtual-views"), catalogBytes)` with content hash; update CAS via commit metadata side channel or atomic replace through `TransactionEngine` namespace meta op (if missing, add `KdbOp.NamespaceMetaWrite` in Component 7 follow-up — v1 may store in adapter metadata map).

### Integration with `SqlEngine`
Extend parser: `CreateVirtualView`, `DropVirtualView` statements. `SqlEngine.execute` dispatches to `VirtualViewEngine` before SELECT planning.

### BI tooling
Stable column names and types are the primary deliverable — avoid anonymous expression columns without alias.

### KMP
Pure `commonMain`.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `VirtualViewRegistry` + catalog wire | 350 |
| `VirtualViewEngine` rewrite | 400 |
| DDL hooks + column inference | 250 |
| Integration with `SqlEngine` | 150 |
| Tests | 350 |
| **Total** | **~1,500** |
