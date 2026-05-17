# KDB Component Spec — Layer 8
## Component 24: JDBC Driver
### `dev.kdb.jdbc`

**File:** `kdb-spec-layer8-component24-jdbc-driver.md`  
**Layer:** 8 — Advanced Sync + JDBC  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-jdbc` (JVM only)  
**Depends on:** Layer 6 (`HybridQueryEngine`), Layer 5 (`SqlEngine`, schema), Layer 2 (`CommitDag`), Layer 3 (`StorageAdapter`, `TransactionEngine`), Layer 8 Component 23 (optional — not required for embedded memory mode)

-----

## 1. Purpose

Provides a **thin JDBC 4.x adapter** (master §5) that maps `java.sql.*` to KDB engine APIs. Query execution delegates to `HybridQueryEngine`; the driver does not reimplement SQL planning. v1 targets **embedded memory mode** (`jdbc:kdb:memory:///catalog`) for ORM/IDE compatibility tests.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.query.hybrid` | `HybridQueryEngine`, `HybridQueryRequest` |
| `dev.kdb.sql` | `QueryResult`, `SqlCell` |
| `dev.kdb.schema` | `KdbSchema` |
| `dev.kdb.dag` | `CommitDag` |
| `dev.kdb.storage` | `StorageAdapter` |
| `dev.kdb.policy` | `NamespacePolicyRegistry` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.jdbc

import dev.kdb.query.hybrid.HybridQueryEngine
import java.sql.*

class KdbDriver : Driver {
    override fun connect(url: String, info: Properties?): Connection?
    override fun acceptsURL(url: String): Boolean
    companion object {
        const val URL_PREFIX = "jdbc:kdb:"
        init { DriverManager.registerDriver(KdbDriver()) }
    }
}

/** Parsed connection target. */
data class KdbJdbcUrl(
    val mode: JdbcMode,
    val catalog: String,
    val readOnly: Boolean,
)

enum class JdbcMode { MEMORY, FILE, NETWORK }

class KdbConnection(
    private val engine: HybridQueryEngine,
    private val runtime: EmbeddedKdbRuntime,
    private val url: KdbJdbcUrl,
) : Connection { /* Statement, MetaData, commit/rollback */ }

class KdbStatement(...) : Statement
class KdbPreparedStatement(...) : PreparedStatement
class KdbResultSet(...) : ResultSet
class KdbDatabaseMetaData(...) : DatabaseMetaData

/** In-memory embedded engine for tests and tools. */
class EmbeddedKdbRuntime(
    val catalog: String,
    val dag: CommitDag,
    val storage: StorageAdapter,
    val hybrid: HybridQueryEngine,
    val schema: KdbSchema,
    val defaultNamespace: String,
)

fun openMemoryRuntime(catalog: String, namespaceId: String, schema: KdbSchema): EmbeddedKdbRuntime
```

-----

## 4. Data Structures

### URL mapping (master §5.1–5.2)
| JDBC | KDB |
|---|---|
| `jdbc:kdb:memory:///myapp` | catalog=`myapp`, default namespace `myapp/main` |
| Table `users` in connection | namespace `myapp/users` |
| `readOnly=true` property | SELECT only; DML throws |

### ResultSet columns
Schema fields + `kdb_id` + `_doc` per master §3.2. `SqlCell` mapped to JDBC types (`String`, `Long`, `Boolean`, `Double`, `NULL`).

### SQL extensions
`AT VERSION` / `AT COMMIT` / `AT TIME` pass through `HybridSqlParser` unchanged.

-----

## 5. Contracts

### `KdbDriver.connect`
- **Preconditions:** `acceptsURL(url)`.
- **Postconditions:** Returns `KdbConnection` for memory URLs; `null` if URL not handled.
- **Memory mode:** New isolated `EmbeddedKdbRuntime` per connection (tests).

### `KdbStatement.executeQuery`
- **Postconditions:** `ResultSet` positioned before first row; column labels match projection aliases.

### `KdbConnection.commit`
- v1 memory mode: no-op when `autoCommit=true`; when false, flushes pending `TransactionEngine` work (if any).

-----

## 6. Error Cases

| Type | When |
|---|---|
| `SQLException` | Parse errors, closed connection, unsupported API |
| `SQLFeatureNotSupportedException` | `CallableStatement`, savepoints, network mode in v1 |
| `HistoryDisabledException` | Wrapped when version SQL on NONE policy |

-----

## 7. Test Cases

| # | Name | Expected |
|---|---|---|
| 1 | `driverRegisters` | `DriverManager.getDriver("jdbc:kdb:memory:///t")` non-null |
| 2 | `acceptsMemoryUrl` | true for memory prefix |
| 3 | `rejectOtherUrls` | false for `jdbc:postgresql:` |
| 4 | `selectStar` | ResultSet rows match seeded doc |
| 5 | `metadataTables` | `getTables` lists namespace as table |
| 6 | `metadataColumns` | columns include `kdb_id`, `_doc` |
| 7 | `preparedSelect` | `?` binding works for WHERE |
| 8 | `atVersion` | historical row count |
| 9 | `readOnlyRejectsUpdate` | UPDATE throws |
| 10 | `closeConnection` | further execute throws |
| 11 | `resultSetTypes` | `getString`/`getLong` for typed columns |
| 12 | `catalogMatchesUrl` | `getCatalog()` == parsed catalog |

-----

## 8. Non-Goals

- Network `jdbc:kdb://host:port` (Layer 9 transport + future driver work).
- File `jdbc:kdb:file://` persistence wiring (uses storage engine later).
- Full Hibernate dialect (integration tests are Layer 10 Component 30).
- `CallableStatement`, UDTs, sharding.

-----

## 9. NBNC Estimate

~4,500 lines JDBC + ~500 URL parser (included in master §14 JDBC row).
