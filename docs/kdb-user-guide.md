# KDB User Guide

This guide is for developers who want to **run**, **inspect**, or **embed** KDB in an application. It describes what works in this repository today and what is still planned.

For architecture and protocol details, see the [architecture specification](kdb-spec.md).

---

## Status

KDB has a **first Kotlin implementation** across Layers 0–10 (see [kdb-spec.md §0](kdb-spec.md#0-session-state--read-this-first)).

| Capability | Status |
|------------|--------|
| Core engine (codec, storage, SQL, indexes, wire, peer sync, …) | Implemented; unit and integration tests |
| **Product CLI** (`:kdb-cli`) — `init`, `put`, `get`, `query`, `log`, `status`, `sync`, `shell` | Implemented via Gradle `runCli` |
| **Inspect CLI** (`:kdb-inspect`) — `dump-delta`, `dump-wire`, … | Implemented via Gradle `inspectCli` |
| **JDBC driver** — `jdbc:kdb:memory://…`, `jdbc:kdb:file://…` | Embedded SELECT, metadata, prepared statements; file mode persists under `dataRoot/ns/{namespaceId}/` |
| Peer sync (in-memory hub + TCP loopback) | Implemented (`:kdb-peer-sync`, `:kdb-transport-tcp`) |
| Integration test suite | `:kdb-integration` |
| **JDBC** — `jdbc:kdb://…` network URLs | Parsed but not implemented (`SQLFeatureNotSupportedException`) |
| JDBC DML (`INSERT` / `UPDATE` / `DELETE`) | Implemented (embedded memory/file); auto-commit per statement; multi-statement transactions on network SQL (`BEGIN` … `COMMIT`) |
| CLI persistence (`--data-dir`) | `put` / `get` / `query` survive separate CLI invocations (delta log + SERVER engine) |
| Published Maven / npm artifacts | Not yet; use Gradle composite build or project dependency from source |
| Full git-style CLI (branch, merge, `schema migrate`, …) | Specified in [§11](kdb-spec.md#11-cli-interface); not in v1 CLI |
| **File attachments** (`file put` / `get` / `meta`, ZIP, bundles, `fileId` GUID) | Implemented — see [file attachments spec](kdb-spec-layer1-component3b-file-attachments.md) |

---

## Prerequisites

- **JDK 17+** (JVM targets, CLIs, JDBC)
- **Gradle 8.x** — use the included wrapper: `./gradlew`
- For **JavaScript / browser** embedding: Kotlin Multiplatform with `js(IR) { browser() }`

```bash
./gradlew build
./gradlew :kdb-jdbc:test
./gradlew :kdb-cli:test
./gradlew :kdb-integration:test
```

---

## Command-line usage

KDB ships two command-line entry points.

### Product CLI (`kdb`)

Git-style namespace commands for documents, queries, history, and peer sync.

```bash
./gradlew :kdb-cli:runCli --args="<command> ..."
```

Global options (before the subcommand):

| Flag | Description |
|------|-------------|
| `--data-dir DIR` | Workspace root (default: `~/.kdb`) |
| `--quiet` | Suppress informational output |

Commands:

| Command | Usage | Description |
|---------|-------|-------------|
| `init` | `init <namespace>` | Create namespace metadata under `--data-dir` |
| `put` | `put <namespace> <file\|json>` | Write a JSON document and append a commit |
| `get` | `get <namespace> <docId>` | Print document JSON by UUID |
| `query` | `query <namespace> <sql>` | Run hybrid SQL; print tab-separated rows |
| `log` | `log <namespace>` | Print commit history |
| `status` | `status <namespace>` | Print HEAD hash and document count |
| `sync` | `sync <namespace> <peer-uri>` | Bidirectional peer sync (e.g. TCP loopback URI) |
| `file put` | `file put <namespace> [--id UUID] [--zip] <path>` | Store opaque file (metadata doc + blob) |
| `file put` (bundle) | `file put <namespace> --bundle <UUID> [--zip] <paths...>` | Store multiple files in one ZIP blob |
| `file get` | `file get <namespace> --id <UUID> [-o path]` | Fetch file bytes |
| `file meta` | `file meta <namespace> --id <UUID>` | Print `kdb.file` JSON metadata |
| `shell` | `shell <namespace>` | Interactive REPL (one open runtime per session) |
| `unlock` | `unlock` | Remove a stale `.kdb.lock` after a crash (holder process must be gone) |

Example:

```bash
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb init myapp/users"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb put myapp/users '{\"userId\":\"u1\"}'"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb query myapp/users 'SELECT _doc FROM users'"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file put myapp/files --id 00000000-0000-0000-0000-0000000000f1 --zip ./report.pdf"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file get myapp/files --id 00000000-0000-0000-0000-0000000000f1 -o ./report-copy.pdf"
```

**Persistence:** Namespace data lives under `{dataDir}/ns/{namespaceId}/` (delta log, WAL, SSTables). Each CLI invocation replays the delta log on open; commits from a prior `put` are visible to a later `get` or `query` with the same `--data-dir`.

**Workspace lock:** File mode takes an exclusive lock on `{dataDir}/.kdb.lock` while a process has the database open (CLI, JDBC `jdbc:kdb:file://…`, or your app via `openFileRuntime`). A second process opening the same `--data-dir` fails with a clear error naming the holder PID when known. Only one live writer per workspace.

If your app crashes and leaves the lock file behind, the OS releases the underlying lock when the process exits; you can remove a leftover file with:

```bash
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb unlock"
```

`unlock` deletes `.kdb.lock` only when the PID recorded in the file is **not** running. If another instance is still open, `unlock` refuses and tells you to stop that process first.

### Interactive shell

For multiple commands against the same workspace without paying a full JVM + delta replay per line, start the shell once:

```bash
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb shell myapp/users"
# kdb:myapp/users> put '{"userId":"u1"}'
# kdb:myapp/users> query SELECT _doc FROM users
# kdb:myapp/users> status
# kdb:myapp/users> use myapp/archive
# kdb:myapp/archive> exit
```

Shell commands omit the namespace on each line (it is fixed at startup; use `use <namespace>` to switch and reopen the runtime):

| Line command | Description |
|--------------|-------------|
| `put <file\|json>` | Same rules as one-shot `put` |
| `get <docId>` | Print document JSON |
| `query <sql>` | Single-line SQL only |
| `log` | Commit history |
| `status` | HEAD hash and namespace |
| `sync <peer-uri>` | Bidirectional peer sync |
| `use <namespace>` | Switch namespace (reopens runtime) |
| `help`, `?` | Command summary |
| `exit`, `quit` | Leave shell (exit code 0) |

Errors on a line are printed to stderr and the shell continues. Gradle still starts one JVM per `runCli` invocation; within a session, only the first line pays full open/replay cost. A second shell or CLI against the same `--data-dir` is rejected while the lock is held; use `unlock` only after a crash if a stale lock file remains.

Additional git-style commands (`branch`, `merge`, `schema migrate`, `push`, …) are described in [§11 CLI Interface](kdb-spec.md#11-cli-interface) and are not yet exposed on the v1 CLI.

### Inspect CLI (debug tooling)

Non-authoritative JSON views of binary on-disk or captured wire data. Does not modify source files.

```bash
./gradlew :kdb-inspect:inspectCli --args="<subcommand> <options>"
```

| Subcommand | Purpose |
|------------|---------|
| `dump-delta` | Decode delta segments (`--data-dir`, `--namespace`, optional `--segment`, `--codec`) |
| `dump-wire` | Decode a wire frame file (`--file`, optional `--compact`) |
| `dump-commit` | Decode a commit payload (`--file`) |
| `dump-blob` | Decode a content-addressed blob (`--data-dir`, `--hash`) |

```bash
./gradlew :kdb-inspect:inspectCli --args="dump-delta --data-dir ./data --namespace myapp/users"
```

---

## JDBC (Java) — what you can do today

The **JDBC driver** in `:kdb-jdbc` maps `java.sql.*` to the KDB hybrid query engine. v1 supports **embedded memory** and **embedded file** modes for ORM/IDE compatibility, local apps, and tests.

### Add the dependency (from source)

Artifacts are not on Maven Central yet. Include KDB via Gradle composite build or subproject:

```kotlin
// settings.gradle.kts — subproject example
include(":kdb-jdbc")
// project(":kdb-jdbc").projectDir = file("/path/to/kdb/kdb-jdbc")

dependencies {
    implementation(project(":kdb-jdbc"))
}
```

Load the driver once:

```java
Class.forName("dev.kdb.jdbc.KdbDriver");
```

### Connection URLs

| URL | Namespace | v1 |
|-----|-----------|-----|
| `jdbc:kdb:memory:///demo/users` | `demo/users` | **Yes** — shared in-process DB per URL (pool-safe) |
| `jdbc:kdb:memory:///myapp` | `myapp/main` (no slash in path) | **Yes** |
| `jdbc:kdb:memory:///demo/users` + `readOnly=true` property | same | **Yes** (SELECT only) |
| `jdbc:kdb:memory:///demo/users;unique=true` | new empty DB per connect (tests) | **Yes** |
| `jdbc:kdb:memory:///demo/users;isolate=mytest` | separate DB per isolate name | **Yes** |
| `jdbc:kdb:memory:///demo/users;dropOnClose=true` | dropped when last connection closes | **Yes** |
| `jdbc:kdb:file:///path/to/data/demo/users` | `demo/users`; data under `/path/to/data/ns/demo/users/` | **Yes** — survives process restart |
| `jdbc:kdb:file:///path/to/data/myapp` | `myapp/main` (path ends with catalog only) | **Yes** |
| `jdbc:kdb://host:port/catalog` | network SQL wire | Multi-client sessions; see [component 25](kdb-spec-layer8-component25-multi-client-sessions.md) |

**Mapping** (see [spec §5](kdb-spec.md#5-jdbc-driver-highest-priority)):

- **Catalog** → instance root (e.g. `demo`)
- **Table** in SQL → namespace `catalog/table` (e.g. `FROM users` → `demo/users`)
- Rows include **`kdb_id`** and **`_doc`** (full document JSON) plus schema columns when a schema is registered

### What works

| Area | Details |
|------|---------|
| **Driver** | `dev.kdb.jdbc.KdbDriver`; registers with `DriverManager`; `acceptsURL("jdbc:kdb:…")` |
| **Connection** | `getCatalog()`, `setReadOnly`, `close`, `isValid`, `getMetaData`, `setAutoCommit` / `commit` / `rollback` (embedded memory + file: transaction buffer; network SQL: wire session) |
| **Statement** | `executeQuery` for `SELECT`; `FROM table` auto-qualified to `catalog/table` |
| **PreparedStatement** | `setString`, `setInt`/`setLong`/`setFloat`/`setDouble`, `setBoolean`, `setNull`, `setObject`; `executeQuery` |
| **ResultSet** | Forward-only; `next`, `getString`/`getLong`/`getInt`/`getBoolean`/`getDouble`/`getObject` by index or column label; `findColumn`, `getMetaData` |
| **SQL** | `SELECT` with `WHERE` on schema/indexed fields (including `IN (…)`, `IS NOT NULL`); `COUNT(*)` / `COUNT(col)`; `GROUP BY` with `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`; `INNER JOIN` (same catalog, two tables); `SELECT _doc …`; `AT VERSION` / `AT COMMIT` / `AT TIME`; `BEGIN` / `COMMIT` / `ROLLBACK` on embedded and network — see [SQL transactions](#sql-transactions) |
| **DatabaseMetaData** | Product name `KDB`; `getTables`, `getColumns` (`kdb_id`, `_doc`), `getCatalogs`, `getSchemas`; keywords include `BEGIN`, `COMMIT`, `ROLLBACK`, `START`, `TRANSACTION`, `AT`, `VERSION`, `TIME`, `WORK`; functions `kdb_json_get`, `kdb_json_set` |

### SQL transactions

Multi-statement write transactions are supported on **embedded** (`jdbc:kdb:memory://…`, `jdbc:kdb:file://…`) and **network SQL** (`jdbc:kdb://…` with the SQL wire hub). With `autoCommit=false` or `BEGIN`, DML is buffered until `COMMIT` or `ROLLBACK`. On shared memory/file URLs, other connections see committed writes only after `COMMIT` (read-committed).

**Not supported in v1:** `SAVEPOINT`, `SET TRANSACTION` as SQL, and DDL inside an open transaction (`CREATE INDEX`, `CREATE VIRTUAL VIEW`, etc.) are rejected.

**SQL syntax:**

```sql
BEGIN;
-- or: START TRANSACTION;

UPDATE users SET name = 'Alice' WHERE userId = 'u1';
INSERT INTO users (_doc) VALUES ('{"userId":"u2","name":"Bob"}');
DELETE FROM users WHERE userId = 'u3';

COMMIT;
-- or: ROLLBACK;
```

Optional `WORK` after `BEGIN`, `COMMIT`, or `ROLLBACK` is accepted (`BEGIN WORK`, `COMMIT WORK`, …).

**JDBC (embedded or network):**

```java
conn.setAutoCommit(false);   // begins transaction (embedded) or sends BEGIN (network)
stmt.executeUpdate("UPDATE users SET name = 'Alice' WHERE userId = 'u1'");
stmt.executeUpdate("UPDATE users SET status = 'active' WHERE userId = 'u1'");
conn.commit();               // sends COMMIT (one DAG commit for both updates)
// conn.rollback();          // sends ROLLBACK (discards pending ops, releases locks)
conn.setAutoCommit(true);    // sends COMMIT if a transaction is still open
```

**Semantics:**

| Topic | Behaviour |
|-------|-----------|
| **Visibility** | Buffered DML is visible only to the same session until `COMMIT`. |
| **Atomicity** | All buffered ops succeed or fail together on `COMMIT` (git-style transaction engine). |
| **Conflicts** | Optimistic detection at commit (`STRICT` policy); overlapping writers get a conflict report. |
| **Locks** | Pessimistic exclusive lock per document for the session from first buffered write until commit/rollback. |
| **Reads** | `SELECT` during a transaction uses the session read consistency (`READ_COMMITTED` default; `SNAPSHOT` when isolation maps to repeatable read). Historical reads still use `AT COMMIT` / `AT VERSION` / `AT TIME` on the `SELECT`. |

See also [component 25 — multi-client sessions](kdb-spec-layer8-component25-multi-client-sessions.md).

### What does not work (throws)

| Area | Behaviour |
|------|-----------|
| **Network URLs (legacy)** | Plain `jdbc:kdb://host:port/catalog` without wire hub may still throw; use documented wire/inproc forms |
| **DML** | `UPDATE` / `INSERT` / `DELETE` via `executeUpdate` (embedded); read-only connections reject writes |
| **Read-only connection** | `executeUpdate` → `SQLException` |
| **Advanced JDBC** | `CallableStatement`, `Savepoint`, `Blob`/`Clob`, batch, generated keys → `SQLFeatureNotSupportedException` |
| **Compliance** | `jdbcCompliant()` returns `false` |

### Seeding data (required before SELECT returns rows)

**Memory mode (`jdbc:kdb:memory://…`):** Connections to the **same URL** share one in-process engine (like file mode shares disk). The database starts empty until you seed or write. Use `;unique=true` on the URL when a test needs a fresh isolated database per connect; use `;isolate=name` to pin a named shared instance for parallel test classes.

**Connection pools (HikariCP, etc.):** Point the pool at a stable memory URL (no `unique=true`). All pooled connections see the same data. Optional `;dropOnClose=true` removes the database when the last connection closes (H2-style test cleanup).

**File mode (`jdbc:kdb:file://…`):** Data is replayed from disk on connect. After a prior run (CLI `put`, `openFileRuntime`, or an earlier JDBC session) wrote commits, `SELECT` can return rows without re-seeding. New empty directories still need an initial write.

**Kotlin example** (from `KdbJdbcTest`):

```kotlin
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.jdbc.KdbConnection
import dev.kdb.jdbc.KdbDriver
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import java.sql.DriverManager
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    KdbDriver
    DriverManager.getConnection("jdbc:kdb:memory:///demo/users").use { conn ->
        val kdb = conn as KdbConnection
        seedUsers(kdb)
        conn.createStatement().executeQuery("SELECT _doc FROM users WHERE userId = 'u1'").use { rs ->
            while (rs.next()) {
                println(rs.getString(1))
            }
        }
    }
}

private suspend fun seedUsers(conn: KdbConnection) {
    val runtime = conn.embedded
    val ns = runtime.defaultNamespace
    val dag = runtime.dag
    val storage = runtime.storage
    val manager = runtime.indexManager
    val schema = KdbSchema.build(
        listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)),
    )
    manager.registryFor(ns).syncSchema(
        KdbSchema.NONE, schema, compositeIndexStoreFactory(dag, storage), dag, storage,
    )
    val doc = KdbDocument(KdbUuid.random(), """{"userId":"u1"}""")
    storage.putDocument(ns, doc)
    val parent = dag.head()
    val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
    val tx = KdbTransaction(
        KdbUuid.random(), parent,
        listOf(KdbOp.Write(doc.id, doc.json)),
        KdbTimestamp.now(), KdbUuid.random(),
    )
    val commit = dag.appendCommit(tx, parent, tree, null)
    manager.writer.applyCommit(commit, manager.registryFor(ns), storage, schema)
}
```

**Java query-only example** (after seeding in Kotlin or a test fixture):

```java
import java.sql.*;

public class KdbQueryExample {
    static { try { Class.forName("dev.kdb.jdbc.KdbDriver"); } catch (ClassNotFoundException e) { throw new RuntimeException(e); } }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")) {
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }
}
```

### PreparedStatement example

```java
try (Connection conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
     PreparedStatement ps = conn.prepareStatement("SELECT _doc FROM users WHERE userId = ?")) {
    ps.setString(1, "u1");
    try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) System.out.println(rs.getString(1));
    }
}
```

### DatabaseMetaData (ORM / IDE discovery)

```java
try (Connection conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users")) {
    DatabaseMetaData meta = conn.getMetaData();
    System.out.println(meta.getDatabaseProductName()); // KDB
    try (ResultSet tables = meta.getTables(null, null, null, null)) {
        while (tables.next()) {
            System.out.println(tables.getString("TABLE_CAT") + "." + tables.getString("TABLE_NAME"));
        }
    }
}
```

**Memory mode:** Connections to the same `jdbc:kdb:memory://…` URL share one in-process database. Use `conn.applyQuerySchema(schema)` (or register schema on first write) so indexed `WHERE` clauses work after reconnect.

**File mode:** Connections to the same `jdbc:kdb:file://…` URL share on-disk state (replay on open). Pass a `KdbSchema` when opening programmatically so indexes rebuild after reload (see `openFileRuntime`).

### Kotlin without JDBC

Use `openMemoryRuntime` or `openFileRuntime` from `:kdb-jdbc` for direct access to `dag`, `storage`, `hybrid`, and `indexManager`:

```kotlin
import dev.kdb.jdbc.openMemoryRuntime
import dev.kdb.query.hybrid.HybridQueryRequest
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val runtime = openMemoryRuntime(catalog = "demo", namespaceId = "demo/users")
    val result = runtime.hybrid.execute(
        "SELECT _doc FROM users",
        HybridQueryRequest(namespaceId = "demo/users", schema = runtime.schema),
    )
    result.result.rows.forEach { println(it) }
}
```

**Durable JVM embedding** (`dev.kdb.jdbc.file.openFileRuntime`):

```kotlin
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.schema.KdbSchema

val runtime = openFileRuntime(
    dataRoot = "/var/lib/myapp",
    catalog = "myapp",
    namespaceId = "myapp/users",
    schema = myUsersSchema, // required for indexed WHERE after reopen
)
```

---

## Embedding in a JavaScript project

KDB ships a **Kotlin/JS** embed layer (`:kdb-embed`) with an exported **`KdbBrowser`** API. Build the bundle with Gradle; there is no published npm package yet.

### Phase 1 — Standalone browser (memory mode)

The engine runs entirely in the tab: put JSON documents and run SQL without JDBC or a backend.

**Build the demo:**

```bash
./gradlew :kdb-browser-demo:jsBrowserDevelopmentWebpack
cd kdb-browser-demo/build/dist/js/developmentExecutable
python3 -m http.server 8080
```

Then open `http://localhost:8080/`.

**JavaScript API** (from the compiled `kdb-embed` artifact):

```javascript
const schema = JSON.stringify({
  fields: [
    { name: "userId", type: "string", required: true, indexed: true },
  ],
});
const db = await KdbBrowser.openWithSchema("demo/users", schema);
await db.put('{"userId":"u1","name":"Alice"}');
const result = await db.query("SELECT userId FROM users WHERE userId = 'u1'");
console.log(JSON.parse(result)); // { columns: [...], rows: [...] }
await db.close();
```

**Schema JSON shape** (maps to `KdbSchema`):

| Field | Type | Meaning |
|-------|------|---------|
| `fields[].name` | string | Field name |
| `fields[].type` | string | `string`, `int32`, `int64`, `float64`, `bool`, `timestamp`, `uuid`, `object`, `array` |
| `fields[].required` | boolean | optional |
| `fields[].indexed` | boolean | optional; required for indexed `WHERE` |
| `fields[].unique` | boolean | optional |

**Concurrency:** The network SQL server uses pessimistic **document write locks** per session (exclusive per document until commit/rollback) plus optimistic conflict detection at commit. Multi-statement SQL transactions use the same model; see [SQL transactions](#sql-transactions). `JsonPath` and virtual-view registries are synchronized for multi-threaded JDBC. Details: [component 25](kdb-spec-layer8-component25-multi-client-sessions.md).

**SQL limits (v1):** hybrid `SELECT` and single-table DML (`INSERT`/`UPDATE`/`DELETE`) work in embedded runtimes. `SELECT _doc` works without a schema; indexed predicates need a non-empty schema. `BETWEEN`, `IS NULL`, `ORDER BY`, `LIMIT`/`OFFSET`, and prepared `?` parameters are supported. `ORDER BY similarity(col, 'text')` requires a text-embedding path (not yet available). No `JOIN`s or aggregates.

**Kotlin embed** (same behavior as the CLI put/query path):

```kotlin
val runtime = openMemoryRuntime("demo", "demo/users", schema)
putJson(runtime, "demo/users", """{"userId":"u1"}""", schema)
val rows = querySql(runtime, "demo/users", "SELECT userId FROM users WHERE userId = 'u1'", schema)
```

### Phase 2 — Backend service + remote sync

Run a JVM **peer-sync host** over WebSocket; the browser keeps a **local in-memory copy**, syncs commits, then queries locally (same model as CLI `sync` over TCP).

**Start the service** (Gradle keeps running until you press **Ctrl+C**; you should not get an immediate `BUILD SUCCESSFUL` prompt while the server is up):

```bash
./gradlew :kdb-service:runService --args="--memory --namespace demo/users --listen-ws kdb-ws://127.0.0.1:7443/kdb?bind=true"
```

Use `--data-dir /path/to/data` instead of `--memory` for a persistent file-backed runtime.

**Serve the demo UI** (separate terminal):

```bash
./gradlew :kdb-browser-demo:jsBrowserDevelopmentWebpack
cd kdb-browser-demo/build/dist/js/developmentExecutable
python3 -m http.server 8080
```

Open `http://localhost:8080/` for memory mode, or  
`http://localhost:8080/?mode=remote&ws=kdb-ws://127.0.0.1:7443/kdb` with the service running above.

**Remote browser:**

```javascript
const db = await KdbBrowser.openRemote(
  "demo/users",
  "kdb-ws://127.0.0.1:7443/kdb",
  schemaJson,
);
await db.sync(); // pullMissing over WebSocket
const result = await db.query("SELECT userId FROM users WHERE userId = 'u1'");
```

**Demo with remote mode:** open the demo page with  
`index.html?mode=remote&ws=kdb-ws://127.0.0.1:7443/kdb` after seeding data on the server (or put locally after sync).

**v1 caveats:**

- **Remote writes:** `put()` commits locally and **pushes** to the server over peer sync.
- **Stream subscribe:** `await db.subscribe((eventJson) => { ... })` receives `DeltaReceived` when the server commits (peer `/kdb` → stream `/kdb/stream`). On disconnect or error, the client runs **`sync()` once** via peer sync (`SyncFallback` / `SyncRecovered` events), then **reconnects the stream** with backoff. Call `sync()` manually anytime for recovery; `unsubscribe()` stops the stream loop without closing the DB.
- **Dev networking:** use `kdb-ws://localhost` in development (TLS off). For encrypted wire traffic, switch listen URIs to `kdb-wss://` and configure a `tls` block in `config.json` (PKCS12 keystore/truststore paths; passwords via `KDB_TLS_KEYSTORE_PASSWORD` / `KDB_TLS_TRUSTSTORE_PASSWORD`). JDBC remote: `ssl=true` plus `sslTrustStore` / `sslKeyStore` properties. mTLS is JVM-only (not browser WebSocket). Browser embeds use `kdb-wss://` / `wss://` and rely on the platform TLS stack.

Wire transport: [component 25 WebSocket spec](kdb-spec-layer9-component25-transport-websocket.md). Integration tests: `WebSocketPeerSyncIntegrationTest` and `layer9_tcpPeerSync` in `:kdb-integration`.

### Advanced — custom Kotlin/JS app

Add `:kdb-embed` (or lower-level modules) to your own KMP `js(IR) { browser() }` target and webpack task, same as the demo. Optional: `:kdb-compute-webgpu` for vector acceleration with CPU fallback.

### Stream subscribe over WebSocket

With `kdb-service` running peer sync, stream mode listens on the same host with path `/kdb/stream` (e.g. `kdb-ws://127.0.0.1:7443/kdb/stream`). After `openRemote`, call `subscribe` for live server commits; `put()` still pushes via peer sync. If the stream drops, subscribe recovery runs `sync()` automatically and retries the stream connection. See [stream mode spec](kdb-spec-layer7-component22-stream-mode.md).

```javascript
await db.subscribe((e) => {
  const ev = JSON.parse(e);
  if (ev.type === "SyncRecovered") console.log("caught up via peer sync", ev);
});
```

---

## Data layout (for inspect CLI)

File-backed storage on the JVM typically uses:

```
data-dir/
  ns/
    <namespace-id>/
      delta/
    blobs/
      <aa>/
        <full-hash>
```

Use `dump-delta` and `dump-blob` against this tree for debugging.

---

## Performance benchmarks

JMH microbenchmarks live in the `:kdb-benchmark` module. They measure CLI and JDBC workloads on shared seeded datasets (file-backed and in-memory). Results are **informational only** — they are not part of `./gradlew build` or `check`, and CI does not fail on latency.

```bash
./gradlew :kdb-benchmark:jmh
```

Reports are written under `kdb-benchmark/build/reports/jmh/` (HTML) and `kdb-benchmark/build/results/jmh/` (text). To run a subset:

```bash
./gradlew :kdb-benchmark:jmh -Pjmh.include='.*CliOpen.*'
```

Scheduled or manual CI runs upload those directories as artifacts via the **Benchmark** workflow (`.github/workflows/benchmark.yml`).

| Benchmark family | What it measures |
|------------------|------------------|
| `CliOpen*` | `openCliRuntime` cold vs warm on file data |
| `CliWrite*` | `cliPut_batch` (one session) vs `cliPut_oneShot` (`KdbCli.run` per put) |
| `CliQuery*` | Point SELECT, full scan, get by id |
| `JdbcConnect*` | JDBC connect memory / file cold / file warm |
| `JdbcQuery*` | SELECT loops, prepared statements, direct `hybrid.execute` vs JDBC |

One-shot CLI commands reopen the file runtime on every invocation; compare `cliPut_batch` and `cliPut_oneShot` to see that cost.

---

## Getting help

| Topic | Document |
|-------|----------|
| Architecture, roadmap, JDBC design | [kdb-spec.md](kdb-spec.md) |
| JDBC driver spec | [kdb-spec-layer8-component24-jdbc-driver.md](kdb-spec-layer8-component24-jdbc-driver.md) |
| Product CLI spec | [kdb-spec-layer10-component29-cli.md](kdb-spec-layer10-component29-cli.md) |
| Inspect tooling spec | [kdb-spec-layer10-component31-inspect-tooling.md](kdb-spec-layer10-component31-inspect-tooling.md) |
| Stream / browser modes | [kdb-spec-layer7-component22-stream-mode.md](kdb-spec-layer7-component22-stream-mode.md) |

---

## Quick reference

```bash
./gradlew build
./gradlew :kdb-cli:runCli --args="init myapp/users"
./gradlew :kdb-jdbc:test
./gradlew :kdb-embed:jvmTest :kdb-embed:jsNodeTest
./gradlew :kdb-browser-demo:jsBrowserDevelopmentWebpack
./gradlew :kdb-service:runService --args="--memory --listen-ws kdb-ws://127.0.0.1:7443/kdb?bind=true"
./gradlew :kdb-benchmark:jmh
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
```

```java
Class.forName("dev.kdb.jdbc.KdbDriver");
Connection c = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
// Seed data via conn.embedded — see JDBC section above
```
