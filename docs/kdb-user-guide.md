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
| JDBC DML (`INSERT` / `UPDATE` / `DELETE`) | Not implemented in v1 (`HybridDmlNotSupportedException`) |
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

Example:

```bash
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb init myapp/users"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb put myapp/users '{\"userId\":\"u1\"}'"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb query myapp/users 'SELECT _doc FROM users'"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file put myapp/files --id 00000000-0000-0000-0000-0000000000f1 --zip ./report.pdf"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file get myapp/files --id 00000000-0000-0000-0000-0000000000f1 -o ./report-copy.pdf"
```

**Persistence:** Namespace data lives under `{dataDir}/ns/{namespaceId}/` (delta log, WAL, SSTables). Each CLI invocation replays the delta log on open; commits from a prior `put` are visible to a later `get` or `query` with the same `--data-dir`. Assume a **single writer** per data directory (no cross-process file locking in v1).

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

Errors on a line are printed to stderr and the shell continues. Gradle still starts one JVM per `runCli` invocation; within a session, only the first line pays full open/replay cost. Do not run two shells (or a shell plus another CLI process) against the same `--data-dir` concurrently.

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
| `jdbc:kdb:memory:///demo/users` | `demo/users` | **Yes** |
| `jdbc:kdb:memory:///myapp` | `myapp/main` (no slash in path) | **Yes** |
| `jdbc:kdb:memory:///demo/users` + `readOnly=true` property | same | **Yes** (SELECT only) |
| `jdbc:kdb:file:///path/to/data/demo/users` | `demo/users`; data under `/path/to/data/ns/demo/users/` | **Yes** — survives process restart |
| `jdbc:kdb:file:///path/to/data/myapp` | `myapp/main` (path ends with catalog only) | **Yes** |
| `jdbc:kdb://host:port/catalog` | parsed | **No** — `SQLFeatureNotSupportedException` |

**Mapping** (see [spec §5](kdb-spec.md#5-jdbc-driver-highest-priority)):

- **Catalog** → instance root (e.g. `demo`)
- **Table** in SQL → namespace `catalog/table` (e.g. `FROM users` → `demo/users`)
- Rows include **`kdb_id`** and **`_doc`** (full document JSON) plus schema columns when a schema is registered

### What works

| Area | Details |
|------|---------|
| **Driver** | `dev.kdb.jdbc.KdbDriver`; registers with `DriverManager`; `acceptsURL("jdbc:kdb:…")` |
| **Connection** | `getCatalog()`, `setReadOnly`, `close`, `isValid`, `getMetaData`, `setAutoCommit` / `commit` (no-op in v1 memory mode) |
| **Statement** | `executeQuery` for `SELECT`; `FROM table` auto-qualified to `catalog/table` |
| **PreparedStatement** | `setString`, `setInt`/`setLong`/`setFloat`/`setDouble`, `setBoolean`, `setNull`, `setObject`; `executeQuery` |
| **ResultSet** | Forward-only; `next`, `getString`/`getLong`/`getInt`/`getBoolean`/`getDouble`/`getObject` by index or column label; `findColumn`, `getMetaData` |
| **SQL** | `SELECT` with `WHERE` on schema/indexed fields; `SELECT _doc …`; `AT VERSION` / `AT COMMIT` / `AT TIME` (parsed by hybrid engine when history exists) |
| **DatabaseMetaData** | Product name `KDB`; `getTables`, `getColumns` (`kdb_id`, `_doc`), `getCatalogs`, `getSchemas`; keywords `AT`, `VERSION`, `COMMIT`, `TIME`; functions `kdb_json_get`, `kdb_json_set` |

### What does not work (throws)

| Area | Behaviour |
|------|-----------|
| **Network URLs** | `SQLFeatureNotSupportedException` |
| **DML** | `UPDATE` / `INSERT` / `DELETE` → `HybridDmlNotSupportedException` |
| **Read-only connection** | `executeUpdate` → `SQLException` |
| **Advanced JDBC** | `CallableStatement`, `Savepoint`, `Blob`/`Clob`, batch, generated keys → `SQLFeatureNotSupportedException` |
| **Compliance** | `jdbcCompliant()` returns `false` |

### Seeding data (required before SELECT returns rows)

**Memory mode (`jdbc:kdb:memory://…`):** Each connection is an **isolated** empty engine. Seed documents in the same process (or use the CLI with `--data-dir` and file mode instead).

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

**Memory mode:** Each connection is an **isolated** in-memory database; two connections do not share data.

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

KDB’s browser runtime is the **same Kotlin engine** compiled to **Kotlin/JS**. There is no standalone pure-JavaScript npm package yet.

### Option 1 — Full engine in the browser (Kotlin/JS)

Add KDB modules to a Kotlin Multiplatform project with a `js` browser target (`kdb-hybrid-query`, `kdb-storage`, `kdb-stream`, `kdb-transport-ws`, …). Bootstrap with `inMemoryCommitDag`, `InMemoryStorageAdapter`, `sqlEngine`, and `hybridQueryEngine` (same pattern as `openMemoryRuntime` on the JVM).

```bash
./gradlew :your-app:jsBrowserDevelopmentWebpack
```

Optional: `:kdb-compute-webgpu` for vector / bulk-read acceleration with CPU fallback.

### Option 2 — Stream client over WebSocket

Connect to a JVM coordinator using **read-only** or **write-back** stream mode:

```kotlin
val transport = JsWebSocketWireTransport()
val subscriber = streamSubscriber(defaultWireCodec(), transport, indexManager)
subscriber.connect(
    StreamSubscriberConfig(
        namespaceId = "myapp/events",
        nodeId = "browser-1",
        mode = StreamClientMode.READ_ONLY,
        coordinatorUri = "wss://api.example.com/kdb/stream",
        resumeFrom = lastKnownHead,
    ),
)
```

See [wire protocol spec](kdb-spec-layer7-component21-wire-protocol-framing.md) and integration tests (`layer9_tcpPeerSync` in `:kdb-integration`).

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
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
```

```java
Class.forName("dev.kdb.jdbc.KdbDriver");
Connection c = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
// Seed data via conn.embedded — see JDBC section above
```
