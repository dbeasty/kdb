# KDB User Guide

This guide is for developers who want to **run**, **inspect**, or **embed** KDB in an application. It describes what works in this repository today and what is still specified but not yet shipped.

For architecture and protocol details, see the [architecture specification](kdb-spec-v0_9.md).

---

## Status

KDB is under active development. The following are available now:

| Capability | Status |
|------------|--------|
| Core engine modules (codec, storage, SQL, indexes, wire, peer sync, …) | Implemented; covered by unit tests |
| **Inspect CLI** (`dump-delta`, `dump-wire`, …) | JVM command-line tooling via Gradle |
| **JDBC driver** — `jdbc:kdb:memory://…` | Embedded in-memory mode |
| **JDBC** — `jdbc:kdb:file://…`, network URLs | Parsed but not implemented (throws `SQLFeatureNotSupportedException`) |
| **Full `kdb` CLI** (init, put, commit, sync, …) | Specified in [§11 of the master spec](kdb-spec-v0_9.md#11-cli-interface); not yet implemented |
| Published Maven / npm artifacts | Not yet published; use a Gradle composite build or project dependency from source |

---

## Prerequisites

- **JDK 17+** (for JVM targets and the inspect CLI)
- **Gradle 8.x** — use the included wrapper: `./gradlew`
- For **JavaScript / browser** embedding: a Kotlin Multiplatform project with the `js(IR) { browser() }` target (Kotlin/JS IR)

Clone the repository and build from the root:

```bash
./gradlew build
```

Run tests for a specific area:

```bash
./gradlew :kdb-jdbc:test
./gradlew :kdb-inspect:jvmTest
```

---

## Command-line usage

### Inspect tooling (available today)

The shipped CLI is **debug / inspect** tooling. It prints **non-authoritative** JSON views of on-disk or captured binary data. It does not modify data and is not used for sync or hashing.

Run it via the Gradle application task:

```bash
./gradlew :kdb-inspect:inspectCli --args="<subcommand> <options>"
```

#### `dump-delta`

Decode delta segment files under a namespace data directory.

```bash
./gradlew :kdb-inspect:inspectCli --args="dump-delta --data-dir /path/to/data --namespace myapp/users"
```

| Option | Description |
|--------|-------------|
| `--data-dir DIR` | Root data directory (required) |
| `--namespace NS` | Namespace id, e.g. `myapp/users` (required) |
| `--segment SEG` | Single segment file name; omit to scan all segments in `ns/<NS>/delta/` |
| `--codec zstd\|none` | Compression codec (default: `zstd`) |
| `--compact` | Single-line JSON per record |

#### `dump-wire`

Decode a captured wire frame file.

```bash
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
```

Add `--compact` for single-line output.

#### `dump-commit`

Decode a raw commit payload blob.

```bash
./gradlew :kdb-inspect:inspectCli --args="dump-commit --file /path/to/payload.bin"
```

#### `dump-blob`

Decode a content-addressed blob by hash.

```bash
./gradlew :kdb-inspect:inspectCli --args="dump-blob --data-dir /path/to/data --hash <64-char-hex>"
```

Blobs are read from `ns/blobs/<first-two-hex>/<full-hex>` under `--data-dir`.

### Planned `kdb` CLI (not yet available)

The product CLI will be modelled on **git** for namespaces, commits, branches, and peer sync. Planned commands include `kdb init`, `kdb put`, `kdb query`, `kdb log`, `kdb push`, and others — see [§11 CLI Interface](kdb-spec-v0_9.md#11-cli-interface) in the master spec. When that lands, it will be distributed as a JVM binary (fat JAR or native image) and will call the same public engine APIs as embedders.

---

## Embedding in a Java project

The primary integration surface for Java (and Kotlin on the JVM) is the **JDBC driver** in the `:kdb-jdbc` module. It maps standard `java.sql.*` types to the KDB hybrid query engine.

### Add the dependency (from source)

Artifacts are not published to Maven Central yet. Include KDB in your build in one of these ways:

**Option A — composite build** (recommended for local development):

```kotlin
// settings.gradle.kts
includeBuild("/path/to/kdb")
```

```kotlin
// build.gradle.kts
dependencies {
    implementation("dev.kdb:kdb-jdbc") // when publishing exists
    // Today, depend on the included build’s project:
    // implementation(project(":kdb-jdbc"))  // if kdb is a subproject
}
```

**Option B — Gradle subproject** — point `projectDir` at `kdb-jdbc` and depend on `implementation(project(":kdb-jdbc"))`, transitively pulling engine modules.

Ensure `KdbDriver` is loaded once (registration happens in the driver’s static initializer):

```java
Class.forName("dev.kdb.jdbc.KdbDriver");
```

### Connection URLs

| URL | Meaning | v1 support |
|-----|---------|------------|
| `jdbc:kdb:memory:///catalog` | In-memory database; default namespace `catalog/main` | **Yes** |
| `jdbc:kdb:memory:///catalog/table` | In-memory; namespace `catalog/table` | **Yes** |
| `jdbc:kdb:memory:///demo/users?read_only=true` | Read-only connection | **Yes** |
| `jdbc:kdb:file:///path/to/data/catalog` | File-backed embedded | Not yet |
| `jdbc:kdb://host:port/catalog` | Network peer | Not yet |

**JDBC mapping** (see [master spec §5](kdb-spec-v0_9.md#5-jdbc-driver-highest-priority)):

- **Catalog** → KDB instance root (e.g. `myapp`)
- **Schema / table** → namespace (table `users` → namespace `myapp/users`)
- Every queryable row exposes **`kdb_id`** and **`_doc`** (full document JSON) in addition to schema fields

### Query example (Java)

```java
import java.sql.*;

public class KdbExample {
    static {
        try {
            Class.forName("dev.kdb.jdbc.KdbDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users")) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")) {
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }
    }
}
```

Populate data before querying by using the embedded runtime from Kotlin tests, or by wiring writes through the engine API (see below). The JDBC driver v1 focuses on **read path / SQL SELECT** against an in-memory engine instance per connection.

### Read-only connections

```java
Properties props = new Properties();
props.setProperty("readOnly", "true");
Connection conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users", props);
// executeUpdate / DML throws SQLException
```

### Kotlin / engine-level embedding

For full control (DAG, storage, indexes, transactions) without JDBC, use `openMemoryRuntime` from the same module:

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
    result.result.rows.forEach { row -> println(row) }
}
```

Seed documents and commits via `EmbeddedKdbRuntime` (`dag`, `storage`, `indexManager`) as in `KdbJdbcTest` in the repository.

### ORMs and BI tools

Point the tool at `jdbc:kdb:memory:///…` once file and network modes exist. Today, use in-memory URLs for integration tests. Hibernate / jOOQ / plain JDBC should use the catalog and table names that map to KDB namespaces.

### JVM limitations (v1)

- One isolated in-memory engine per JDBC connection
- `jdbc:kdb:file://` and `jdbc:kdb://` URLs are rejected
- Not all JDBC APIs are implemented (`SQLFeatureNotSupportedException` for unsupported calls)
- DML via JDBC is limited; complex writes use the transaction / storage APIs directly

---

## Embedding in a JavaScript project

KDB’s browser runtime is the **same Kotlin engine** compiled to **Kotlin/JS** (IR). There is no standalone pure-JavaScript npm package yet. You integrate in one of two ways.

### Option 1 — Full engine in the browser (Kotlin/JS)

Add KDB modules to a **Kotlin Multiplatform** project with a `js` browser target, depending on the same artifacts the JVM build uses (`kdb-hybrid-query`, `kdb-storage`, `kdb-stream`, `kdb-transport-ws`, etc.).

Minimal pattern (mirrors `openMemoryRuntime` on the JVM):

1. `inMemoryCommitDag(namespaceId)`
2. `InMemoryStorageAdapter()`
3. `productionIndexManager` / `sqlEngine` / `hybridQueryEngine`
4. Run queries with `hybrid.execute(sql, HybridQueryRequest(...))` inside coroutines

Build the JS bundle:

```bash
./gradlew :your-app:jsBrowserDevelopmentWebpack
```

Serve the generated JS from your web app’s static assets. Storage in the browser uses in-memory and enlistment snapshots (see master spec §2 — `jsMain` I/O shim).

**WebGPU** (optional): the `:kdb-compute-webgpu` module provides vector / bulk-read acceleration in supporting browsers, with CPU fallback.

### Option 2 — Stream client over WebSocket (JavaScript or Kotlin/JS)

If a **coordinator** runs on the JVM (full peer or stream server), browser clients can use **Mode 1 (read-only stream)** or **Mode 2 (write-back stream)** without hosting the full DAG locally:

1. Coordinator: `streamCoordinator(...)` + transport (TCP or WebSocket on JVM)
2. Client: `streamSubscriber(...)` with `JsWebSocketWireTransport` and a `wss://` URI

```kotlin
// jsMain — connect to a remote coordinator
val transport = JsWebSocketWireTransport()
val subscriber = streamSubscriber(defaultWireCodec(), transport, indexManager)
val connection = subscriber.connect(
    StreamSubscriberConfig(
        namespaceId = "myapp/events",
        nodeId = "browser-1",
        mode = StreamClientMode.READ_ONLY,
        coordinatorUri = "wss://api.example.com/kdb/stream",
        resumeFrom = lastKnownHead,
    ),
)
```

Wire framing and message types are defined in `:kdb-wire` and [Component 21](kdb-spec-layer7-component21-wire-protocol-framing.md).

Pure JavaScript clients can speak the same **binary WebSocket protocol** once a thin JS binding is published; until then, use Kotlin/JS or connect from a JVM backend.

### Node.js

Several modules also expose a `nodejs()` target for headless tests and tooling. Use the same Kotlin/JS dependencies with the Node target when you do not need a browser DOM (e.g. `JsWebSocketWireTransport` is browser-oriented; JVM TCP transport is used for server-side tests).

---

## Data layout (for inspect CLI)

When using file-backed storage on the JVM, a typical layout under `--data-dir` is:

```
data-dir/
  ns/
    <namespace-id>/
      delta/          # append-only delta segments
    blobs/
      <aa>/           # first two hex digits of content hash
        <full-hash>   # blob bytes
```

Use `kdb inspect dump-delta` and `dump-blob` against this tree for debugging.

---

## Getting help

| Topic | Document |
|-------|----------|
| Architecture, CLI roadmap, JDBC URL spec | [kdb-spec-v0_9.md](kdb-spec-v0_9.md) |
| Inspect tooling spec | [kdb-spec-layer10-component31-inspect-tooling.md](kdb-spec-layer10-component31-inspect-tooling.md) |
| JDBC driver spec | [kdb-spec-layer8-component24-jdbc-driver.md](kdb-spec-layer8-component24-jdbc-driver.md) |
| Stream / browser modes | [kdb-spec-layer7-component22-stream-mode.md](kdb-spec-layer7-component22-stream-mode.md) |

---

## Quick reference

```bash
# Build everything
./gradlew build

# Inspect delta segments
./gradlew :kdb-inspect:inspectCli --args="dump-delta --data-dir ./data --namespace myapp/users"

# JDBC in-memory (from Java, after depending on :kdb-jdbc)
# jdbc:kdb:memory:///myapp/users
```
