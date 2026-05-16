# KDB — Portable Embedded Database Engine

## Architecture Specification v0.8

-----

## 0. Session State — Read This First

### Current Status

```
Layer 0 — Foundation         [COMPLETE]
  [x] 1. BSON Codec          — interface in Section 17
  [x] 2. Error Model         — interface in Section 17

Layer 1 — Core Types         [IN PROGRESS — specs generated, awaiting implementation]
  [~] 3. Document + Commit Model   — component spec generated (see file: kdb-spec-layer1-component3-document-commit-model.md)
  [~] 4. JSON Functions Engine     — component spec generated (see file: kdb-spec-layer1-component4-json-functions-engine.md)

Layer 2 — Schema + DAG       [IN PROGRESS — specs generated, awaiting implementation]
  [~] 5. Schema Engine             — component spec generated (see file: kdb-spec-layer2-component5-schema-engine.md)
  [~] 6. Commit DAG                — component spec generated (see file: kdb-spec-layer2-component6-commit-dag.md)

All other layers             [NOT STARTED]
```

### What Has Been Done

- Layer 0 component specs generated (BSON Codec, Error Model)
- Both Layer 0 components implemented and tested (per plan)
- Public interfaces extracted and recorded in Section 17 → Layer 0
- Layer 1 component specs generated (Document + Commit Model, JSON Functions Engine)
- Layer 1 draft interfaces recorded in Section 17 → Layer 1 (replace with final after implementation)
- Layer 2 component specs generated (Schema Engine, Commit DAG)
- Layer 2 draft interfaces recorded in Section 17 → Layer 2 (replace with final after implementation)
- Component spec files saved as downloadable `.md` files (see file-output convention in Section 16.4)
- Master spec updated from v0.6 → v0.7
- Storage engine design decisions applied (v0.7 → v0.8): removed external storage dependencies, introduced two-store architecture (Delta Store + Realized Store), browser multi-enlistment model, delta authorship envelope, Storage Manager layer, split Layer 4 into 4a (Storage Engine) and 4b (Storage Manager), renumbered Layers 5–9

### What To Do Next — Layer 1 + Layer 2 Implementation

Layer 1 and Layer 2 component specs are complete. Implement in dependency order: Layer 1 first, then Layer 2.

**Step 1 — Implementation session (new conversation, per component):**

Paste this document + the component spec file and say:

```
You are implementing KDB, a portable embedded database engine in Kotlin Multiplatform.
This document is the master architecture spec. The attached component spec is your implementation contract.
Implement [Component Name] in Kotlin Multiplatform (commonMain).
All dependencies are in Section 17 — treat those interfaces as already existing.
Produce production-quality Kotlin. No placeholders.
```

Implement in this order:

1. Component 3 (Document + Commit Model)
1. Component 4 (JSON Functions Engine) — independent of Component 3, may be done in parallel
1. Component 5 (Schema Engine) — depends on Layer 1 interfaces
1. Component 6 (Commit DAG) — depends on Layer 1 interfaces; independent of Component 5, may be done in parallel

**Step 2 — After each component is implemented:**

1. Replace the draft interface in Section 17 for that component with the actual public interface extracted from the implementation
1. Mark `[x]` in the checklist above
1. Save the updated spec (increment version after each component or after each layer)
1. Once all of Layer 1 AND Layer 2 components are `[x]`, generate Layer 3 specs using the spec session prompt in Section 16

**Step 3 — Generating Layer 3 specs (new conversation, after Layer 1 + 2 are complete):**

Paste this document and say:

```
You are implementing KDB, a portable embedded database engine in Kotlin Multiplatform.
This document is the master architecture spec and implementation plan.
Please generate implementation-ready component specs for Layer 3: Transaction Engine, Index Layer Core, Storage Adapter Interface.
Interfaces for completed layers are in Section 17 — treat them as fixed contracts.
Each component spec must follow the standard structure defined in Section 16.2.
Save each component spec as a separate .md file for download.
```

### Dependency Rules

- Layer 1 depends on Layer 0 only — interfaces are in Section 17
- Layer 2 depends on Layer 0 and Layer 1 — interfaces are in Section 17
- Component 3 and Component 4 within Layer 1 are independent of each other and may be implemented in parallel
- Component 5 and Component 6 within Layer 2 are independent of each other and may be implemented in parallel
- Do not start Layer 3 until all of Layer 1 and Layer 2 components are complete and their final interfaces are in Section 17
- Never mix spec generation and implementation in the same session
- Always save component spec output as `.md` files for download (see Section 16.4)

-----

## 1. Overview

KDB is a portable, multi-runtime embedded database engine written in Kotlin Multiplatform. The **entire engine** — not a client library, not a thin SDK — compiles and runs on browser clients (via Kotlin/JS), JVM backends, and native targets (via Kotlin/Native). The same Kotlin codebase produces all three runtimes. Only storage adapters and transport adapters differ per platform; all engine logic is shared.

KDB is best understood as **source control for structured documents**. You store whole JSON documents. You retrieve whole JSON documents. Optionally you declare a schema — a typed, indexed lens over part of each document — which unlocks SQL querying, JDBC connectivity, and ORM integration. The document is always the truth. The schema is always a lens. Both coexist without friction.

Primary storage is JSON. Binary storage uses BSON (Apache 2.0 licensed open spec) compressed with zstd. SQL operates as an index and query layer over schema-declared fields, but raw JSON access is always available alongside SQL in the same query. All data lives in versioned, content-addressed namespaces with git-like history. Peer synchronisation follows a source-control model: peers are fully independent, can diverge arbitrarily, and merge when they choose to.

### 1.1 Goals

- Full engine runs on every target: browser (JS), JVM backend, native binary
- Single Kotlin codebase compiled to all targets via Kotlin Multiplatform
- Documents are stored and retrieved as whole JSON — always, exactly as provided
- Schema is an optional typed lens over document fields, not a constraint on document shape
- SQL and raw JSON access work together in the same query via the `_doc` column
- JDBC driver for full compatibility with Java ORMs, SQL IDEs, and BI tools — highest priority
- Git-like versioning per namespace: full commit DAG, branches, tags, checkout, diff
- Source-control peer protocol: peers diverge independently and merge on reconnect
- Three client modes: pure stream, write-back stream, full peer
- Transaction-based conflict resolution with application-controlled replay
- Tiered storage: hot → warm → cold → ice archive
- CLI interface modelled on git for developer tooling
- GPU-accelerated bulk read path and vector similarity as an optional index type

### 1.2 Non-Goals

- KDB is not a general-purpose SQL database; SQL is an index and query interface, not the storage model
- KDB does not enforce a central authoritative node; all peers are equal
- KDB does not replace Kafka as a high-throughput event bus
- KDB does not manage network topology; peer discovery is the application’s responsibility

### 1.3 Design Principles

- The document is always the truth; schema is always a lens
- The engine is the same on every platform; only adapters differ
- JSON is always the canonical representation of meaning
- BSON+zstd is always a storage and transport optimisation, never a requirement
- SQL addresses data via schema; `_doc` always gives access to the whole document
- Peers are equal; any peer can sync with any other peer directly
- Divergence is normal; merging is explicit and application-controlled
- Conflicts surface to the application; KDB never silently resolves them
- History is cheap because unchanged content is shared by hash

-----

## 2. Kotlin Multiplatform Runtime Model

The entire engine lives in `commonMain`. Every platform — browser, JVM, native — runs identical database logic. Only platform I/O shims and transport adapters differ. **All storage logic is implemented in shared Kotlin — no external storage library dependencies.**

```
commonMain  ←  the entire engine lives here
  │
  ├── Document model, BSON codec, commit DAG
  ├── Transaction engine, conflict resolution
  ├── Schema engine, validation, migration
  ├── Hybrid query engine (SQL + JSON functions)
  ├── SQL DSL, query planner, index layer
  ├── JDBC driver interface (implemented in jvmMain)
  ├── Peer sync protocol (source-control model)
  ├── Stream protocol (read-only and write-back clients)
  ├── Compaction engine, storage tier manager
  ├── KDB Storage Engine (WAL, MemTable, SSTable, Delta Store, Realized Store)
  ├── Storage Manager (global orchestrator, Enlistment Manager)
  └── Vector index interface, GPU dispatch interface

jsMain      ←  adapters only
  ├── Platform I/O shim: in-memory + localStorage/sessionStorage (zstd snapshot per enlistment)
  ├── Transport adapter: WebSocket
  └── Compute adapter: WebGPU (vector search, bulk scan)

jvmMain     ←  adapters only
  ├── Platform I/O shim: java.nio file-backed append segments
  ├── Transport adapter: raw TCP + WebSocket server
  ├── Compute adapter: CUDA / Vulkan compute
  └── JDBC driver implementation

nativeMain  ←  adapters only
  ├── Platform I/O shim: POSIX file I/O
  ├── Transport adapter: raw TCP
  └── Compute adapter: Vulkan compute
```

A browser running KDB/JS is a **first-class local repository participant**, not a thin HEAD mirror. It supports multiple simultaneous enlistments, each on its own independent branch. Each enlistment maintains its own delta store and realized store. The Storage Manager tracks all active enlistments and applies a global memory budget across them.

The distinction between a browser node and a backend node is only in their platform I/O shims and transport adapters, not in their database capabilities.

-----

## 3. The Hybrid Document Model

This is the central design of KDB. It is neither a document database with SQL bolted on, nor a SQL database with JSON support added. It is a document store where schema is an optional, additive lens.

### 3.1 Core Principle

```
You store whole JSON documents.
You get whole JSON documents back.

Schema declares which fields SQL can see and index.
Schema does not constrain what the document contains.

The _doc column always contains the full document.
Schema field columns are pre-extracted projections of _doc.
SQL and _doc are available together in every query.
```

### 3.2 The `_doc` Column

Every namespace table automatically exposes the following columns via JDBC and the SQL DSL:

```
kdb_id        →  document UUID (always present, primary key)
[schema fields] →  one column per declared schema field (typed, indexed)
_doc          →  the complete JSON document as a JSON string (always present)
```

Schema field columns are exactly equivalent to `kdb_json_get(_doc, '$.fieldName')` with an index on top. They are convenience projections, nothing more. `_doc` is always the source of truth.

### 3.3 Three Interaction Modes

#### Mode A — Pure Document (no schema required)

Store and retrieve whole JSON. Like a document database. No schema declaration needed.

```kotlin
// Kotlin API
db.namespace("myapp/notes").put("""
    {
        "title": "Q1 Planning",
        "date": "2024-01-15",
        "attendees": ["alice", "bob"],
        "body": "...",
        "tags": ["planning", "q1"],
        "clientField": { "source": "ios", "version": "2.1" }
    }
""")

val doc = db.namespace("myapp/notes").get(id)
// returns the whole JSON exactly as stored
```

```sql
-- via JDBC/SQL, get the whole document
SELECT _doc FROM notes WHERE kdb_id = 'abc123';
```

#### Mode B — Schema + SQL (structured querying)

Declare a schema, get typed SQL querying with indexes over declared fields. Documents beyond the schema are still stored whole.

```sql
-- query via schema fields (uses indexes)
SELECT title, date FROM notes
WHERE date > '2024-01-01'
ORDER BY date DESC;
```

#### Mode C — Hybrid (SQL + raw JSON together)

The most powerful mode. Schema fields and raw JSON access in the same query. No need to choose.

```sql
-- schema fields for filtering (uses index), _doc for full document return
SELECT title, date, _doc
FROM notes
WHERE date > '2024-01-01'
  AND status = 'published';

-- filter on schema field, extract specific JSON path, return whole doc
SELECT
    title,
    kdb_json_get(_doc, '$.clientField.source') AS source,
    _doc
FROM notes
WHERE status = 'active'
  AND kdb_json_get(_doc, '$.tags[0]') = 'planning';

-- schema field index for performance, JSON path for fields not in schema
SELECT title, _doc
FROM notes
WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
  AND kdb_json_get(_doc, '$.clientField.version') = '2.1';
```

### 3.4 JSON Functions

Available in SELECT, WHERE, UPDATE, and ORDER BY clauses. Work on `_doc` or any JSON-valued expression.

```sql
kdb_json_get(_doc, '$.path')              -- extract value at JSONPath
kdb_json_set(_doc, '$.path', value)       -- return new doc with value set at path
kdb_json_delete(_doc, '$.path')           -- return new doc with path removed
kdb_json_merge(_doc, '{"a":1,"b":2}')     -- return new doc merged with JSON object
kdb_json_contains(_doc, '$.tags', 'x')   -- true if array at path contains value
kdb_json_keys(_doc, '$.metadata')         -- array of keys at path
kdb_json_type(_doc, '$.score')            -- type name of value at path
kdb_json_array_length(_doc, '$.tags')     -- length of array at path
```

Path syntax is JSONPath ($.field, $.nested.field, $.array[0], $.array[*]).

### 3.5 Writing Documents

#### Write a schema field — validated, indexed, rest of document untouched

```sql
UPDATE users SET status = 'inactive' WHERE userId = 'abc123';
```

KDB reads the current document, patches the `status` field, validates the new value against the schema, updates the index, and writes the whole document back. All non-schema fields in the document are preserved exactly.

#### Patch a non-schema field — no validation, no index update

```sql
UPDATE users
SET _doc = kdb_json_set(_doc, '$.clientField.source', 'web')
WHERE kdb_id = 'abc123';
```

Patches the JSON directly. Schema fields are not affected. No schema validation (field is not schema-declared). No index update. Document written back whole.

#### Replace the whole document — schema fields validated

```sql
UPDATE users
SET _doc = '{"userId":"abc","email":"a@b.com","status":"active","extra":"whatever"}'
WHERE kdb_id = 'abc123';
```

KDB validates that schema-declared fields in the new document pass all constraints, then stores the whole document. Extra fields beyond schema are stored as-is.

#### Kotlin API — most natural for application code

```kotlin
val doc = db.namespace("myapp/users").get("abc123")
val updated = doc.merge("""{"status":"inactive","clientField":{"source":"web"}}""")
db.namespace("myapp/users").put(updated)
// KDB validates schema fields, stores everything, produces a versioned commit
```

### 3.6 Schema Fields and Extensions — Guarantee

Extension fields (any field not declared in the namespace schema) carry the following guarantees:

- **Preserved through all writes** — any write that touches only schema fields leaves extension fields exactly unchanged
- **Preserved through all sync** — extension fields propagate through peer sync identically to schema fields
- **Preserved through all versioning** — checkout, diff, and history include extension fields
- **Preserved through compaction** — extension fields survive squash and ice archival
- **Never validated** — KDB applies no type or constraint enforcement to extension fields
- **Never indexed** — extension fields do not appear in any index; SQL cannot use them for index-accelerated queries
- **Always accessible** — extension fields are always present in `_doc` and accessible via `kdb_json_get`

### 3.7 Virtual Views for Extension Fields

When specific extension fields are queried frequently, a virtual view promotes them to named columns without modifying the schema:

```sql
CREATE VIRTUAL VIEW users_extended AS
SELECT
    userId,
    email,
    status,
    kdb_json_get(_doc, '$.clientField.source')   AS source,
    kdb_json_get(_doc, '$.clientField.platform') AS platform,
    kdb_json_get(_doc, '$.tags')                 AS tags_json,
    _doc
FROM users;

-- now available as a normal table in any JDBC tool
SELECT userId, source, platform
FROM users_extended
WHERE status = 'active' AND source = 'ios';
```

Virtual views are stored in the namespace metadata and visible to JDBC clients as regular tables. This is the bridge between extension fields and BI tools like Metabase or Tableau that expect fixed columns.

When an extension field appears in a virtual view and is queried heavily, that is a signal it has graduated to being worth declaring as a proper schema field with an index.

-----

## 4. Schema Layer

### 4.1 Purpose

The schema declares the fields that SQL can index and validate. It does not constrain document shape. A document written to a namespace with a schema must contain all required schema fields with correct types — but may contain any number of additional fields beyond the schema, which are stored and versioned without restriction.

### 4.2 Schema Declaration

```kotlin
namespace("myapp/users") {
    schema {
        field("userId",    StringType,    required = true,  indexed = true,  unique = true)
        field("email",     StringType,    required = true,  indexed = true,  unique = true)
        field("createdAt", TimestampType, required = true,  indexed = true)
        field("status",    EnumType("active", "inactive", "suspended"),
                                          required = true,  indexed = true)
        field("score",     Int32Type,     required = false, indexed = true)
        field("profile",   ObjectType,    required = false, indexed = false)
    }
}
```

### 4.3 Field Types

```
StringType          UTF-8 string
Int32Type           32-bit signed integer
Int64Type           64-bit signed integer
Float64Type         64-bit IEEE 754 float
BoolType            boolean
TimestampType       microsecond-precision timestamp
UuidType            UUID
EnumType(...)       declared string values only
ObjectType          JSON object (stored, not indexed unless queried via kdb_json_get)
ArrayType           JSON array (stored, not indexed unless queried via kdb_json_contains)
```

### 4.4 Schema Validation on Write

On every write, schema field validation applies:

- Required fields must be present and non-null
- Types are enforced per the field type declaration
- Unique constraints are checked against the index
- Enum values must be declared members

A write failing validation returns `SchemaViolationException` with details per failing field. Extension fields are never validated.

### 4.5 Schema-Optional Namespaces

Namespaces can operate with no schema. All fields are extension fields. SQL field queries are unavailable; `_doc`, `kdb_id`, and JSON functions are still available.

```kotlin
namespace("myapp/scratch") {
    schema = NONE
}
```

```sql
-- still works without schema
SELECT _doc FROM scratch WHERE kdb_json_get(_doc, '$.type') = 'note';
SELECT kdb_id, _doc FROM scratch;
```

### 4.6 Schema Evolution

Schema changes are versioned commits. Backward-compatible changes apply immediately. Breaking changes require a migration transaction that propagates to peers via the sync protocol like any other commit.

```kotlin
namespace("myapp/users").migrateSchema {
    addField("displayName", StringType, required = false, default = "")
    widenEnum("status", add = "deleted")
    addIndex("score")
}
```

-----

## 5. JDBC Driver (Highest Priority)

JDBC connectivity makes KDB immediately usable with the entire existing Java and Kotlin ecosystem — ORMs, SQL IDEs, BI dashboards, migration tools — without any special integration.

### 5.1 Concept Mapping

```
JDBC concept       KDB concept
────────────────────────────────────────────────
Connection     →   node connection to a KDB instance
Catalog        →   KDB instance root (e.g. "myapp")
Schema         →   namespace prefix
Table          →   namespace (e.g. "myapp/users" → table "users")
Column         →   schema field + kdb_id + _doc
Row            →   document
ResultSet      →   query results
PreparedStatement  parameterised SQL query
Transaction    →   KDB transaction (produces a versioned commit)
DatabaseMetaData   namespace schema introspection
```

### 5.2 Connection URL

```
jdbc:kdb://host:port/catalog                    network connection
jdbc:kdb://host:port/catalog?mode=read_only     read-only connection
jdbc:kdb:file:///path/to/data/myapp             embedded, local filesystem
jdbc:kdb:memory:///myapp                        in-memory, for tests
```

Embedded mode (`jdbc:kdb:file://`) is particularly important — an application embeds KDB with no network dependency and accesses it via standard JDBC, while still getting versioning, schema, and sync.

### 5.3 JDBC Driver Architecture

```
KdbDriver           implements java.sql.Driver
KdbConnection       implements java.sql.Connection
KdbStatement        implements java.sql.Statement
KdbPreparedStatement implements java.sql.PreparedStatement
KdbResultSet        implements java.sql.ResultSet
KdbMetaData         implements java.sql.DatabaseMetaData
KdbParameterMeta    implements java.sql.ParameterMetaData
```

The JDBC driver is a thin adapter layer. It translates JDBC calls into KDB engine calls — it does not reimplement query execution. The same SQL engine runs in the browser, in the JVM, and via JDBC.

### 5.4 JDBC Transaction Mapping

Every JDBC commit produces a versioned KDB commit in the namespace DAG. Tools using JDBC — Hibernate, Flyway, jOOQ, Spring Data — automatically produce full version history without knowing anything about KDB’s internals.

```kotlin
connection.autoCommit = false
val stmt = connection.prepareStatement(
    "UPDATE users SET status = ? WHERE userId = ?"
)
stmt.setString(1, "inactive")
stmt.setString(2, "abc123")
stmt.executeUpdate()
connection.commit()
// → KDB transaction committed
// → new commit in namespace DAG
// → delta streamed to all subscribers
// → extension fields on the document untouched
```

### 5.5 ORM Integration

Standard Hibernate entity mapped to a KDB namespace. Extension fields on documents are preserved through every ORM write even though Hibernate never touches them.

```kotlin
@Entity
@Table(name = "users")
data class User(
    @Id @Column(name = "kdb_id")
    val id: String,

    @Column(name = "userId", unique = true)
    val userId: String,

    @Column(name = "email")
    val email: String,

    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    val status: UserStatus

    // extension fields not mapped here
    // KDB preserves them through every Hibernate write
)
```

### 5.6 SQL Extensions Beyond Standard JDBC

These KDB-specific SQL constructs work via the JDBC driver. External tools that don’t parse them can fall back to `_doc` for raw JSON access.

```sql
-- version-aware query
SELECT userId, email FROM users AT VERSION 'pre-migration';
SELECT userId, email FROM users AT COMMIT 'a3f9c2...';
SELECT userId, email FROM users AT TIME '2024-06-01T00:00:00Z';

-- full document access
SELECT _doc FROM users WHERE userId = 'abc123';

-- JSON path functions (see section 3.4)
SELECT kdb_json_get(_doc, '$.metadata.source') FROM users;

-- document ID
SELECT kdb_id, userId FROM users;
```

### 5.7 Compatible Tooling (Works Out of the Box)

```
SQL IDEs:       IntelliJ Database tool, DBeaver, DataGrip
ORMs:           Hibernate, Exposed (Kotlin), jOOQ, Spring Data JDBC
Migrations:     Flyway, Liquibase
BI tools:       Metabase, Tableau (via virtual views for extension fields)
Connection pools: HikariCP, c3p0
Testing:        H2-compatible via jdbc:kdb:memory://
```

-----

## 6. Version Model

### 6.1 Namespace as Repository

A namespace is the equivalent of a git repository. It maintains an independent commit DAG, branches, tags, and supports checkout to any historical state. The git analogy is intentional and complete.

```
git concept         KDB concept
────────────────────────────────────────────────────────
repository      →   namespace
commit          →   committed transaction set + document tree snapshot
branch          →   named pointer to a commit, diverges independently
merge           →   transaction replay + conflict resolution
cherry-pick     →   replay single transaction onto different branch
tag             →   named immutable checkpoint, survives compaction
clone           →   full namespace replication to new node
pull            →   fetch peer commits and apply
push            →   send local commits to peer
fetch           →   get peer DAG without applying
log             →   namespace commit history
diff            →   changes between two commits or branches
gc              →   compaction + garbage collection
bundle          →   ice archive (self-contained, portable snapshot)
```

### 6.2 Checkout

```kotlin
db.namespace("myapp/users").checkout(tag = "pre-migration")
db.namespace("myapp/users").checkout(hash = "a3f9c2...")
db.namespace("myapp/users").checkout(at = Instant.parse("2024-06-01T00:00:00Z"))
db.namespace("myapp/users").checkout(branch = "experiment")
```

A checked-out view is read-only. Full SQL queries, JSON functions, `_doc` access, and all index types operate correctly against the historical state.

### 6.3 Tagging

Tags are named immutable pointers to commits. All tags survive compaction and ice archival. Tag before any risky operation — migrations, bulk imports, major releases.

### 6.4 Squash and Compaction

Compaction collapses untagged intermediate commits into synthetic full-snapshot roots. Tagged commits, branch points, and commits referenced by known peers are never squashed. Compaction granularity graduates with age — older history gets progressively coarser.

Before compacting, the node broadcasts `CompactionIntent` to all known peers and waits for confirmation that they are at or above the compaction boundary.

### 6.5 Ice Archival

Tagged snapshots are materialised as self-contained BSON+zstd archive bundles and shipped to configured archive storage. A stub commit replaces the original in the DAG. Accessing an archived commit returns `IceStorageException`. Restore targets an isolated namespace to avoid disturbing live data.

-----

## 7. Transaction Model

### 7.1 Transaction Object

```
Transaction {
  id:            UUID
  baseVersion:   Hash         // commit hash this was built against
  operations:    List<Op>
  timestamp:     Instant
  authorNode:    NodeID
  resultVersion: Hash?        // null until committed
}

Op (sealed):
  Write          { docId: UUID, patch: JsonPatch }
  Delete         { docId: UUID }
  FileWrite      { path: String, blobHash: Hash }
  SchemaMigration { migration: SchemaMigration }
```

### 7.2 Conflict Policies

```
APPEND_ONLY   always succeeds; suited to event logs and audit trails
LAST_WRITE    incoming write wins; no conflict surfaced
STRICT        any conflict produces a ConflictReport; application decides
CUSTOM        application-provided resolver function called per conflict
```

### 7.3 Transaction Replay

When a peer’s transaction was built against an older base version, KDB attempts replay against the current HEAD. Each operation is evaluated per the declared conflict policy. On full success a new commit is produced. On any failure a `ConflictReport` is returned with affected operations, documents, and diffs. KDB never silently resolves conflicts in STRICT or CUSTOM mode.

-----

## 8. Peer Sync Protocol (Source-Control Model)

### 8.1 Three Client Modes

Not all nodes need full peer capabilities. The mode reflects what a node actually needs to do.

```
MODE 1 — PURE STREAM (read-only subscriber)
  Receives delta commits from a coordinator.
  Never writes. No local commit DAG.
  Tracks position by last commit hash received.
  Examples: analytics dashboards, news feeds, monitoring UIs.

MODE 2 — WRITE-BACK STREAM (occasional writer)
  Receives delta commits like Mode 1.
  Submits write transactions upstream to the coordinator.
  Coordinator attempts replay, returns success or ConflictReport.
  No independent local DAG — writes are always against coordinator HEAD.
  Examples: standard browser app users, mobile apps with simple write needs.

MODE 3 — FULL PEER (independent node)
  Maintains own complete commit DAG.
  Can diverge from any other peer indefinitely.
  Syncs directly with any other peer — no coordinator required.
  Can collaborate peer-to-peer before merging to main.
  Examples: offline-first mobile, collaborative editing, backend servers,
            browser nodes doing long-running offline work.
```

The same engine runs all three modes. Mode is declared at connection time and can transition (e.g. a Mode 2 client upgrades to Mode 3 when going offline-first).

### 8.2 Peers Are Equal

There is no central broker or authoritative node. Any peer can sync directly with any other peer. Two browser nodes, two mobile devices, a browser and a backend — all equal. Any can act as coordinator for Mode 1/2 clients attached to it.

### 8.3 Divergence Is Normal

```
Node A:  [v1]──►[v2a]──►[v3a]──►[v4a]
               ↑
Node B:  [v1]──►[v2b]──►[v3b]        ← diverged, operating independently

On reconnect:
  1. Exchange HEAD hashes → discover divergence from v1
  2. Exchange missing commits
  3. Replay B's transactions onto A's HEAD per conflict policy
  4. Merge commit produced if successful
  5. ConflictReport surfaced if not
```

### 8.4 Wire Frame Format

```
[int32]   frame length
[int16]   message type
[int16]   protocol version
[int32]   correlation id
[bytes]   payload (BSON or JSON per negotiated encoding)
```

### 8.5 Message Types

```
0x01  Handshake            capability + encoding negotiation, HEAD exchange
0x02  DeltaCommit          stream mode: one commit delta to subscriber
0x03  CommitFetch          sync mode: request commits since hash
0x04  CommitPush           sync mode: send commits to peer
0x05  DAGDiff              sync mode: exchange DAG structure, find divergence
0x06  TransactionReplay    sync mode: request replay of transaction
0x07  ConflictReport       sync mode: replay failed, report attached
0x08  CompactionNotice     warning: squashing below this hash
0x09  IceArchiveNotice     this hash now archived, stub in DAG
0x0A  SnapshotRequest      request full namespace snapshot
0x0B  SnapshotResponse     full snapshot delivery
0x0C  PositionAck          subscriber acknowledges receipt to this hash
0x0D  SchemaPush           schema change propagation
```

### 8.6 DeltaCommit Payload

```
BSON {
  namespace:    string
  commitHash:   BinData(hash)
  parentHash:   BinData(hash)
  timestamp:    Date
  operations:   [ Op, ... ]
  indexHints:   [ { index, key, action }, ... ]   // pre-computed for read-only clients
  schemaDelta:  SchemaDelta?                       // present if schema changed
}
```

Index hints allow Mode 1/2 clients to update their local indexes without recomputing from document patches — critical for browser-side query performance.

### 8.7 Transport

```
Browser nodes   →  WebSocket binary frames
Backend peers   →  raw TCP with KDB frame protocol
Mobile nodes    →  WebSocket binary frames
Offline nodes   →  local storage only; full sync on reconnect
```

-----

## 9. Index Layer

### 9.1 Principle

SQL indexes project over schema-declared fields only. Extension fields are not indexed — queries on them via `kdb_json_get` perform a full scan. Index state is versioned — historical checkouts produce consistent historical index state.

### 9.2 Index Types

**Hash index** — exact equality on a schema field. O(1). Hash map.

**B-tree index** — range queries, ordering, composite fields. LSM-tree backed by KDB Storage Engine (pure Kotlin, commonMain, all platforms).

**Full-text index** — tokenised keyword search, prefix matching, Levenshtein edit-distance fuzzy matching. No GPU or embeddings required.

**Vector index** — semantic approximate nearest neighbour search via HNSW graph. Embeddings generated at write time. Uses GPU path when available. Optional per namespace.

### 9.3 Query Examples

```sql
-- hash index (O(1))
SELECT _doc FROM users WHERE userId = 'abc123';

-- btree range + return full document
SELECT userId, email, _doc
FROM users
WHERE createdAt BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY createdAt DESC;

-- full-text with typo tolerance
SELECT _doc FROM users WHERE MATCH(email, 'alice@exampl');

-- vector semantic search
SELECT _doc FROM docs
ORDER BY similarity(embedding, 'fast embedded storage') LIMIT 10;

-- hybrid: schema index + JSON path filter + full doc return
SELECT userId, kdb_json_get(_doc, '$.clientField.source') AS source, _doc
FROM users
WHERE status = 'active'
  AND createdAt > '2024-01-01'
  AND kdb_json_get(_doc, '$.clientField.platform') = 'ios'
ORDER BY score DESC
LIMIT 50;
```

-----

## 10. Namespace Policies

```kotlin
namespace("myapp/users") {
    schema {
        field("userId",    StringType,    required = true,  indexed = true,  unique = true)
        field("email",     StringType,    required = true,  indexed = true,  unique = true)
        field("createdAt", TimestampType, required = true,  indexed = true)
        field("status",    EnumType("active", "inactive", "suspended"),
                                          required = true,  indexed = true)
        field("score",     Int32Type,     required = false, indexed = true)
    }
    mode = MUTABLE
    history = FULL
    conflict = STRICT

    compaction {
        keepTagged = true
        keepBranchPoints = true
        retainGranularity {
            olderThan(7.days)    then FULL_HISTORY
            olderThan(30.days)   then DAILY_SNAPSHOTS
            olderThan(365.days)  then TAGGED_ONLY
        }
    }

    tiers {
        hot  { maxAge = 7.days;    storage = LOCAL_DB }
        warm { maxAge = 90.days;   storage = LOCAL_FS;     format = BSON_ZSTD }
        cold { maxAge = 365.days;  storage = OBJECT_STORE; format = BSON_ZSTD }
        ice  { storage = ARCHIVE;  format = BSON_ZSTD;     restoreLatency = HOURS }
    }
}

namespace("myapp/events") {
    schema {
        field("eventType",  StringType,    required = true, indexed = true)
        field("occurredAt", TimestampType, required = true, indexed = true)
        // all event-specific payload lives in extension fields
        // retrieved via _doc or kdb_json_get
    }
    mode = APPEND_ONLY
    history = FULL
    conflict = ALWAYS_ACCEPT
    compaction { squashAfter = NEVER }
}

namespace("myapp/scratch") {
    schema = NONE     // pure document store, no SQL field queries
    mode = MUTABLE
    history = FULL
    conflict = LAST_WRITE
}

namespace("myapp/cache") {
    schema = NONE
    mode = MUTABLE
    history = NONE    // no versioning overhead
    conflict = LAST_WRITE
}
```

-----

## 11. CLI Interface

The CLI makes KDB’s source-control-of-documents model tangible. It is modelled directly on git so developers have immediate intuition.

```bash
# ── Namespace management ──────────────────────────────────────
kdb init myapp/users                          # create namespace locally
kdb clone peer://192.168.1.10:4242/myapp/users # replicate from peer
kdb remote add origin peer://myserver.com:4242

# ── Schema ────────────────────────────────────────────────────
kdb schema show myapp/users                   # print current schema
kdb schema set  myapp/users schema.json       # declare schema from file
kdb schema migrate myapp/users migration.kql  # apply migration

# ── Writing documents ─────────────────────────────────────────
kdb put    myapp/users doc.json               # write a document (from file)
kdb put    myapp/users '{"userId":"abc",...}' # write inline JSON
kdb delete myapp/users abc123                 # delete document by ID
kdb commit myapp/users -m "add alice"         # commit staged writes

# ── Reading documents ─────────────────────────────────────────
kdb get    myapp/users abc123                 # get full document JSON
kdb query  myapp/users "SELECT _doc FROM users WHERE status='active'"
kdb find   myapp/users --where '{"status":"active"}' # simple filter

# ── History ───────────────────────────────────────────────────
kdb log    myapp/users                        # commit history
kdb log    myapp/users --doc abc123           # history of one document
kdb diff   myapp/users v3 v7                  # diff two commits
kdb show   myapp/users a3f9c2                 # inspect a commit
kdb blame  myapp/users abc123                 # per-field change history

# ── Branching ─────────────────────────────────────────────────
kdb branch   myapp/users feature-x            # create branch
kdb checkout myapp/users feature-x            # switch to branch
kdb checkout myapp/users -t pre-migration     # checkout by tag
kdb checkout myapp/users -d 2024-06-01        # checkout by date
kdb merge    myapp/users feature-x            # merge branch

# ── Tagging ───────────────────────────────────────────────────
kdb tag  myapp/users pre-migration            # create tag at HEAD
kdb tag  myapp/users v1.0
kdb tags myapp/users                          # list all tags

# ── Sync ──────────────────────────────────────────────────────
kdb push   myapp/users origin                 # push local commits to peer
kdb pull   myapp/users origin                 # pull + apply peer commits
kdb fetch  myapp/users origin                 # fetch without applying
kdb sync   myapp/users origin                 # bidirectional peer sync
kdb status myapp/users                        # local changes, peer divergence

# ── Maintenance ───────────────────────────────────────────────
kdb compact myapp/users                       # run compaction
kdb compact myapp/users --preview             # show what would be removed
kdb archive myapp/users --tag v1.0            # push snapshot to ice storage
kdb restore myapp/users --tag v1.0 \
            --into myapp/users-recovered      # restore from ice
kdb gc      myapp/users                       # garbage collect blobs

# ── Node ──────────────────────────────────────────────────────
kdb node status                               # node ID, peers, namespaces
kdb node peers                                # known peer list
kdb node connect peer://192.168.1.10:4242     # connect to peer
```

The CLI is implemented in `jvmMain` and distributed as a native binary via GraalVM native-image or Kotlin/Native. It calls the same engine APIs as any other consumer — no special internal access.

-----

## 12. Storage Format Details

### 12.1 BSON + zstd

BSON (bsonspec.org, Apache 2.0 licensed) is used for binary storage and peer wire transport. BSON provides typed values, length-prefixed traversal, and efficient encoding of numbers, timestamps, and binary data. The field-name repetition overhead inherent to BSON is mitigated by zstd compression, which handles repeated strings extremely well. Typical size reduction: 50–75% over equivalent JSON, with fast decompression suitable for read-heavy workloads.

```
hot tier    →  BSON uncompressed  (fast random access)
warm tier   →  BSON + zstd
cold tier   →  BSON + zstd        (object storage)
ice tier    →  BSON + zstd        (archive bundle, fully self-contained)
wire        →  BSON uncompressed between peers; zstd for bulk/snapshots
```

### 12.2 BSON Conventions for KDB Internal Types

```
BinData subtype 0x04  →  UUID  (16 bytes): document IDs, node IDs, tx IDs
BinData subtype 0x00  →  Hash  (32 bytes): SHA-256 commit and blob hashes
BSON Date             →  Timestamp: microsecond precision as int64
```

### 12.3 Ice Archive Bundle

Self-contained BSON+zstd file, restorable into any KDB instance without external references:

```
{
  commitMetadata:   Commit object
  schemaSnapshot:   schema declaration at this commit
  documentTree:     complete materialised document tree
  indexSnapshots:   serialised index state (avoids full rebuild on restore)
  blobManifest:     [ { hash, size, path }, ... ]
  blobs:            all referenced file blobs packed inline
}
```

-----

## 13. Error Model

```
SchemaViolationException       write violates declared schema; per-field details attached
IceStorageException            commit is archived; restore required; archive location attached
ConflictException              transaction replay failed; ConflictReport attached
CompactionBoundaryException    peer base hash compacted away; snapshot exchange required
NamespaceNotFoundException     namespace does not exist on this node
VersionNotFoundException       commit hash not in local DAG
UnsupportedProtocolVersion     peer requires unsupported protocol version
EncodingNegotiationFailure     no mutually supported encoding
ArchiveRestoreException        ice archive retrieval failed
IndexCorruptionException       index inconsistent with document tree; rebuild triggered
StorageTierException           tier transition failed (e.g. object store unreachable)
SchemaMigrationException       migration failed; namespace rolled back to pre-migration state
JsonPathException              invalid or non-matching JSONPath expression; path string attached
DocumentDecodeException        document BSON/JSON decode failed; optional docId attached
                               (reuses KdbErrorCode.BSON_DECODE_ERROR)
CommitDecodeException          commit BSON decode failed; optional hash attached
                               (reuses KdbErrorCode.BSON_DECODE_ERROR)
```

-----

## 14. Code Size Estimate

Estimated non-blank non-comment Kotlin source lines for production-quality v1.0.

|Module                                                                           |Est. lines |
|---------------------------------------------------------------------------------|-----------|
|BSON codec (commonMain, all types + KDB conventions)                             |2,500      |
|Document + commit data model                                                     |1,800      |
|JSON Functions Engine (JSONPath eval, kdb_json_* functions)                      |2,250      |
|Schema engine (declaration, validation, migration, evolution)                    |3,500      |
|Commit DAG (traversal, diff, branch, tag, ancestor resolution)                   |3,000      |
|Transaction engine (write path, conflict detection, replay)                      |3,000      |
|Hybrid query engine (_doc, AT VERSION, schema+JSON integration)                  |2,000      |
|Index layer — core (registry, projection, consistency)                           |2,000      |
|Index layer — B-tree (KDB LSM adapter, range scan, composite)                   |3,500      |
|Index layer — full-text (tokeniser, inverted index, fuzzy)                       |3,500      |
|Index layer — vector (HNSW, embedding interface, ANN, GPU dispatch)              |4,000      |
|SQL DSL (parser, planner, index selection, result assembly)                      |5,000      |
|Virtual view engine                                                              |1,500      |
|JDBC driver (Driver, Connection, Statement, ResultSet, MetaData)                 |4,500      |
|JDBC SQL extensions (AT VERSION, kdb_json_*, kdb_id, _doc)                       |1,000      |
|Connection URL parser + embedded + memory modes                                  |500        |
|Namespace policy engine                                                          |1,500      |
|Compaction engine (squash, granularity, peer coordination, GC)                   |3,000      |
|Storage tier manager (hot/warm/cold/ice, archive bundle, restore)                |3,500      |
|KDB Storage Engine — WAL + MemTable + SSTable + Block Cache                      |5,000      |
|KDB Storage Engine — Delta Segment Writer (BSON-native, authorship envelope)     |2,500      |
|KDB Storage Engine — Storage Compaction (SSTable + delta segment merge)          |2,000      |
|KDB Storage Engine — Platform I/O Shim (JVM/Native/Browser)                     |1,500      |
|Storage Manager — Realized Store Pool + Eviction Manager                         |2,000      |
|Storage Manager — Rebuild Scheduler                                              |1,000      |
|Storage Manager — Enlistment Manager (browser push/resolve lifecycle)            |1,500      |
|Delta authorship envelope handling (principal, rights token, blame queries)      |1,000      |
|Wire protocol (frame codec, all message types, version negotiation)              |3,000      |
|Stream mode (delta broadcast, index hints, position tracking)                    |2,500      |
|Peer sync mode (DAG exchange, transaction replay, schema sync)                   |4,500      |
|Transport adapter — WebSocket (jsMain + jvmMain)                                 |1,500      |
|Transport adapter — raw TCP (jvmMain + nativeMain)                               |2,000      |
|Compute adapter — WebGPU (jsMain)                                                |3,000      |
|Compute adapter — CUDA/Vulkan (jvmMain)                                          |3,000      |
|CLI (argument parsing, output formatting, conflict UI, config)                   |3,000      |
|Error model                                                                      |500        |
|Test infrastructure (fixtures, in-memory adapters, test DSL)                    |5,000      |
|JDBC integration tests (Hibernate, jOOQ, Spring Data)                           |2,000      |
|**Total**                                                                        |**~96,550**|

> **Note:** Storage adapter line items for RocksDB, IndexedDB, LMDB, and mmap are removed. The KDB Storage Engine and Storage Manager components above replace them entirely. The B-tree index estimate increases slightly because it now owns the full LSM path in pure Kotlin rather than delegating to RocksDB.

### Build Phases

```
Phase 1 — Core engine + JDBC  (highest priority, 3–4 months)
  BSON codec, document + commit model, schema engine,
  hybrid query engine (_doc + kdb_json_* + schema fields),
  commit DAG, B-tree index, SQL engine, transaction engine,
  KDB Storage Engine (WAL, MemTable, SSTable, Delta Segment Writer),
  Storage Manager (Realized Store Pool, Rebuild Scheduler),
  Platform I/O shim — JVM (java.nio), TCP transport,
  JDBC driver (full), embedded + memory modes
  ≈ 35,000 lines
  Deliverable: drop-in JDBC-compatible document database with
               versioning, schema, hybrid SQL+JSON queries,
               works with Hibernate, jOOQ, DBeaver on day one

Phase 2 — Browser + stream sync  (2–3 months)
  Kotlin/JS compilation,
  Platform I/O shim — Browser (in-memory + localStorage/sessionStorage zstd snapshot),
  Enlistment Manager (multi-enlistment, push/resolve lifecycle),
  WebSocket transport, stream mode (Mode 1 + Mode 2),
  full-text index, compaction engine, index hints for browser clients
  ≈ 18,000 lines
  Deliverable: full engine in browser with multi-enlistment support,
               syncing with JVM backend,
               standard browser app write-back pattern working

Phase 3 — Full peer protocol + advanced  (3–4 months)
  Full peer sync (Mode 3, source-control model),
  schema sync across peers, storage tiers, ice archival,
  vector index, GPU adapters, native target + POSIX I/O shim, CLI
  ≈ 27,000 lines
  Deliverable: true distributed peer network across all targets,
               offline-first capable, CLI tooling

Phase 4 — Hardening  (ongoing)
  Test coverage, performance profiling, edge cases,
  developer documentation, example projects
  ≈ 10,000+ lines
```

Solo engineer: 22–28 months to stable v1.0.
Team of three: 11–14 months.

The SQL engine, JDBC driver, peer sync protocol, and KDB Storage Engine are the four hardest components.

-----

## 15. Open Questions

1. **SQL parser library** — build from scratch vs. adapt an existing Kotlin/JVM SQL parser (e.g. JSQLParser, calcite) to avoid reinventing parsing and planning
1. **Peer discovery** — manual configuration, mDNS for LAN, or lightweight rendezvous server
1. **WebRTC transport** — enables browser-to-browser P2P without server relay; significant complexity
1. **Embedding model for vector index** — hosted API, bundled ONNX/WASM, or application-provided
1. **Encryption at rest** — AES-256 per namespace; browser key management is non-trivial
1. **Schema registry** — centralised versioning across peer network, or fully decentralised
1. **Conflict UI conventions** — recommended patterns for surfacing ConflictReports to end users
1. **Mode 2 → Mode 3 upgrade path** — how does a write-back client transition to full peer mid-session
1. **Delta compression strategy** — zstd over raw BSON deltas vs. a custom BSON-aware diff format that exploits document structure for better compression ratios on sparse changes
1. **Rights validation boundary** — the storage engine stores the rights token in every delta's authorship envelope but does not enforce it. A Transaction Engine or Auth Interceptor layer above must validate the token before allowing a delta to be appended. The exact boundary and trust model between these layers needs to be specified.
1. **Conflict resolution UX contract for browser enlistments** — when an enlistment enters resolve state after a rejected push, what is the exact API contract presented to the caller? What conflict-resolution primitives does the Enlistment Manager expose, and how does the caller signal resolution before re-attempting push?

-----

*KDB Architecture Specification v0.8*
*Status: Living document — update completed interfaces in Section 17 after each layer.*

-----

## 16. Implementation Plan — Spec-Driven Development

This section is the execution plan. Each new session should receive this master spec and the instruction below. As layers are completed, paste their public interfaces into Section 17 so subsequent sessions have everything they need.

### How To Use This Document In A New Session

Paste this entire document into a new conversation with the following prompt:

```
You are implementing KDB, a portable embedded database engine in Kotlin Multiplatform.
This document is the master architecture spec and implementation plan.
Please generate implementation-ready component specs for [LAYER N: name, name, name].
Interfaces for completed layers are in Section 17 — treat them as fixed contracts.
Each component spec must follow the standard structure defined in Section 16.2.
```

That is the entire instruction needed. No other context required.

### 16.1 Layer Execution Order

Complete layers in order. Do not start a layer until all layers it depends on are done and their interfaces are recorded in Section 17.

```
STATUS KEY:  [ ] not started   [~] in progress   [x] complete
```

#### LAYER 0 — Foundation (no dependencies)

```
[x] 1.  BSON Codec
[x] 2.  Error Model
```

#### LAYER 1 — Core Types (depends on Layer 0)

```
[~] 3.  Document + Commit Model    — spec complete (kdb-spec-layer1-component3-document-commit-model.md)
[~] 4.  JSON Functions Engine      — spec complete (kdb-spec-layer1-component4-json-functions-engine.md)
```

#### LAYER 2 — Schema + DAG (depends on Layer 1)

```
[~] 5.  Schema Engine      — spec complete (kdb-spec-layer2-component5-schema-engine.md)
[~] 6.  Commit DAG         — spec complete (kdb-spec-layer2-component6-commit-dag.md)
```

#### LAYER 3 — Write Path (depends on Layer 2)

```
[ ] 7.  Transaction Engine
[ ] 8.  Index Layer — Core
[ ] 9.  Storage Adapter Interface
```

#### LAYER 4a — KDB Storage Engine (depends on Layer 3)

```
[ ] 10a. WAL (write-ahead log)
[ ] 10b. MemTable
[ ] 10c. SSTable + Block Cache
[ ] 10d. Delta Segment Writer (BSON-native, large pages, authorship envelope per delta)
[ ] 10e. Storage Engine Core (coordinates above, implements StorageAdapter interface)
[ ] 10f. Storage Compaction (SSTable + delta segment merge, tier policy)
[ ] 10g. Platform I/O Shim (JVM: java.nio | Native: POSIX | Browser: in-memory + localStorage/sessionStorage zstd snapshot)
```

#### LAYER 4b — Storage Manager (depends on Layer 4a)

```
[ ] 11a. Realized Store Pool (per-namespace / per-enlistment materialised document stores)
[ ] 11b. Eviction Manager (LRU eviction, reference counting)
[ ] 11c. Rebuild Scheduler (async delta-to-realized materialisation pipeline)
[ ] 11d. Enlistment Manager (browser enlistments, branch refs, local delta log, push/resolve state machine)
[ ] 11e. Delta Log Tier Signals (coordinates with Compaction Engine on segment lifecycle)
```

#### LAYER 5 — Index Implementations (depends on Layers 3 + 4a/4b)

```
[ ] 12. Index — B-tree
[ ] 13. Index — Full-text
[ ] 14. Index — Vector
[ ] 15. SQL DSL + Query Planner
[ ] 16. Virtual View Engine
```

#### LAYER 6 — Query + Policy (depends on Layer 5)

```
[ ] 17. Hybrid Query Engine (_doc, kdb_json_*, AT VERSION)
[ ] 18. Namespace Policy Engine
[ ] 19. Compaction Engine
```

#### LAYER 7 — Network Foundation (depends on Layer 6)

```
[ ] 20. Storage Tier Manager
[ ] 21. Wire Protocol + Framing
[ ] 22. Stream Mode (Mode 1 + Mode 2)
```

#### LAYER 8 — Advanced Sync + JDBC (depends on Layer 7)

```
[ ] 23. Peer Sync Mode (Mode 3)
[ ] 24. JDBC Driver
```

#### LAYER 9 — Platform Adapters (depends on interfaces, not implementations)

```
[ ] 25. Transport Adapter — WebSocket
[ ] 26. Transport Adapter — TCP
[ ] 27. Compute Adapter — WebGPU (jsMain)
[ ] 28. Compute Adapter — CUDA/Vulkan (jvmMain)
```

> **Note:** Storage adapters (IndexedDB, RocksDB, LMDB, mmap) are removed from this layer. Their role is replaced by the Platform I/O Shim in Layer 4a and the KDB Storage Engine running in commonMain.

#### LAYER 10 — Tooling (depends on everything)

```
[ ] 29. CLI
[ ] 30. Integration Test Suite
```

### 16.2 Component Spec Structure

Every component spec produced must contain exactly these sections:

```
1. Purpose
   2–3 sentences. What this module does and why it exists.
   What problem it solves within KDB.

2. Dependencies
   List of other KDB modules this depends on.
   For each: module name + which interfaces are used.
   Only interfaces from Section 17 (completed layers) may be referenced.

3. Public Interface
   Complete Kotlin signatures for everything this module exposes.
   No implementation. Exact types. Multiplatform annotations where needed.
   This is what gets pasted into Section 17 when the module is complete.

4. Data Structures
   All data classes, sealed classes, enums this module owns.
   With field names, types, and doc comments.

5. Contracts
   For each public function: preconditions, postconditions, guarantees.
   What the caller can rely on. What the caller must provide.

6. Error Cases
   Which KdbException subclasses this module throws and exactly when.

7. Test Cases
   Minimum 8 named test cases per module.
   Format: name / input / expected output or behaviour.
   Include at least 2 edge cases and 1 error case per module.

8. Non-Goals
   Explicit list of what this module does NOT do.
   Prevents scope creep during implementation.

9. Implementation Notes
   Key algorithmic decisions.
   Known pitfalls to avoid.
   Performance considerations.
   Kotlin Multiplatform constraints (expect/actual boundaries).

10. Estimated Lines
    Realistic NBNC line count for production implementation.
```

**File output requirement:** Every component spec must be saved as a separate `.md` file and presented for download before the session ends. File naming convention: `kdb-spec-layer{N}-component{N}-{kebab-name}.md`. One file per component — never combine two components in one file.

### 16.3 After Completing Each Component

When implementation of a component is done and tested:

1. Extract the public interface (section 3 of its spec)
1. Paste it into Section 17 of this master spec under the correct layer
1. Mark the component `[x]` in the layer checklist above
1. Save the updated master spec — this is the context for the next session

### 16.4 Session Token Strategy

- Start a **new conversation** for each layer
- Paste **only this master spec** as context — nothing else
- Keep implementation sessions separate from spec-generation sessions
- A spec session produces the component spec document
- A separate implementation session takes that spec and produces Kotlin code
- Never mix spec generation and implementation in the same session

**File output convention (mandatory):** Every spec-generation session must save each component spec as a separate `.md` file presented for download. Every implementation session must save the generated Kotlin source files for download. This ensures all outputs are portable across sessions without copy-paste. The master spec is also always saved as a new versioned file after each session.

-----

## 17. Completed Interface Registry

This section is populated as layers are completed. It starts empty. Each entry is the exact public Kotlin interface of a completed module — enough for dependent modules to be implemented correctly without needing the full spec or implementation.

When a session asks about a dependency, point it here. Do not re-explain what the module does — the interface says it.

### Layer 0 Interfaces

#### 1. BSON Codec — `dev.kdb.codec`

```kotlin
// ── Primitive types ────────────────────────────────────────────────────────────

data class KdbUuid(val msb: Long, val lsb: Long) {
    override fun toString(): String
    companion object {
        fun random(): KdbUuid
        fun fromString(s: String): KdbUuid
        fun fromBytes(bytes: ByteArray): KdbUuid
    }
}

@JvmInline
value class KdbHash(val bytes: ByteArray) {
    fun toHex(): String
    companion object {
        fun fromHex(hex: String): KdbHash
        fun fromBytes(bytes: ByteArray): KdbHash
    }
}

data class KdbTimestamp(val epochMillis: Long, val microRemainder: Int = 0) : Comparable<KdbTimestamp> {
    fun toEpochMicros(): Long
    companion object {
        fun now(): KdbTimestamp
        fun fromEpochMicros(micros: Long): KdbTimestamp
        fun fromIso8601(s: String): KdbTimestamp
    }
}

// ── BSON value hierarchy ──────────────────────────────────────────────────────

sealed class BsonValue { abstract val bsonType: BsonType }
data class BsonString(val value: String) : BsonValue()
data class BsonInt32(val value: Int) : BsonValue()
data class BsonInt64(val value: Long) : BsonValue()
data class BsonDouble(val value: Double) : BsonValue()
data class BsonBoolean(val value: Boolean) : BsonValue()
data class BsonDateTime(val epochMillis: Long) : BsonValue()
data class BsonBinary(val subtype: Byte, val data: ByteArray) : BsonValue()
object BsonNull : BsonValue()
data class BsonDocument(val fields: LinkedHashMap<String, BsonValue> = LinkedHashMap()) : BsonValue() {
    operator fun get(key: String): BsonValue?
    operator fun set(key: String, value: BsonValue)
    fun getString(key: String): String?
    fun getInt32(key: String): Int?
    fun getInt64(key: String): Long?
    fun getDouble(key: String): Double?
    fun getBoolean(key: String): Boolean?
    fun getDocument(key: String): BsonDocument?
    fun getArray(key: String): BsonArray?
    fun getBinary(key: String): BsonBinary?
    fun getDateTime(key: String): BsonDateTime?
    fun containsKey(key: String): Boolean
    fun keys(): Set<String>
    fun isEmpty(): Boolean
    companion object
}
data class BsonArray(val elements: MutableList<BsonValue> = mutableListOf()) : BsonValue() {
    operator fun get(index: Int): BsonValue
    fun size(): Int
    fun isEmpty(): Boolean
    fun add(value: BsonValue)
}
enum class BsonType(val byte: Byte) {
    DOUBLE(0x01), STRING(0x02), DOCUMENT(0x03), ARRAY(0x04),
    BINARY(0x05), BOOLEAN(0x08), DATETIME(0x09), NULL(0x0A),
    INT32(0x10), INT64(0x12)
}
object BsonBinarySubtype { const val GENERIC: Byte = 0x00; const val UUID: Byte = 0x04 }

// ── Codec registry ─────────────────────────────────────────────────────────────

interface BsonCodec<T> {
    fun encode(value: T): BsonValue
    fun decode(bson: BsonValue): T
}
object BsonCodecRegistry {
    fun <T : Any> register(kClass: kotlin.reflect.KClass<T>, codec: BsonCodec<T>)
    fun <T : Any> get(kClass: kotlin.reflect.KClass<T>): BsonCodec<T>?
    fun <T : Any> getOrThrow(kClass: kotlin.reflect.KClass<T>): BsonCodec<T>
}

// ── Top-level encode / decode ─────────────────────────────────────────────────

fun BsonDocument.toBytes(): ByteArray
fun BsonDocument.writeTo(sink: kotlinx.io.Sink)
fun BsonDocument.Companion.fromBytes(bytes: ByteArray): BsonDocument
fun BsonDocument.Companion.fromSource(source: kotlinx.io.Source): BsonDocument
fun BsonDocument.Companion.fromJson(json: String): BsonDocument
fun BsonDocument.toJson(): String
fun BsonDocument.toPrettyJson(indent: Int = 2): String
fun BsonDocument.encodedSize(): Int
fun BsonValue.encodedSize(): Int
fun <T : Any> T.toBsonValue(): BsonValue
inline fun <reified T : Any> BsonValue.decode(): T

// ── KDB convention helpers ────────────────────────────────────────────────────

fun KdbUuid.toBsonBinary(): BsonBinary
fun BsonBinary.toKdbUuid(): KdbUuid
fun KdbHash.toBsonBinary(): BsonBinary
fun BsonBinary.toKdbHash(): KdbHash
fun KdbTimestamp.toBsonDate(): BsonDateTime
fun BsonDateTime.toKdbTimestamp(microRemainder: Long = 0L): KdbTimestamp

// ── Exceptions ────────────────────────────────────────────────────────────────

class BsonDecodeException(message: String, val offset: Int = -1, cause: Throwable? = null) : KdbException(message, cause)
class BsonEncodeException(message: String, cause: Throwable? = null) : KdbException(message, cause)
```

#### 2. Error Model — `dev.kdb.error`

```kotlin
// ── Root + code enum ──────────────────────────────────────────────────────────

sealed class KdbException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    abstract val code: KdbErrorCode
}

enum class KdbErrorCode(val numericCode: Int) {
    BSON_DECODE_ERROR(1001), BSON_ENCODE_ERROR(1002),
    JSON_PATH_ERROR(2001),
    SCHEMA_VIOLATION(3001), SCHEMA_MIGRATION_FAILED(3002),
    VERSION_NOT_FOUND(3101), ICE_STORAGE(3102), COMPACTION_BOUNDARY(3103),
    CONFLICT(4001),
    STORAGE_TIER_ERROR(4101), NAMESPACE_NOT_FOUND(4201),
    INDEX_CORRUPTION(5001),
    UNSUPPORTED_PROTOCOL_VERSION(6001), ENCODING_NEGOTIATION_FAILURE(6002),
    ARCHIVE_RESTORE(7001),
}

// ── Exception types ───────────────────────────────────────────────────────────

class BsonDecodeException(message: String, val offset: Int = -1, cause: Throwable? = null) : KdbException(message, cause)
class BsonEncodeException(message: String, cause: Throwable? = null) : KdbException(message, cause)
class JsonPathException(message: String, val path: String, cause: Throwable? = null) : KdbException(message, cause)
class SchemaViolationException(message: String, val violations: List<FieldViolation>) : KdbException(message)
class SchemaMigrationException(message: String, val namespaceName: String, val failedStep: String, cause: Throwable? = null) : KdbException(message, cause)
class VersionNotFoundException(message: String, val namespaceName: String, val reference: String) : KdbException(message)
class IceStorageException(message: String, val namespaceName: String, val commitHash: String, val archiveLocation: String?) : KdbException(message)
class CompactionBoundaryException(message: String, val namespaceName: String, val requestedBaseHash: String, val compactionBoundaryHash: String) : KdbException(message)
class ConflictException(message: String, val report: ConflictReport) : KdbException(message)
class StorageTierException(message: String, val tier: String, cause: Throwable? = null) : KdbException(message, cause)
class NamespaceNotFoundException(message: String, val namespaceName: String) : KdbException(message)
class IndexCorruptionException(message: String, val namespaceName: String, val indexName: String, cause: Throwable? = null) : KdbException(message, cause)
class UnsupportedProtocolVersionException(message: String, val peerRequiredVersion: Int, val localMaxVersion: Int) : KdbException(message)
class EncodingNegotiationFailureException(message: String, val localEncodings: List<String>, val peerEncodings: List<String>) : KdbException(message)
class ArchiveRestoreException(message: String, val archiveLocation: String, cause: Throwable? = null) : KdbException(message, cause)

// ── Payload data classes ──────────────────────────────────────────────────────

data class FieldViolation(val fieldName: String, val violationType: ViolationType, val detail: String)
enum class ViolationType { REQUIRED_FIELD_MISSING, TYPE_MISMATCH, UNIQUE_CONSTRAINT, ENUM_VALUE_NOT_DECLARED, CUSTOM_CONSTRAINT }
data class ConflictReport(val transactionId: String, val baseHash: String, val targetHash: String, val conflicts: List<ConflictItem>)
data class ConflictItem(val documentId: String, val operationType: ConflictOperationType, val localDoc: String?, val incomingDoc: String?)
enum class ConflictOperationType { CONCURRENT_WRITE, WRITE_DELETE, DELETE_WRITE, SCHEMA_INCOMPATIBLE }

// ── Result type ───────────────────────────────────────────────────────────────

sealed class KdbResult<out T> {
    data class Success<T>(val value: T) : KdbResult<T>()
    data class Failure(val exception: KdbException) : KdbResult<Nothing>()
    val isSuccess: Boolean get() = this is Success
    val isFailure: Boolean get() = this is Failure
    fun getOrNull(): T?
    fun exceptionOrNull(): KdbException?
    fun getOrThrow(): T
    inline fun <R> map(transform: (T) -> R): KdbResult<R>
    inline fun onSuccess(action: (T) -> Unit): KdbResult<T>
    inline fun onFailure(action: (KdbException) -> Unit): KdbResult<T>
}
inline fun <T> kdbRunCatching(block: () -> T): KdbResult<T>
```

### Layer 1 Interfaces

> **Status: DRAFT** — these interfaces were generated during the spec phase. Replace each with the final extracted interface after implementation is complete and tested. Mark the component `[x]` in the Section 0 checklist at that point.

#### 3. Document + Commit Model — `dev.kdb.document`

```kotlin
// ── Document ──────────────────────────────────────────────────────────────────

data class KdbDocument(
    val id: KdbUuid,
    val json: String,
) {
    val bson: BsonDocument                        // lazy
    val contentHash: KdbHash                      // lazy; SHA-256 of BSON storage form

    fun merge(patchJson: String): KdbDocument     // root-level shallow merge
    fun withJson(newJson: String): KdbDocument    // full body replacement, ID preserved

    companion object {
        fun fromJson(json: String): KdbDocument
        fun fromJson(id: KdbUuid, json: String): KdbDocument
        fun fromBson(bson: BsonDocument): KdbDocument
    }
}

fun KdbDocument.toBson(): BsonDocument
fun computeContentHash(doc: KdbDocument): KdbHash

// ── Operations ────────────────────────────────────────────────────────────────

sealed class KdbOp {
    data class Write(val docId: KdbUuid, val patch: String) : KdbOp()
    data class Delete(val docId: KdbUuid) : KdbOp()
    data class FileWrite(val path: String, val blobHash: KdbHash) : KdbOp()
    data class SchemaMigration(val migrationId: KdbUuid, val migrationPayload: String) : KdbOp()
}

fun KdbOp.toBson(): BsonDocument
fun KdbOp.Companion.fromBson(bson: BsonDocument): KdbOp

// ── Transaction ───────────────────────────────────────────────────────────────

data class KdbTransaction(
    val id: KdbUuid,
    val baseVersion: KdbHash,
    val operations: List<KdbOp>,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val resultVersion: KdbHash? = null,
)

// ── Commit ────────────────────────────────────────────────────────────────────

data class KdbCommit(
    val hash: KdbHash,
    val parentHashes: List<KdbHash>,         // [] root | [h] linear | [h1,h2] merge
    val namespaceId: String,
    val transactionId: KdbUuid,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val operations: List<KdbOp>,
    val documentTreeHash: KdbHash,
    val schemaHash: KdbHash?,
    val message: String = "",
)

fun computeCommitHash(commit: KdbCommit): KdbHash
fun KdbCommit.toBson(): BsonDocument
fun KdbCommit.toBytes(): ByteArray
fun KdbCommit.Companion.fromBson(bson: BsonDocument): KdbCommit
fun KdbCommit.Companion.fromBytes(bytes: ByteArray): KdbCommit

// ── Document tree ─────────────────────────────────────────────────────────────

data class DocumentTree(
    val treeHash: KdbHash,
    val entries: Map<KdbUuid, KdbHash>,      // docId → contentHash
) {
    val size: Int
    fun contains(docId: KdbUuid): Boolean
    fun hashFor(docId: KdbUuid): KdbHash?
    fun with(docId: KdbUuid, contentHash: KdbHash): DocumentTree
    fun without(docId: KdbUuid): DocumentTree

    companion object {
        val EMPTY: DocumentTree
        fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree
    }
}

fun DocumentTree.toBson(): BsonDocument
fun DocumentTree.Companion.fromBson(bson: BsonDocument): DocumentTree

// ── Branch + Tag + Stub ───────────────────────────────────────────────────────

data class KdbBranch(
    val name: String,
    val namespaceId: String,
    val headHash: KdbHash,
    val createdAt: KdbTimestamp,
    val updatedAt: KdbTimestamp,
)

data class KdbTag(
    val name: String,
    val namespaceId: String,
    val commitHash: KdbHash,
    val createdAt: KdbTimestamp,
    val message: String = "",
)

data class CommitStub(
    val originalHash: KdbHash,
    val archiveLocation: String,
    val stubbedAt: KdbTimestamp,
)

// ── Exceptions ────────────────────────────────────────────────────────────────

class DocumentDecodeException(
    message: String,
    val docId: KdbUuid? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.BSON_DECODE_ERROR
}

class CommitDecodeException(
    message: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.BSON_DECODE_ERROR
}
```

#### 4. JSON Functions Engine — `dev.kdb.json`

```kotlin
// ── JsonPath ──────────────────────────────────────────────────────────────────

class JsonPath private constructor(val expression: String) {
    companion object {
        fun compile(expression: String): JsonPath       // throws JsonPathException on invalid syntax
        fun compileOrNull(expression: String): JsonPath?
    }
    override fun toString(): String
    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
}

// ── JsonValue ─────────────────────────────────────────────────────────────────

sealed class JsonValue {
    data class JString(val value: String) : JsonValue()
    data class JNumber(val value: Double) : JsonValue()
    data class JInt(val value: Long) : JsonValue()
    data class JBool(val value: Boolean) : JsonValue()
    object JNull : JsonValue()
    data class JObject(val fields: Map<String, JsonValue>) : JsonValue()
    data class JArray(val elements: List<JsonValue>) : JsonValue()

    fun toJsonString(): String
    fun toBsonValue(): BsonValue

    companion object {
        fun fromJsonString(json: String): JsonValue
    }
}

fun BsonValue.toJsonValue(): JsonValue

// ── Core functions ────────────────────────────────────────────────────────────

fun kdbJsonGet(json: String, path: JsonPath): JsonValue?
fun kdbJsonGet(json: String, path: String): JsonValue?

fun kdbJsonSet(json: String, path: JsonPath, value: JsonValue): String
fun kdbJsonSet(json: String, path: String, value: JsonValue): String

fun kdbJsonDelete(json: String, path: JsonPath): String
fun kdbJsonDelete(json: String, path: String): String

fun kdbJsonMerge(json: String, patchJson: String): String

fun kdbJsonContains(json: String, path: JsonPath, value: JsonValue): Boolean
fun kdbJsonContains(json: String, path: String, value: JsonValue): Boolean

fun kdbJsonKeys(json: String, path: JsonPath): List<String>?
fun kdbJsonKeys(json: String, path: String): List<String>?

fun kdbJsonType(json: String, path: JsonPath): String?
fun kdbJsonType(json: String, path: String): String?

fun kdbJsonArrayLength(json: String, path: JsonPath): Int?
fun kdbJsonArrayLength(json: String, path: String): Int?

fun kdbJsonGetAll(json: String, path: JsonPath): List<JsonValue>
fun kdbJsonGetAll(json: String, path: String): List<JsonValue>

// ── SQL engine registry ───────────────────────────────────────────────────────

object KdbJsonFunctionRegistry {
    val all: List<KdbJsonFunctionDescriptor>
    fun get(sqlName: String): KdbJsonFunctionDescriptor?
}

data class KdbJsonFunctionDescriptor(
    val sqlName: String,
    val minArgs: Int,
    val maxArgs: Int,
    val returnType: JsonFunctionReturnType,
    val evaluate: (args: List<JsonValue?>) -> JsonValue?,
)

enum class JsonFunctionReturnType {
    JSON_STRING, SCALAR, BOOLEAN, INTEGER, STRING_LIST,
}

// Supported path syntax:
//   $            root
//   $.field      named field
//   $.a.b        nested field
//   $.arr[0]     array index
//   $.arr[-1]    last element
//   $.arr[*]     wildcard array (kdbJsonGetAll only)
//   $.*          wildcard object fields (kdbJsonGetAll only)
// Wildcards are not permitted in kdbJsonSet or kdbJsonDelete.
```

### Layer 2 Interfaces

> **Status: DRAFT** — these interfaces were generated during the spec phase. Replace each with the final extracted interface after implementation is complete and tested. Mark the component `[x]` in the Section 0 checklist at that point.

#### 5. Schema Engine — `dev.kdb.schema`

```kotlin
// ── Field type hierarchy ───────────────────────────────────────────────────────

sealed class KdbFieldType {
    object StringType    : KdbFieldType()
    object Int32Type     : KdbFieldType()
    object Int64Type     : KdbFieldType()
    object Float64Type   : KdbFieldType()
    object BoolType      : KdbFieldType()
    object TimestampType : KdbFieldType()
    object UuidType      : KdbFieldType()
    object ObjectType    : KdbFieldType()
    object ArrayType     : KdbFieldType()
    data class EnumType(val values: Set<String>) : KdbFieldType()

    fun sqlTypeName(): String
    fun bsonTypeName(): String
}

// ── Field declaration ─────────────────────────────────────────────────────────

data class SchemaField(
    val name: String,
    val type: KdbFieldType,
    val required: Boolean,
    val indexed: Boolean,
    val unique: Boolean = false,
)

// ── Schema declaration ─────────────────────────────────────────────────────────

data class KdbSchema(
    val schemaHash: KdbHash,
    val fields: List<SchemaField>,
    val version: Int,
    val createdAt: KdbTimestamp,
    val description: String = "",
) {
    val fieldsByName: Map<String, SchemaField>

    fun hasField(name: String): Boolean
    fun field(name: String): SchemaField?
    fun fieldOrThrow(name: String): SchemaField
    fun indexedFields(): List<SchemaField>
    fun requiredFields(): List<SchemaField>
    fun uniqueFields(): List<SchemaField>

    companion object {
        val NONE: KdbSchema
        fun build(fields: List<SchemaField>, version: Int = 1, createdAt: KdbTimestamp = KdbTimestamp.now(), description: String = ""): KdbSchema
    }
}

val KdbSchema.isNone: Boolean

fun KdbSchema.toBson(): BsonDocument
fun KdbSchema.Companion.fromBson(bson: BsonDocument): KdbSchema
fun KdbSchema.toBytes(): ByteArray
fun KdbSchema.Companion.fromBytes(bytes: ByteArray): KdbSchema

// ── Migration DSL ─────────────────────────────────────────────────────────────

data class SchemaMigration(
    val migrationId: KdbUuid,
    val fromVersion: Int,
    val toVersion: Int,
    val steps: List<MigrationStep>,
    val description: String = "",
)

sealed class MigrationStep {
    data class AddField(val field: SchemaField)                                    : MigrationStep()
    data class DropField(val fieldName: String)                                    : MigrationStep()
    data class RenameField(val oldName: String, val newName: String)               : MigrationStep()
    data class ChangeType(val fieldName: String, val newType: KdbFieldType)        : MigrationStep()
    data class AddIndex(val fieldName: String)                                     : MigrationStep()
    data class DropIndex(val fieldName: String)                                    : MigrationStep()
    data class SetRequired(val fieldName: String, val required: Boolean)           : MigrationStep()
    data class SetUnique(val fieldName: String, val unique: Boolean)               : MigrationStep()
    data class WidenEnum(val fieldName: String, val addValues: Set<String>)        : MigrationStep()
    data class NarrowEnum(val fieldName: String, val removeValues: Set<String>)    : MigrationStep()
}

fun MigrationStep.isBreaking(): Boolean

class SchemaMigrationBuilder(private val baseSchema: KdbSchema) {
    fun addField(name: String, type: KdbFieldType, required: Boolean = false, indexed: Boolean = false, unique: Boolean = false): SchemaMigrationBuilder
    fun dropField(name: String): SchemaMigrationBuilder
    fun renameField(oldName: String, newName: String): SchemaMigrationBuilder
    fun changeType(fieldName: String, newType: KdbFieldType): SchemaMigrationBuilder
    fun addIndex(fieldName: String): SchemaMigrationBuilder
    fun dropIndex(fieldName: String): SchemaMigrationBuilder
    fun setRequired(fieldName: String, required: Boolean): SchemaMigrationBuilder
    fun setUnique(fieldName: String, unique: Boolean): SchemaMigrationBuilder
    fun widenEnum(fieldName: String, vararg addValues: String): SchemaMigrationBuilder
    fun narrowEnum(fieldName: String, vararg removeValues: String): SchemaMigrationBuilder
    fun description(text: String): SchemaMigrationBuilder
    fun build(migrationId: KdbUuid = KdbUuid.random()): SchemaMigration
}

fun KdbSchema.migrate(block: SchemaMigrationBuilder.() -> Unit): SchemaMigration

// ── Schema engine ─────────────────────────────────────────────────────────────

object SchemaEngine {
    fun validate(document: KdbDocument, schema: KdbSchema): KdbResult<KdbDocument>
    fun applyMigration(currentSchema: KdbSchema, migration: SchemaMigration): KdbResult<KdbSchema>
    fun computeSchemaHash(schema: KdbSchema): KdbHash
    fun isBackwardCompatible(currentSchema: KdbSchema, migration: SchemaMigration): Boolean
    fun diff(from: KdbSchema, to: KdbSchema): SchemaDiff
    fun checkFieldValue(field: SchemaField, value: JsonValue?): FieldViolation?
}

// ── Schema diff ───────────────────────────────────────────────────────────────

data class SchemaDiff(
    val addedFields: List<SchemaField>,
    val removedFields: List<SchemaField>,
    val modifiedFields: List<FieldDiff>,
    val fromVersion: Int,
    val toVersion: Int,
) {
    val isEmpty: Boolean
    val isBreaking: Boolean
}

data class FieldDiff(
    val fieldName: String,
    val changes: List<FieldChange>,
)

sealed class FieldChange {
    data class TypeChanged(val from: KdbFieldType, val to: KdbFieldType)               : FieldChange()
    data class RequiredChanged(val from: Boolean, val to: Boolean)                     : FieldChange()
    data class IndexedChanged(val from: Boolean, val to: Boolean)                      : FieldChange()
    data class UniqueChanged(val from: Boolean, val to: Boolean)                       : FieldChange()
    data class EnumValuesChanged(val added: Set<String>, val removed: Set<String>)     : FieldChange()
}

fun SchemaMigration.toBson(): BsonDocument
fun SchemaMigration.Companion.fromBson(bson: BsonDocument): SchemaMigration

// ── Exceptions ────────────────────────────────────────────────────────────────

class SchemaDecodeException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.BSON_DECODE_ERROR
}

class SchemaMigrationConflictException(
    message: String,
    val step: MigrationStep,
    val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_MIGRATION_FAILED
}
```

#### 6. Commit DAG — `dev.kdb.dag`

```kotlin
// ── DAG store interface ────────────────────────────────────────────────────────

interface CommitDag {
    val namespaceId: String

    // Commit read
    suspend fun getCommit(hash: KdbHash): KdbCommit?
    suspend fun getCommitOrThrow(hash: KdbHash): KdbCommit
    suspend fun getStub(hash: KdbHash): CommitStub?
    suspend fun hasCommit(hash: KdbHash): Boolean
    suspend fun hasStub(hash: KdbHash): Boolean

    // Commit write
    suspend fun putCommit(commit: KdbCommit, requireParents: Boolean = true)
    suspend fun stubCommit(hash: KdbHash, archiveLocation: String): CommitStub

    // Document tree
    suspend fun getDocumentTree(treeHash: KdbHash): DocumentTree?
    suspend fun getDocumentTreeOrThrow(treeHash: KdbHash): DocumentTree
    suspend fun putDocumentTree(tree: DocumentTree)

    // HEAD + branch
    suspend fun head(): KdbHash
    suspend fun setHead(branchName: String, hash: KdbHash)
    suspend fun getBranch(name: String): KdbBranch?
    suspend fun getBranchOrThrow(name: String): KdbBranch
    suspend fun listBranches(): List<KdbBranch>
    suspend fun createBranch(name: String, fromHash: KdbHash): KdbBranch
    suspend fun deleteBranch(name: String)

    // Tags
    suspend fun getTag(name: String): KdbTag?
    suspend fun getTagOrThrow(name: String): KdbTag
    suspend fun listTags(): List<KdbTag>
    suspend fun createTag(name: String, commitHash: KdbHash, message: String = ""): KdbTag
    suspend fun deleteTag(name: String)

    // Traversal
    suspend fun walk(from: KdbHash, until: KdbHash? = null, limit: Int = Int.MAX_VALUE): List<TraversalEntry>
    suspend fun commitsSince(from: KdbHash, exclude: Set<KdbHash>): List<KdbHash>

    // Ancestor resolution
    suspend fun commonAncestor(hashA: KdbHash, hashB: KdbHash): KdbHash?
    suspend fun isAncestor(ancestor: KdbHash, descendant: KdbHash): Boolean

    // Diff
    suspend fun diff(fromHash: KdbHash, toHash: KdbHash): CommitDiff

    // Commit factory
    suspend fun appendCommit(
        transaction: KdbTransaction,
        parentHash: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    suspend fun appendMergeCommit(
        transaction: KdbTransaction,
        primaryParent: KdbHash,
        mergedParent: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    // Compaction
    suspend fun compactableBefore(boundary: KdbHash, peerHeads: Set<KdbHash>): List<KdbHash>
    suspend fun squash(
        squashHashes: List<KdbHash>,
        boundary: KdbHash,
        syntheticTree: DocumentTree,
        syntheticSchemaHash: KdbHash?,
        message: String = "compaction",
    ): KdbCommit
}

// ── Traversal + diff results ───────────────────────────────────────────────────

sealed class TraversalEntry {
    data class Full(val commit: KdbCommit)    : TraversalEntry()
    data class Stubbed(val stub: CommitStub)  : TraversalEntry()
}

data class CommitDiff(
    val fromHash: KdbHash,
    val toHash: KdbHash,
    val entries: List<DiffEntry>,
) {
    val added: List<DiffEntry.Added>
    val removed: List<DiffEntry.Removed>
    val modified: List<DiffEntry.Modified>
    val isEmpty: Boolean
}

sealed class DiffEntry {
    data class Added(val docId: KdbUuid, val contentHash: KdbHash)                                          : DiffEntry()
    data class Removed(val docId: KdbUuid, val contentHash: KdbHash)                                        : DiffEntry()
    data class Modified(val docId: KdbUuid, val fromContentHash: KdbHash, val toContentHash: KdbHash)       : DiffEntry()
}

// ── Checkout reference ────────────────────────────────────────────────────────

sealed class CommitRef {
    data class ByHash(val hex: String)              : CommitRef()
    data class ByBranch(val name: String)           : CommitRef()
    data class ByTag(val name: String)              : CommitRef()
    data class ByTime(val timestamp: KdbTimestamp)  : CommitRef()
}

suspend fun CommitDag.resolveRef(ref: CommitRef): KdbHash?
suspend fun CommitDag.resolveRefOrThrow(ref: CommitRef): KdbHash

// ── Factory ───────────────────────────────────────────────────────────────────

fun inMemoryCommitDag(namespaceId: String): CommitDag

// ── Exceptions ────────────────────────────────────────────────────────────────

class DagConsistencyException(
    message: String,
    val namespaceId: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class BranchNotFoundException(
    message: String,
    val namespaceId: String,
    val branchName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class TagNotFoundException(
    message: String,
    val namespaceId: String,
    val tagName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class CompactionSafetyException(
    message: String,
    val namespaceId: String,
    val blockerHash: KdbHash,
    val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}
```

### Layer 3 Interfaces

```
[ not yet completed — depends on Layer 2 ]
```

### Layer 4a Interfaces — KDB Storage Engine

```
[ not yet completed — depends on Layer 3 ]

Interfaces to define:
  - StorageAdapter          (implemented by Storage Engine Core; consumed by Transaction Engine + Index Layer)
  - DeltaSegmentWriter      (BSON-native append-only log; includes DeltaRecord with authorship envelope)
  - DeltaRecord / AuthorshipEnvelope  (principal, timestamp, rights_token, client_context, delta payload)
  - PlatformIoShim          (expect/actual; JVM = java.nio, Native = POSIX, Browser = in-memory + snapshot)
  - StorageEngineConfig     (page size, memory budget, shim selection)
```

### Layer 4b Interfaces — Storage Manager

```
[ not yet completed — depends on Layer 4a ]

Interfaces to define:
  - StorageManager              (global singleton per node; owns memory budget + eviction globally)
  - RealizedStoreHandle         (reference-counted handle; released when caller is done)
  - EnlistmentHandle            (browser enlistment: branchRef, local delta log, push/resolve state)
  - EnlistmentManager           (create/release enlistments; manage push → rejected → resolve cycle)

Key methods:
  fun requestRealized(namespaceId: String, commitHash: KdbHash): RealizedStoreHandle
  fun requestEnlistment(namespaceId: String, branchRef: String): EnlistmentHandle
  fun RealizedStoreHandle.release()
  fun EnlistmentHandle.push(): PushResult          // Success | Rejected(missingDeltas)
  fun EnlistmentHandle.fetchMissing()              // enters resolve state
  fun EnlistmentHandle.resolveAndPush(): PushResult

Browser policy enforced by StorageManager:
  - Max 1 realized store per enlistment (in-memory only, always at branch tip)
  - Delta store: in-memory + localStorage/sessionStorage zstd snapshot for durability
  - Same StorageManager + StorageAdapter interfaces as server; policy differs, not interface
```

### Layer 5 Interfaces

```
[ not yet completed — depends on Layers 3 + 4a/4b ]
```

### Layer 6 Interfaces

```
[ not yet completed — depends on Layer 5 ]
```

### Layer 7 Interfaces

```
[ not yet completed — depends on Layer 6 ]
```

### Layer 8 Interfaces

```
[ not yet completed — depends on Layer 7 ]
```

### Layer 9 Interfaces

```
[ not yet completed — depends on Layer 3 Storage Adapter Interface ]
```

### Layer 10 Interfaces

```
[ not yet completed — depends on all layers ]
```