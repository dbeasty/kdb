# KDB — Portable Embedded Database Engine

## Architecture Specification

Document version: **v0.9**

> **Companion documents.** This file is the *normative* design and roadmap. For what the code
> actually does today, read the [high-level architecture](kdb-architecture.md) (system shape,
> decisions, quality attributes) and the [low-level design](kdb-lld.md) —
> [components](kdb-lld-components.md), [flows](kdb-lld-flows.md),
> [concurrency](kdb-lld-concurrency.md), [storage formats](kdb-lld-storage.md),
> [KDB-SQL](kdb-lld-query.md), [protocol and operations](kdb-lld-protocol.md). Usage lives in the
> [user guide](kdb-user-guide.md).

-----

## 0. Session State — Read This First

### Current Status

```
Layer 0 — Foundation         [COMPLETE]
  [x] 1. Type System & Codec — interface in Section 17; normative detail in `kdb-spec-layer0-codec.md`
  [x] 2. Error Model         — interface in Section 17

Layer 1 — Core Types         [COMPLETE]
  [x] 3. Document + Commit Model   — module `:kdb-document`; normative detail in `kdb-spec-layer1-component3-document-commit-model.md`
  [x] 3b. File attachments — `:kdb-file`; spec `kdb-spec-layer1-component3b-file-attachments.md`; CLI `file put`/`get`/`meta`
  [x] 4. JSON Functions Engine     — module `:kdb-json`; normative detail in `kdb-spec-layer1-component4-json-functions-engine.md`

Layer 2 — Schema + DAG       [COMPLETE]
  [x] 5. Schema Engine             — module `:kdb-schema`; normative detail in `kdb-spec-layer2-component5-schema-engine.md`
  [x] 6. Commit DAG                — module `:kdb-dag`; normative detail in `kdb-spec-layer2-component6-commit-dag.md`

Layer 3 — Write Path         [COMPLETE]
  [x] 7. Transaction Engine        — module `:kdb-transaction`; `dev.kdb.transaction`; spec: `kdb-spec-layer3-component7-transaction-engine.md`
  [x] 8. Index Layer — Core        — module `:kdb-index`; `dev.kdb.index`; spec: `kdb-spec-layer3-component8-index-layer-core.md`
  [x] 9. Storage Adapter Interface — module `:kdb-storage`; `dev.kdb.storage`; spec: `kdb-spec-layer3-component9-storage-adapter-interface.md`

Layer 4a — Storage Engine    [COMPLETE]
  [x] 10a. WAL                     — `:kdb-storage-wal`; `dev.kdb.storage.wal`; spec: `kdb-spec-layer4a-component10a-wal.md`
  [x] 10b. MemTable                — `:kdb-storage-memtable`; `dev.kdb.storage.memtable`; spec: `kdb-spec-layer4a-component10b-memtable.md`
  [x] 10c. SSTable + Block Cache   — `:kdb-storage-sstable`; `dev.kdb.storage.sstable`; spec: `kdb-spec-layer4a-component10c-sstable-block-cache.md`
  [x] 10d. Delta Segment Writer    — `:kdb-storage-delta`; `dev.kdb.storage.delta`; spec: `kdb-spec-layer4a-component10d-delta-segment-writer.md`
  [x] 10e. Storage Engine Core     — `:kdb-storage-engine`; `dev.kdb.storage.engine`; spec: `kdb-spec-layer4a-component10e-storage-engine-core.md`
  [x] 10f. Storage Compaction      — `:kdb-storage-compaction`; `dev.kdb.storage.compaction`; spec: `kdb-spec-layer4a-component10f-storage-compaction.md`
  [x] 10g. Platform I/O Shim       — `:kdb-storage-io`; `dev.kdb.storage.io`; spec: `kdb-spec-layer4a-component10g-platform-io-shim.md`

Layer 4b — Storage Manager   [COMPLETE]
  [x] 11a. Realized Store Pool     — `:kdb-storage-manager`; `dev.kdb.storage.manager.pool`; spec: `kdb-spec-layer4b-component11a-realized-store-pool.md`
  [x] 11b. Eviction Manager        — `:kdb-storage-manager`; `dev.kdb.storage.manager.eviction`; spec: `kdb-spec-layer4b-component11b-eviction-manager.md`
  [x] 11c. Rebuild Scheduler       — `:kdb-storage-manager`; `dev.kdb.storage.manager.rebuild`; spec: `kdb-spec-layer4b-component11c-rebuild-scheduler.md`
  [x] 11d. Enlistment Manager      — `:kdb-storage-manager`; `dev.kdb.storage.manager.enlistment`; spec: `kdb-spec-layer4b-component11d-enlistment-manager.md`
  [x] 11e. Delta Log Tier Signals  — `:kdb-storage-manager`; `dev.kdb.storage.manager.tier`; spec: `kdb-spec-layer4b-component11e-delta-log-tier-signals.md`

Layer 5 — Index + Query      [IMPLEMENTED — first Kotlin cut]
  [x] 12. Index — Hash + B-tree    — `:kdb-index-hash`, `:kdb-index-btree`; shared `VersionedIndexEngine` in `:kdb-index`
  [x] 13. Index — Full-text        — `:kdb-index-fulltext`
  [x] 14. Index — Vector           — `:kdb-index-vector` (flat cosine ANN v1; HNSW graph deferred)
  [x] 15. SQL DSL + Query Planner  — `:kdb-sql`
  [x] 16. Virtual View Engine      — `dev.kdb.sql.view` in `:kdb-sql`
  [x] Composite factory            — `:kdb-index-composite` — `compositeIndexStoreFactory`, `productionIndexManager`

Layer 6 — Hybrid Query + Policy [IMPLEMENTED — first Kotlin cut]
  [x] 17. Hybrid Query Engine      — `:kdb-hybrid-query`; spec `kdb-spec-layer6-component17-hybrid-query-engine.md`
  [x] 18. Namespace Policy Engine  — `:kdb-namespace-policy`; spec `kdb-spec-layer6-component18-namespace-policy-engine.md`
  [x] 19. Compaction Engine (DAG)    — `:kdb-compaction`; spec `kdb-spec-layer6-component19-compaction-engine.md`

Layer 7 — Network Foundation [IMPLEMENTED — first Kotlin cut]
  [x] 20. Storage Tier Manager     — `:kdb-storage-tier`; spec `kdb-spec-layer7-component20-storage-tier-manager.md`
  [x] 21. Wire Protocol + Framing  — `:kdb-wire`; spec `kdb-spec-layer7-component21-wire-protocol-framing.md`
  [x] 22. Stream Mode (Mode 1 + 2) — `:kdb-stream`; spec `kdb-spec-layer7-component22-stream-mode.md`

Layer 8 — Advanced Sync + JDBC [IMPLEMENTED — first Kotlin cut]
  [x] 23. Peer Sync Mode (Mode 3)  — `:kdb-peer-sync`; spec `kdb-spec-layer8-component23-peer-sync-mode.md`
  [x] 24. JDBC Driver              — `:kdb-jdbc`; spec `kdb-spec-layer8-component24-jdbc-driver.md`

Layer 9 — Platform Adapters    [IMPLEMENTED — first Kotlin cut]
  [x] 25. Transport — WebSocket     — `:kdb-transport-ws`; spec `kdb-spec-layer9-component25-transport-websocket.md`
  [x] 26. Transport — TCP           — `:kdb-transport-tcp` + `:kdb-transport-core`; spec `kdb-spec-layer9-component26-transport-tcp.md`
  [x] 27. Compute — WebGPU          — `:kdb-compute-webgpu`; spec `kdb-spec-layer9-component27-compute-webgpu.md`
  [x] 28. Compute — CUDA/Vulkan     — `:kdb-compute-jvm` + `:kdb-compute`; spec `kdb-spec-layer9-component28-compute-cuda-vulkan.md`

Layer 10 — Tooling            [IMPLEMENTED — first Kotlin cut]
  [x] 31. Inspect / Debug Tooling — `:kdb-inspect`; spec `kdb-spec-layer10-component31-inspect-tooling.md`
  [x] 29. CLI                     — `:kdb-cli`; spec `kdb-spec-layer10-component29-cli.md`
  [x] 30. Integration Test Suite  — `:kdb-integration`; spec `kdb-spec-layer10-component30-integration-test-suite.md`

Layer 11 — RBAC + Stored Procedures [IMPLEMENTED — Kotlin/JVM, Go-side partial]
  [x] —.  User Mgmt & Resource-Scoped RBAC (not a numbered component) — `:kdb-auth`, `:kdb-auth-store`; plan `kdb-rbac-plan.md`; phases 1–4 done, Kotlin/JVM only
  [x] 32. Stored Procedure Engine — `:kdb-script`; spec `kdb-spec-layer11-component32-stored-procedures.md`; library-level API only, no wire frame/CLI yet

Layer 12 — Go-Native Server + Peer-Sync Hardening [IMPLEMENTED — P0/P1; 42/43 deferred]
  [x] 38. Go-Native Server              — `go/kdb/server`; spec `kdb-spec-layer12-component38-go-native-server.md`
  [x] 39. Peer-Sync Conflict Detection  — `:kdb-peer-sync` + `go/kdb/peersync`; spec `kdb-spec-layer12-component39-peersync-conflict-detection.md`
  [x] 40. Go Client SDK                 — `go/kdb/client`; spec `kdb-spec-layer12-component40-go-client-sdk.md`
  [x] 41. Auth Session/Token Issuance   — `:kdb-auth` (`dev.kdb.auth.token`); spec `kdb-spec-layer12-component41-auth-tokens.md`
  [x] 44–46. Minor fixes — commit notification bridge, connection-disconnect lock cleanup, stream write-back mode fix; spec'd inline, gap analysis §5
  [ ] 42. Native TCP Transport (embed)   — deferred pending Phase 0 spike; spec `kdb-spec-layer12-component42-native-transport.md`
  [ ] 43. Embed Durable + Mobile Storage — deferred pending Phase 0 spike; spec `kdb-spec-layer12-component43-embed-durable-storage.md`
```

### What Has Been Done

- Layer 0 component specs generated (Type System & Codec — `kdb-spec-layer0-codec.md`, Error Model)
- Both Layer 0 components implemented and tested (per plan)
- Public interfaces extracted and recorded in Section 17 → Layer 0
- Layer 1 components implemented: `:kdb-document` (Component 3), `:kdb-json` (Component 4); public interfaces recorded in Section 17 → Layer 1
- Layer 1 draft interfaces recorded in Section 17 → Layer 1 (historical note; Layer 1 is implemented — see interfaces there)
- Layer 2 components implemented: `:kdb-schema` (Component 5), `:kdb-dag` (Component 6); public interfaces recorded in Section 17 → Layer 2 (final)
- Component spec files saved as downloadable `.md` files (see file-output convention in Section 16.4)
- Master spec updated from v0.6 → v0.7
- Storage engine design decisions applied (v0.7 → v0.8): removed external storage dependencies, introduced two-store architecture (Delta Store + Realized Store), browser multi-enlistment model, delta authorship envelope, Storage Manager layer, split Layer 4 into 4a (Storage Engine) and 4b (Storage Manager), renumbered Layers 5–9
- Layer 3 component specs generated (v0.8 → v0.9): Transaction Engine (Component 7), Index Layer Core (Component 8), Storage Adapter Interface (Component 9). Storage Adapter Interface incorporates all v3 design decisions: StorageCapabilitySet with GPU fields, DeltaAuthorshipEnvelope, sub-enlistment eviction state machine (FULL/DOC_EVICTED/EVICTED/RELEASED), IndexRetention (PINNED/EVICTABLE), GPU direct delta ingest path, browser snapshot repair model, IndexPinViolationEvent escalation path.
- Layer 3 draft interfaces recorded in Section 17 → Layer 3
- Layer 3 specs and §17 drafts are aligned with **implemented** Layer 2: transactional writes are expected to go through `CommitDag.appendCommit` / `appendMergeCommit` (with `schemaHash` carried on `KdbCommit`); `SchemaEngine.computeSchemaHash` and commit payload hashing (`KdbCommit.build` / `computeCommitHash`) are the normative hooks for schema- and content-addressed heads. Component 7 (`DefaultTransactionEngine`) is now the orchestration path atop those primitives.
- Layer 3 Gradle modules landed: `:kdb-storage` (adapter surface + memory adapter +expect/actual `PlatformIoShim`), `:kdb-index` (`IndexStore`, `MemoryIndexStore`, registry/writer/reader/manager stack, wire helpers), `:kdb-transaction` (`TransactionEngine`, builder, replay/merge atop the DAG).
- Layer 4 component specs generated (12 files): Layer 4a Components 10a–10g and Layer 4b Components 11a–11e.
- Layer 4 implemented (Gradle modules): `:kdb-compression`, `:kdb-storage-io` (10g), `:kdb-storage-wal` (10a), `:kdb-storage-sstable` (10c), `:kdb-storage-memtable` (10b), `:kdb-storage-delta` (10d), `:kdb-storage-engine` (10e), `:kdb-storage-compaction` (10f), `:kdb-storage-manager` (11a–11e). Production I/O via `FileBackedPlatformIoShimFactory`; tests use `InMemoryPlatformIoShim`.
- Layer 5 component specs generated (5 files): Components 12–16 — hash+btree, full-text, vector, SQL DSL + planner, virtual view engine. See Section 16.1 Layer 5 execution order.
- Layer 5 implemented (first Kotlin cut): `:kdb-index-hash`, `:kdb-index-btree`, `:kdb-index-fulltext`, `:kdb-index-vector`, `:kdb-index-composite`, `:kdb-sql`. Index writer JSONPath fixed (`$.field`). Vector index uses brute-force cosine v1.
- Layer 6 component specs generated (3 files): Components 17–19 — hybrid query, namespace policy, DAG compaction engine. Execution plan: `kdb-spec-layer6-execution-plan.md`. See Section 16.1 Layer 6 implementation order.
- Layer 6 implemented (first Kotlin cut): `:kdb-namespace-policy`, `:kdb-hybrid-query`, `:kdb-compaction`. Hybrid engine wraps `:kdb-sql` with `AT VERSION`/`AT COMMIT`/`AT TIME`; policy JSON via kotlinx.serialization; DAG compaction orchestrates `CommitDag.squash` (distinct from `:kdb-storage-compaction`).
- Layer 7 component specs generated (3 files): Components 20–22 — storage tier manager, wire protocol + framing, stream mode. Execution plan: `kdb-spec-layer7-execution-plan.md`. Draft interfaces in Section 17 → Layer 7.
- Layer 7 implemented (first Kotlin cut): `:kdb-wire`, `:kdb-stream`, `:kdb-storage-tier`. Wire frame codec (JSON payload v1), in-memory transport, stream coordinator/subscriber, ice bundle archive/stub/restore.
- Layer 8 component specs generated (2 files): Components 23–24 — peer sync Mode 3, JDBC driver. Execution plan: `kdb-spec-layer8-execution-plan.md`.
- Layer 8 implemented (first Kotlin cut): `:kdb-peer-sync` (FULL_PEER handshake, CommitFetch/Push, bidirectional in-memory sync), `:kdb-jdbc` (memory- and file-mode Driver/Connection/Statement/ResultSet/MetaData; delta replay on open).
- Layer 9 component specs generated (4 files + shared modules): Components 25–28 — WebSocket transport, TCP transport, WebGPU compute, CUDA/Vulkan/CPU compute. Execution plan: `kdb-spec-layer9-execution-plan.md`.
- Layer 9 implemented (first Kotlin cut): `:kdb-transport-core`, `:kdb-transport-tcp`, `:kdb-transport-ws`, `:kdb-compute`, `:kdb-compute-jvm`, `:kdb-compute-webgpu`. TCP loopback + peer sync integration; CPU compute fallback for WebGPU/CUDA.
- Layer 10 component specs generated: Components 29–30 — CLI, integration test suite. Execution plan: `kdb-spec-layer10-execution-plan.md`.
- Layer 10 implemented (first Kotlin cut): `:kdb-cli` (init/put/get/query/log/status/sync), `:kdb-integration` (cross-layer scenarios). Component 31 inspect landed earlier.
- Layer 11 implemented: user management & resource-scoped RBAC (`:kdb-auth`, `:kdb-auth-store` — `RegistryAuthStore`, `PasswordHasher` PBKDF2-HMAC-SHA256, `RegistryAuthEngine`; phases 1–4 of `kdb-rbac-plan.md`, Kotlin/JVM only — Go-side store/enforcement landed later as part of Layer 12 Component 38); Component 32 Stored Procedure Engine (`:kdb-script` — `ProcedureRegistry` + sandboxed GraalVM JS runtime, host-authorized `kdb` API; library-level only, no wire protocol frame or CLI subcommand yet).
- Layer 12 component specs generated: master spec `kdb-spec-layer12-zolik-gap-analysis.md` (driven by a Lightsail hosting-cost calculation and a peer-sync correctness audit), Components 38–43. Execution plan: `kdb-spec-layer12-execution-plan.md`.
- Layer 12 implemented (P0/P1): Component 38 Go-Native Server (`go/kdb/server` — wire listener, `KdbServerRuntime.Commit`/`Upsert` against the real `TransactionEngine`, RBAC port with cross-verified PBKDF2 hashing; a Go deployment now runs the wire-compatible server with zero JVM processes); Component 39 Peer-Sync Conflict Detection (`resolveHeadUpdate`/`resolveDivergence` in both `:kdb-peer-sync` and `go/kdb/peersync` — fast-forward/already-ancestor/diverged classification replacing an unconditional `setHead`, auto-merge via a real two-parent commit for disjoint writes, explicit conflict report otherwise); Component 40 Go Client SDK (`go/kdb/client` — `Connect`/`PutJSON`/`GetJSON`/`Upsert`/`Commit`/`Query`/`Exec`); Component 41 Auth Session/Token Issuance (`dev.kdb.auth.token` — `TokenAuthEngine`, `CompositeAuthEngine`, `SessionIssuer`); Components 44–46 (SQL-write commits now notify Mode 1 stream subscribers, not just peer-sync-arrived ones; a disconnected connection releases its document locks instead of leaking them; Mode 2 write-back actually awaits and applies a replay instead of returning immediately). Components 42/43 (Kotlin/Native mobile storage/transport) deferred: the Phase 0 spike found `go/kdb/embed` binds directly via `gomobile` on both Android and iOS with no dedicated Kotlin/Native target needed.

### What To Do Next

**All planned layers (0–12) have at least a first implementation; Layer 12's P0/P1 scope is done.** Remaining Layer 12 items: a Go↔JVM cross-implementation interop test (component 38 spec §7 test 1 / component 40 spec §7 test 1), a Lightsail load test measuring the actual cost savings (component 38 spec §7 test 8), and Components 42/43 only if a future mobile-storage need can't be met by binding `go/kdb/embed` directly.

Other follow-on work: browser snapshot persistence, CUDA/Vulkan real backends, GraalVM CLI binary, Hibernate file-mode integration tests, TLS on transport, WAL-backed document recovery on SERVER open, **file attachments** (Component 3b — ingest, CLI `file put`/`get`, blob GC reachability, peer blob sync), RBAC admin SQL surface + Go-side store, stored procedures' wire protocol frame/CLI subcommand.

Optional parallel work: Layer 3 hardening — add `commonTest` coverage for `:kdb-transaction`, `:kdb-index`, and in-memory `:kdb-storage` per Layer 3 specs.

### Dependency Rules

- Layer 1 depends on Layer 0 only — interfaces are in Section 17
- Layer 2 depends on Layer 0 and Layer 1 — interfaces are in Section 17
- Component 3 and Component 4 within Layer 1 are independent of each other and may be implemented in parallel
- Component 5 and Component 6 within Layer 2 are independent of each other and may be implemented in parallel
- Layer 3 depends on Layers 0, 1, and 2 — interfaces are in Section 17 (Layers 0–2 are implemented and stable inputs for Layer 3)
- Component 9 (Storage Adapter Interface) within Layer 3 has no inter-Layer-3 dependencies and should be implemented first
- Component 7 and Component 8 within Layer 3 are independent of each other and may be implemented in parallel; both depend on Component 9
- Component 7 should use `CommitDag.appendCommit` / `appendMergeCommit` for publishing commits (not ad-hoc `putCommit` alone); `putCommit` remains for replication/ingest paths that already have a materialised `KdbCommit`
- Layer 4a depends on Layer 3 (`StorageAdapter`, `DeltaSegmentWriter`, `PlatformIoShim` expect); implement 10g → 10a–c → 10d → 10e → 10f
- Layer 4b depends on Layer 4a; implement 11a → 11b → 11c, then 11d and 11e (11d/11e may overlap once pool + rebuild exist)
- Layer 5 depends on Layers 3 + 4a/4b — interfaces in Section 17 (Layers 0–4). Implement 12 (hash + btree) first; 13 and 14 may proceed in parallel once 12’s `CompositeIndexStoreFactory` pattern exists; 15 requires 12–14 for index planning; 16 requires 15 parser/planner hooks
- Layer 6 depends on Layer 5 — implement **18 → 17 → 19** (policy before hybrid query and DAG compaction). Component 19 is **not** `:kdb-storage-compaction` (10f); DAG squash only. Physical tier moves remain Layer 7 Component 20.
- Layer 7 depends on Layer 6 — implement **21 → 22 → 20** (wire before stream; tier manager subscribes to 11e). Component 22 is Mode 1/2 only; Mode 3 peer sync is Layer 8 Component 23. Transport sockets are Layer 9 (25–26).
- Layer 8 depends on Layer 7 — implement **23 → 24** (peer sync before JDBC; JDBC delegates to `HybridQueryEngine`). Network JDBC requires Layer 9 transport.
- Layer 9 depends on Layer 7–8 — implement **transport-core → 26 → 25 → `:kdb-compute` API → 28 (CPU first) → 27** (TCP before WebSocket for backend; compute CPU path before CUDA/WebGPU). Transport implements `WireTransport` from Component 22; compute implements `ComputeAdapter` for `GpuStorageEngine` and vector index.
- Layer 11 depends on Layer 10 (RBAC needs the full engine surface to enforce against; stored procedures need `HybridQueryEngine`/`TransactionEngine` as the only privileged path in). RBAC and Component 32 are independent of each other.
- Layer 12 depends on Layer 7–8 (wire protocol, peer sync) and Layer 11 (Component 38's RBAC port needs the Kotlin RBAC reference to cross-verify against). Components 38 and 39 are independent (different languages, different modules) and may run in parallel; 40 and 41 are each independent of everything else in the layer; 44–46 are minor fixes, opportunistic. Components 42/43 are gated on the Phase 0 spike's answer (whether `go/kdb/embed` already binds via `gomobile`) before starting at all.
- Component 15 DML paths delegate writes to Component 7 (`TransactionEngine`) — do not duplicate commit logic; Component 17 routes hybrid `_doc` DML through the same engine
- Never mix spec generation and implementation in the same session
- Always save component spec output as `.md` files for download (see Section 16.4)

-----

## 1. Overview

KDB is a portable, multi-runtime embedded database engine written in Kotlin Multiplatform. The **entire engine** — not a client library, not a thin SDK — compiles and runs on browser clients (via Kotlin/JS), JVM backends, and native targets (via Kotlin/Native). The same Kotlin codebase produces all three runtimes. Only storage adapters and transport adapters differ per platform; all engine logic is shared.

KDB is best understood as **source control for structured documents**. You store whole JSON documents. You retrieve whole JSON documents. Optionally you declare a schema — a typed, indexed lens over part of each document — which unlocks SQL querying, JDBC connectivity, and ORM integration. The document is always the truth. The schema is always a lens. Both coexist without friction.

Primary storage is JSON. Binary storage and on-the-wire structured data use the **Layer 0 typed binary codec** (tagged physical types, schema-driven records, deterministic encoding per `kdb-spec-layer0-codec.md`), typically **compressed with zstd** in warm/cold tiers. **Layer 0 binary is the only in-tree structured binary interchange.** SQL operates as an index and query layer over schema-declared fields, but raw JSON access is always available alongside SQL in the same query. All data lives in versioned, content-addressed namespaces with git-like history. Peer synchronisation follows a source-control model: peers are fully independent, can diverge arbitrarily, and merge when they choose to.

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
- Typed binary + zstd is always a storage and transport optimisation; JSON remains valid end-to-end
- SQL addresses data via schema; `_doc` always gives access to the whole document
- Peers are equal; any peer can sync with any other peer directly
- Divergence is normal; merging is explicit and application-controlled
- Conflicts surface to the application; KDB never silently resolves them
- History is cheap because unchanged content is shared by hash
- Debug JSONL sidecars and `kdb inspect` dumps are **non-authoritative**; never used for content hashing, peer sync, or tier moves

-----

## 2. Kotlin Multiplatform Runtime Model

The entire engine lives in `commonMain`. Every platform — browser, JVM, native — runs identical database logic. Only platform I/O shims and transport adapters differ. **All storage logic is implemented in shared Kotlin — no external storage library dependencies.**

```
commonMain  ←  the entire engine lives here
  │
  ├── Document model, Layer 0 codec, commit DAG
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

Tagged snapshots are materialised as self-contained **typed-binary+zstd** archive bundles and shipped to configured archive storage. A stub commit replaces the original in the DAG. Accessing an archived commit returns `IceStorageException`. Restore targets an isolated namespace to avoid disturbing live data.

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
[bytes]   payload (KDB typed binary or JSON per negotiated encoding)
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

Structured payload encoded as **Layer 0 values** (records / unions per `dev.kdb.document.*` and related wire schemas). Illustrative logical layout:

```
DeltaCommitPayload {
  namespace:     string
  commitHash:    bytes[32]     // SHA-256
  parentHash:    bytes[32]
  timestamp:     timestamp     // microsecond instant
  operations:    [ Op, ... ]   // union-discriminated ops
  indexHints:    [ { index, key, action }, ... ]   // pre-computed for read-only clients
  schemaDelta:   SchemaDelta?   // present if schema changed
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
        warm { maxAge = 90.days;   storage = LOCAL_FS;     format = KDB_BINARY_ZSTD }
        cold { maxAge = 365.days;  storage = OBJECT_STORE; format = KDB_BINARY_ZSTD }
        ice  { storage = ARCHIVE;  format = KDB_BINARY_ZSTD;     restoreLatency = HOURS }
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
kdb shell  myapp/users                        # interactive REPL (v1: put, query, get, log, status, sync, use)

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

# ── Debug / inspect (non-authoritative JSON views) ───────────
kdb inspect dump-delta  --data-dir DIR --namespace NS [--segment SEG]
kdb inspect dump-wire   --file FRAME.bin [--pretty]
kdb inspect dump-commit --file PAYLOAD.bin
kdb inspect dump-blob   --data-dir DIR --hash HEX
```

The CLI is implemented in `jvmMain` and distributed as a native binary via GraalVM native-image or Kotlin/Native. It calls the same engine APIs as any other consumer — no special internal access.

-----

## 12. Storage Format Details

### 12.1 Layer 0 typed binary + zstd

Structured engine data uses the **Layer 0 codec** (`kdb-spec-layer0-codec.md`): physical type tags, varints, schema-addressed records, and deterministic encoding suitable for content hashing. **zstd** applies as an outer compression wrapper on segments, snapshots, and bulk sync — repeated field structure compresses well over this layout.

```
hot tier    →  typed binary uncompressed (fast random access)
warm tier   →  typed binary + zstd
cold tier   →  typed binary + zstd        (object storage)
ice tier    →  typed binary + zstd       (archive bundle, fully self-contained)
wire        →  typed binary or JSON per handshake; zstd for bulk/snapshots
```

### 12.2 Physical encoding of KDB primitives

UUIDs (16-byte RFC 4122), hashes (32-byte SHA-256), and timestamps (microsecond instants) map onto Layer 0 logical types (`uuid`, `timestamp-micros`, etc.) over fixed/binary/int64 physical forms — see Layer 0 §4.

### 12.3 Ice Archive Bundle

Self-contained **typed-binary+zstd** file, restorable into any KDB instance without external references:

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

### 12.4 Debug observability (non-normative for interchange)

Production storage and sync use Layer 0 typed binary + zstd (§12.1). For operator and developer visibility, KDB supports **optional debug JSON** that does not participate in hashing or replication:

| Mechanism | Location | Purpose |
|---|---|---|
| Delta JSONL sidecar | `{dataDir}/{namespaceId}/debug/delta.jsonl` | One JSON object per appended `DeltaRecord` (commit + patches when available) |
| Wire JSONL sidecar | `{dataDir}/{namespaceId}/debug/wire.jsonl` | Optional log of decoded `WireMessage` frames (`in` / `out`) |
| Offline dump | `kdb inspect dump-*` | Decode on-disk KDBP delta frames, wire captures, commit payloads, blobs |

Sidecar writes are **best-effort**; their absence must not affect engine correctness. Offline dump reads the same bytes as production (KDBP-framed commit payloads → `KdbCommit.fromPayloadBytes`). Normative detail: `kdb-spec-layer10-component31-inspect-tooling.md`.

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
DocumentDecodeException        document typed-binary / JSON decode failed; optional docId attached
                               (`KdbErrorCode.KDB_DECODE_ERROR`, Layer 0 typed codec decode)
CommitDecodeException          commit payload decode failed; optional hash attached
                               (`KdbErrorCode.KDB_DECODE_ERROR`)
```

-----

## 14. Code Size Estimate

Estimated non-blank non-comment Kotlin source lines for production-quality v1.0.

|Module                                                                           |Est. lines |
|---------------------------------------------------------------------------------|-----------|
|Layer 0 typed codec (`kdb-codec`, physical types + schema + JSON/binary bridges)           |2,500      |
|Document + commit data model                                                     |1,800      |
|JSON Functions Engine (JSONPath eval, kdb_json_* functions)                      |2,250      |
|Schema engine (declaration, validation, migration, evolution)                    |3,500      |
|Commit DAG (traversal, diff, branch, tag, ancestor resolution)                   |3,000      |
|Transaction engine (write path, conflict detection, replay)                      |3,000      |
|Hybrid query engine (_doc, AT VERSION, schema+JSON integration)                  |2,000      |
|Index layer — core (registry, projection, consistency)                           |2,000      |
|Index layer — hash (exact equality, unique constraints)                            |800        |
|Index layer — B-tree (KDB LSM adapter, range scan, composite)                   |3,500      |
|Index layer — full-text (tokeniser, inverted index, fuzzy)                       |3,500      |
|Index layer — vector (HNSW, embedding interface, ANN, GPU dispatch)              |4,000      |
|SQL DSL (parser, planner, index selection, result assembly)                      |5,000      |
|Virtual view engine                                                              |1,500      |
|*Layer 5 subtotal (Components 12–16)*                                            |*~18,800*  |
|*Layer 6 subtotal (Components 17–19)*                                            |*~6,500*   |
|*Layer 7 subtotal (Components 20–22)*                                            |*~9,000*   |
|*Layer 8 subtotal (Components 23–24)*                                            |*~9,500*   |
|*Layer 9 subtotal (Components 25–28 + transport-core)*                          |*~9,500*   |
|JDBC driver (Driver, Connection, Statement, ResultSet, MetaData)                 |4,500      |
|JDBC SQL extensions (AT VERSION, kdb_json_*, kdb_id, _doc)                       |1,000      |
|Connection URL parser + embedded + memory modes                                  |500        |
|Namespace policy engine                                                          |1,500      |
|Compaction engine (squash, granularity, peer coordination, GC)                   |3,000      |
|Storage tier manager (hot/warm/cold/ice, archive bundle, restore)                |3,500      |
|KDB Storage Engine — WAL + MemTable + SSTable + Block Cache                      |5,000      |
|KDB Storage Engine — Delta Segment Writer (Layer-0-binary-native, authorship envelope)|2,500      |
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
|**Total**                                                                        |**~131,850**|

> **Cumulative note:** Layers 9–10 implemented add ~12,700 NBNC (9: ~9,500; 10: ~3,200). **~135,050** total planned NBNC with all layers at first Kotlin cut.

> **Note:** Storage adapter line items for RocksDB, IndexedDB, LMDB, and mmap are removed. The KDB Storage Engine and Storage Manager components above replace them entirely. The B-tree index estimate increases slightly because it now owns the full LSM path in pure Kotlin rather than delegating to RocksDB.

### Build Phases

```
Phase 1 — Core engine + JDBC  (highest priority, 3–4 months)
  Layer 0 typed codec, document + commit model, schema engine,
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
1. **Delta compression strategy** — zstd over raw typed-binary deltas vs. a structure-aware diff format for better compression ratios on sparse changes
1. **Rights validation boundary** — the storage engine stores the rights token in every delta's authorship envelope but does not enforce it. A Transaction Engine or Auth Interceptor layer above must validate the token before allowing a delta to be appended. The exact boundary and trust model between these layers needs to be specified.
1. **Conflict resolution UX contract for browser enlistments** — when an enlistment enters resolve state after a rejected push, what is the exact API contract presented to the caller? What conflict-resolution primitives does the Enlistment Manager expose, and how does the caller signal resolution before re-attempting push?

-----

*KDB Architecture Specification v0.9 (Layer 6 specs added)*
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
[x] 1.  Type System & Codec (`kdb-spec-layer0-codec.md`)
[x] 2.  Error Model
```

#### LAYER 1 — Core Types (depends on Layer 0)

```
[x] 3.  Document + Commit Model    — `:kdb-document`; spec `kdb-spec-layer1-component3-document-commit-model.md`
[x] 4.  JSON Functions Engine      — `:kdb-json`; spec `kdb-spec-layer1-component4-json-functions-engine.md`
```

#### LAYER 2 — Schema + DAG (depends on Layer 1)

```
[x] 5.  Schema Engine      — `:kdb-schema`; spec `kdb-spec-layer2-component5-schema-engine.md`
[x] 6.  Commit DAG         — `:kdb-dag`; spec `kdb-spec-layer2-component6-commit-dag.md`
```

#### LAYER 3 — Write Path (depends on Layer 2)

```
[x] 7.  Transaction Engine         — `:kdb-transaction`; spec `kdb-spec-layer3-component7-transaction-engine.md`
[x] 8.  Index Layer — Core         — `:kdb-index`; spec `kdb-spec-layer3-component8-index-layer-core.md`
[x] 9.  Storage Adapter Interface  — `:kdb-storage`; spec `kdb-spec-layer3-component9-storage-adapter-interface.md`
```

#### LAYER 4a — KDB Storage Engine (depends on Layer 3)

```
[x] 10a. WAL                        — `:kdb-storage-wal`; spec `kdb-spec-layer4a-component10a-wal.md`
[x] 10b. MemTable                   — `:kdb-storage-memtable`; spec `kdb-spec-layer4a-component10b-memtable.md`
[x] 10c. SSTable + Block Cache      — `:kdb-storage-sstable`; spec `kdb-spec-layer4a-component10c-sstable-block-cache.md`
[x] 10d. Delta Segment Writer       — `:kdb-storage-delta`; spec `kdb-spec-layer4a-component10d-delta-segment-writer.md`
[x] 10e. Storage Engine Core        — `:kdb-storage-engine`; spec `kdb-spec-layer4a-component10e-storage-engine-core.md`
[x] 10f. Storage Compaction         — `:kdb-storage-compaction`; spec `kdb-spec-layer4a-component10f-storage-compaction.md`
[x] 10g. Platform I/O Shim          — `:kdb-storage-io`; spec `kdb-spec-layer4a-component10g-platform-io-shim.md`
```

#### LAYER 4b — Storage Manager (depends on Layer 4a)

```
[x] 11a. Realized Store Pool        — `:kdb-storage-manager`; spec `kdb-spec-layer4b-component11a-realized-store-pool.md`
[x] 11b. Eviction Manager           — `:kdb-storage-manager`; spec `kdb-spec-layer4b-component11b-eviction-manager.md`
[x] 11c. Rebuild Scheduler          — `:kdb-storage-manager`; spec `kdb-spec-layer4b-component11c-rebuild-scheduler.md`
[x] 11d. Enlistment Manager         — `:kdb-storage-manager`; spec `kdb-spec-layer4b-component11d-enlistment-manager.md`
[x] 11e. Delta Log Tier Signals     — `:kdb-storage-manager`; spec `kdb-spec-layer4b-component11e-delta-log-tier-signals.md`
```

#### LAYER 5 — Index + Query (depends on Layers 3 + 4a/4b)

```
[x] 12. Index — Hash + B-tree       — `:kdb-index-hash`, `:kdb-index-btree`; spec `kdb-spec-layer5-component12-index-hash-btree.md`
[x] 13. Index — Full-text            — `:kdb-index-fulltext`; spec `kdb-spec-layer5-component13-index-fulltext.md`
[x] 14. Index — Vector               — `:kdb-index-vector`; spec `kdb-spec-layer5-component14-index-vector.md`
[x] 15. SQL DSL + Query Planner      — `:kdb-sql`; spec `kdb-spec-layer5-component15-sql-dsl-query-planner.md`
[x] 16. Virtual View Engine          — `dev.kdb.sql.view` (in `:kdb-sql`); spec `kdb-spec-layer5-component16-virtual-view-engine.md`
[x] —  Composite index factory       — `:kdb-index-composite`
```

**Layer 5 implementation order (normative):**

1. **12 — Hash + B-tree** — durable `IndexStore` for `IndexType.HASH` and `IndexType.BTREE`; wire `CompositeIndexStoreFactory` into node bootstrap (replace test-only `MemoryIndexStore` for production).
2. **13 — Full-text** and **14 — Vector** — may run in parallel after 12; register factories on `CompositeIndexStoreFactory`.
3. **15 — SQL DSL + planner** — parser, index rule planner, executor; depends on all index types for `MATCH` / `similarity` / equality plans.
4. **16 — Virtual views** — metadata catalog + query rewrite; integrate into `SqlEngine` planning path.

**Estimated NBNC (Layer 5 production + tests):** ~18,430 lines (12: ~4,430; 13: ~3,500; 14: ~4,000; 15: ~5,000; 16: ~1,500).

#### LAYER 6 — Query + Policy (depends on Layer 5)

```
[x] 17. Hybrid Query Engine          — `:kdb-hybrid-query`; spec `kdb-spec-layer6-component17-hybrid-query-engine.md`
[x] 18. Namespace Policy Engine      — `:kdb-namespace-policy`; spec `kdb-spec-layer6-component18-namespace-policy-engine.md`
[x] 19. Compaction Engine (DAG)      — `:kdb-compaction`; spec `kdb-spec-layer6-component19-compaction-engine.md`
```

**Layer 6 implementation order (normative):**

1. **18 — Namespace Policy** — policy registry, JSON/DSL parse, `CompactionPolicyEvaluator`; required by 17 and 19.
2. **17 — Hybrid Query** — `AT VERSION` / `AT COMMIT` / `AT TIME`, checkout, `_doc` + `kdb_json_*` DML routing; facade over `:kdb-sql`.
3. **19 — DAG Compaction** — squash + peer coordination hooks + orphan GC; **not** SSTable merge (`:kdb-storage-compaction` / 10f).

**Detailed execution plan:** `docs/kdb-spec-layer6-execution-plan.md`

**Estimated NBNC (Layer 6 production + tests):** ~6,500 lines (17: ~2,000; 18: ~1,500; 19: ~3,000).

#### LAYER 7 — Network Foundation (depends on Layer 6)

```
[x] 20. Storage Tier Manager         — `:kdb-storage-tier`; spec `kdb-spec-layer7-component20-storage-tier-manager.md`
[x] 21. Wire Protocol + Framing      — `:kdb-wire`; spec `kdb-spec-layer7-component21-wire-protocol-framing.md`
[x] 22. Stream Mode (Mode 1 + Mode 2)  — `:kdb-stream`; spec `kdb-spec-layer7-component22-stream-mode.md`
```

**Layer 7 implementation order (normative):**

1. **21 — Wire Protocol** — frame codec, handshake, encoding negotiation, message types `0x01`–`0x0D` (decode all; stream path uses subset).
2. **22 — Stream Mode** — coordinator/subscriber, `DeltaCommit` + `PositionAck`, Mode 2 `TransactionReplay`; `InMemoryWireTransport` for tests.
3. **20 — Storage Tier Manager** — WARM→COLD moves, ice bundle + `stubCommit`, restore to isolated namespace; subscribe to `TierSignalHooks`.

**Detailed execution plan:** `docs/kdb-spec-layer7-execution-plan.md`

**Estimated NBNC (Layer 7 production + tests):** ~9,000 lines (20: ~3,500; 21: ~3,000; 22: ~2,500).

#### LAYER 8 — Advanced Sync + JDBC (depends on Layer 7)

```
[x] 23. Peer Sync Mode (Mode 3)  — `:kdb-peer-sync`; spec `kdb-spec-layer8-component23-peer-sync-mode.md`
[x] 24. JDBC Driver              — `:kdb-jdbc`; spec `kdb-spec-layer8-component24-jdbc-driver.md`
```

**Layer 8 implementation order (normative):**

1. **23 — Peer Sync** — `CommitPush` payload codec; `PeerSyncHost` / `PeerSyncClient`; `computeSyncPlan`; in-memory wire tests.
2. **24 — JDBC Driver** — `KdbDriver`, memory URL, `EmbeddedKdbRuntime`, Statement/ResultSet/MetaData over `HybridQueryEngine`.

**Detailed execution plan:** `docs/kdb-spec-layer8-execution-plan.md`

**Estimated NBNC (Layer 8 production + tests):** ~9,500 lines (23: ~4,500; 24: ~5,000).

#### LAYER 9 — Platform Adapters (depends on Layer 7–8)

```
[x] 25. Transport Adapter — WebSocket   — `:kdb-transport-ws`; spec `kdb-spec-layer9-component25-transport-websocket.md`
[x] 26. Transport Adapter — TCP         — `:kdb-transport-tcp`, `:kdb-transport-core`; spec `kdb-spec-layer9-component26-transport-tcp.md`
[x] 27. Compute Adapter — WebGPU        — `:kdb-compute-webgpu`; spec `kdb-spec-layer9-component27-compute-webgpu.md`
[x] 28. Compute Adapter — CUDA/Vulkan   — `:kdb-compute-jvm`, `:kdb-compute`; spec `kdb-spec-layer9-component28-compute-cuda-vulkan.md`
```

**Layer 9 implementation order (normative):**

1. **transport-core + 26 — TCP** — `FrameStreamReader`/`Writer`; JVM + native loopback; `PeerSyncClient` over `kdb-tcp://`.
2. **25 — WebSocket** — binary message = one wire frame; jsMain client + jvmMain test server.
3. **`:kdb-compute` API + 28 (CPU)** — `ComputeAdapter` reference backend; hook `GpuStorageEngine` + vector index.
4. **27 — WebGPU** — WGSL cosine top-k v1; CPU fallback in headless CI.
5. **28 — CUDA/Vulkan** — optional JVM GPU backends behind `kdb.cuda` property.

**Detailed execution plan:** `docs/kdb-spec-layer9-execution-plan.md`

**Estimated NBNC (Layer 9 production + tests):** ~9,500 lines (core: ~350; 26: ~1,650; 25: ~1,500; compute API: ~200; 27: ~3,000; 28: ~2,800).

> **Note:** Storage adapters (IndexedDB, RocksDB, LMDB, mmap) are removed from this layer. Their role is replaced by the Platform I/O Shim in Layer 4a and the KDB Storage Engine running in commonMain.

#### LAYER 10 — Tooling (depends on everything)

```
[x] 31. Inspect / Debug Tooling       — `:kdb-inspect`; JSON sidecar + dump; spec: `kdb-spec-layer10-component31-inspect-tooling.md`
[x] 29. CLI                           — `:kdb-cli`; spec: `kdb-spec-layer10-component29-cli.md`
[x] 30. Integration Test Suite        — `:kdb-integration`; spec: `kdb-spec-layer10-component30-integration-test-suite.md`
```

**Layer 10 implementation order (normative):** 31 (inspect, early) → **29 CLI** → **30 integration suite**. Detailed plan: `docs/kdb-spec-layer10-execution-plan.md`.

**Estimated NBNC (Layer 10 production + tests):** ~4,200 lines (31: ~1,000; 29: ~1,800; 30: ~1,400).

#### LAYER 11 — RBAC + Stored Procedures (depends on Layer 10)

```
[x] —.  User Mgmt & Resource-Scoped RBAC  — `:kdb-auth`, `:kdb-auth-store`; not a numbered component; plan `kdb-rbac-plan.md`
[x] 32. Stored Procedure Engine           — `:kdb-script`; spec `kdb-spec-layer11-component32-stored-procedures.md`
```

**Layer 11 implementation order (normative):** RBAC and Component 32 have no dependency on each other and may run in parallel. RBAC's phases 1–4 (resource hierarchy, registry store, PBKDF2 password hashing, wire-layer enforcement) are done; a Go-side store/enforcement port and an admin SQL surface are still open (see `kdb-rbac-plan.md`'s own status). Component 32 is a single embedding (GraalVM Polyglot on the JVM inside `kdb-server`); the script never touches storage directly, only the same authorized `HybridQueryEngine`/`TransactionEngine` entry points ordinary SQL/document requests use — no new privileged path into storage. A wire protocol frame and CLI subcommand are not yet built (library-level API only).

**Detailed plan:** `docs/kdb-rbac-plan.md` (RBAC), `docs/kdb-spec-layer11-component32-stored-procedures.md` (Component 32, see its own §9/§11 for implementation phases/status).

#### LAYER 12 — Go-Native Server + Peer-Sync Hardening (depends on Layer 7–8, 11)

```
[x] 38.    Go-Native Server               — `go/kdb/server`; spec `kdb-spec-layer12-component38-go-native-server.md`
[x] 39.    Peer-Sync Conflict Detection   — `:kdb-peer-sync` + `go/kdb/peersync`; spec `kdb-spec-layer12-component39-peersync-conflict-detection.md`
[x] 40.    Go Client SDK                  — `go/kdb/client`; spec `kdb-spec-layer12-component40-go-client-sdk.md`
[x] 41.    Auth Session/Token Issuance    — `:kdb-auth`; spec `kdb-spec-layer12-component41-auth-tokens.md`
[x] 44–46. Minor fixes (notification bridge, disconnect cleanup, write-back mode) — spec'd inline, gap analysis §5
[ ] 42.    Native TCP Transport (embed)   — deferred; spec `kdb-spec-layer12-component42-native-transport.md`
[ ] 43.    Embed Durable + Mobile Storage — deferred; spec `kdb-spec-layer12-component43-embed-durable-storage.md`
```

**Layer 12 implementation order (normative):** a Phase 0 spike answers the Component 42/43 question first (does `go/kdb/embed` cross-compile under `gomobile bind` today, with enough feature coverage for on-device needs? — answered yes, for both Android and iOS, once `EmbeddedKdbRuntime.Release()` was renamed to `Close()` to clear an Objective-C ARC selector collision), which makes 42/43 very likely unnecessary. **38 and 39 have no dependency on each other** (different languages, different modules — `go/kdb/server` vs. `kdb-peer-sync`/`go/kdb/peersync`) and ran in parallel; **40** built against Component 38's wire shapes once confirmed; **41** is independent of everything else in the layer; **44–46** are minor, opportunistic fixes found while hardening 38/39 (a Go peer-sync client blind-head-move bug matching 39's own fix, and a file-backed `KdbServerRuntime` commit/persistence bug, were both found and fixed as follow-ons, not part of the original plan).

**Detailed execution plan:** `docs/kdb-spec-layer12-execution-plan.md`. **Master spec:** `docs/kdb-spec-layer12-zolik-gap-analysis.md`.

**Estimated NBNC (Layer 12 P0/P1, excl. deferred 42/43):** ~3,850–6,300 lines (38: ~1,800–2,800; 39: ~400–700; 40: ~600–1,000; 41: ~450–750; 44–46: ~600–1,050).

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

#### 1. Type System & Codec — `dev.kdb.codec`

> **Normative detail:** `docs/kdb-spec-layer0-codec.md` (v0.2). Dependents use `KdbValue`, schemas (`KdbType`, `RecordSchema`, …), and the binary / JSON bridges below.

```kotlin
package dev.kdb.codec

import dev.kdb.codec.schema.*
import kotlinx.io.Sink
import kotlinx.io.Source

// ── Primitive helpers (engine-facing ids, hashes, timestamps) ─────────────────

data class KdbUuid(val msb: Long, val lsb: Long) {
    override fun toString(): String
    companion object {
        fun random(): KdbUuid
        fun fromString(s: String): KdbUuid
        fun fromBytes(bytes: ByteArray): KdbUuid
    }
}

class KdbHash(bytes: ByteArray) {
    val bytes: ByteArray // copy-on-construct; `equals`/`hashCode` use digest contents (not array identity)
    fun toHex(): String
    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
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

// ── Typed value model (sum type — see spec for full sealed subclasses) ────────

sealed class KdbValue {
    data object Null : KdbValue()
    data class Bool(val v: Boolean) : KdbValue()
    data class Int64Val(val v: Long) : KdbValue()
    data class Float64Val(val v: Double) : KdbValue()
    data class StringVal(val v: String) : KdbValue()
    data class BytesVal(val v: ByteArray) : KdbValue()
    data class ArrayVal(val elements: List<KdbValue>) : KdbValue()
    data class MapVal(val entries: List<Pair<KdbValue, KdbValue>>) : KdbValue()
    data class RecordVal(val fields: Map<Int, KdbValue>) : KdbValue()
    // … plus remaining physical primitives, unions, enums, fixed,
    //   and logical variants (DateVal, TimestampVal, UuidVal, …) per Layer 0 §6.

    companion object {
        fun decodeFromBytes(bytes: ByteArray, type: KdbType, registry: KdbTypeRegistry): KdbValue
        fun decodeFrom(source: Source, type: KdbType, registry: KdbTypeRegistry): KdbValue
        fun fromJson(json: String, type: KdbType, registry: KdbTypeRegistry): KdbValue
    }
}

// ── Binary codec (canonical persistence / hashing wire form) ─────────────────

fun KdbValue.encodeToBytes(type: KdbType, registry: KdbTypeRegistry): ByteArray
fun KdbValue.encodeTo(sink: Sink, type: KdbType, registry: KdbTypeRegistry)
fun KdbValue.encodedSize(type: KdbType, registry: KdbTypeRegistry): Int

// ── JSON ↔ typed model (schema-guided; JSON only at the boundary) ─────────────

fun KdbValue.toJson(type: KdbType, registry: KdbTypeRegistry): String
fun KdbValue.toPrettyJson(type: KdbType, registry: KdbTypeRegistry, indent: Int = 2): String

// ── Kotlin ↔ KdbValue registry ─────────────────────────────────────────────────

interface KdbCodec<T : Any> {
    val schema: KdbType
    fun encode(value: T): KdbValue
    fun decode(value: KdbValue): T
}

object KdbCodecRegistry {
    fun <T : Any> register(kClass: kotlin.reflect.KClass<T>, codec: KdbCodec<T>)
    fun <T : Any> get(kClass: kotlin.reflect.KClass<T>): KdbCodec<T>?
    fun <T : Any> getOrThrow(kClass: kotlin.reflect.KClass<T>): KdbCodec<T>
}

fun KdbUuid.toUuidVal(): KdbValue.UuidVal
fun KdbValue.UuidVal.toKdbUuid(): KdbUuid

fun KdbTimestamp.toTimestampVal(): KdbValue.TimestampVal
fun KdbValue.TimestampVal.toKdbTimestamp(): KdbTimestamp

// Typed codec errors are `KdbDecodeException` / `KdbEncodeException` in `dev.kdb.error`.
```

#### 2. Error Model — `dev.kdb.error`

```kotlin
// ── Root + code enum ──────────────────────────────────────────────────────────

abstract class KdbException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    abstract val code: KdbErrorCode
}

enum class KdbErrorCode(val numericCode: Int) {
    KDB_DECODE_ERROR(1001),   // Layer 0 typed codec decode
    KDB_ENCODE_ERROR(1002),
    KDB_SCHEMA_ERROR(1005),
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

class KdbDecodeException(message: String, val offset: Int = -1, cause: Throwable? = null) : KdbException(message, cause)
class KdbEncodeException(message: String, cause: Throwable? = null) : KdbException(message, cause)
class KdbSchemaException(message: String, cause: Throwable? = null) : KdbException(message, cause)
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

> **Status: COMPLETE (v0.9)** — source modules `:kdb-document`, `:kdb-json`. Normative behaviour remains the component spec `.md` files; this section summarizes the public Kotlin API.

#### 3. Document + Commit Model — `dev.kdb.document`

```kotlin
import dev.kdb.codec.*

// ── Layer 0 wire registry (built-in `dev.kdb.document.*` schemas) ──────────────

fun KdbDocumentWireRegistry(): dev.kdb.codec.schema.KdbTypeRegistry

val DocumentBodyType: dev.kdb.codec.schema.KdbType
val CommitPayloadType: dev.kdb.codec.schema.KdbType
val KdbOpWireType: dev.kdb.codec.schema.KdbType
val DocumentTreeWireType: dev.kdb.codec.schema.KdbType
val CommitStubWireType: dev.kdb.codec.schema.KdbType

data class KdbDocument(
    val id: KdbUuid,
    val json: String,
) {
    val contentHash: KdbHash               // lazy; SHA-256 of `DocumentBody` bytes (Layer 0 record)

    fun toDocumentBodyValue(): KdbValue

    fun merge(patchJson: String): KdbDocument     // root-level shallow merge
    fun withJson(newJson: String): KdbDocument    // full body replacement, ID preserved

    companion object {
        fun fromJson(json: String): KdbDocument
        fun fromJson(id: KdbUuid, json: String): KdbDocument
        fun fromDocumentBodyValue(value: KdbValue): KdbDocument
    }
}

fun computeContentHash(doc: KdbDocument): KdbHash

// ── Operations ────────────────────────────────────────────────────────────────

sealed class KdbOp {
    data class Write(val docId: KdbUuid, val patch: String) : KdbOp()
    data class Delete(val docId: KdbUuid) : KdbOp()
    data class FileWrite(val path: String, val blobHash: KdbHash) : KdbOp()
    data class SchemaMigration(val migrationId: KdbUuid, val migrationPayload: String) : KdbOp()
}

fun KdbOp.toKdbValue(): KdbValue
fun KdbOp.Companion.fromKdbValue(value: KdbValue): KdbOp

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
fun KdbCommit.toPayloadBytes(): ByteArray
fun KdbCommit.toBytes(): ByteArray
fun KdbCommit.Companion.fromPayloadBytes(bytes: ByteArray): KdbCommit
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
        fun fromKdbValue(value: KdbValue): DocumentTree
    }
}

fun DocumentTree.toKdbValue(): KdbValue

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
) {
    fun toKdbValue(): KdbValue
    companion object {
        fun fromKdbValue(value: KdbValue): CommitStub
    }
}

// ── Exceptions ────────────────────────────────────────────────────────────────

class DocumentDecodeException(
    message: String,
    val docId: KdbUuid? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

class CommitDecodeException(
    message: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}
```

#### 4. JSON Functions Engine — `dev.kdb.json`

```kotlin
import dev.kdb.codec.KdbValue

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
    data object JNull : JsonValue()
    data class JObject(val fields: LinkedHashMap<String, JsonValue>) : JsonValue()
    data class JArray(val elements: List<JsonValue>) : JsonValue()

    fun toJsonString(): String
    fun toKdbValue(): KdbValue                 // structural bridge — see Layer 1 Component 4 spec

    companion object {
        fun fromJsonString(json: String): JsonValue
        fun fromKdbValue(value: KdbValue): JsonValue
    }
}

fun KdbValue.toJsonValue(): JsonValue

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

> **Status:** Implemented — modules `:kdb-schema` (Component 5) and `:kdb-dag` (Component 6); normative behaviour remains in the Layer 2 component spec files.

> **Encoding:** Normative detail in `kdb-spec-layer2-component5-schema-engine.md` and `kdb-spec-layer2-component6-commit-dag.md`. **Schema** snapshots and migrations use Layer 0 typed binary via **`KdbSchemaWireRegistry()`** and **`encodeToBytes` / `decodeFromBytes`**. **Commits and document trees** in the DAG use the Layer 1 wire shapes (**`KdbCommit.toPayloadBytes()`**, **`CommitPayloadType`**, **`DocumentTree`**, **`DocumentTreeWireType`**, **`KdbDocumentWireRegistry()`**). Structured binary interchange is Layer 0 only throughout.

> **`CommitDag`:** Implementations supply **`lookupHashPrefix`** for opaque-store hash scans. **`resolveRef`** / **`resolveRefOrThrow`** are **default methods on the interface** (Kotlin MPP), implemented in terms of branches/tags/`walk` plus **`lookupHashPrefix`** for `ByHash`.

#### 5. Schema Engine — `dev.kdb.schema`

```kotlin
import dev.kdb.codec.*
import dev.kdb.document.KdbDocument
import dev.kdb.error.*
import dev.kdb.json.JsonValue

// ── Layer 0 registry (`dev.kdb.schema.*` wire shapes) ──────────────────────────

fun KdbSchemaWireRegistry(): dev.kdb.codec.schema.KdbTypeRegistry

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
    data class EnumType(val values: Set<String>) : KdbFieldType() {
        init { require(values.isNotEmpty()) { "EnumType must have at least one value" } }
    }

    fun sqlTypeName(): String
    /** JDBC / introspection hint aligned with Layer 0 physical mapping. */
    fun codecTypeLabel(): String
}

// ── Field declaration ─────────────────────────────────────────────────────────

data class SchemaField(
    val name: String,
    val type: KdbFieldType,
    val required: Boolean,
    val indexed: Boolean,
    val unique: Boolean = false,
) {
    init {
        require(name.matches(Regex("[a-zA-Z_][a-zA-Z0-9_]*"))) {
            "Field name must be a valid identifier: $name"
        }
        require(!(unique && !indexed)) { "unique=true requires indexed=true: $name" }
    }
}

// ── Schema declaration ─────────────────────────────────────────────────────────

data class KdbSchema(
    /** SHA-256 of canonical Layer 0 bytes of the schema wire record. */
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

fun KdbSchema.toKdbValue(): KdbValue
fun KdbSchema.Companion.fromKdbValue(value: KdbValue): KdbSchema

/** Canonical typed-binary form via [KdbSchemaWireRegistry] and the registered schema wire type. */
fun KdbSchema.toBytes(): ByteArray
fun KdbSchema.Companion.fromBytes(bytes: ByteArray): KdbSchema

// ── Migration DSL ─────────────────────────────────────────────────────────────

data class SchemaMigration(
    val migrationId: KdbUuid,
    val fromVersion: Int,
    val toVersion: Int,
    val steps: List<MigrationStep>,
    val description: String = "",
) {
    companion object {
        fun fromKdbValue(value: KdbValue): SchemaMigration
        fun fromBytes(bytes: ByteArray): SchemaMigration
    }
}

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

    /** SHA-256 of canonical Layer 0 bytes for [schema]. */
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

fun SchemaMigration.toKdbValue(): KdbValue
fun SchemaMigration.toBytes(): ByteArray

// ── Exceptions ────────────────────────────────────────────────────────────────

class SchemaDecodeException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
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
// Commits persist via Layer 1: KdbCommit.toPayloadBytes() / fromPayloadBytes(), CommitPayloadType,
// KdbDocumentWireRegistry(). Document trees: DocumentTree + DocumentTreeWireType (same registry).

// ── DAG store interface ────────────────────────────────────────────────────────

interface CommitDag {
    val namespaceId: String

    /** Returns commits whose canonical lowercase hex starts with [hexPrefixLower]. */
    suspend fun lookupHashPrefix(hexPrefixLower: String): List<KdbHash>

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

    /** Default implementation resolves branches/tags/time/`ByHash` via [lookupHashPrefix] + graph accessors. */
    suspend fun resolveRef(ref: CommitRef): KdbHash?

    suspend fun resolveRefOrThrow(ref: CommitRef): KdbHash
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

// resolveRef / resolveRefOrThrow: default methods on CommitDag (see note above).

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

> **Status: IMPLEMENTED (first Kotlin cut) / §17 PENDING SYNC** — the modules `:kdb-storage`, `:kdb-index`, and `:kdb-transaction` now contain the runnable contracts. Treat the excerpts below as design shorthand until they are refreshed from source. Notable deltas already known: `TransactionBuilder` methods are `suspend` (Mutex-backed buffering), `suspend fun transactionBuilder(...)` snaps `dag.head()` as `baseVersion`, migration ops round-trip via `SchemaMigrationCodec` (`toBytes` / `fromBytes` from `SchemaSerialization.kt`), and `OperationConflict.baseDoc` is populated for merge/replay tooling.

#### 7. Transaction Engine — `dev.kdb.transaction`

```kotlin
// ── Conflict policy ───────────────────────────────────────────────────────────

enum class ConflictPolicy {
    APPEND_ONLY,
    LAST_WRITE,
    STRICT,
    CUSTOM,
}

// ── Custom resolver ───────────────────────────────────────────────────────────

fun interface ConflictResolver {
    suspend fun resolve(conflict: DocumentConflict): KdbDocument?
}

data class DocumentConflict(
    val docId: KdbUuid,
    val operationType: ConflictOperationType,
    val existingDoc: KdbDocument?,
    val incomingDoc: KdbDocument?,
    val baseDoc: KdbDocument?,
)

// ── Transaction builder ───────────────────────────────────────────────────────

class TransactionBuilder(
    val namespaceId: String,
    val baseVersion: KdbHash,
    val authorNodeId: KdbUuid,
    val schema: KdbSchema = KdbSchema.NONE,
) {
    suspend fun write(docId: KdbUuid, patchJson: String): TransactionBuilder
    suspend fun writeDocument(document: KdbDocument): TransactionBuilder
    suspend fun delete(docId: KdbUuid): TransactionBuilder
    suspend fun fileWrite(path: String, blobHash: KdbHash): TransactionBuilder
    suspend fun schemaMigration(migration: SchemaMigration): TransactionBuilder
    suspend fun build(timestamp: KdbTimestamp = KdbTimestamp.now()): KdbTransaction
}

// ── Transaction engine ────────────────────────────────────────────────────────

interface TransactionEngine {
    val conflictPolicy: ConflictPolicy
    val customResolver: ConflictResolver?

    suspend fun commit(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        targetHead: KdbHash? = null,
        message: String = "",
    ): TransactionResult

    suspend fun replay(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        replayTarget: KdbHash,
        message: String = "",
    ): TransactionResult

    suspend fun merge(
        primaryHead: KdbHash,
        mergedHead: KdbHash,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        message: String = "",
    ): TransactionResult

    suspend fun validate(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
    ): List<OperationViolation>
}

// ── Transaction result ────────────────────────────────────────────────────────

sealed class TransactionResult {
    data class Success(val commit: KdbCommit, val newTreeHash: KdbHash) : TransactionResult()
    data class Conflict(val report: ConflictReport, val conflictingOps: List<OperationConflict>) : TransactionResult()
    data class SchemaError(val violations: List<OperationViolation>) : TransactionResult()
}

data class OperationConflict(
    val opIndex: Int,
    val op: KdbOp,
    val type: ConflictOperationType,
    val existingDoc: KdbDocument?,
    val incomingDoc: KdbDocument?,
    val baseDoc: KdbDocument?,
)

data class OperationViolation(
    val opIndex: Int,
    val op: KdbOp,
    val violations: List<FieldViolation>,
)

sealed class DocWriteOutcome {
    data class Written(val newDoc: KdbDocument, val contentHash: KdbHash) : DocWriteOutcome()
    data class Deleted(val docId: KdbUuid) : DocWriteOutcome()
    data class Conflicted(val conflict: OperationConflict) : DocWriteOutcome()
    data class SchemaRejected(val violation: OperationViolation) : DocWriteOutcome()
}

// ── Factory ───────────────────────────────────────────────────────────────────

fun transactionEngine(conflictPolicy: ConflictPolicy, customResolver: ConflictResolver? = null): TransactionEngine

suspend fun transactionBuilder(namespaceId: String, dag: CommitDag, authorNodeId: KdbUuid, schema: KdbSchema = KdbSchema.NONE): TransactionBuilder

// ── Exceptions ────────────────────────────────────────────────────────────────

class TransactionBaseNotFoundException(message: String, val transactionId: KdbUuid, val missingHash: KdbHash) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}
class TransactionSchemaException(message: String, val transactionId: KdbUuid, val violations: List<OperationViolation>) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}
class MergeBaseNotFoundException(message: String, val primaryHead: KdbHash, val mergedHead: KdbHash) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}
```

#### 8. Index Layer — Core — `dev.kdb.index`

```kotlin
// ── Index type ────────────────────────────────────────────────────────────────

enum class IndexType { HASH, BTREE, FULLTEXT, VECTOR }

// ── Index descriptor + entry + key ───────────────────────────────────────────

data class IndexDescriptor(
    val indexId: KdbUuid,
    val namespaceId: String,
    val fieldName: String,
    val fields: List<String>,
    val type: IndexType,
    val unique: Boolean,
    val schemaVersion: Int,
    val createdAtHash: KdbHash,
)

data class IndexEntry(val docId: KdbUuid, val key: IndexKey, val commitHash: KdbHash)

sealed class IndexKey {
    data class StringKey(val value: String)          : IndexKey()
    data class Int32Key(val value: Int)              : IndexKey()
    data class Int64Key(val value: Long)             : IndexKey()
    data class Float64Key(val value: Double)         : IndexKey()
    data class BoolKey(val value: Boolean)           : IndexKey()
    data class TimestampKey(val epochMillis: Long)   : IndexKey()
    data class UuidKey(val id: KdbUuid)              : IndexKey()
    data class VectorKey(val embedding: FloatArray)  : IndexKey()
    data class CompositeKey(val parts: List<IndexKey>) : IndexKey()
    object NullKey                                   : IndexKey()
}

fun indexKeyFromJsonValue(value: JsonValue?, fieldType: KdbFieldType): IndexKey

// ── Index store ───────────────────────────────────────────────────────────────

interface IndexStore {
    val descriptor: IndexDescriptor
    suspend fun put(entry: IndexEntry)
    suspend fun delete(docId: KdbUuid, atCommit: KdbHash)
    suspend fun bulkLoad(entries: List<IndexEntry>)
    suspend fun lookup(key: IndexKey, atCommit: KdbHash? = null): List<KdbUuid>
    suspend fun range(from: IndexKey?, to: IndexKey?, atCommit: KdbHash? = null, limit: Int = Int.MAX_VALUE, ascending: Boolean = true): List<KdbUuid>
    suspend fun search(query: String, atCommit: KdbHash? = null, limit: Int = Int.MAX_VALUE): List<KdbUuid>
    suspend fun nearestNeighbours(queryVector: FloatArray, k: Int, atCommit: KdbHash? = null): List<RankedResult>
    suspend fun rebuild(entries: List<IndexEntry>)
    suspend fun clear()
    suspend fun isValid(atCommit: KdbHash): Boolean
    suspend fun snapshot(): ByteArray
    suspend fun restoreSnapshot(data: ByteArray)
}

data class RankedResult(val docId: KdbUuid, val score: Float)

// ── Index registry ────────────────────────────────────────────────────────────

interface IndexRegistry {
    val namespaceId: String
    val indexes: List<IndexStore>
    fun get(fieldName: String, type: IndexType): IndexStore?
    fun getById(indexId: KdbUuid): IndexStore?
    suspend fun syncSchema(oldSchema: KdbSchema, newSchema: KdbSchema, storeFactory: IndexStoreFactory, dag: CommitDag, storage: StorageAdapter): SchemaSyncResult
}

data class SchemaSyncResult(
    val created: List<IndexDescriptor>,
    val removed: List<IndexDescriptor>,
    val unchanged: List<IndexDescriptor>,
    val rebuilding: List<IndexDescriptor>,
)

// ── Index writer + reader ─────────────────────────────────────────────────────

interface IndexWriter {
    suspend fun applyCommit(commit: KdbCommit, registry: IndexRegistry, storage: StorageAdapter, schema: KdbSchema)
    suspend fun rebuildAll(fromCommit: KdbHash, dag: CommitDag, registry: IndexRegistry, storage: StorageAdapter, schema: KdbSchema, onProgress: ((rebuilt: Int, total: Int) -> Unit)? = null)
}

interface IndexReader {
    suspend fun lookupExact(registry: IndexRegistry, fieldName: String, key: IndexKey, atCommit: KdbHash? = null): List<KdbUuid>
    suspend fun lookupRange(registry: IndexRegistry, fieldName: String, from: IndexKey?, to: IndexKey?, atCommit: KdbHash? = null, limit: Int = Int.MAX_VALUE, ascending: Boolean = true): List<KdbUuid>
    suspend fun lookupFullText(registry: IndexRegistry, fieldName: String, query: String, atCommit: KdbHash? = null, limit: Int = Int.MAX_VALUE): List<KdbUuid>
    suspend fun lookupVector(registry: IndexRegistry, fieldName: String, queryVector: FloatArray, k: Int, atCommit: KdbHash? = null): List<RankedResult>
}

fun interface IndexStoreFactory { fun create(descriptor: IndexDescriptor): IndexStore }

// ── Index manager ─────────────────────────────────────────────────────────────

interface IndexManager {
    fun registryFor(namespaceId: String): IndexRegistry
    suspend fun releaseRegistry(namespaceId: String)
    val writer: IndexWriter
    val reader: IndexReader
}

fun indexManager(storeFactory: IndexStoreFactory): IndexManager

// ── Index hint ────────────────────────────────────────────────────────────────

data class IndexHint(
    val indexId: KdbUuid,
    val fieldName: String,
    val type: IndexType,
    val action: IndexHintAction,
    val docId: KdbUuid,
    val key: IndexKey?,
    val commitHash: KdbHash,
)

enum class IndexHintAction { PUT, DELETE }

fun IndexHint.toBytes(): ByteArray              // Layer 0 encoded hint record (draft)
fun IndexHint.Companion.fromBytes(bytes: ByteArray): IndexHint

// ── Exceptions ────────────────────────────────────────────────────────────────

class IndexNotFoundException(message: String, val namespaceId: String, val fieldName: String, val type: IndexType) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
class IndexTypeMismatchException(message: String, val fieldName: String, val expectedType: IndexType, val actualType: IndexType) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
class IndexRebuildException(message: String, val namespaceId: String, val indexId: KdbUuid, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
```

#### 9. Storage Adapter Interface — `dev.kdb.storage`

```kotlin
// ── Capability + compression ──────────────────────────────────────────────────

enum class CompressionCodec { NONE, ZSTD }

data class StorageCapabilitySet(
    val persistsDeltaLog: Boolean,
    val persistsAcrossReload: Boolean,
    val supportsGpuBulkRead: Boolean,
    val supportsDirectDeltaIngest: Boolean,
    val maxEnlistments: Int?,
    val indexRetentionDefault: IndexRetention,
)

enum class IndexRetention { PINNED, EVICTABLE }
enum class EnlistmentEvictionState { FULL, DOC_EVICTED, EVICTED, RELEASED }

data class IndexPinViolationEvent(
    val namespaceId: String,
    val enlistmentId: KdbUuid,
    val currentPressureBytes: Long,
    val pinnedIndexSizeBytes: Long,
)

// ── Delta record + authorship envelope ───────────────────────────────────────

data class DeltaAuthorshipEnvelope(
    val principal: String,
    val timestamp: KdbTimestamp,
    val rightsToken: String,
    val clientContext: String,
)

data class DeltaRecord(
    val commitHash: KdbHash,
    val namespaceId: String,
    val authorship: DeltaAuthorshipEnvelope,
    /** Canonical persisted commit payload (`CommitPayload` + hash), Layer 0 binary — see Layer 1 Component 3. */
    val commitWireBytes: ByteArray,
    val documentPatches: List<DocumentPatch>,
)

data class DocumentPatch(
    val docId: KdbUuid,
    /** JSON document body immediately before op; null on insert. */
    val beforeJson: String?,
    /** JSON document body immediately after op; null on delete. */
    val afterJson: String?,
    val contentHashAfter: KdbHash?,
)

data class DeltaSegmentRef(
    val segmentId: KdbUuid,
    val namespaceId: String,
    val firstCommitHash: KdbHash,
    val lastCommitHash: KdbHash,
    val sizeBytes: Long,
    val compressionCodec: CompressionCodec,
)

// ── Delta segment writer + reader ─────────────────────────────────────────────

interface DeltaSegmentWriter {
    val namespaceId: String
    val segmentId: KdbUuid
    suspend fun append(record: DeltaRecord): Long
    suspend fun flush()
    suspend fun seal(): DeltaSegmentRef
    val currentSizeBytes: Long
    val isSealed: Boolean
}

interface DeltaSegmentReader {
    val namespaceId: String
    suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord>
    suspend fun readRange(segment: DeltaSegmentRef, sinceCommit: KdbHash, untilCommit: KdbHash): List<DeltaRecord>
    suspend fun listSegments(): List<DeltaSegmentRef>
}

// ── Storage adapter ───────────────────────────────────────────────────────────

interface StorageAdapter {
    val capabilities: StorageCapabilitySet
    suspend fun getDocument(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument?
    suspend fun getDocumentOrThrow(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument
    suspend fun getDocuments(namespaceId: String, docIds: List<KdbUuid>, atCommit: KdbHash): List<KdbDocument?>
    suspend fun scanDocuments(namespaceId: String, atCommit: KdbHash, batchSize: Int = 256, onBatch: suspend (List<KdbDocument>) -> Unit)
    suspend fun putDocument(namespaceId: String, document: KdbDocument)
    suspend fun deleteDocument(namespaceId: String, docId: KdbUuid)
    suspend fun commitTree(namespaceId: String, parentTreeHash: KdbHash): DocumentTree
    suspend fun flush(namespaceId: String)
    suspend fun readBlob(contentHash: KdbHash): ByteArray?
    suspend fun writeBlob(bytes: ByteArray): KdbHash
    suspend fun ingestDeltaSegment(segment: DeltaSegmentRef)
}

// ── Evictable storage adapter ─────────────────────────────────────────────────

interface EvictableStorageAdapter : StorageAdapter {
    suspend fun evictDocuments(enlistmentId: KdbUuid)
    suspend fun evictIndex(enlistmentId: KdbUuid)
    suspend fun rebuildDocuments(enlistmentId: KdbUuid, fromDeltaLog: DeltaSegmentReader)
    suspend fun rebuildIndex(enlistmentId: KdbUuid, fromDocuments: StorageAdapter)
    fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState
}

// ── Realized store handle + enlistment handle ─────────────────────────────────

enum class RebuildBlockingPolicy { WAIT, PARTIAL_OK }

interface RealizedStoreHandle : AutoCloseable {
    val namespaceId: String
    val commitHash: KdbHash
    val enlistmentId: KdbUuid
    val isReady: Boolean
    suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy = RebuildBlockingPolicy.WAIT)
    val storage: StorageAdapter
    override fun close()
    fun release() = close()
    fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit)
}

enum class EnlistmentPushState { IDLE, PUSHING, REJECTED, RESOLVING }

sealed class PushResult {
    object Success : PushResult()
    data class Rejected(val missingDeltaHashes: List<KdbHash>) : PushResult()
}

sealed class SnapshotRestoreResult {
    data class Restored(val anchorHash: KdbHash) : SnapshotRestoreResult()
    data class Failed(val reason: SnapshotFailureReason) : SnapshotRestoreResult()
    object AnchorCompactedAway : SnapshotRestoreResult()
}

enum class SnapshotFailureReason { NOT_FOUND, INTEGRITY_CHECK_FAILED, DESERIALIZATION_ERROR, ANCHOR_COMPACTED_AWAY }

interface EnlistmentHandle : RealizedStoreHandle {
    val branchRef: String
    val pushState: EnlistmentPushState
    suspend fun push(): PushResult
    suspend fun fetchMissing()
    suspend fun resolveAndPush(): PushResult
    val snapshotAnchorHash: KdbHash?
    suspend fun writeSnapshot()
    suspend fun restoreSnapshot(): SnapshotRestoreResult
}

// ── Platform I/O shim (expect/actual boundary) ────────────────────────────────

expect interface PlatformIoShim {
    suspend fun appendToSegment(segmentName: String, bytes: ByteArray): Long
    suspend fun readFromSegment(segmentName: String, offset: Long, length: Int): ByteArray
    suspend fun flushSegment(segmentName: String)
    suspend fun sealSegment(segmentName: String)
    suspend fun listSegments(namespaceId: String): List<String>
    suspend fun deleteSegment(segmentName: String)
    suspend fun availableBytes(): Long
    suspend fun readSnapshot(key: String): ByteArray?
    suspend fun writeSnapshot(key: String, data: ByteArray)
    suspend fun deleteSnapshot(key: String)
}

// ── Config + GPU policy ───────────────────────────────────────────────────────

data class StorageEngineConfig(
    val pageTargetSizeBytes: Long = 8L * 1024 * 1024,
    val pageMaxSizeBytes: Long = 16L * 1024 * 1024,
    val globalMemoryBudgetBytes: Long,
    val compressionCodec: CompressionCodec = CompressionCodec.ZSTD,
    val defaultIndexRetention: IndexRetention = IndexRetention.EVICTABLE,
    val ioShim: PlatformIoShim,
)

enum class GpuPromotionStrategy { PROMOTE_ON_QUERY, PROMOTE_EAGERLY, NEVER }

data class GpuPromotionPolicy(
    val strategy: GpuPromotionStrategy,
    val minSegmentAgeMillis: Long = 5 * 60 * 1000L,
    val minSegmentSizeBytes: Long = 64L * 1024 * 1024,
    val maxChangeRatePerMinute: Int = 100,
)

// ── Exceptions ────────────────────────────────────────────────────────────────

class DocumentNotFoundException(message: String, val namespaceId: String, val docId: KdbUuid, val atCommit: KdbHash) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}
class StorageAdapterException(message: String, val namespaceId: String, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
class DeltaSegmentSealedException(message: String, val segmentId: KdbUuid) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
class SnapshotIntegrityException(message: String, val key: String, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
class EnlistmentNotFoundException(message: String, val enlistmentId: KdbUuid) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}
```

### Layer 4a Interfaces — KDB Storage Engine

> **Status: IMPLEMENTED (first Kotlin cut)** — modules `:kdb-compression`, `:kdb-storage-io`, `:kdb-storage-wal`, `:kdb-storage-memtable`, `:kdb-storage-sstable`, `:kdb-storage-delta`, `:kdb-storage-engine`, `:kdb-storage-compaction`. Normative detail remains in `docs/kdb-spec-layer4a-component10*.md`. Entry points: `FileBackedPlatformIoShimFactory.open`, `DefaultWriteAheadLogFactory`, `DefaultSsTableWriter`/`Reader`, `MemTableManager`, `DefaultDeltaSegmentWriter`, `DefaultStorageEngineFactory.forTarget`, `runSstableCompaction`.

| Component | Module | Primary packages |
|---|---|---|
| 10g Platform I/O Shim | `:kdb-storage-io` | `dev.kdb.storage.io` — `SegmentNameBuilder`, `FileBackedPlatformIoShimFactory` |
| 10a WAL | `:kdb-storage-wal` | `dev.kdb.storage.wal` — `WriteAheadLog`, `DefaultWriteAheadLog` |
| 10b MemTable | `:kdb-storage-memtable` | `dev.kdb.storage.memtable` — `SortedMemTable`, `MemTableManager` |
| 10c SSTable + Block Cache | `:kdb-storage-sstable` | `dev.kdb.storage.sstable` — `SsTableWriter`, `BlockCache`, `LsmBlobStore` |
| 10d Delta Segment Writer | `:kdb-storage-delta` | `dev.kdb.storage.delta` — `DefaultDeltaSegmentWriter` |
| 10e Storage Engine Core | `:kdb-storage-engine` | `dev.kdb.storage.engine` — `ServerStorageEngine`, `StorageEngineFactory` |
| 10f Storage Compaction | `:kdb-storage-compaction` | `dev.kdb.storage.compaction` — `CompactionPlanner`, `runSstableCompaction` |

### Layer 4b Interfaces — Storage Manager

> **Status: IMPLEMENTED (first Kotlin cut)** — module `:kdb-storage-manager`. Exposes `StorageManager.install/get`, `DefaultStorageManager`, `EvictionManager`, `RebuildScheduler`, `EnlistmentManager`, `DeltaLogTierRegistry`.

| Component | Module | Primary packages |
|---|---|---|
| 11a Realized Store Pool | `:kdb-storage-manager` | `dev.kdb.storage.manager.pool` |
| 11b Eviction Manager | `:kdb-storage-manager` | `dev.kdb.storage.manager.eviction` |
| 11c Rebuild Scheduler | `:kdb-storage-manager` | `dev.kdb.storage.manager.rebuild` |
| 11d Enlistment Manager | `:kdb-storage-manager` | `dev.kdb.storage.manager.enlistment` |
| 11e Delta Log Tier Signals | `:kdb-storage-manager` | `dev.kdb.storage.manager.tier` |

### Layer 5 Interfaces — Index + Query

> **Status: IMPLEMENTED (first Kotlin cut)** — normative detail in `docs/kdb-spec-layer5-component12-*.md` … `component16-*.md`.

| Component | Module(s) | Primary entry points |
|---|---|---|
| 12 Hash + B-tree | `:kdb-index-hash`, `:kdb-index-btree`, `:kdb-index` (`VersionedIndexEngine`) | `hashIndexStoreFactory`, `btreeIndexStoreFactory` |
| 13 Full-text | `:kdb-index-fulltext` | `fullTextIndexStoreFactory`, `parseFullTextQuery` |
| 14 Vector | `:kdb-index-vector` | `vectorIndexStoreFactory(dag, storage, dimensions)` |
| Composite | `:kdb-index-composite` | `compositeIndexStoreFactory`, `productionIndexManager` |
| 15 SQL | `:kdb-sql` | `sqlEngine`, `defaultSqlParser`, `DefaultQueryPlanner` |
| 16 Virtual views | `:kdb-sql` | `virtualViewRegistry`, `virtualViewEngine`, `InMemoryVirtualViewRegistry` |

```kotlin
// Bootstrap
fun productionIndexManager(dag: CommitDag, storage: StorageAdapter, vectorDimensions: Int = 128): IndexManager
fun compositeIndexStoreFactory(dag: CommitDag, storage: StorageAdapter, vectorDimensions: Int = 128): IndexStoreFactory
fun sqlEngine(indexManager: IndexManager, storage: StorageAdapter, dag: CommitDag, ...): SqlEngine
```

### Layer 6 Interfaces — Hybrid Query + Policy

> **Status: IMPLEMENTED (first Kotlin cut)** — normative detail in `docs/kdb-spec-layer6-component17-*.md` … `component19-*.md`.

| Component | Module | Primary entry points |
|---|---|---|
| 17 Hybrid Query | `:kdb-hybrid-query` | `hybridQueryEngine`, `hybridSqlParser`, `defaultVersionResolver`, `CheckoutStore` |
| 18 Namespace Policy | `:kdb-namespace-policy` | `namespacePolicyRegistry`, `inMemoryNamespacePolicyRegistry`, `defaultNamespacePolicyParser`, `DefaultCompactionPolicyEvaluator` |
| 19 Compaction (DAG) | `:kdb-compaction` | `compactionEngine`, `InProcessCompactionCoordinator`, `DefaultSnapshotMaterializer` |

```kotlin
package dev.kdb.policy

interface NamespacePolicyRegistry {
    suspend fun get(namespaceId: String): NamespacePolicy
    suspend fun getOrNull(namespaceId: String): NamespacePolicy?
    suspend fun put(policy: NamespacePolicy)
    suspend fun delete(namespaceId: String): Boolean
    suspend fun list(): List<String>
}
fun namespacePolicyRegistry(storage: StorageAdapter): NamespacePolicyRegistry
fun inMemoryNamespacePolicyRegistry(): NamespacePolicyRegistry

data class NamespacePolicy(
    val namespaceId: String,
    val schema: KdbSchema?,
    val mode: NamespaceMode,
    val history: HistoryMode,
    val conflict: ConflictPolicy,
    val compaction: CompactionPolicy,
    val tiers: TierPolicy = TierPolicy(),
    val indexRetentionDefault: IndexRetention = IndexRetention.EVICTABLE,
    val revision: Long = 1L,
)
enum class HistoryMode { FULL, NONE }
enum class SquashMode { AUTO, NEVER }

interface CompactionPolicyEvaluator {
    fun boundaryCandidates(...): List<CompactionBoundaryPlan>
}
object DefaultCompactionPolicyEvaluator : CompactionPolicyEvaluator

package dev.kdb.query.hybrid

interface HybridQueryEngine {
    suspend fun execute(sql: String, request: HybridQueryRequest): HybridQueryResult
    suspend fun explain(sql: String, request: HybridQueryRequest): ExplainResult
    suspend fun checkout(namespaceId: String, ref: CommitRef): CheckoutHandle
    suspend fun resetCheckout(namespaceId: String)
}
fun hybridQueryEngine(sql: SqlEngine, dag: CommitDag, policyRegistry: NamespacePolicyRegistry, ...): HybridQueryEngine

sealed class VersionClause {
    data class AtTag(val tag: String) : VersionClause()
    data class AtCommit(val hex: String) : VersionClause()
    data class AtTime(val iso8601: String) : VersionClause()
}

package dev.kdb.compaction

interface CompactionEngine {
    suspend fun runCycle(request: CompactionRequest): CompactionResult
    suspend fun plan(request: CompactionRequest): CompactionPlan
    fun updatePeerHeads(namespaceId: String, heads: Map<String, KdbHash>)
}
fun compactionEngine(dag: CommitDag, storage: StorageAdapter, policyRegistry: NamespacePolicyRegistry, ...): CompactionEngine
```

### Layer 7 Interfaces

| Component | Module | Key types |
|---|---|---|
| 20 Storage Tier Manager | `:kdb-storage-tier` | `storageTierManager`, `DefaultIceBundleWriter`, `inMemoryTierBackendRegistry` |
| 21 Wire Protocol | `:kdb-wire` | `defaultWireCodec`, `WireMessage`, `defaultHandshakeNegotiator` |
| 22 Stream Mode | `:kdb-stream` | `streamCoordinator`, `streamSubscriber`, `InMemoryWireTransport` |

```kotlin
package dev.kdb.tier

interface StorageTierManager {
    fun start()
    fun stop()
    suspend fun runCycle(namespaceId: String): TierCycleResult
    suspend fun archiveCommit(request: ArchiveRequest): ArchiveResult
    suspend fun restoreArchive(request: RestoreRequest): RestoreResult
}
fun storageTierManager(dag: CommitDag, storage: StorageAdapter, tierRegistry: DeltaLogTierRegistry, ...): StorageTierManager

package dev.kdb.wire

const val KDB_WIRE_PROTOCOL_VERSION: Int = 1
interface WireCodec {
    fun encode(message: WireMessage): ByteArray
    fun decode(frame: ByteArray): WireMessage
}
fun defaultWireCodec(encoding: PayloadEncoding = PayloadEncoding.KDB_BINARY): WireCodec

sealed class WireMessage { /* Handshake, DeltaCommit, PositionAck, … */ }

package dev.kdb.stream

interface StreamCoordinator {
    suspend fun start(session: StreamSessionConfig)
    suspend fun publish(commit: PublishedCommit)
}
interface StreamSubscriber {
    suspend fun connect(config: StreamSubscriberConfig): StreamConnection
}
fun streamCoordinator(wire: WireCodec, transport: WireTransport, ...): StreamCoordinator
fun streamSubscriber(wire: WireCodec, transport: WireTransport, ...): StreamSubscriber

interface WireTransport {
    suspend fun connect(uri: String): WireConnection
}
```

### Layer 8 Interfaces

| Component | Module | Key types |
|---|---|---|
| 23 Peer Sync Mode 3 | `:kdb-peer-sync` | `peerSyncHost`, `peerSyncClient`, `computeSyncPlan` |
| 24 JDBC Driver | `:kdb-jdbc` | `KdbDriver`, `openMemoryRuntime`, `KdbConnection` |

```kotlin
package dev.kdb.peersync

interface PeerSyncHost {
    suspend fun start(config: PeerHostConfig)
    suspend fun stop()
    suspend fun handleFrame(frame: ByteArray): ByteArray?
}
interface PeerSyncClient {
    suspend fun connect(config: PeerClientConfig): PeerSession
}
interface PeerSession {
    val remoteHead: KdbHash
    suspend fun pullMissing(): PeerSyncResult
    suspend fun syncBidirectional(): PeerSyncResult
}
fun peerSyncHost(wire: WireCodec, dag: CommitDag, storage: StorageAdapter, ...): PeerSyncHost
fun peerSyncClient(wire: WireCodec, transport: WireTransport, dag: CommitDag, ...): PeerSyncClient
suspend fun computeSyncPlan(dag: CommitDag, localHead: KdbHash, remoteHead: KdbHash): DagSyncPlan

package dev.kdb.wire
object CommitPushCodec {
    fun encodeCommits(commits: List<KdbCommit>): ByteArray
    fun decodeCommits(bytes: ByteArray): List<KdbCommit>
}

package dev.kdb.jdbc

class KdbDriver : java.sql.Driver
class KdbConnection(...) : java.sql.Connection
fun openMemoryRuntime(catalog: String, namespaceId: String, schema: KdbSchema): EmbeddedKdbRuntime
```

### Layer 9 Interfaces

> **Normative detail:** `kdb-spec-layer9-component25-*.md` through `28-*.md`, `kdb-spec-layer9-execution-plan.md`.

| Component | Module | Key types |
|---|---|---|
| (shared) | `:kdb-transport-core` | `FrameStreamReader`, `FrameFramer`, `TransportConnectOptions` |
| 25 WebSocket | `:kdb-transport-ws` | `WebSocketWireTransport`, `defaultWebSocketWireTransport()` |
| 26 TCP | `:kdb-transport-tcp` | `TcpWireTransport`, `defaultTcpWireTransport()` |
| (shared) | `:kdb-compute` | `ComputeAdapter`, `GpuVectorSearchRequest` |
| 27 WebGPU | `:kdb-compute-webgpu` | `createWebGpuComputeAdapter()` |
| 28 JVM compute | `:kdb-compute-jvm` | `createJvmComputeAdapter()`, `probeComputeAdapter()` |

```kotlin
package dev.kdb.transport.core

class FrameStreamReader(maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES)
data class TransportConnectOptions(
    val connectTimeoutMs: Long = 10_000,
    val readTimeoutMs: Long = 0,
    val maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES,
)

package dev.kdb.transport.tcp
interface TcpWireTransport : dev.kdb.stream.WireTransport {
    suspend fun listen(uri: String, handler: suspend (dev.kdb.stream.WireConnection) -> Unit)
}
fun defaultTcpWireTransport(): TcpWireTransport

package dev.kdb.transport.ws
interface WebSocketWireTransport : dev.kdb.stream.WireTransport {
    suspend fun listen(uri: String, handler: suspend (dev.kdb.stream.WireConnection) -> Unit)
}
fun defaultWebSocketWireTransport(): WebSocketWireTransport

package dev.kdb.compute

interface ComputeAdapter {
    val capabilities: ComputeAdapterCapabilities
    val isAvailable: Boolean
    val backend: ComputeBackend
    suspend fun ingestDeltaSegment(request: GpuSegmentIngestRequest): GpuSegmentHandle
    suspend fun releaseSegment(handle: GpuSegmentHandle)
    suspend fun vectorNearestNeighbours(request: GpuVectorSearchRequest): List<dev.kdb.index.RankedResult>
    suspend fun shutdown()
}

enum class ComputeBackend { CPU, CUDA, VULKAN, WEBGPU }

package dev.kdb.compute.jvm
fun createJvmComputeAdapter(config: JvmComputeConfig = JvmComputeConfig()): ComputeAdapter

package dev.kdb.compute.webgpu
expect fun createWebGpuComputeAdapter(): ComputeAdapter?
```

### Layer 10 Interfaces

#### 29. CLI — `dev.kdb.cli`

```kotlin
package dev.kdb.cli

data class CliConfig(val dataDir: String, val nodeId: String = "local", val quiet: Boolean = false)
fun openCliRuntime(config: CliConfig, namespaceId: String): CliRuntime
class CliRuntime(val namespaceId: String, internal val embedded: dev.kdb.jdbc.EmbeddedKdbRuntime)
object KdbCli { fun run(args: Array<String>): Int }
fun main(args: Array<String>)
```

#### 30. Integration fixtures — `dev.kdb.integration.fixtures`

```kotlin
package dev.kdb.integration.fixtures

class IntegrationFixture(val namespaceId: String = "integration/test") {
    val runtime: dev.kdb.jdbc.EmbeddedKdbRuntime
    suspend fun writeJson(json: String): dev.kdb.codec.KdbUuid
    suspend fun head(): dev.kdb.codec.KdbHash
}
fun integrationFixture(namespaceId: String = "integration/test"): IntegrationFixture
```

#### 31. Inspect — `dev.kdb.inspect` (debug JSON; non-authoritative)

```kotlin
// dev.kdb.inspect — Component 31
object InspectJson {
    fun commitToJsonLine(commit: KdbCommit): String
    fun deltaRecordToJsonLine(record: DeltaRecord, segmentId: KdbUuid, offset: Long): String
    fun wireMessageToJsonLine(message: WireMessage, direction: String): String
}

object DeltaSegmentScanner {
    fun scanSegmentBytes(bytes: ByteArray, compression: CompressionCodec): List<ScannedCommit>
}

class WireFrameInspector(codec: WireCodec) {
    fun dumpFrame(frame: ByteArray, pretty: Boolean = true): String
}

data class DebugSidecarConfig(
    val enabled: Boolean,
    val directory: String,
    val logDelta: Boolean = true,
    val logWire: Boolean = false,
)

fun interface DeltaDebugHook {
    suspend fun onAppend(record: DeltaRecord, segmentId: KdbUuid, offset: Long)
}

fun deltaDebugHookOrNoOp(config: DebugSidecarConfig?): DeltaDebugHook
fun wireDebugHookOrNoOp(config: DebugSidecarConfig?, namespaceId: String): WireDebugHook
```

### Layer 11 Interfaces

#### RBAC — `dev.kdb.auth.store` (not a numbered component)

```kotlin
package dev.kdb.auth.store

data class UserRecord(val id: String, val passwordHash: String, val passwordSalt: String, val roles: Set<String> = emptySet())
data class RoleRecord(val name: String, val grants: Set<String> = emptySet())

interface UserStore {
    suspend fun createUser(id: String, password: String, roles: Set<String> = emptySet())
    suspend fun getUser(id: String): UserRecord?
    suspend fun listUsers(): List<UserRecord>
    suspend fun updateCredentials(id: String, newPassword: String)
    suspend fun deleteUser(id: String)
    suspend fun assignRole(id: String, role: String)
    suspend fun revokeRole(id: String, role: String)
    suspend fun verifyPassword(id: String, password: String): Boolean  // false for unknown users too
}

interface RoleStore {
    suspend fun createRole(name: String, grants: Set<String> = emptySet())
    suspend fun getRole(name: String): RoleRecord?
    suspend fun listRoles(): List<RoleRecord>
    suspend fun updateGrants(name: String, grants: Set<String>)
    suspend fun deleteRole(name: String)
}

/** Slow password hashing (PBKDF2-HMAC-SHA256, 120k iterations, 256-bit key) - cross-verified
 * against the Go port (go/kdb/auth) for hash portability between implementations. */
object PasswordHasher {
    fun hash(password: String, salt: ByteArray = randomSalt()): Pair<String, String>  // (hashHex, saltHex)
    fun verify(password: String, expectedHashHex: String, saltHex: String): Boolean
}

/** UserStore/RoleStore persisted as documents inside KDB itself, through the normal commit path
 * (TransactionEngine/CommitDag/StorageAdapter) - not a static file, so roles/users can be
 * added/removed at runtime and it durably survives a restart (given a durable CommitDag). */
class RegistryAuthStore(
    userDag: CommitDag,
    roleDag: CommitDag,
    storage: StorageAdapter,
    authorNodeId: KdbUuid = KdbUuid.random(),
    engine: TransactionEngine = transactionEngine(ConflictPolicy.LAST_WRITE),
) : UserStore, RoleStore

fun dynamicAuthEngine(store: RegistryAuthStore): AuthEngine
```

#### 32. Stored Procedure Engine — `dev.kdb.script`

```kotlin
package dev.kdb.script

data class ProcedureDefinition(
    val namespaceId: String, val name: String, val source: String,
    val requiredPermission: String? = null, val revision: Long = 1L,
    val createdBy: String = "", val createdAt: Long = 0L,
)
data class ProcResult(val value: String, val logs: List<String>)
data class ProcLimits(
    val wallClockMillis: Long = 5_000, val maxHostCalls: Int = 1_000,
    val maxLogBytes: Int = 64 * 1024, val maxStatements: Long = 1_000_000,
) { companion object { val DEFAULT: ProcLimits } }

sealed class ProcException(message: String, cause: Throwable? = null) : KdbException(message, cause) {
    class NotFound(namespace: String, name: String) : ProcException
    class CompileError(detail: String, cause: Throwable? = null) : ProcException
    class Timeout(millis: Long) : ProcException
    class ResourceLimitExceeded(detail: String) : ProcException
    class ScriptRuntimeError(detail: String, cause: Throwable? = null) : ProcException
    /** Authorization failure for a specific kdb.* call made *inside* a running script. */
    class Denied(detail: String) : ProcException
}

/** Keyed by (namespaceId, name); defining/redefining requires AuthAction.ProcManage - the
 * caller (wire host) authorizes, this registry does not. */
interface ProcedureRegistry {
    suspend fun put(def: ProcedureDefinition): ProcedureDefinition
    suspend fun get(namespaceId: String, name: String): ProcedureDefinition?
    suspend fun list(namespaceId: String): List<String>
    suspend fun delete(namespaceId: String, name: String): Boolean
}
fun inMemoryProcedureRegistry(): ProcedureRegistry
fun procedureRegistry(storage: StorageAdapter): ProcedureRegistry  // content-addressed blob backup

interface ProcedureRuntime {
    suspend fun invoke(
        principal: Principal, namespaceId: String, name: String,
        argsJson: String, limits: ProcLimits = ProcLimits.DEFAULT,
    ): ProcResult
}

/** The only embedding needed: GraalVM Polyglot (org.graalvm.js:js) on the JVM, running inside
 * kdb-server. The script never talks to storage directly - only the same authorized
 * HybridQueryEngine/TransactionEngine entry points ordinary SQL/document requests use. */
fun graalProcedureRuntime(
    registry: ProcedureRegistry, hybrid: HybridQueryEngine, dag: CommitDag,
    storage: StorageAdapter, schema: KdbSchema, txEngine: TransactionEngine,
    indexManager: IndexManager, authorizer: Authorizer, maxCallDepth: Int = 3,
): ProcedureRuntime
```

### Layer 12 Interfaces

#### 38. Go-Native Server — `go/kdb/server`

```go
package server

// KdbServerRuntime wraps an embedded runtime with write coordination - one instance serves one
// namespace, matching the Kotlin KdbServerRuntime's per-namespace TransactionEngine cache.
type KdbServerRuntime struct {
    Runtime           *embed.EmbeddedKdbRuntime
    TransactionEngine transaction.Engine // ConflictPolicyStrict
    UpsertEngine      transaction.Engine // ConflictPolicyLastWrite
    SQLEngine         sql.Engine
    DocumentLocks     *transaction.LockManager
    AuthEngine        auth.Engine // defaults to auth.AllowAll
}

func NewKdbServerRuntime(rt *embed.EmbeddedKdbRuntime) *KdbServerRuntime
func (s *KdbServerRuntime) Commit(namespaceID string, tx document.Transaction, sessionID string, principal auth.Principal) (document.Commit, error)
func (s *KdbServerRuntime) Upsert(namespaceID string, docID codec.UUID, jsonBody string, principal auth.Principal) (document.Commit, error)
func (s *KdbServerRuntime) GetDocument(namespaceID string, docID codec.UUID) (json, commitHex string, found bool, err error)
func (s *KdbServerRuntime) Retain()
func (s *KdbServerRuntime) Release()

type Listener struct{ /* ... */ }
func ListenSqlWire(addr string, runtime *KdbServerRuntime) (*Listener, error)
func (l *Listener) Addr() net.Addr
func (l *Listener) Close() error

// ServerRuntimeRegistry holds shared server runtimes by key (multi-namespace deployments).
type ServerRuntimeRegistry struct{ /* ... */ }
func NewServerRuntimeRegistry() *ServerRuntimeRegistry
func (r *ServerRuntimeRegistry) GetOrOpen(key string, open func() (*KdbServerRuntime, error)) (*KdbServerRuntime, error)
func (r *ServerRuntimeRegistry) Release(key string)
```

```go
package auth // go/kdb/auth - PBKDF2-HMAC-SHA256, cross-verified against the Kotlin PasswordHasher

func HashPassword(password string, salt []byte) (hashHex, saltHex string)
func VerifyPassword(password, expectedHashHex, saltHex string) bool

type RegistryAuthStore struct{ /* ... */ } // implements UserStore + RoleStore over a CommitDAG
func NewRegistryAuthStore(userDag, roleDag dag.CommitDAG, storage storage.Adapter) (*RegistryAuthStore, error)

type RegistryAuthEngine struct{ /* ... */ } // implements auth.Engine
func NewRegistryAuthEngine(store *RegistryAuthStore) *RegistryAuthEngine
```

#### 39. Peer-Sync Conflict Detection — `dev.kdb.peersync` + `go/kdb/peersync`

```kotlin
package dev.kdb.peersync

sealed class HeadUpdate {
    object FastForward : HeadUpdate()      // incomingHead is a descendant of localHead
    object AlreadyAncestor : HeadUpdate()  // localHead already at or ahead of incomingHead
    object Diverged : HeadUpdate()         // neither is an ancestor of the other
}
suspend fun resolveHeadUpdate(dag: CommitDag, localHead: KdbHash, incomingHead: KdbHash): HeadUpdate

sealed class CommitPushOutcome {
    object NoOp : CommitPushOutcome()
    object FastForwarded : CommitPushOutcome()
    data class Merged(val mergeCommit: KdbCommit) : CommitPushOutcome()      // disjoint writes, auto-merged
    data class Conflict(val report: ConflictReport) : CommitPushOutcome()   // same-doc divergence, main untouched
}

/** Serialized per namespace (a Mutex-guarded lock map) - shared by both the push-receiving side
 * (PeerSyncFrameHandler.handleCommitPush) and the pull side (PeerSession.pullMissing), so
 * there's one decision function, not two independently maintained copies. */
suspend fun resolveDivergence(
    dag: CommitDag, storage: StorageAdapter, namespaceId: String,
    localHead: KdbHash, incomingHead: KdbHash, conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
): CommitPushOutcome
```

```go
package peersync // go/kdb/peersync - ports the above field-for-field

type HeadUpdate int
const (HeadFastForward HeadUpdate = iota; HeadAlreadyAncestor; HeadDiverged)
func ResolveHeadUpdate(d *dag.InMemoryCommitDag, localHead, incomingHead codec.Hash) HeadUpdate

type CommitPushOutcomeKind int
const (OutcomeNoOp CommitPushOutcomeKind = iota; OutcomeFastForwarded; OutcomeMerged; OutcomeConflict)
type CommitPushOutcome struct {
    Kind        CommitPushOutcomeKind
    MergeCommit *document.Commit
    Report      *kdberr.ConflictReport
}
func ResolveDivergence(d *dag.InMemoryCommitDag, store storage.Adapter, namespaceID string, localHead, incomingHead codec.Hash) (CommitPushOutcome, error)

// Result.Conflict is non-nil only on a genuine same-document divergence; FinalHead is then
// deliberately left unmoved from what it was before the pull.
type Result struct {
    AppliedCommits, PushedCommits int
    FinalHead                     codec.Hash
    Plan                          *DagSyncPlan
    Conflict                      *kdberr.ConflictReport
}
```

#### 40. Go Client SDK — `go/kdb/client`

```go
package client

func Connect(ctx context.Context, addr string, token string) (*Client, error)
func (c *Client) Close() error
func (c *Client) PutJSON(ctx context.Context, ns, docID string, jsonBody []byte) (commitHex string, err error)
func (c *Client) GetJSON(ctx context.Context, ns, docID string) (jsonBody []byte, commitHex string, err error)
func (c *Client) Upsert(ctx context.Context, ns, docID string, jsonBody []byte) (commitHex string, err error)
func (c *Client) Commit(ctx context.Context, tx Transaction) (commitHex string, err error)
func (c *Client) Query(ctx context.Context, ns, sqlText string, args []any, dest any) error
func (c *Client) Exec(ctx context.Context, ns, sqlText string, args []any) error
func (c *Client) AppendEvent(ctx context.Context, ns, docID string, jsonBody []byte) error

var ErrConflict = errors.New(...)
type ConflictError struct{ Report kdberr.ConflictReport }
var ErrNotFound = errors.New(...)
var ErrUnauthenticated = errors.New(...)
```

#### 41. Auth Session/Token Issuance — `dev.kdb.auth.token`

```kotlin
package dev.kdb.auth.token

data class TokenAuthConfig(val documentReader: DocumentReader, val now: () -> KdbTimestamp = { KdbTimestamp.now() })
enum class RejectReason { TOKEN_NOT_FOUND, TOKEN_EXPIRED, MALFORMED_CREDENTIALS }
class TokenAuthRejectedException(val reason: RejectReason, message: String) : KdbAuthenticationException(message)

/** Validates a session token against a stored session document - implements the real
 * Authenticator interface (throws on failure), not the spec's illustrative AuthResult shape. */
class TokenAuthEngine(config: TokenAuthConfig) : Authenticator

/** Tries multiple Authenticators in order; first success wins, last rejection rethrown if all fail. */
class CompositeAuthEngine(authenticators: List<Authenticator>) : Authenticator

data class SessionToken(val token: String, val principal: Principal, val expiresAt: KdbTimestamp)

/** Mints/revokes session documents via a narrow DocumentWriter, independent of TokenAuthEngine's
 * DocumentReader - revoke derives the same deterministic doc id (SHA-256 of the token value)
 * issue used, so it needs no read-then-delete lookup. */
class SessionIssuer(documentWriter: DocumentWriter) {
    suspend fun issue(principal: Principal, ttl: Duration): SessionToken
    suspend fun revoke(token: String)
}
```

#### 44–46. Minor fixes — spec'd inline, gap analysis §5

```kotlin
// Component 44: kdb-embed's EmbeddedKdbRuntime — commit notification bridge
class EmbeddedKdbRuntime {
    suspend fun addCommitListener(listener: suspend (namespaceId: String, commit: KdbCommit) -> Unit)
}

// Component 45: kdb-server's SqlWireHost/SessionManager — disconnect releases locks
class SqlWireHost { suspend fun endSession() }
class SessionManager { suspend fun endAll() }

// Component 46: kdb-stream's StreamBroadcastHub — Mode 2 write-back replay routing
class StreamBroadcastHub(
    wire: WireCodec, namespaceId: String, headProvider: suspend () -> KdbHash,
    transactionReplayer: (suspend (WireMessage.TransactionReplay) -> WireMessage)? = null,
)
// StreamConnection.submitTransaction (StreamSubscriber.kt) now encodes the real transaction via
// TransactionWireCodec and awaits a correlated response (10s timeout) instead of returning
// immediately without ever encoding more than the transaction's id.
```