# KDB Control UI — Plan

Status: **PROPOSED**. Not a numbered spec component yet; if adopted it becomes Layer 18
(Components 66–70: control API, control UI, history/revert engine ops, live settings, recovery
control plane).

Written against `7947cce` (v0.3.2). Every "what exists today" claim below is a file reference that
was checked in the tree at that commit.

## Status

Last audited against `c16d8ab` (PR #40 merged). **The control UI is not finished.** Roughly a third
of the specified API exists: the git-viewer read path, the settings view, and revert. The
SQL-client half and all of recovery are untouched.

Counting endpoints from §5: **14 implemented**, 4 declared and answering 501, ~30 not started.

### Milestones (§11)

| | Milestone | State |
|---|---|---|
| M0 | Foundations | **partial** — control plane, auth, SSE hub and embedded UI are in; the multi-namespace host (§6.1) is **not**, and the plan calls it the thing that blocks everything |
| M1 | Read-only viewer | **partial** — log, commit detail, tree diff, refs, schema; no data browser and no SQL console |
| M2 | Time travel | **partial** — `?at=` works on a document read and the UI has the read-only mode; other read endpoints ignore it, and `AT COMMIT` still is not honoured over the wire |
| M3 | Writes | **not started** — no document CRUD, no transactions, no DML |
| M4 | Rollback | **partial** — plan/apply with `expectHead` are done; per-document history and the document timeline are not |
| M5 | Refs and ops | **partial** — branches and tags list; no create, delete or compare; ops is one endpoint |
| M6a | Settings (read) | **done** — provenance, the env-only surface, ignored-value warnings |
| M6b | Settings (mutation) | **not started** — `PATCH /v1/settings` answers 501; no live setters wired, no persistence, no drift |
| M7 | Recovery | **not started** — nothing from §8 exists |

### API surface (§5)

**Implemented:** `GET /v1/health`, `/v1/namespaces`, `/v1/ns/{ns}/status`, `/schema`, `/log`,
`/commits/{hash}`, `/commits/{hash}/diff`, `/refs`, `/docs/{id}` (including `?at=`), `/events`,
`GET /v1/settings`, `GET /v1/ops/runtime`, `POST /v1/ns/{ns}/revert/plan`, `/revert/apply`.

**Declared, answering 501:** `PUT`/`DELETE /v1/ns/{ns}/docs/{id}`, `POST /v1/ns/{ns}/sql`,
`PATCH /v1/settings`.

**Not started:** the document list (`/docs`), per-document history, per-document diff
(`/commits/{hash}/diff/{docId}`), `/compare`, branch and tag mutation, `/tx`, `/settings/{key}`,
`/settings/drift`, `/settings/validate`, `/policy`, `/indexes`, `/ops/sessions|leases|peers|metrics`,
`POST /v1/ops/drain`, and every recovery endpoint (`/integrity`, `/backups`, `/restore/staging`,
`/maintenance/plan`, `/checkpoints`).

`GET /v1/ops/runtime` is an addition, not in §5 as written.

### UI screens (§9)

| | Screen | State |
|---|---|---|
| 1 | Data browser | **not built** — needs the `/docs` list endpoint first |
| 2 | SQL console | **not built** |
| 3 | Commit graph | **partial** — a flat, paged list with parent and ref badges. There is no lane assignment, so it is a log, not a graph; a merge is flagged with a chip rather than drawn |
| 4 | Document timeline | **not built** |
| 5 | Branches & tags | **partial** — lists only; a tag row is a shortcut into time travel |
| 6 | Time travel | **built** |
| 7 | Rollback | **built** — preview, typed confirmation, forward-commit semantics explained in the dialog |
| 8 | Schema & indexes | **partial** — schema only |
| 9 | Operations dashboard | **partial** — process and admission state; no sessions, leases or peers |
| 10 | Settings | **built** (read-only), including the ignored-configuration panel |
| 11 | Recovery | **not built** |

### What to do next, in order

1. **Multi-namespace host (§6.1).** `service.go` still calls `OpenFileRuntimeWithOptions` with one
   namespace, so the namespace list can only ever have one entry. `embed.Host` already does this;
   it is wiring, and everything namespace-shaped is stuck behind it.
2. **The data browser (M1's missing half).** A `/docs` list endpoint plus a grid is what makes this
   a database tool rather than a history viewer.
3. **The SQL console (M3).** The single most-asked-for thing in any database UI, and the read half
   needs no write permission.
4. **Live settings (M6b).** The six knobs §7.3 names are already safe to change at runtime; the
   read view that makes them legible is done.
5. **Recovery (M7).** §8.3 - restore to staging, then attach read-only - is the highest-value piece
   and is mostly composition of things that already exist.

## 1. What this is

One web application that gives an operator the two things KDB uniquely has and currently exposes
only through a CLI and a wire protocol:

- **The SQL-client half** — browse namespaces, read and edit documents, run KDB-SQL, inspect
  schema and indexes. Think DataGrip / TablePlus / Mongo Compass.
- **The Git-viewer half** — the commit DAG, per-commit diffs, per-document timelines, branches,
  tags, time-travel reads, and *rollback*. Think GitKraken / `tig` / GitHub's commit view.
- **The operator half** — what this server's settings actually are, where each value came from,
  which of them can be changed without a restart (and changing them), plus integrity verification,
  backup, and restore. Think `pg_settings` + a backup console.

The second half is the point. Every database ships the first half. KDB is "source control for
structured documents" (README) and today none of that is reachable except through
`kdb log` / `kdb branch` on a locally-mounted data directory.

The third half exists as machinery but has no surface at all: KDB has four separate configuration
mechanisms (§2.4) and none of them can be inspected from a running server, and it has a complete
verify/repair/backup/restore toolkit (§2.5) that can only run when the server is stopped.

## 2. What exists today

### 2.1 Reachable over the network

| Capability | Where | Notes |
|---|---|---|
| SQL exec (SELECT/INSERT/UPDATE/DELETE) | `go/kdb/server/wire_listen.go` | via `MsgSqlExec`/`MsgSqlResult` |
| Sessions, tx commit/rollback | `wire_listen.go`, `session_manager.go` | `MsgSessionBegin`, `MsgTxCommit` |
| Document get/upsert by id | `go/kdb/wire/document_ops.go` | `MsgDocumentGet` 0x14, `MsgUpsert` 0x16 |
| Conditional writes / CAS | `go/kdb/client/conditional.go` | `PutIfAbsent`, `ReplaceIf`, `CompareAndSwap` |
| Document leases | `go/kdb/wire/lock_ops.go`, `client/lease.go` | 0x19–0x1C |
| Search | `go/kdb/wire/search_ops.go` | 0x1D/0x1E |
| Delta-commit fan-out (live) | `go/kdb/server/stream_listen.go` | `StreamHub`, driven by `KdbServerRuntime.CommitListener` |
| Peer sync / DAG diff | `go/kdb/server/peersync_listen.go`, `MsgDagDiff` 0x05 | between peers, not for browsing |
| WebSocket transport | `go/kdb/server/ws_listen.go`, `--ws-addr` | the one transport a browser can open |
| gRPC | `go/grpc/` (separate module) | frame service over HTTP/2 |
| Health / metrics / pprof | `go/kdb/server/admin.go`, `--admin-addr` | unauthenticated by design; localhost only |
| TypeScript driver | `packages/kdb-client` | Component 63 Phase 1; wire codec + WS/TCP + op set |

### 2.2 History primitives that exist, but only in-process

All on `dag.InMemoryCommitDag` (`go/kdb/dag/`):

- `Walk(from, until, limit)` and `WalkWithOperations(...)` — commit log traversal.
- `Diff(fromHash, toHash) (CommitDiff, error)` — tree-level diff yielding
  `DiffAdded` / `DiffModified` / `DiffRemoved` with content hashes
  (`in_memory_commit_dag.go:610`).
- `ListBranches` / `CreateBranch` / `DeleteBranch` / `SetHead` / `GetBranch`.
- `CommonAncestor`, `IsAncestor`, `CommitsSince`, `AncestorSet` (`ancestry.go`).
- `GetDocumentTree(treeHash)`, `CommitForTree`, `LookupHashPrefix` (short-hash resolution).
- `CommitOperations(hash)` with an ops-retention budget and a hydration loader
  (`ops_retention.go`) — **operations are evictable**; trees are not.
- A tag map (`in_memory_commit_dag.go:105`) and `RefByTag` (`types.go:106`) exist, but no
  create/list/delete tag methods — tag support is a stub.

Time travel exists as a *query* concept: `hybrid.VersionClause` with `AtTag` / `AtCommit` /
`AtTime`, stripped from SQL by `StripVersionClause` (`AT VERSION` / `AT COMMIT` / `AT TIME`,
`go/kdb/query/hybrid/parser.go:41`), plus a session `CheckoutHandle` and `CheckoutStore`
(`engine.go:87`).

`KdbServerRuntime` also has `getDocumentAt(ns, docID, at)` (`index_wiring.go:95`) and
`scanAtCommit(commitHash)` (`index_wiring.go:72`) — point reads and scans at an arbitrary commit
already work internally.

### 2.3 The CLI's git surface

`go/cmd/kdb/cli/`: `init`, `put`, `get`, `query`, `log`, `status`, `branch list|create|checkout`,
`unlock`. All of it opens the data directory **in-process** via
`embed.OpenFileRuntime` (`commands.go:86`) and takes the exclusive `.kdb.write.lock`. None of it
goes over the wire, and it cannot be pointed at a running server.

### 2.4 The four configuration surfaces

They are disjoint. A setting lives in exactly one of these, and there is no single place — in the
process or on disk — where an operator can see the resolved set.

**(a) Service flags → `config.ServiceSettings`** (`go/kdb/config/service.go:20`), resolved by
`ResolveService` with the precedence *config file < `KDB_*` env < explicitly-set flags*
(`--config` help text, `service.go:53`). ~30 settings: listener addresses (`--sql-addr`,
`--ws-addr`, `--peer-addr`, `--stream-addr`, `--grpc-addr`, `--admin-addr`), TLS
(cert/key/CA/client-auth), governance (`--memory-budget-mb`, `--memory-reserve-mb`,
`--max-connections`, `--scan-row-budget`, `--abort-after`, `--drain-timeout`), storage
(`--durability`, `--compression`, `--sync-mode`, `--async-sync-interval-ms`), logging
(`--log-level`, `--log-format`), expiry (`--expire-field`, `--expire-grace`, `--expire-interval`),
`--peer-conflict-policy`, `--rbac`. The JSON file shape is `config.ServiceFile` (`service.go:124`),
which rejects unknown fields so a typo fails loudly.

**(b) `KDB_*` environment variables read only by `embed.FileRuntimeOptionsFromEnv`**
(`go/kdb/embed/storage_options.go:157`). These have **no flag and no config-file field**:
`KDB_HISTORY_STRATEGY`, `KDB_HISTORY_MODE`, `KDB_RETAIN_DURATION`, `KDB_RETAIN_COMMITS`,
`KDB_DOCUMENT_CACHE_BYTES`, `KDB_COMMIT_OPS_BYTES`, `KDB_HISTORY_TREE_CACHE_BYTES`,
`KDB_CHECKPOINTS`, `KDB_ANCESTRY_PRUNING`, `KDB_GRAPH_FILE`, `KDB_GRAPH_REBUILD_COMMITS`,
`KDB_HISTORY_ANCHOR_INTERVAL`, plus the `KDB_S3_*` set via `s3io.ConfigFromEnv`. Every one of them
**ignores an unparseable value silently** and falls back to the default — deliberate (the comments
explain why: for `KDB_HISTORY_MODE` the other reading "starts deleting segments"), but it means an
operator has no way to find out that their typo did nothing. This is precisely the case a settings
view fixes.

**(c) `policy.NamespacePolicy`** (`go/kdb/policy/types.go:135`) — a rich per-namespace model with
mode, history mode, retention window and rules, conflict policy, compaction policy, four-band tier
policy, index retention, GPU promotion, vector index settings, document expiry, and a `Revision`.
It has a DSL and JSON parser (`policy/parser.go`), a validator, and presets (`DefaultMutable`,
`AppendOnlyEvents`, `ScratchDocument`, `CacheNoHistory`). **It is in-memory and unpersisted**
(`policy.InMemoryRegistry` is the only implementation) and essentially unwired: the only non-test
consumers of `policy.Registry` are `go/kdb/compaction/engine.go` and `go/kdb/query/hybrid/`, and
`service.go` touches `policy` only to build a `DocumentExpiryPolicy` (`service.go:233`).

**(d) Live setters already on the runtime.** These exist and are documented safe to call while
serving:

| Setter | Effect | Concurrency note |
|---|---|---|
| `KdbServerRuntime.SetMemoryBudget(limit, rejectFraction, reserve, scanRowBudget)` | replaces the memory guard + admission | "safe to call again to change it, and safe to call concurrently with in-flight work" (`server_runtime.go:479`) |
| `KdbServerRuntime.SetDocumentExpiry(*policy.DocumentExpiryPolicy)` | read predicate immediate, restarts the sweeper | nil disables; no-ops the sweeper on a read-only runtime (`expiry.go:53`) |
| `KdbServerRuntime.SetSchemaChecked(schema)` | schema evolution | validated (`server_runtime.go:349`) |
| `ServerEngine.SetMemoryBudgetBytes(n)` | re-cuts hot tier across doc cache / tree cache / memtable | "safe to call while the namespace is serving traffic"; honours explicitly-pinned sub-budgets (`memory_budget.go:37`) |
| `InMemoryCommitDag.SetOperationsBudget(n)` | ops retention budget | "safe to call while the DAG is being read and written" (`ops_retention.go:255`) |
| `InMemoryCommitDag.SetGraphSettings(s)` | commit-graph features | recomputes generations on a populated DAG; explicitly says call it before the DAG is shared (`graph_settings.go:112`) |
| `BudgetArbiter.SetFloorBytes`, `Register`, `Reserve` | cross-namespace memory arbitration | already runs on a timer (`storage/budget_arbiter.go:98`) |

Two near-misses worth knowing: `MaxConnections` is copied into the transport options *at listener
construction* (`wire_listen.go:53`, `ws_listen.go:33`), so changing the runtime field afterwards
affects only listeners created later; and the write gate's queue is a fixed-capacity channel
(`write_gate.go:48`), so its depth cannot be resized without rebuilding the gate.

### 2.5 Recovery, backup and integrity tooling

All of it exists, and all of it is CLI-only and offline.

| Capability | Where | Notes |
|---|---|---|
| Verify (L1 physical CRC/framing, L2 commit hash + parent closure) | `go/kdb/integrity/verify.go:194` | `Report{Segments, Findings}`, `Clean()`; classifications `torn_tail`, `mid_log_corruption`, `missing_parent`, `sequence_gap` |
| Repair | `go/kdb/integrity/repair.go:55` | acts only on L1 findings; actions `truncated_torn_tail`, `rewrote_segment_prefix`, `refused`, `no_op`; quarantines bytes before mutating; idempotent |
| Restore / hybrid restore | `go/kdb/recovery/restore.go:56` | CRC-verified union of N sources, applied topologically; reports `MissingHashes` rather than applying out of order; directory sources only (peer/S3 sources are Components 60/62, unbuilt) |
| Backup create (full + incremental) | `go/kdb/backup/backup.go:78` | manifest with head hashes, commit count, per-segment entries; incremental references unchanged segments |
| Backup verify / list / fetch | `backup.go:213`, `:271`, `:243` | manifest-verified |
| Whole-database backup / verify | `go/kdb/backup/database.go:67`, `:145` | every namespace under one root, under one lock |
| CLI | `go/cmd/kdb-inspect/` | `verify`, `repair-segments`, `migrate-history`, `restore`, `backup`, `backup-verify`, `backup-list`, `backup-fetch`, `dump-wire` |

One detail that turns out to matter a lot for online backup: `backup.Create` already handles a
segment that is still being appended to, capturing its **verified prefix** and recording it as
`VerifiedPrefix` in the manifest (`activePrefixKey`, `backup.go:70`). Backing up a live namespace
is therefore not a new algorithm — it is the algorithm that is already there.

## 3. The gaps that block a control UI

These are the actual work. The UI itself is the easy half.

**G1 — No history over the network.** `Walk`, `Diff`, branches, and tags have no wire message, no
gRPC method, and no HTTP endpoint. `go/kdb/client` has ~15 methods and not one of them is a
history operation. A browser cannot ask a running server for a commit log today.

**G2 — Server-side time travel is not wired up.** `grep -rl "query/hybrid" go --include='*.go'`
returns exactly one non-test file: `go/embedbundle/main.go`. The server's SQL path
(`wire_listen.go`) resolves reads against the session's base version and never constructs a
`hybrid.Request`, so `AT COMMIT` / `AT TIME` sent over the wire is not honoured. `SqlExecMessage`
has no version field.

**G3 — No rollback of any kind.** No `Revert`, no `Restore`, no "reset to commit" exists at any
layer. `dag.Squash` exists but is a retention/compaction operation, not a user-facing undo.

**G4 — The service opens exactly one namespace.** `service.go:171` calls
`embed.OpenFileRuntimeWithOptions(dataDir, catalog, namespace, …)` with a single `--namespace`.
`embed.Host` (Layer 17 Component 64 phases A–D, `go/kdb/embed/host.go`) already supports N
namespaces over one data root with `Namespaces()`, `Namespace(...)`, `CloseNamespace(...)` — the
service simply does not use it. A UI whose left-hand tree is "databases → collections" needs this.

**G5 — Auth is not fit to sit in front of a control plane.** Go-side bearer tokens are literally
parsed as `"user:pass"` against a static JSON file (`kdb-auth-static` and its Go mirror; see
`docs/kdb-rbac-plan.md`). Resource-scoped RBAC (database → collection → document) is implemented
Kotlin/JVM only; the Go store and enforcement are not done. Component 41 (auth tokens) is spec'd,
not built. Enforcement lives at the wire boundary only, not in `StorageAdapter` /
`TransactionEngine`.

**G6 — No per-document history.** Nothing answers "show me every version of this document".
`CommitOperations` can be filtered by `DocID` but operations are evictable under the retention
budget, so op-scanning is not a reliable basis for a UI feature.

**G7 — History is not invertible.** `document.Op` is `WriteOp{DocID, Patch}` /
`DeleteOp{DocID}` / `FileWriteOp` (`go/kdb/document/kdb_op.go`). No op carries a pre-image, so a
commit cannot be undone by inverting its operations. Any rollback must be reconstructed from
**trees**, not from ops. This shapes the whole revert design (§6.3).

**G8 — There is no way to read a running server's settings, and no way to change any of them.**
No API returns `ServiceSettings`, the resolved `KDB_*` values, or the effective policy. Nothing
calls the live setters of §2.4(d) after startup — `SetMemoryBudget` and `SetDocumentExpiry` are
invoked exactly once each from `service.go` and never again. There is also no *provenance*: given
a running process, an operator cannot tell whether `--memory-budget-mb` came from a flag, an env
var, the config file, or auto-detection, and cannot see that a `KDB_HISTORY_MODE` typo was
silently discarded. §7.

**G9 — Every recovery operation demands the exclusive data-directory lock, which the running
server holds.** `kdb-inspect verify`, `repair-segments`, `backup`, `migrate-history` and
`restore --out` all call `embed.LockDataDir` first (`verify.go:62`, `backup.go:46`,
`restore.go:96`, `migrate.go:12`), and the help text says so plainly: "no service may be running
against it" (`main.go:106`). `flock(2)` is per open file description, so this holds even inside
one process. A control UI therefore **cannot** simply call these functions — the architecture has
to distinguish what the server can do while holding its own lock from what genuinely requires the
server to be down. §8.

## 4. Architecture

```
  browser
    │  HTTPS  (JSON + SSE)
    ▼
  ┌──────────────────────────────────────────────┐
  │ kdb-service                                   │
  │                                               │
  │  --control-addr ──► go/kdb/control            │  NEW: HTTP/JSON control plane
  │                        │  + go:embed SPA      │
  │                        ▼                      │
  │  --sql-addr  ─────► server.KdbServerRuntime ◄─┤  history/revert methods live HERE
  │  --ws-addr   ─────►        │                  │  (go/kdb/server/history.go, revert.go)
  │  --stream-addr ───►        ▼                  │
  │  --admin-addr ────►  embed.Host (N namespaces)│
  └──────────────────────────────────────────────┘
```

### 4.1 Decision: an HTTP/JSON control plane, not new wire opcodes

**Recommendation: add `go/kdb/control`, an authenticated HTTP/JSON API on its own listener, and
build the SPA against that.**

Why not extend the wire protocol instead:

- The wire protocol carries a **Kotlin/Go parity obligation** and cross-language golden tests.
  History browsing needs roughly a dozen new message types; each one doubles into Kotlin plus
  fixtures. The fixed project decision is that the Go server is the production deployment target
  (`docs/kdb-finish-up-plan.md`), so a Go-only control surface is consistent, and a
  browser-facing admin UI is not a thing the Kotlin embedded engine needs to serve.
- The UI's needs are HTTP-shaped: cursor pagination over a 100k-commit log, `ETag`/`If-None-Match`
  on immutable commit objects (commits *are* content-addressed — near-perfect cache keys),
  range-limited result sets, SSE for live commits, bearer auth, CORS, standard browser tooling.
- The wire protocol's `SqlResult` returns `[][]string`. A data grid wants typed JSON and the raw
  `_doc`. Fighting that in the UI is worse than a purpose-built DTO.

What stays on the wire: nothing changes for existing clients. The control API is *additive* and
calls the same `KdbServerRuntime`.

**The load-bearing rule:** every capability the control API exposes is implemented as a method on
`KdbServerRuntime` (or on `dag`), and the HTTP handler is a thin DTO shim. That way a later
`MsgHistoryLog` wire message, a gRPC method, or a Kotlin port reuses the same code. No business
logic in handlers.

### 4.2 Deliverables and their homes

| Path | What |
|---|---|
| `go/kdb/server/history.go` | `Log`, `CommitDetail`, `DiffCommits`, `DocumentHistory`, `ResolveRef` on `KdbServerRuntime` |
| `go/kdb/server/revert.go` | `PlanRevert` / `ApplyRevert` (§6.3) |
| `go/kdb/server/refs.go` | branch + tag CRUD passthrough, `SetHead` guards |
| `go/kdb/control/` | router, DTOs, auth middleware, SSE hub, `go:embed`ed SPA assets |
| `packages/kdb-ui/` | Vite + React + TypeScript SPA (sibling of `packages/kdb-client`) |
| `go/kdb/service/service.go` | `--control-addr`, `--control-write`, `--control-ui`; switch to `embed.Host` |
| `docs/kdb-spec-layer18-*.md` | the specs, once the plan is accepted |

The SPA is built to static assets and embedded with `go:embed` so a single `kdb-service` binary
serves it — no second process, no nginx, no separate deploy. A build tag
(`-tags noui`) drops the assets for deployments that do not want the UI compiled in.

## 5. Control API surface

Versioned under `/v1`. JSON in, JSON out. Cursor pagination (`?cursor=&limit=`), never offsets —
the DAG is append-only and cursors are commit hashes.

### Discovery and shape
```
GET  /v1/health                          → version, uptime, readiness, memory zone, draining
GET  /v1/namespaces                      → [{id, catalog, head, branch, docCount, sizeBytes}]
GET  /v1/ns/{ns}/schema                  → KdbSchema JSON (the "lens")
GET  /v1/ns/{ns}/indexes                 → [{id, kind, fields, state, rebuildProgress}]
GET  /v1/ns/{ns}/status                  → head, branch, dirty, pending writes, lease count
```

### Data plane
```
POST /v1/ns/{ns}/sql                     {sql, params, limit, at?} → {columns, rows, rowsAffected,
                                                                      resolvedCommit, readOnly}
GET  /v1/ns/{ns}/docs?cursor=&limit=&at= → paged document list (id, contentHash, preview)
GET  /v1/ns/{ns}/docs/{id}?at=           → {id, body, contentHash, commit}
PUT  /v1/ns/{ns}/docs/{id}               {body, ifContentHash?} → CAS write, 409 on mismatch
DEL  /v1/ns/{ns}/docs/{id}               {ifContentHash?}
POST /v1/ns/{ns}/tx                      {ops[], message} → one atomic commit
```

`at=` accepts a commit hash (full or ≥8-hex prefix, via `LookupHashPrefix`), a branch name, a tag,
or an RFC 3339 timestamp — the same grammar `hybrid.VersionClause` already models.

### History
```
GET  /v1/ns/{ns}/log?from=&until=&limit=&cursor=&doc=
       → [{hash, shortHash, parents[], timestamp, author, message, opCount, treeHash, lane}]
GET  /v1/ns/{ns}/commits/{hash}          → commit + resolved ops + parent hashes + branch refs
GET  /v1/ns/{ns}/commits/{hash}/diff?against=  (default: first parent)
       → {added[], modified[], removed[]} with content hashes and sizes
GET  /v1/ns/{ns}/commits/{hash}/diff/{docId}
       → {before, after} full JSON bodies for the document-level diff view
GET  /v1/ns/{ns}/docs/{id}/history?cursor=&limit=
       → every commit where this document's content hash changed (§6.4)
GET  /v1/ns/{ns}/refs                    → {branches[], tags[], head}
POST /v1/ns/{ns}/branches                {name, fromCommit}
DEL  /v1/ns/{ns}/branches/{name}
POST /v1/ns/{ns}/tags                    {name, commit, message}
GET  /v1/ns/{ns}/compare?from=&to=       → ahead/behind counts, merge base, diff summary
```

### Rollback
```
POST /v1/ns/{ns}/revert/plan   {to, scope:{docs[]|all}}  → dry run: full diff + op count + warnings
POST /v1/ns/{ns}/revert/apply  {to, scope, expectHead, planId, message} → new forward commit
```
`expectHead` is mandatory: the apply is a compare-and-swap against the head the plan was computed
from, so a concurrent write invalidates the plan rather than silently reverting more than the
operator saw. `planId` is a server-side handle with a short TTL, so apply cannot diverge from what
the preview showed.

### Live
```
GET  /v1/ns/{ns}/events   (text/event-stream) → commit events bridged from
                                                KdbServerRuntime.CommitListener
```

### Operations
```
GET  /v1/ops/sessions      → active sessions, principals, base versions, age
GET  /v1/ops/leases        → held document leases, holder, expiry, fence token
GET  /v1/ops/peers         → peers, last sync, ahead/behind (from MsgDagDiff data)
GET  /v1/ops/metrics       → the /metrics numbers, pre-parsed into JSON for the dashboard
POST /v1/ops/drain         → begin orderly drain (mirrors SIGTERM path)
```

### Settings (§7)
```
GET   /v1/settings                → every setting as a SettingDescriptor (value, source,
                                    default, scope, mutability, restartRequired, sensitive)
GET   /v1/settings/{key}          → one descriptor, with its full help text
PATCH /v1/settings                {changes:{key:value}, persist:bool, expectRevision}
                                  → applies live-mutable keys, reports per-key outcome
GET   /v1/settings/drift          → running values that differ from the config file on disk
GET   /v1/ns/{ns}/policy          → effective policy.NamespacePolicy for this namespace
PUT   /v1/ns/{ns}/policy          {policy | dsl} → validate + apply (validator already exists)
POST  /v1/settings/validate       {changes} → dry run: what would change, what needs a restart
```

### Recovery (§8)
```
GET   /v1/ns/{ns}/integrity                 → last verify report (cached), or 404 if never run
POST  /v1/ns/{ns}/integrity/verify          {level:"L1"|"L2"} → run online verify, returns Report
GET   /v1/ns/{ns}/checkpoints               → checkpoint state, replay cost estimate at next open
GET   /v1/backups?ns=                       → ListBackups / ListDatabaseBackups
POST  /v1/backups                           {ns|all, baseBackupId?} → create (online, §8.2)
GET   /v1/backups/{id}                      → manifest: head hashes, commit count, segments
POST  /v1/backups/{id}/verify               → backup.Verify / VerifyDatabase
POST  /v1/restore/staging                   {backupId, sources[], outDir} → restore into a NEW
                                              directory; never touches the live one
GET   /v1/restore/staging/{jobId}           → progress, Result{applied, sourcesUsed, missing}
POST  /v1/restore/staging/{jobId}/attach    → open the restored dir read-only and expose it as a
                                              browsable pseudo-namespace (§8.3)
GET   /v1/ns/{ns}/maintenance/plan          → the exact kdb-inspect command for an offline
                                              operation, with preconditions (§8.4)
```

## 6. Engine work

### 6.1 Multi-namespace service (blocks everything)

Replace the single `OpenFileRuntimeWithOptions` call in `service.go:171` with `embed.OpenFileHost`
+ lazy `Host.Namespace(...)` per namespace, and make `--namespace` optional (it becomes a default
for the wire listeners, which are still single-namespace on the frame path). This is
`docs/kdb-spec-layer17-multi-namespace-runtime.md` phases A–D being *used* rather than newly
built, so it is wiring plus a budget-arbiter check under load — not new engine work.

Watch: the wire/peersync/stream listeners take a `namespaceID` today
(`ListenPeerSyncTLS(addr, srv, namespace, …)`). Keep them as-is for M1; multi-namespace on the
data-plane listeners is a separate, larger change and is not on the UI's critical path.

### 6.2 Time travel on the server path

Wire `hybrid.Engine` into `KdbServerRuntime` so a read carries a `VersionClause`:

1. Add `ExecuteSQLAt(ns, sql, params, at)` to the runtime, constructing a `hybrid.Request` with
   `Version` set and `ReadConsistency: ReadConsistencySnapshot`.
2. Reads at a non-head commit are **hard read-only** — `hybrid.Result.ReadOnly` already exists;
   the control API must return 409 on any write attempted while `at` is set.
3. `AtTime` resolution needs a timestamp→commit search. Commits carry `Timestamp`
   (`document.Commit`), and generations exist (`dag/generations.go`), but a binary search needs
   monotonic timestamps, which are not guaranteed across peers. Resolve `AtTime` as
   "newest commit on the selected branch with `Timestamp <= t`" by walking, and cap the walk;
   document the semantics rather than pretending it is exact.
4. Only after this does the control API's `at=` parameter mean anything. It is also the missing
   piece for `AT COMMIT` over the wire, which is a general win beyond the UI.

Cost note: reads at an old commit go through `scanAtCommit` and may force a tree rebuild. Trees
are LRU-cached and bounded (`docs/kdb-bounded-history-trees.md`), so a UI that lets someone
scrub a timeline can trigger repeated rebuilds. The control API must rate-limit `at=` reads and
surface "materializing…" rather than blocking.

### 6.3 Rollback — design

**Constraint (G7): commits cannot be inverted.** `WriteOp` carries only the new patch; nothing
records the pre-image. The DAG is also append-only and shared with peers, so rewriting it is not
an option — a rollback that mutated history would diverge every peer that had already fetched.

**Therefore: rollback is a forward commit, exactly like `git revert`, computed from trees.**

```
PlanRevert(ns, target, scope) → RevertPlan
  head        := current branch head
  diff        := dag.Diff(head, target)        // entries transform head → target
  for each entry in diff, filtered by scope:
    DiffAdded{doc}        → doc exists at target, absent at head → WriteOp{doc, bodyAt(target)}
    DiffModified{doc}     → WriteOp{doc, bodyAt(target)}
    DiffRemoved{doc}      → doc exists at head, absent at target → DeleteOp{doc}
  returns ops, per-doc before/after, counts, size estimate, expectHead = head

ApplyRevert(plan, expectHead, message) → document.Commit
  one transaction, baseVersion = expectHead, message = "revert to <short> (control-ui, <principal>)"
  → normal conflict detection, normal indexing, normal stream fan-out, HeadConflictError if raced
```

Three things make this safe and cheap:

- `dag.Diff` already returns precisely the right entry kinds with content hashes
  (`in_memory_commit_dag.go:610`).
- Bodies at the target come from the existing `getDocumentAt(ns, docID, targetTreeHash)`
  (`index_wiring.go:95`).
- At the transaction layer, `WriteOp.Patch` is treated as the **whole document body**, not a
  merge patch — `documentFromPatch` parses it as the document
  (`go/kdb/storage/engine/cold_loader.go:143`). Shallow-merge semantics live above, in
  UPSERT/`SET _doc`. So a revert can restore a body that *removes* keys added later. **Verify this
  with a test as step one of M4** — if any write path re-merges, revert needs an explicit
  replace-op and that is a wire/spec change.

Scopes: whole namespace, a document set (from the UI's checkbox selection), or a
schema-collection filter. Same code path; only the entry filter differs.

Limits to state up front: `dag.Diff` calls `MaterializedEntries()` on both trees — O(namespace
size) in memory and time. For a large namespace a whole-namespace revert is an expensive
operation, not an instant one. Gate it: plan runs with a configurable entry cap, returns a
"too large, revert by document set" error above it, and the apply goes through the normal write
gate so it cannot starve other writers.

**Not in scope for v1:** history rewriting, drop-commit, squash-from-UI, force-move a branch to an
ancestor. `dag.Squash` exists but is retention machinery; putting it behind a button invites
exactly the divergence the peer model is built to avoid.

### 6.4 Per-document history (G6)

Do **not** build this on `CommitOperations` — ops are evictable under the retention budget
(`ops_retention.go`), so a document's history would silently truncate.

Build it on trees instead. `DocumentTree` has `HashFor(docID)` / `Contains(docID)`, and every
commit records `DocumentTreeHash`. Walk the branch from head, compare `HashFor(docID)` between a
commit and its first parent, and emit an entry when it changes (including
present→absent = deleted, absent→present = created). That is always available, needs no op
retention, and is exact.

Cost is O(commits walked) tree lookups, so it must be paginated and cursor-based from the start. If
it becomes hot, the follow-up is a per-document commit index maintained on the write path — note
it as future work, do not build it in v1.

### 6.5 Tags

Finish the stub: `CreateTag` / `ListTags` / `DeleteTag` / `GetTag` on `InMemoryCommitDag` over the
existing `tags` map, persisted alongside branches, plus resolution of `RefByTag` in
`DefaultVersionResolver` (it already has the `AtTag` case; confirm it reaches a real store).
Small, and it makes "pin a known-good state before a risky migration" a first-class UI action —
which is most of why an operator wants this tool.

## 7. Settings — showing them, and opening up the ones that can move

### 7.1 The model

One type describes every setting, whatever surface it came from. This is the thing that does not
exist today and is most of the value:

```go
type SettingDescriptor struct {
    Key         string      // "memory.budgetMB", "storage.durability", "graph.ancestryPruning"
    Value       any         // the resolved, running value
    Default     any
    Source      Source      // Default | ConfigFile | Env | Flag | AutoDetected | RuntimeChange
    SourceDetail string     // "--memory-budget-mb", "KDB_HISTORY_MODE", "/etc/kdb/config.json:12"
    Scope       Scope       // Process | Namespace | Listener
    Mutability  Mutability  // see below
    Restart     Restart     // None | NewConnections | NamespaceReopen | ProcessRestart
    Sensitive   bool        // redact in responses and logs
    Help        string      // the flag's existing help text, which is already excellent
    Unit        string      // "MiB", "ms", "bytes", ""
    Warnings    []string    // e.g. "KDB_HISTORY_MODE was set to 'ful' and ignored"
}
```

**Five mutability classes**, derived from what the code actually supports (§2.4d):

| Class | Meaning | Examples |
|---|---|---|
| `live` | a setter exists and is documented safe under load | memory budget, reserve, scan-row budget, document expiry (field/grace/interval), ops-retention budget, hot-tier budget, arbiter floor, log level (§7.3) |
| `live-new-connections` | takes effect for connections/listeners created after the change | `max-connections`, TLS material, per-listener options |
| `namespace-reopen` | needs that namespace closed and reopened; `embed.Host.CloseNamespace` makes this possible without stopping the process | document cache bytes, history tree cache bytes, tree chain limit, checkpoints on/off, graph settings on a populated DAG |
| `restart` | process-level, startup only | listener addresses, data dir, log format, drain timeout, abort-after, RBAC on/off |
| `immutable` | refused at open if it disagrees with what is on disk — changing it is a migration, not a setting | `HistoryStrategy`, `HistoryMode` (`storage_options.go:75-88`; `kdb-inspect migrate-history` is the path) |

`immutable` is a real category, not a euphemism for "hard": the open path deliberately *refuses* a
disagreeing value rather than ignoring it, "because the two leave different things on disk". The UI
must show these as settings with a **Migrate** action, never an editable field.

### 7.2 Provenance and the silent-typo problem

`ResolveService` already knows the precedence (file < env < explicitly-set flags) and already takes
a `flagWasSet` predicate (`config/service.go:184`) — so it *has* the information to report where
each value came from; it just discards it. The work is to have it emit a
`map[key]SettingDescriptor` alongside the resolved struct. Low risk, high payoff.

For surface (b), the `KDB_*` variables that `FileRuntimeOptionsFromEnv` parses leniently: add a
parallel `FileRuntimeOptionsFromEnvVerbose` that returns the same options **plus** a list of
variables that were set and discarded. Do not change the lenient behaviour — the comments make a
good case for it — but stop losing the fact that it happened. `Warnings` on the descriptor is where
that surfaces, and the settings screen shows a badge. This alone justifies the feature: today a
typo'd `KDB_RETAIN_DURATION` in a systemd unit is undetectable from outside the process.

### 7.3 What to actually open up in v1

Concrete, in order of value-to-risk:

1. **Memory budget, rescue reserve, scan-row budget** — `SetMemoryBudget` already does all four in
   one call and is explicitly safe concurrently. This is the knob an operator most wants at 3am.
2. **Document expiry** (field path, grace, sweep interval) — `SetDocumentExpiry` already handles
   the sweeper restart and the read-only case.
3. **Log level** — currently baked into a `slog.HandlerOptions{Level: lvl}` at
   `service.go:536`. Changing it to a `slog.LevelVar` makes it live-adjustable in about five lines,
   and "turn on debug logging for two minutes without a restart" is worth more than its cost.
4. **Hot-tier budget per namespace** — `ServerEngine.SetMemoryBudgetBytes`, respecting the pinned
   sub-budget rule the setter already enforces.
5. **Ops-retention budget** — `SetOperationsBudget`, which no-ops safely without a loader.
6. **Namespace policy** — `PUT /v1/ns/{ns}/policy` through the existing `policy.Validator`, then
   apply the parts that map to live setters (expiry today; retention and compaction once the
   registry is wired and persisted). This is where §2.4(c) stops being dead code.

Explicitly **not** in v1: durability, compression, sync mode. They are per-write engine settings
where a mid-flight change has to be reasoned about against group commit and the delta writer, and
"changing durability from sync to async on a live server" is a decision that should cost a restart.

### 7.4 Persistence and drift

A live change that vanishes on restart is a trap. Two rules:

- `PATCH /v1/settings` takes `persist: bool`. With `persist:true` and a `--config` file in use, the
  change is written back to the `ServiceFile` JSON (which already has pointer fields distinguishing
  "absent" from zero, so a partial file is its natural shape) and applied. Without a config file,
  `persist:true` is refused with an explanation rather than silently ignored.
- `GET /v1/settings/drift` reports every running value that differs from what the config file and
  environment would produce on a restart. The settings screen shows a persistent banner while
  drift is non-empty. An operator should never be surprised by a restart.

`expectRevision` on the PATCH is a compare-and-swap against a settings revision counter, so two
operators cannot silently overwrite each other.

### 7.5 UI

One **Settings** screen, grouped by scope (Process / Storage / Memory & governance / Listeners &
TLS / Logging / Namespace policy). Each row: key, current value, a source chip
(`flag` / `env` / `file` / `default` / `auto` / `changed at 14:22 by alice`), and either an inline
editor (for `live`), an editor with a "takes effect on new connections" note, a "requires namespace
reopen — reopen now?" action, or a read-only value with a lock icon and the reason. Warnings render
as an amber row. A diff-style **Review changes** step before any PATCH, showing exactly which keys
move, which need a restart, and whether the change will be persisted. Full-text filter, because
thirty settings is already too many to scan.

## 8. Recovery, backup and integrity

### 8.1 The constraint that shapes everything (G9)

The recovery toolkit is complete and good. It is also, today, entirely offline: every entry point
takes the data directory's exclusive lock, which the running service holds. So the design question
is not "how do we expose these functions" but **"which of them can the server run while holding its
own lock, and what do we do about the rest"**. Three tiers:

| Tier | Operations | How |
|---|---|---|
| **Online, server-executed** | verify (L1/L2), checkpoint status, backup create/verify/list, restore into a *staging* directory | the server already holds the lock and owns the shim — it does not need to acquire anything |
| **Online, staged** | restore + inspect a backup before trusting it | write to a new directory, attach read-only (§8.3) |
| **Offline only** | `repair-segments`, `migrate-history`, restore *in place* | the service must stop; the UI's job is to make that safe and scripted, not to pretend otherwise (§8.4) |

### 8.2 Online verify and backup

Both are additive and low-risk, because neither needs a lock the server does not already hold:

- **Verify.** `integrity.Verify(shim, ns, opts)` is a pure read over the delta segments. Expose it
  as a runtime method that passes the engine's own shim, run it on the write-gate's slow path or a
  bounded worker so a full L2 scan cannot starve writers, and cache the `Report` with a timestamp.
  Caveat to state in the UI: a verify concurrent with writes can flag the *active* segment's torn
  tail, which is normal, not damage — the scanner already models this (`consumedBytes` and
  `ScanSegmentBytes`' short-tail tolerance). Mark findings on the active segment as informational.
- **Backup.** As noted in §2.5, `backup.Create` already captures the active segment's verified
  prefix rather than requiring a sealed log, so an online backup is the existing algorithm. Do add
  a **quiesce option**: briefly hold the write gate, seal the current segment, capture, release —
  which turns a prefix-consistent backup into a segment-aligned one for operators who want it.
  Whole-database backup across N namespaces needs the multi-namespace host (§6.1) and one
  consistent point across all of them; `backup.CreateDatabase` already expects to run under one
  lock, which the host holds.

Both become scheduled jobs later (nightly verify, incremental backup on a cron) — but v1 is
on-demand, with progress reported over SSE, because a running verify on a large log is minutes, not
seconds.

### 8.3 Restore to staging, then attach — the feature worth building

Restoring in place requires the server down. Restoring **elsewhere** does not:
`kdb-inspect restore --out` locks only the *output* directory (`restore.go:96`). So the server can
run a restore into a staging directory while it keeps serving.

That unlocks the thing operators actually want, which is not "restore" but "*check* the backup":

1. Pick a backup (or several sources — `recovery.HybridRestore` takes N sources and unions the
   CRC-verified commits, so "the damaged local log + last night's backup" is one call).
2. Restore into `dataRoot/.staging/<jobId>/`, progress over SSE.
3. Read the `Result`: applied count, which sources contributed, and `MissingHashes` — the
   restore reports what it could not resolve instead of applying it out of order, which is exactly
   the information a "can I trust this backup" screen should lead with.
4. **Attach it read-only.** `embed.FileRuntimeOptions.ReadOnly` opens under a *shared* lock with
   no WAL and no delta writer (`storage_options.go:27`), so the staged directory can be opened
   alongside the live one and exposed as a browsable pseudo-namespace. Every read view already
   built — data browser, SQL console, commit graph, document timeline — points at it unchanged.
   The operator verifies the restored data with the same tools they use on production, before
   anything is promoted.
5. Promote is deliberately *not* a button. Promotion means swapping data directories, which means a
   restart, which means §8.4.

This is the single highest-value recovery feature and it is almost entirely composition of things
that already exist.

### 8.4 Offline operations: maintenance mode, not a lie

For `repair-segments`, `migrate-history` and in-place restore, the UI must not pretend it can do
something it cannot. It does three honest things instead:

1. **Show the findings** from the online verify, classified, per segment, with the exact byte
   offsets the report already carries — so the operator knows whether they have a torn tail
   (repairable, `truncated_torn_tail`) or a missing parent (`refused` — needs restore, and the
   repair code already draws that line at L1 vs L2).
2. **Generate the exact command**, filled in with this server's data dir, namespace and options,
   copy-pasteable, with its preconditions stated ("stop kdb-service first"). `GET
   /v1/ns/{ns}/maintenance/plan` returns it as structured data so the UI can also show what it will
   do and what it will quarantine.
3. **Offer a supervised maintenance flow** where a supervisor exists. The pieces are already there:
   `BeginDraining` / `WaitForWritesToDrain`, `/readyz` flipping to 503 with reason `draining`
   (`admin.go`), and the exit-75 + `Restart=on-failure` contract from Component 50. The flow is:
   readiness off → drain → close storage and release the lock → run the operation → exit 75 → the
   supervisor restarts the process → readiness on. The control API can drive steps 1–3 and report
   the outcome after restart; it cannot restart the process itself, and the UI should say so.

`repair-segments` also quarantines bytes before mutating anything (`RepairStep.QuarantineName`),
which the UI should surface prominently — it is the difference between a scary button and a
recoverable one.

### 8.5 UI

A **Recovery** section per namespace, plus a database-level view:

- **Integrity** — last verify: when, at what level, clean or N findings. Findings table grouped by
  segment with classification, offset, and a plain-English explanation of each of the four
  classifications. "Run verify (L1)" / "Run verify (L2)" with a cost warning and live progress.
  Active-segment findings visually separated as informational.
- **Backups** — list with created-at, commit count, head hashes, full-vs-incremental and base
  chain, total size. Create (with quiesce toggle), verify, fetch. A verified-recently badge, since
  an unverified backup is a guess.
- **Restore** — the staging workflow of §8.3 as a wizard: choose sources → restore → results
  (applied / missing hashes, prominently) → attach read-only → browse → promotion instructions.
- **Maintenance** — the offline operations, presented as generated commands plus the supervised
  flow, with the preconditions checklist.
- **Checkpoints** — checkpoint state and an estimate of what the next open would have to replay,
  which is the number that predicts restart time. `DisableCheckpoints` is a setting (§7), and this
  is where its consequence is visible.

## 9. UI surface

React + TypeScript + Vite in `packages/kdb-ui`. Data access via a generated typed client over the
control API; `@kdb/client` is *not* required by the UI (the control API is HTTP), but the two
should share the schema/DTO types where they overlap.

**Shell.** Left rail: connection → namespaces → per-namespace tree (Documents, SQL, History,
Branches, Schema, Indexes). Top bar: current ref indicator — `main @ a3f9c1e` — which turns amber
and read-only when time travel is active. Command palette (`⌘K`) for jumping to a commit hash,
document id, or namespace.

**1. Data browser.** Virtualized grid over `/docs`, schema columns plus a `_doc` expander. Row
click opens a side panel with a JSON editor (Monaco, schema-aware). Saves go through
`PUT /docs/{id}` with `ifContentHash` — optimistic concurrency out of the box, and a 409 shows a
three-way view (yours / theirs / base) rather than a generic error. New-document and delete with
confirmation. Everything is a commit, and the commit appears in the History view immediately via
SSE.

**2. SQL console.** Monaco with KDB-SQL highlighting, `⌘↵` to run, results grid with JSON cell
expansion, `AT COMMIT`/`AT TIME` recognised and reflected in the ref indicator, query history
persisted per namespace, CSV/JSON export, hard row cap with a visible "showing N of M" and a
kill-query control. DML runs behind an explicit "this will write" confirmation when the UI is in
read-write mode.

**3. Commit graph.** The centrepiece. Lane-assigned DAG rendering (the classic git-log graph),
virtualized so a 100k-commit history scrolls. Row = hash, message, author, time, op count, branch
and tag badges. Selecting a commit opens the detail pane: parents, ops summary, and the tree diff
against the first parent as a file-list-style pane (added / modified / removed documents with
sizes). Selecting a document in that list shows a proper JSON diff (before → after). Range-select
two commits to diff arbitrary pairs; `compare` gives ahead/behind and merge base.

**4. Document timeline.** For one document: every version, in order, with the JSON diff between
adjacent versions, the commit that produced each, and a **Restore this version** button that
routes into the scoped revert flow. This is the feature that makes the tool worth having.

**5. Branches & tags.** List with ahead/behind against the default branch, create branch from any
commit (drag from graph), create tag at a commit, delete with confirmation. "Checkout" is a
read-only pin of the UI's ref — it does not move the server's head. Moving `HEAD` is a separate,
explicitly-labelled action.

**6. Time travel.** Any commit → "View at this commit". The whole app switches to that ref: data
browser, SQL, document panels all read at it, the top bar turns amber, and every write control is
disabled with a tooltip explaining why. One click back to HEAD.

**7. Rollback.** Never one click. From a commit or a document version: *Revert to here* →
`revert/plan` → a full preview screen (N documents restored, M deleted, per-document diffs,
warnings, estimated cost) → type the namespace name to confirm → `revert/apply`. The result is a
new commit shown in the graph with a revert badge, and a "revert the revert" affordance because
it is just another commit.

**8. Schema & indexes.** Read-only in v1: schema fields and types, index list with kind, backing
fields, and rebuild state. Index creation and schema evolution are DDL and belong to a later
phase.

**9. Operations dashboard.** Health, readiness, memory zone and admission state (the runtime
already tracks these — `memory_guard.go`, `admission.go`), write-queue depth, stage latencies from
`/metrics`, active sessions, held leases, peer divergence, and a drain button.

**10. Settings.** Detailed in §7.5 — every setting with its resolved value, where that value came
from, and an editor only where the code actually supports a live change.

**11. Recovery.** Detailed in §8.5 — integrity findings, backups, the restore-to-staging wizard
with read-only attach, and generated commands for the operations that require the service to be
stopped.

The Settings and Recovery screens sit behind their own permission scopes and are the two places
where the UI shows an operator something they cannot get from any other tool today.

## 10. Safety and access control

A web UI that can `DELETE FROM` a production database needs this settled before M1 ships, not
after.

1. **Read-only by default.** `--control-write` (default off) is required for any mutating endpoint.
   Without it the control plane serves the entire git-viewer and SQL-read experience and rejects
   every write with 403. This is the mode that should be safe to enable in production.
2. **Bound to localhost by default**, like `--admin-addr`. Public exposure requires TLS
   (`wss://`/HTTPS is already plumbed through `core.TransportTlsSettings`; note
   `go/kdb/transport/ws/transport.go` historically returned "wss:// not yet implemented" — confirm
   the current state before claiming TLS support).
3. **Authenticated always** — unlike `/admin`. Bearer token, principal resolved through
   `auth.Engine`, and every request authorized. Until Component 41 (real tokens) lands, the
   control plane accepts the existing static-file principals but must **not** be reachable off-box.
   Treat "the control UI is exposed" as a blocker until tokens and Go-side resource-scoped RBAC
   exist (G5). Say this in the release notes.
4. **New permission scopes**: `control:read`, `control:write`, `control:history`,
   `control:revert`, `control:ops`, `control:settings`, `control:recovery`. Revert, settings and
   recovery are separate from ordinary writes — the person who can edit a document should not
   automatically be able to roll back the namespace, halve its memory budget, or repair its log.
5. **Attribution.** Every write from the UI commits with `message` carrying the principal and the
   UI as the source, and `AuthorNodeID` set to the server's node. The commit log becomes the audit
   log — which is the whole argument for building this on top of a versioned store.
6. **Destructive-action gates.** Typed confirmation for revert, branch delete, and unbounded DML;
   a hard row cap on scans; every mutating request idempotency-keyed so a double-submit cannot
   produce two commits.
7. **No credential handling in the UI**: no user creation, no password rotation, no key material.
   That is CLI/admin territory until RBAC lands properly.
8. **Settings are redacted and audited.** Anything carrying a secret — the `KDB_S3_*` credentials
   above all — is returned with its value masked and its *source* shown, never the value itself.
   Every applied change is logged with principal, before, after, and whether it was persisted, and
   appears in a settings-history view. A setting change is an outage-shaped action; it needs the
   same paper trail as a revert.
9. **Recovery actions are gated by consequence, not by category.** Verify, backup list/verify, and
   restore-to-staging are read-shaped and need only `control:recovery`. Backup create and
   restore-to-staging additionally consume disk, so they take a quota check and refuse rather than
   fill the volume. Anything that mutates the live data directory is not exposed at all (§8.4) —
   the UI generates the command, the operator runs it.

## 11. Phasing

Estimates are rough engineer-weeks for one person who knows the codebase, and exclude review.

**M0 — Foundations (≈2w).** `embed.Host` wired into `service.go` (§6.1). `go/kdb/control` skeleton:
listener, auth middleware, `/v1/health`, `/v1/namespaces`, `/v1/ns/{ns}/status`, SSE hub bridged
from `CommitListener`. `packages/kdb-ui` scaffold, `go:embed` build wiring, `make ui`. *Exit: one
binary serves an authenticated page listing namespaces and live-updating on commit.*

**M1 — Read-only viewer (≈3w).** `history.go` on the runtime: log, commit detail, diff, ref list.
Endpoints + the commit graph UI, commit detail with tree diff, document-level JSON diff, branch
list, and the data browser and SQL console in read-only mode. *Exit: the git-viewer half is
complete and safe to point at production.*

**M2 — Time travel (≈2w).** §6.2 — hybrid engine on the server read path, `at=` on every read
endpoint, `AT COMMIT`/`AT TIME` honoured over the wire too, the amber read-only ref mode in the UI.
*Exit: browse the whole database as it was at any commit.*

**M3 — Writes (≈2.5w).** Document CRUD with CAS and conflict resolution UI, transactional multi-op
writes, DML through the SQL console, `--control-write` gating, attribution, the confirmation gates
of §10. *Exit: the SQL-client half is complete.*

**M4 — Rollback (≈2.5w).** §6.3, starting with the `WriteOp`-is-a-full-body verification test.
Plan/apply endpoints, the preview screen, per-document restore from the timeline, revert badges in
the graph. §6.4 per-document history lands here since the timeline depends on it. *Exit: an
operator can undo a bad write and see exactly what will change first.*

**M5 — Refs and ops (≈2w).** Tags (§6.5), branch create/delete, compare view, ops dashboard,
sessions/leases/peers, drain. *Exit: feature-complete v1.*

**M6 — Settings (≈2.5w).** §7. Split in two, and the first half is worth shipping alone:
*(a) read-only settings view* — `SettingDescriptor`, provenance out of `ResolveService`, the
verbose env parse with discarded-value warnings, `GET /v1/settings`, the settings screen. About a
week, no mutation risk, and it closes G8's worst edge (silent typos).
*(b) live mutation* — `PATCH /v1/settings` for the six knobs of §7.3, the `slog.LevelVar` change,
persistence and drift, namespace policy apply. *Exit: an operator can see where every value came
from, and change the ones that are safe to change without a restart.*

**M7 — Recovery (≈3w).** §8. Online verify with cached reports and SSE progress; online backup
create/verify/list with the optional quiesce; the restore-to-staging wizard including read-only
attach — which is the bulk of the value and mostly composition (§8.3); the maintenance-plan
generator and findings view for offline operations. *Exit: an operator can prove a backup is good
before they need it.*

Critical path: M0 → M1 → M2 → M4. M3, M5, M6 and M7 can run in parallel with M4 given more than
one person; M6(a) has no dependency beyond M0 and is the cheapest standalone win in the plan, and
M7 depends on M0's `embed.Host` for anything database-wide.

Deliberately out of v1: merge/conflict resolution UI (peer-sync conflict reporting is Component 39
territory), schema evolution DDL, index creation, user/role management, multi-server fleet view,
saved queries and dashboards.

## 12. Testing

- **Go unit tests** for every runtime method in `history.go` / `revert.go`, especially: revert
  round-trip (write → revert → state equals target tree hash), revert of a delete, revert of a
  create, scoped revert, and revert racing a concurrent write (must return `HeadConflictError`).
- **Control API tests** against an in-process runtime with a real listener — auth rejection,
  read-only mode rejection, cursor pagination stability under concurrent commits, `at=` returning
  409 on write.
- **Python e2e** (`kdb-integration/e2e/`, which the finish-up plan already wants extended): launch
  `kdb-service --control-addr`, drive the API end to end, assert the commit log reflects the UI's
  writes. This also finally gives the harness a server lifecycle, which it does not have today.
- **UI tests**: component tests for the graph lane assignment (pure function, worth unit-testing
  hard) and diff rendering; one Playwright smoke path — connect, browse, time travel, revert with
  preview — run against a real service in CI.
- **Settings tests**: provenance is asserted for each precedence path (default → file → env →
  flag), a deliberately-typo'd `KDB_*` variable produces a warning rather than silence, every
  `live` setting is applied and read back under concurrent load, every `restart` setting is
  refused with the right reason, and `persist:true` round-trips through the `ServiceFile` JSON.
- **Recovery tests**: online verify against a namespace being written to must not report the
  active segment's torn tail as damage (this is the regression that would make the feature
  worthless); online backup of a live namespace, restored to staging, must produce a tree hash
  equal to the source head; a hybrid restore from a deliberately-corrupted segment plus a good
  backup must report the right `MissingHashes` and apply nothing out of order. `go/kdb/integrity`
  already has `testdata_test.go` with corrupted fixtures to build on.
- **Load**: the commit-graph endpoint against a synthetic 100k-commit namespace, to confirm
  pagination and the LRU tree cache hold up. Per `docs/kdb-benchmark-isolation-required.md`, run
  any such measurement in isolation.

## 13. Risks and open decisions

| Risk | Mitigation |
|---|---|
| Auth is not production-grade (G5) | Ship localhost-bound and read-only by default; block public exposure on Component 41 + Go RBAC |
| `dag.Diff` is O(namespace) — whole-namespace revert on a big store is heavy | Entry cap, plan/apply split, run through the write gate, prefer scoped reverts in the UI |
| Time-travel scrubbing thrashes the bounded tree cache | Rate-limit `at=`, async "materializing" state, cache resolved refs |
| Ops eviction makes op-level detail incomplete for old commits | Base every history feature on trees (§6.4); show "operations not retained" honestly in the commit detail pane rather than an empty list |
| `AT TIME` semantics with non-monotonic peer clocks | Define as "newest commit on this branch with timestamp ≤ t", document it, do not claim exactness |
| Control plane drifts from the wire feature set | All logic on `KdbServerRuntime`, handlers stay thin; a wire/gRPC exposure later is a mapping exercise |
| Embedding a SPA bloats the binary / complicates the release | `-tags noui` build, and add the UI bundle to the reproducibility checks in `docs/kdb-release-plan.md` |
| A live settings change is lost on restart and nobody notices | `persist` flag writing back to the `ServiceFile`, plus a permanent drift banner while running ≠ on-disk (§7.4) |
| A settings change destabilises a live server (e.g. budget cut too far) | Only the six §7.3 knobs are mutable in v1; validate against current demand before applying; every change audited and trivially reversible |
| Online verify competes with the write path on a large log | Bounded worker, cancellable, cached report with an age indicator; L2 is opt-in and warned about |
| Operators read "restore" in the UI as "restore in place" | The wizard is named and worded as *restore to staging*; promotion is documented as a restart-shaped operation and has no button (§8.3) |
| Maintenance flow half-runs and leaves the service down | The flow only ever drives drain + release; the actual mutation is the operator's command, and exit-75 + supervisor restart is the existing, tested contract |

**Decisions needed before M0:**

1. Confirm HTTP control plane over new wire opcodes (§4.1). This is the one structural choice.
2. Confirm the UI ships inside `kdb-service` (`go:embed`) rather than as a separate binary or a
   deployed static site.
3. Confirm v1 is Go-only with no Kotlin counterpart obligation.
4. Confirm rollback is forward-revert-only, with no history rewriting in the product ever
   (recommended — it is what makes peers safe).
5. Confirm that live settings changes may be persisted back into the `--config` file by the
   server (§7.4). The alternative — apply-only, never write config — is defensible in a
   GitOps-managed deployment where the file is owned by a deployment tool, and if that is the house
   style, `persist` should be refused rather than merely optional.
6. Confirm the offline recovery boundary (§8.4): the control plane never mutates a live data
   directory, and `repair-segments` / `migrate-history` / in-place restore stay operator-run
   commands. The alternative is building a maintenance-mode supervisor contract, which is a
   larger piece of work than the rest of M7 combined.
7. Decide whether `policy.NamespacePolicy` (§2.4c) gets persisted and wired as part of this work
   or stays out of scope. The UI can render and validate it cheaply; making it *authoritative* is a
   real engine change and arguably its own component.

---

## Appendix A — Implementation status on `feat/control-ui`

Written after the first implementation pass. Records what was built, what it revealed, and where
this deviates from the plan above.

### Delivered (M0, M1 read path, M6a)

| Piece | Where |
|---|---|
| Settings descriptors with provenance | `go/kdb/config/descriptors.go` |
| Env-only surface with discarded-value warnings | `config.EnvOnlyDescriptors` |
| Control plane: router, auth, DTOs, SSE hub | `go/kdb/control/` |
| History reads: resolve-ref, log, commit detail, diff, refs | `go/kdb/control/history.go` |
| Embedded single-page UI | `go/kdb/control/ui/index.html` |
| Service wiring: `--control-addr`, `--control-write`, `--control-ui` | `go/kdb/service/service.go` |

Not built, and refused explicitly rather than omitted: every mutating endpoint (403 read-only /
501 not-built), and reading a document `?at=` a past commit (501).

### Deviations from the plan above

1. **The UI is one dependency-free HTML file, not a React/Vite SPA** (§4.2 said
   `packages/kdb-ui`). For a read-only viewer a framework buys nothing and costs the Go release an
   npm toolchain and a reproducibility problem. The API is the contract; the SPA can replace this
   page without touching the server. Revisit at the write/time-travel milestones, which have real
   client state.
2. **History logic lives in `go/kdb/control/history.go`, not on `KdbServerRuntime`** (§4.1 said
   handlers stay thin over runtime methods). `dag.ListCommits` / `ParseRevision` and
   `embed.RevertTo` / `DiffCommits` are being built in parallel on main; putting a second
   implementation on the runtime would collide with them. Keeping it in one file inside this
   package makes the eventual switch a one-file change.
3. **No new auth action type.** §10 proposed `control:*` scopes. `registryAuthorizer` maps any
   action it does not recognise to kind `"unknown"` on an empty resource, which no grant can
   match - so a new action type would deny every RBAC deployment. Namespace reads authorize as
   `auth.SqlExecAction{ReadOnly: true}` (the same question, the same answer) and process-scoped
   reads as `auth.AdminAction{Scope: "control"}`, which already maps to the `admin:` vocabulary.

### Defects found and fixed

**`ServerEngine.GetTree` never reached the on-demand tree rebuild.** A checkpoint restores the
live tree and the commit graph, not every tree the namespace has ever had - the rest are derivable
from the delta log and rebuilt on demand (`SetTreeRebuilder`). But the rebuild hung off `treeAt`,
which only the storage read path called, while `GetTree` - the `dag.DocumentTreeStore`
implementation the DAG itself calls - stopped at the bounded store. So any caller arriving through
the DAG got a plain miss for a tree that was merely not resident yet.

`dag.Diff` was the visible casualty: it resolves both commits' trees that way, so after a reopen it
failed with `from tree missing` for every commit but the newest - exactly when someone wants to
read history. Invisible in memory-backed tests, total on disk.

Fixed by routing `GetTree` through the same chain as every other historical read: live snapshot,
bounded store, tree objects, then the fold. The live tree still answers from the atomic snapshot
without touching the bounded store, and a hash no commit claims is refused cheaply before any fold
starts. Regression test: `go/kdb/embed/history_tree_resolution_test.go`, which fails with
`from tree missing` without the fix. **This was an engine defect, not a control-plane one** - it
affected anything reading history through the DAG.

**`storage.Adapter`'s `atCommit` parameter is a document *tree* hash, not a commit hash.** The
implementation matches it against the live tree snapshot and the tree store, both keyed by tree
hash. A commit hash passed there does not error - it resolves nothing, and the read reports "no
such document" for a document that is plainly present. The control plane's own
operation-based diff had exactly this bug, and it was silent: every modification was reported as an
addition, with no content hashes, which looks entirely plausible. Fixed, and the interface now
documents the key space; `TestBothDiffPathsAgree` asserts the operation-based and tree-based diffs
produce the same answer, which is the check that catches this class of mistake.

**`ResolveService` panicked on a nil `lookupEnv` or `flagWasSet`.** Both were dereferenced
unconditionally, so any programmatic caller - an embedded runtime, a test, anything not parsing a
command line - crashed instead of getting the defaults. Both now default to "absent", which is a
meaningful answer.

**Two UI bugs caught only by running it.** A `display: flex` rule defeated the `hidden` attribute,
so the tab bar showed on the sign-in screen; and `Node.replaceChildren(null)` renders the literal
text `null`, unlike the `el()` helper which filters. Neither was reachable from the Go tests.

### Still open

- The operation-based diff is preferred for the first-parent case on cost grounds (one read per
  operation, versus materializing two whole trees). `dag.Diff` is the fallback and now works
  everywhere; the response's `basis` field says which ran.
- `rebuildTreeByFolding` reconstructs a tree from commit *operations*, which are themselves
  evictable. It verifies `tree.TreeHash == want` before returning, so it degrades to a clean miss
  rather than a wrong tree - but a namespace that has evicted both its trees and its operations
  cannot diff that far back at all. That is inherent to the retention design, not a defect.

### Verified

`go build ./...`, `go vet ./...`, `gofmt` clean. The **full** `go test ./...` suite passes, and
`go test -race` passes for `storage/...`, `embed`, `dag`, `control`, `config` and `server` - the
`GetTree` change is in the storage engine, so it needs the wide sweep rather than the narrow one. Beyond unit tests, the binary was run against a seeded on-disk namespace and
driven through a browser: sign-in gate, commit list with ref badges, commit detail with operations
and document diff, refs, schema, ops, and the settings view - including two deliberately-malformed
`KDB_*` variables appearing in the "Ignored configuration" panel, which is the case §7.2 exists
for. After the fixes above, re-verified on disk that a document written twice reports as one
*modification* with both content hashes, and that a diff between two arbitrary commits - which
previously failed outright - resolves through the tree path.

### After merging the history work (PR #41)

PR #41 landed the retention modes and, with them, the real history API this plan anticipated. The
control plane now consumes it instead of carrying its own implementations, which is what Appendix A
predicted would happen.

**Deleted from this branch, in favour of the engine's:** local revision resolution, the local
tree-based diff, and the operation-based diff that existed because the tree-based one did not work
on disk. `dag.ResolveRevision`, `dag.ListCommits` and `embed.DiffCommits` replace all of it.

**The `GetTree` fix was withdrawn.** PR #41 reached the same defect and fixed it correctly:
`GetTree` is called *while the DAG holds its own lock*, and the rebuild walks commits, so resolving
from there re-enters the DAG and deadlocks against a waiting writer. #41 added
`ServerEngine.TreeAt` for callers outside that lock and deliberately left `GetTree` alone;
`embed.DiffCommits` resolves both trees through it and then compares them with the pure
`dag.DiffTrees`. That is the right shape and this branch now uses it. The regression test survives,
retargeted at `embed.DiffCommits`, which is the contract callers actually have.

**Now implemented, having been 501 stubs:** reading a document at any past revision, real tags
(the DAG's tag store is no longer a stub), and revert - plan and apply, gated on `--control-write`,
with a mandatory `expectHead` so what is applied is what was previewed.

**Abbreviated hashes** are resolved in this package rather than the engine. `dag.ParseRevision`
takes a full 64-hex hash; a log listing shows eight digits and a person will paste what they see.
The prefix lookup runs only after the engine has already rejected a spec, so no branch name, tag or
walk suffix can be shadowed by it.
