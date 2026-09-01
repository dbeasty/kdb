# KDB User Guide

This guide is for developers who want to **run**, **inspect**, **embed**, or **operate** KDB. It
describes what works in this repository today and what is still planned.

| If you want… | Read |
|--------------|------|
| what KDB is and why it is built this way | [High-level architecture](kdb-architecture.md) |
| how it works internally (types, flows, locks, byte formats) | [Low-level design](kdb-lld.md) |
| the exact SQL you can write | [KDB-SQL reference](kdb-lld-query.md) |
| wire protocol, error codes, governance | [Protocol & operations](kdb-lld-protocol.md) |
| the normative design and roadmap | [Architecture specification](kdb-spec.md) |

---

## Status

KDB has two implementations that share one specification, one on-disk format, and one wire
protocol: a **Kotlin Multiplatform** tree (browser / JVM / native) and a **Go** tree (`go/`) used
for native servers, the CLI, `database/sql`, WASM, and mobile bindings.

| Capability | Status |
|------------|--------|
| Core engine (codec, storage, SQL, indexes, wire, peer sync, …) | Implemented; unit and integration tests |
| **Go native server** (`kdb-service`) — SQL wire, peer sync, stream, admin HTTP, TLS, RBAC | Implemented |
| **Go client SDK** (`go/kdb/client`) — connect, put/get/upsert/commit/query/exec, typed errors | Implemented |
| **Go `database/sql` driver** (`kdb://memory:` / `kdb://file:`) | Implemented (embedded only) |
| **Go CLI** (`kdb`) — `init`, `put`, `get`, `query`, `log`, `status`, `branch`, `unlock` | Implemented |
| **Resource governance** — memory admission, scan budgets, typed backpressure, abort watchdog | Implemented (Go) |
| **Integrity & recovery** — `verify`, `repair-segments`, `backup`, `restore` | Implemented (Go, `kdb-inspect`) |
| Encryption at rest | Specified only ([Layer 14](kdb-spec-layer14-encryption-at-rest.md)) |
| **Product CLI** (`:kdb-cli`) — `init`, `put`, `get`, `query`, `log`, `status`, `sync`, `shell` | Implemented via Gradle `runCli` |
| **Inspect CLI** (`:kdb-inspect`) — `dump-delta`, `dump-wire`, … | Implemented via Gradle `inspectCli` |
| **JDBC driver** — `jdbc:kdb:memory://…`, `jdbc:kdb:file://…` | Embedded SELECT, metadata, prepared statements; file mode persists under `dataRoot/ns/{namespaceId}/` |
| Peer sync (in-memory hub + TCP loopback) | Implemented (`:kdb-peer-sync`, `:kdb-transport-tcp`) |
| Integration test suite | `:kdb-integration` |
| **JDBC** — `jdbc:kdb://…` network URLs | Parsed but not implemented (`SQLFeatureNotSupportedException`) |
| JDBC DML (`INSERT` / `UPDATE` / `DELETE`) | Implemented (embedded memory/file); auto-commit per statement; multi-statement transactions on network SQL (`BEGIN` … `COMMIT`) |
| CLI persistence (`--data-dir`) | Kotlin & Go: `put` / `get` / `query` survive separate CLI invocations (delta log + SERVER engine); Go uses `flock` on `{dataDir}/.kdb.lock` while open |
| Published Maven / npm artifacts | Not yet; use Gradle composite build or project dependency from source |
| Full git-style CLI (branch, merge, `schema migrate`, …) | Specified in [§11](kdb-spec.md#11-cli-interface); not in v1 CLI |
| **File attachments** (`file put` / `get` / `meta`, ZIP, bundles, `fileId` GUID) | Implemented — see [file attachments spec](kdb-spec-layer1-component3b-file-attachments.md) |
| **Stored procedures** (`:kdb-script`, sandboxed JS) | Library-level API implemented and tested (registry, GraalVM sandbox, per-call authorized `kdb` host API); no wire protocol frame or CLI subcommand yet — see [Component 32 spec](kdb-spec-layer11-component32-stored-procedures.md) |

---

## Choosing a setup

| You want to… | Use | Section |
|--------------|-----|---------|
| store data inside one Go process | `database/sql` driver or the embedded runtime | [Go embedded](#go--embedded-databasesql) |
| store data inside one JVM process | JDBC embedded (`jdbc:kdb:memory://` / `file://`) | [JDBC](#jdbc-java--what-you-can-do-today) |
| share one database between processes or hosts | run `kdb-service`, connect with the Go client SDK or JDBC network | [Running a server](#running-a-server-kdb-service) |
| script or inspect a workspace from a shell | the `kdb` CLI (Go) or `:kdb-cli` (Kotlin) | [Command-line usage](#command-line-usage) |
| push live changes to browsers or caches | `kdb-service --stream-addr`, Mode 1 / Mode 2 subscribers | [Stream modes](#stream-subscribe-over-websocket) |
| keep independent replicas that merge later | `kdb-service --peer-addr` peer sync | [Peer sync](#peer-sync) |
| back up, verify, or repair a data directory | `kdb-inspect` | [Operations](#operations--durability-backup-and-recovery) |

**One writer per data directory.** File mode takes an exclusive lock on `{dataDir}/.kdb.lock`.
A second process opening the same directory fails immediately with a clear error. Use a server
when more than one process needs to write.

---

## Prerequisites

**Go tree** (`go/`) — native server, CLI, `database/sql`, client SDK:

- **Go 1.26+**

```bash
cd go && go test ./...
make build-go            # → go/bin/kdb, go/bin/kdb-service, go/bin/kdb-inspect
./go/bin/kdb --version   # 0.1.0 (commit …, built …, go1.26 …)
```

**Kotlin tree** — JVM, browser, JDBC, Gradle CLIs:

- **JDK 17+**, **Gradle 8.x** (use the wrapper `./gradlew`)
- For **JavaScript / browser** embedding: Kotlin Multiplatform with `js(IR) { browser() }`

```bash
./gradlew build
./gradlew :kdb-jdbc:test
./gradlew :kdb-cli:test
./gradlew :kdb-integration:test
```

---

## Go quick start

### CLI

```bash
./go/bin/kdb --data-dir /tmp/kdb-data init myapp/users
OUT=$(./go/bin/kdb --data-dir /tmp/kdb-data put myapp/users '{"name":"Ada"}')
# {"docId":"<uuid>","docIdShort":"<8-hex>","commit":"<64-hex>"}
./go/bin/kdb --data-dir /tmp/kdb-data get myapp/users "$(echo "$OUT" | jq -r .docId)"
./go/bin/kdb --data-dir /tmp/kdb-data query myapp/users "SELECT _doc FROM users"
./go/bin/kdb --data-dir /tmp/kdb-data log myapp/users
./go/bin/kdb --data-dir /tmp/kdb-data status myapp/users
```

| Command | Usage |
|---------|-------|
| `init` | `init <namespace>` |
| `put` | `put <namespace> <file\|json>` — prints `{"docId","docIdShort","commit"}` |
| `get` | `get <namespace> <docId>` — full UUID, 32 hex, or an unambiguous 8+ hex prefix |
| `query` | `query <namespace> <sql>` — tab-separated rows |
| `log` / `status` | commit history / head hash and document count |
| `branch` | `branch list\|create\|checkout <namespace> …` |
| `unlock` | remove a stale `.kdb.lock` when the holder process is gone |

Global flags: `--data-dir DIR` (default `~/.kdb`), `--quiet`, `--version`.

### Go — embedded (`database/sql`)

```go
import (
    "database/sql"
    _ "github.com/limidus/kdb/go/kdb/driver"
)

db, err := sql.Open("kdb", "kdb://file:///var/lib/kdb/myapp/users")
// or in-memory:  kdb://memory:///demo/users?unique=true&dropOnClose=true

rows, err := db.Query("SELECT kdb_id, _doc FROM users WHERE age > ?", 30)
```

| DSN | Meaning |
|-----|---------|
| `kdb://memory:///catalog/namespace` | shared in-process database per URL |
| `…?unique=true` | fresh isolated database per connect (tests) |
| `…?isolate=name` | named shared instance |
| `…?dropOnClose=true` | dropped when the last connection closes |
| `kdb://file:///path/to/data/catalog/namespace` | file-backed under `path/to/data/ns/…` |
| `…?readOnly=true` | reject writes |

The Go driver is **embedded only** — for network access use the client SDK below.

### Go — client SDK (network)

```go
import "github.com/limidus/kdb/go/kdb/client"

c, err := client.Connect(ctx, "tcp://127.0.0.1:9090", "alice:secret") // "" when RBAC is off
defer c.Close()

commit, err := c.PutJSON(ctx, "myapp/users", docID, []byte(`{"name":"Ada"}`))
body, at, err := c.GetJSON(ctx, "myapp/users", docID)
commit, err  = c.Upsert(ctx, "myapp/users", docID, []byte(`{"name":"Ada Lovelace"}`))

var users []struct{ KdbID, Name string }
err = c.Query(ctx, "myapp/users", "SELECT kdb_id, name FROM users WHERE age > ?", []any{30}, &users)

cols, rows, err := c.QueryRaw(ctx, "myapp/users", "SELECT _doc FROM users LIMIT 10", nil)
err = c.Exec(ctx, "myapp/users", "CREATE TABLE users (name VARCHAR NOT NULL, age INT)", nil)
```

Optimistic concurrency (compare-and-set against a base version):

```go
_, err := c.Commit(ctx, client.Transaction{
    Namespace:   "myapp/users",
    BaseVersion: commit,                    // the commit you read at
    Writes:      []client.DocWrite{{DocID: docID, JSON: updated}},
})
if errors.Is(err, client.ErrConflict) {
    var ce *client.ConflictError
    errors.As(err, &ce)   // per-document local vs incoming detail
    // re-read, rebase, retry
}
```

TLS:

```go
c, err := client.ConnectWithOptions(ctx, "tcps://db.internal:9090", token, client.ConnectOptions{
    TLS: &core.TransportTlsSettings{Enabled: true, CAFile: "/etc/kdb/ca.pem"},
})
```

Schemes accepted: `tcp://`, `tcps://`, `ws://`, `wss://`, or a bare `host:port` (treated as
`tcp://`). One `*Client` is safe for concurrent use.

**Error handling** — every failure is typed, so retry logic never parses prose:

| Test | Meaning | What to do |
|------|---------|-----------|
| `errors.Is(err, client.ErrConflict)` | someone else committed against your base version | re-read, rebase, retry |
| `errors.Is(err, client.ErrBusy)` | server queue full or under memory pressure | wait `(*BusyError).RetryAfter()`, retry |
| `errors.Is(err, client.ErrDeadlineExceeded)` | your deadline passed while queued | retry with a longer deadline |
| `errors.Is(err, client.ErrUnavailable)` | server is shutting down | reconnect (likely to a restarted instance) |
| `errors.Is(err, client.ErrNotFound)` | no such document | — |
| `errors.Is(err, client.ErrUnauthenticated)` | handshake auth failed | fix credentials |
| `RESOURCE_EXHAUSTED` in the message | too large / scan budget exceeded | resubmit smaller, or narrow the query |

---

## Running a server (`kdb-service`)

```bash
./go/bin/kdb-service \
  --data-dir /var/lib/kdb \
  --namespace myapp/users \
  --sql-addr    "tcp://0.0.0.0:9090?bind=true" \
  --admin-addr  "127.0.0.1:9099"
```

The startup log line reports the resolved status of every subsystem (listeners, TLS, RBAC,
memory budget, durability, abort watchdog) plus the exact build identity.

### Flags

**Storage and identity**

| Flag | Default | Meaning |
|------|---------|---------|
| `--data-dir DIR` | — | filesystem data root (takes the exclusive directory lock) |
| `--memory` | on when no `--data-dir` | in-memory runtime (nothing survives restart) |
| `--namespace NS` | `demo/users` | the namespace this process serves |
| `--durability sync\|async\|memory` | `sync` | how much of the write-out a commit waits for |
| `--async-sync-interval-ms N` | 5 | background flush period under `async` |
| `--compression zstd\|none` | `zstd` | codec for new delta frames and SSTable blocks (recorded per frame) |
| `--sync-mode full\|fast` | `full` | physical sync primitive (`fast` survives OS crash but not power loss) |
| `--version` | — | print version and exit |

**Listeners** — each is enabled by default on loopback; pass an empty value to disable one.

| Flag | Default | Meaning |
|------|---------|---------|
| `--sql-addr` | `tcp://127.0.0.1:9090?bind=true` | SQL wire listener (client SDK, JDBC network) |
| `--peer-addr` | `tcp://127.0.0.1:9091?bind=true` | peer-sync (Mode 3) listener |
| `--stream-addr` | `tcp://127.0.0.1:9092?bind=true` | stream (Mode 1 read-only / Mode 2 write-back) listener |
| `--admin-addr` | disabled | operational HTTP: `/healthz`, `/readyz`, `/metrics`, `/debug/pprof` — **no auth; bind privately** |
| `--max-connections N` | 256 | cap on concurrently accepted connections per listener (0 = unlimited) |

**Security**

| Flag | Meaning |
|------|---------|
| `--tls-cert` / `--tls-key` | enable TLS; every listener's scheme is upgraded `tcp://` → `tcps://` |
| `--tls-ca` | CA bundle for verifying client certificates |
| `--tls-client-auth` | require and verify client certificates (mTLS); needs `--tls-ca` |
| `--rbac` | enable the user/role registry (durable under `--data-dir`, in-memory otherwise) |

**Resource governance** — see [the governance model](kdb-lld-protocol.md#5-resource-governance)

| Flag | Default | Meaning |
|------|---------|---------|
| `--memory-budget-mb N` | `0` = auto-detect | budget admission control governs against: cgroup limit if present, else 75 % of host RAM; `-1` disables |
| `--memory-reserve-mb N` | 48 | rescue reserve released on entry to the Critical zone (clamped to ¼ of the budget) |
| `--scan-row-budget N` | 1 000 000 | maximum rows a single scan may **examine**; shrinks automatically as pressure rises |
| `--abort-after DUR` | 0 (off) | after sustained pressure, drain and exit 75 so a supervisor restarts clean |
| `--drain-timeout DUR` | 30s | on SIGTERM, how long to wait for admitted writes before closing storage anyway |
| `--memory-limit-mb N` | — | **deprecated** alias for `--memory-budget-mb` (an explicit `0` disables) |

**Peer and logging**

| Flag | Meaning |
|------|---------|
| `--peer-conflict-policy strict\|last-write` | how a same-document divergence pushed by a peer is resolved |
| `--log-level debug\|info\|warn\|error`, `--log-format text\|json` | structured logging |
| `--config FILE` | JSON config file |

### Configuration precedence

```
defaults  <  --config file  <  KDB_* environment  <  explicitly-set flags
```

Unknown keys in the config file are rejected, so a typo fails at startup instead of silently
configuring nothing.

### RBAC bootstrap

With `--data-dir`, users and roles are durable (stored as versioned documents in the reserved
`_system/users` and `_system/roles` namespaces). Create them while the service is **stopped** —
the data-directory lock enforces that:

```bash
# define a role and its grants (create or update)
./go/bin/kdb-service user role   --data-dir /var/lib/kdb \
    --role app-writer --grants 'read:myapp/*,write:myapp/users'
# create a user holding it
./go/bin/kdb-service user create --data-dir /var/lib/kdb \
    --user alice --password 's3cret' --roles app-writer
# later: assign another role, or list what exists
./go/bin/kdb-service user assign --data-dir /var/lib/kdb --user alice --role app-reader
./go/bin/kdb-service user list   --data-dir /var/lib/kdb
```

Grants are `<kind>:<pattern>` over `namespace/collection/document`; a trailing `/*` is a prefix
wildcard that also matches the prefix itself. Matching runs document → collection → database, so
a database-level grant covers everything beneath it and a collection grant never leaks to a
sibling.

| Kind | Allows |
|------|--------|
| `read` | opening a session, `SELECT`, reading a document |
| `write` | non-`SELECT` SQL (including `CREATE TABLE`), committing a transaction, writing or deleting a document |
| `sync` | peer-sync fetch/push on the peer listener |
| `admin` | the RBAC admin surface (create/drop user and role, grant/revoke) |

Clients authenticate with a `"user:secret"` token or user/password at handshake:
`client.Connect(ctx, addr, "alice:s3cret")`.

### Health, readiness, metrics

```bash
curl -s localhost:9099/healthz   # ok + version/commit/build_date
curl -s localhost:9099/readyz    # ready | 503 "not ready: starting|draining"
curl -s localhost:9099/metrics   # Prometheus text
```

Key metrics to alert on: `kdb_memory_zone` (0–3), `kdb_admission_denied_total`
(by class and reason), `kdb_stage_latency_seconds{stage="fsync_wait"}`, `kdb_draining`.
Full list: [Protocol & operations §8](kdb-lld-protocol.md#8-observability).

### Shutdown

`SIGTERM`/`SIGINT` → readiness flips to `draining` (load balancers stop routing) → new writes are
refused with `UNAVAILABLE` → admitted writes finish (up to `--drain-timeout`) → listeners close →
storage is flushed and sealed → exit 0. Skipping all of this (`kill -9`) is safe: the same replay
path runs on the next start.

---

## Command-line usage

The Kotlin tree ships two Gradle-launched command-line entry points. (For the Go binaries, see
[Go quick start](#go-quick-start) above and
[Operations](#operations--durability-backup-and-recovery) below — both trees read and write the
same on-disk format, so either CLI can open a workspace written by the other.)

### Product CLI (`kdb`, Kotlin)

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
| `put` | `put <namespace> <file\|json>` | Write a JSON document and append a commit; prints `{"docId":"…","docIdShort":"…","commit":"…"}` (`docIdShort` is the first 8 hex digits of the UUID, for copy/paste only) |
| `get` | `get <namespace> <docId>` | Print document JSON: full UUID (canonical or 32 hex), or an **unambiguous** case-insensitive hex **prefix** (minimum **8** hex digits, up to 31; if two or more documents at HEAD match, the CLI errors with candidate UUIDs) |
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
# stdout: {"docId":"<uuid>","commit":"<64-hex>"} — copy docId for get
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb get myapp/users <docId>"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb query myapp/users 'SELECT _doc FROM users'"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file put myapp/files --id 00000000-0000-0000-0000-0000000000f1 --zip ./report.pdf"
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb file get myapp/files --id 00000000-0000-0000-0000-0000000000f1 -o ./report-copy.pdf"
```

**Persistence:** Namespace data lives under `{dataDir}/ns/{namespaceId}/` (delta log, WAL, SSTables). Each CLI invocation replays the delta log on open; commits from a prior `put` are visible to a later `get` or `query` with the same `--data-dir`. The Go `kdb` binary uses the same on-disk layout and KDBP-framed delta segments as Kotlin file mode; **either CLI can read data written by the other** on the same `--data-dir` (same SHA-256 and commit payload rules).

**Workspace lock:** File mode takes an exclusive lock on `{dataDir}/.kdb.lock` while a process has the database open (CLI, JDBC `jdbc:kdb:file://…`, or your app via `openFileRuntime`). On macOS/Linux the Go CLI uses `flock(2)` on that file. A second process opening the same `--data-dir` fails with a clear error naming the holder PID when known. Only one live writer per workspace.

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

## SQL reference (KDB-SQL)

KDB's query language is **KDB-SQL**: SQL over documents, where the schema is an optional typed
lens and the whole document is always reachable. Complete grammar, semantics, and limits:
[KDB-SQL reference](kdb-lld-query.md).

Every namespace behaves as one table with this column shape:

| Column | Meaning |
|--------|---------|
| `kdb_id` | the document UUID (always present) |
| *schema fields* | one typed column per declared schema field |
| `_doc` | the entire document JSON (always present) |

```sql
SELECT kdb_id, name, _doc FROM users WHERE age >= 21 ORDER BY name LIMIT 50;
SELECT COUNT(*) AS n FROM users WHERE status = 'active';
SELECT name FROM users WHERE age > ? AND status = ?;      -- positional parameters
SELECT kdb_id FROM users WHERE email IS NULL;
CREATE TABLE users (name VARCHAR NOT NULL, age INT, status VARCHAR);
INSERT INTO users (name, age) VALUES ('Ada', 36);          -- ids are generated
SELECT * FROM users AT TIME '2026-08-01T00:00:00Z';        -- versioned read
```

**Supported surface by implementation** — the *server* parses the SQL, so a JVM client talking to
a Go server gets the Go grammar:

| Feature | Go engine | Kotlin engine |
|---------|-----------|---------------|
| `SELECT` + `WHERE` + `ORDER BY` + `LIMIT`/`OFFSET` | ✅ | ✅ |
| `COUNT(*)` / `COUNT(col)` | ✅ | ✅ |
| `SUM` / `AVG` / `MIN` / `MAX`, `GROUP BY` | ✖ | ✅ |
| `INNER JOIN`, `LIKE`, `IN`, `BETWEEN`, `DISTINCT` applied | ✖ | ✅ |
| `INSERT` | ✅ | ✅ |
| `UPDATE` / `DELETE` | ✖ (use `Upsert` or a delete transaction) | ✅ |
| `CREATE TABLE` | ✅ | ✅ |
| `CREATE INDEX` / `VIRTUAL VIEW` / `ALTER TABLE` | ✖ | ✅ |
| `CREATE USER/ROLE`, `GRANT`/`REVOKE` | ✖ (Go API only) | ✅ |
| `BEGIN` / `COMMIT` / `ROLLBACK` in SQL | ✖ (wire `TX_COMMIT`/`TX_ROLLBACK`) | ✅ |
| `AT VERSION` / `AT COMMIT` / `AT TIME` | ✅ | ✅ |

Things that commonly surprise people (full list in
[Part 5 §7](kdb-lld-query.md#7-semantics-limits-and-gotchas)):

- `INSERT` always mints a new document id — write at a chosen id with `PutJSON`/`Upsert`.
- The Go planner does **full scans only**; bound them with `LIMIT` and a `--scan-row-budget`.
- `NULL = NULL` is true in the Go comparator; use `IS NULL` for standard semantics.
- Comparing a string column with a number (or vice versa) compares as "equal" rather than
  coercing — compare like with like.
- Reads resolve a commit, but documents are materialised from current committed state; exact
  point-in-time document reconstruction is a known gap.

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

### Accept / reject remote changes (Phase 3)

Review incoming commits before advancing `main`:

```javascript
// Optional: notify only, decide later
await db.subscribeWithOptions((e) => console.log(e), false);

const ev = JSON.parse(eventJson);
if (ev.type === "DeltaReceived") {
  const remoteHead = ev.commitHash;
  // Inspect at that version:
  const json = await db.getAtCommit(docId, remoteHead);
  // Keep remote work:
  await db.acceptRemote(remoteHead);
  // Or fork away from it (rewind main, next put branches from ancestor):
  await db.rejectRemote(remoteHead);
  await db.put(updatedJson);
}
```

APIs: `head()`, `getBaseVersion()`, `setBaseVersion(hex)`, `acceptRemote(remoteHead?)`, `rejectRemote(remoteHead)`, `mergeBranches("main", "incoming")`.

---

## Stored procedures (`:kdb-script`)

> **Status: library-level API today.** The pieces below (`ProcedureRegistry`, `GraalProcedureRuntime`) work end-to-end and are unit-tested against the real engines, but there is no wire protocol frame or CLI subcommand yet — you drive them from Kotlin, on the JVM backend, in the same process as the rest of the engine. See [Component 32 spec](kdb-spec-layer11-component32-stored-procedures.md) for the full design and what's left (§9 Implementation phases, §11 Implementation status).

Stored procedures are restricted-JavaScript functions that run **inside the backend process**, next to storage, instead of round-tripping documents to a client for simple read-modify-write logic. They are sandboxed (no filesystem, network, process, or Java-class access) and every data access they make is re-authorized against the *calling* principal's own permissions — a procedure never runs with elevated "owner" rights, so being allowed to invoke one never implies being allowed to do what it attempts.

**Define a procedure** — source is a JS function named `main(args)`; `args` and a `kdb` object (`get`/`put`/`delete`/`query`/`log`/`callProc`) are the only globals available:

```kotlin
val registry = procedureRegistry(storage) // or inMemoryProcedureRegistry() for tests
registry.put(
    ProcedureDefinition(
        namespaceId = "orders",
        name = "shipOrder",
        source = """
            function main(args) {
              const doc = kdb.get(args.id);
              if (!doc) throw new Error("order " + args.id + " not found");
              kdb.put(Object.assign({}, doc, { status: "shipped" }));
              kdb.log("shipped " + args.id);
              return { ok: true, id: args.id };
            }
        """.trimIndent(),
    ),
)
```

**Run one:**

```kotlin
val runtime = graalProcedureRuntime(registry, hybrid, dag, storage, schema, txEngine, indexManager, authorizer)
val result = runtime.invoke(principal, "orders", "shipOrder", """{"id":"$docId"}""")
// result.value  -> JSON string returned by main()
// result.logs   -> lines from kdb.log(), capped by ProcLimits.maxLogBytes
```

All `kdb.put`/`kdb.delete` calls made during one invocation are staged and commit together as a single transaction when `main()` returns — a loop of inserts lands as one commit, not one per insert (see `GraalProcedureRuntimeTest.loopOfInserts_thenCount_allCommitAtomicallyInOneTransaction`). Because reads inside a procedure see the last *committed* head, a script can't `kdb.query` its own in-flight writes from the same invocation — count them from outside after `invoke()` returns, or have the script return its own running total.

Tune sandbox limits per call with `ProcLimits(wallClockMillis, maxHostCalls, maxLogBytes, maxStatements)` — a runaway `while(true){}` is force-interrupted at `wallClockMillis` regardless of what it's doing (`ProcException.Timeout`), not just cooperatively cancelled.

```bash
./gradlew :kdb-script:test
```

---

## Peer sync

Peers are fully independent replicas. Each keeps its own history, may accept writes while
disconnected, and reconciles on contact — fast-forwarding when one side is simply ahead,
auto-merging when both sides changed *different* documents, and reporting a structured conflict
when both changed the *same* document.

```bash
# node A
./go/bin/kdb-service --data-dir /var/lib/kdb-a --namespace myapp/users \
  --sql-addr "tcp://0.0.0.0:9090?bind=true" --peer-addr "tcp://0.0.0.0:9091?bind=true"

# node B, pointed at A
./go/bin/kdb-service --data-dir /var/lib/kdb-b --namespace myapp/users \
  --sql-addr "tcp://0.0.0.0:9190?bind=true" --peer-addr "tcp://0.0.0.0:9191?bind=true"
```

| Setting | Effect |
|---------|--------|
| `--peer-conflict-policy strict` (default) | same-document divergence returns a conflict report; the branch head is left untouched for you to resolve |
| `--peer-conflict-policy last-write` | later timestamp wins, symmetrically on every node |

Peer connections are authenticated and authorized when `--rbac` is on (`PeerSyncAction`), and can
be TLS/mTLS-protected like any other listener. The Kotlin CLI's `sync <namespace> <peer-uri>`
performs a one-shot bidirectional sync.

What to expect operationally: divergence is *normal*, merges create real two-parent commits, and
nothing is ever silently overwritten under the default policy. The classification rules are in
[Flows §12](kdb-lld-flows.md#12-peer-sync-mode-3).

---

## Operations — durability, backup, and recovery

### Durability choices

| `--durability` | Acknowledged when | Loss window |
|----------------|-------------------|-------------|
| `sync` (default) | the commit is fsynced | none for acknowledged writes |
| `async` | the commit is queued in memory | up to one flush interval / in-flight batch |
| `memory` | never written | everything on restart |

`--sync-mode fast` (F_BARRIERFSYNC / fdatasync) is an order of magnitude cheaper than `full` and
still survives process and OS crashes — but not power loss.

Concurrent commits share one physical fsync (group commit), so `sync` does **not** mean one disk
sync per write under load.

### `kdb-inspect`

All of `verify`, `repair-segments`, `backup`, and `restore --out` take the same exclusive
data-directory lock a live service holds, so they refuse to run against a directory that is open.

```bash
# 1. Check a data directory (L1 = per-frame CRC, L2 = parent closure across segments)
./go/bin/kdb-inspect verify --data-dir /var/lib/kdb --namespace myapp/users --level L2 [--json]

# 2. Repair what is provably safe: truncate a torn tail, quarantine a corrupt frame
./go/bin/kdb-inspect repair-segments --data-dir /var/lib/kdb --namespace myapp/users [--dry-run]

# 3. Back up (directory or S3; add --base-backup-id for an incremental backup)
./go/bin/kdb-inspect backup --data-dir /var/lib/kdb --namespace myapp/users --to /backups
./go/bin/kdb-inspect backup-list   --namespace myapp/users --to /backups
./go/bin/kdb-inspect backup-verify --namespace myapp/users --to /backups --backup-id <id>
./go/bin/kdb-inspect backup-fetch  --namespace myapp/users --to /backups --backup-id <id> --out /tmp/fetched

# 4. Rebuild from the verified union of one or more sources
./go/bin/kdb-inspect restore --namespace myapp/users --out /var/lib/kdb-restored \
    --source live=/var/lib/kdb --from-backup /backups --backup-id <id>

# 5. Decode a captured wire frame
./go/bin/kdb-inspect dump-wire --file ./frame.bin
```

**`--to s3`** uses the `KDB_S3_*` environment configuration below.

### What each failure looks like

| Symptom | Meaning | Action |
|---------|---------|--------|
| service starts normally after `kill -9` | a torn tail on the newest segment was tolerated | nothing — this is the designed path |
| open fails naming `repair-segments` | corruption in a segment that is **not** the newest | `verify`, then `repair-segments`; if it refuses, `restore` |
| `repair-segments` refuses and names commits | repairing would drop history later segments still reference | `restore` from a backup and/or the damaged directory |
| open fails with "legacy segment format" | a pre-Layer-13 data directory | `repair-segments` migrates it |
| `data directory locked` | another process holds the workspace | stop it, or use a server; `kdb unlock` removes a stale lock file only when the recorded PID is gone |

### Replicating to object storage

Set these before starting a file-backed runtime or service; sealed segments and snapshots are
mirrored to an S3-compatible target:

| Variable | Meaning |
|----------|---------|
| `KDB_S3_BUCKET` | bucket name — **unset disables S3 entirely** |
| `KDB_S3_REGION` | region (default `us-east-1`) |
| `KDB_S3_ENDPOINT` | custom endpoint (LocalStack / MinIO); implies path-style |
| `KDB_S3_PREFIX` | key prefix |
| `KDB_S3_PATH_STYLE`, `KDB_S3_ENSURE_BUCKET` | addressing style, create-if-missing |

### Capacity and memory

Governance is **on by default**: with no `--memory-budget-mb`, the service governs against the
container's cgroup limit, or 75 % of host RAM. Because operations reserve their estimated memory
before running, the budget can be set at the container's real limit rather than 60–80 % of it.

| Signal | Meaning |
|--------|---------|
| `kdb_memory_zone` 1 (Elevated) | scan row budgets halved; nothing client-visible yet |
| `kdb_memory_zone` 2 (High) | writes and scans refused with `BUSY`; point reads still served |
| `kdb_memory_zone` 3 (Critical) | only point reads; rescue reserve released; abort timer running |
| rising `kdb_admission_denied_total{reason="capacity"}` | the budget is too small for the offered load |
| rising `…{reason="too_large"}` | individual operations exceed the whole budget — resubmit smaller |
| exit code **75** | the abort watchdog performed an orderly shutdown; a supervisor should restart the process |

Because the commit DAG grows monotonically, a long-lived busy namespace will eventually throttle:
that is the designed degradation, and the levers are a larger budget, DAG compaction, or splitting
the namespace.

---

## Troubleshooting

| Message / symptom | Cause | Fix |
|-------------------|-------|-----|
| `data directory locked: …/.kdb.lock` | another CLI, JDBC file connection, or service holds the workspace | stop the holder, or run a server; `kdb unlock` for a stale file |
| `BUSY` / `errors.Is(err, ErrBusy)` | write queue full or memory pressure | honour `RetryAfter()`; check `kdb_memory_zone` |
| `DEADLINE_EXCEEDED` | your call's deadline passed while queued | raise the deadline; check write latency (`kdb_stage_latency_seconds`) |
| `RESOURCE_EXHAUSTED` | operation larger than the whole grant capacity, or scan row budget exceeded | resubmit smaller / narrow the query / raise `--scan-row-budget` |
| `CONFLICT` | optimistic concurrency | re-read at the reported head and retry, or use `Upsert` |
| `UNAUTHORIZED` | RBAC denial | check the principal's grants |
| `SCHEMA_VIOLATION` | the document does not satisfy the declared schema | fix the payload or migrate the schema |
| handshake rejected with a reason | wrong client mode, bad credentials, or namespace not authorized | check the listener you connected to and the token |
| `unsupported protocol version` | client and server wire versions differ | align versions |
| readiness stuck at `not ready: starting` | a listener failed to bind | check the startup log |
| `readyz` reports `draining` | shutdown or abort in progress | expected during deploys |

---

## Data layout (for inspect CLI)

Both implementations write the same tree:

```
<dataRoot>/
├── .kdb.lock                       exclusive lock while the workspace is open
├── costmodel.json                  learned scan-cost priors (kdb-service; a cache — safe to delete)
└── ns/
    └── <namespaceId>/
        ├── meta.json
        ├── delta/00000000000000000000.seg   the commit log — sequence order is commit order
        ├── wal/<walId>[.<firstSeq>]         blob write-ahead log
        ├── sstable/L0/<fileId>              flushed blob generations
        └── quarantine/                      only after `repair-segments`
```

The **delta log alone** can rebuild a namespace, which is why backup, verify, and restore all
operate on it. Byte-level formats: [Storage, Part 4](kdb-lld-storage.md#3-byte-formats).

Use `kdb-inspect dump-wire` (Go) or `dump-delta` / `dump-blob` (Kotlin) against this tree for
debugging.

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
| What KDB is, decisions, quality attributes, risks | [High-level architecture](kdb-architecture.md) |
| How it works internally (index + data model) | [Low-level design, Part 0](kdb-lld.md) |
| Every package and type | [Part 1 — Components](kdb-lld-components.md) |
| End-to-end sequences | [Part 2 — Flows](kdb-lld-flows.md) |
| Threads, locks, backpressure | [Part 3 — Concurrency](kdb-lld-concurrency.md) |
| On-disk and in-memory formats | [Part 4 — Storage](kdb-lld-storage.md) |
| Complete SQL reference | [Part 5 — KDB-SQL](kdb-lld-query.md) |
| Wire protocol, error codes, governance, metrics | [Part 6 — Protocol & operations](kdb-lld-protocol.md) |
| Normative spec, roadmap, JDBC design | [kdb-spec.md](kdb-spec.md) |
| Go module layout, interop rules | [go-porting.md](go-porting.md) |
| JDBC driver spec | [kdb-spec-layer8-component24-jdbc-driver.md](kdb-spec-layer8-component24-jdbc-driver.md) |
| Product CLI spec | [kdb-spec-layer10-component29-cli.md](kdb-spec-layer10-component29-cli.md) |
| Inspect tooling spec | [kdb-spec-layer10-component31-inspect-tooling.md](kdb-spec-layer10-component31-inspect-tooling.md) |
| Stream / browser modes | [kdb-spec-layer7-component22-stream-mode.md](kdb-spec-layer7-component22-stream-mode.md) |
| Resource governance | [kdb-spec-layer13-resource-governance.md](kdb-spec-layer13-resource-governance.md) |
| Integrity, backup, recovery | [kdb-spec-layer15-integrity-backup-recovery.md](kdb-spec-layer15-integrity-backup-recovery.md) |
| Stored procedures | [kdb-spec-layer11-component32-stored-procedures.md](kdb-spec-layer11-component32-stored-procedures.md) |

---

## Quick reference

**Go**

```bash
cd go && go test ./...
make build-go
./go/bin/kdb --data-dir /tmp/kdb-data init myapp/users
./go/bin/kdb-service --data-dir /var/lib/kdb --namespace myapp/users \
  --sql-addr "tcp://0.0.0.0:9090?bind=true" --admin-addr 127.0.0.1:9099
./go/bin/kdb-inspect verify --data-dir /var/lib/kdb --namespace myapp/users --level L2
./go/bin/kdb-inspect backup --data-dir /var/lib/kdb --namespace myapp/users --to /backups
```

**Kotlin**

```bash
./gradlew build
./gradlew :kdb-cli:runCli --args="init myapp/users"
./gradlew :kdb-jdbc:test
./gradlew :kdb-embed:jvmTest :kdb-embed:jsNodeTest
./gradlew :kdb-browser-demo:jsBrowserDevelopmentWebpack
./gradlew :kdb-service:runService --args="--memory --listen-ws kdb-ws://127.0.0.1:7443/kdb?bind=true"
./gradlew :kdb-benchmark:jmh
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
./gradlew :kdb-script:test
```

```java
Class.forName("dev.kdb.jdbc.KdbDriver");
Connection c = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
// Seed data via conn.embedded — see JDBC section above
```
