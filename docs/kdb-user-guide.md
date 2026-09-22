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
| **Multi-writer safety** — `unique` constraints enforced on write, compare-and-set / insert-if-absent, document leases with fencing | Implemented (Go); Kotlin has none of the three |
| **Read-only replicas** — several reader processes alongside one live writer | Implemented (Go, unix only) |
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
| write from several application instances at once | unique constraints, compare-and-set, or leases | [Concurrent writers and readers](#concurrent-writers-and-readers) |
| read from several processes on one data directory | read-only runtimes alongside the writer | [Read-only replicas](#4-read-only-replicas-unix) |
| script or inspect a workspace from a shell | the `kdb` CLI (Go) or `:kdb-cli` (Kotlin) | [Command-line usage](#command-line-usage) |
| push live changes to browsers or caches | `kdb-service --stream-addr`, Mode 1 / Mode 2 subscribers | [Stream modes](#stream-subscribe-over-websocket) |
| keep independent replicas that merge later | `kdb-service --peer-addr` + `--peer` | [Peer sync and replication](#peer-sync-and-replication) |
| back up, verify, or repair a data directory | `kdb-inspect` | [Operations](#operations--durability-backup-and-recovery) |

**One writer per data directory.** A writable runtime holds `{dataDir}/.kdb.write.lock`
exclusively; a second writer fails immediately with a clear error. **Readers are different:**
read-only runtimes attach under a shared `{dataDir}/.kdb.lock` and coexist with a live writer
(unix). Use a server when more than one process needs to *write*.

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
res, err := db.Exec("UPDATE users SET age = age + 1 WHERE name = ?", "Ada") // commits immediately

tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
tx.Exec("UPDATE users SET plan = 'pro' WHERE name = ?", "Ada")
tx.Exec("INSERT INTO myapp.billing (name, plan) VALUES (?, 'pro')", "Ada") // another namespace
err = tx.Commit() // both namespaces or neither; errors.Is(err, driver.ErrConflict) on a conflict
```

- `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE` (with `UNIQUE`), and `SELECT` with positional `?`
  parameters. Values come back typed: `int64`, `float64`, `bool`, `string`, and `NULL`.
- Outside a transaction every write commits immediately. `db.Begin`/`BeginTx`, or `BEGIN` /
  `COMMIT` / `ROLLBACK` statements on one `sql.Conn`, buffer writes until commit.
- A table names a namespace the way it does on the server: `myapp.billing` is `myapp/billing`;
  an unqualified table is `<catalog>/<table>` if that namespace exists, and otherwise the URL's
  own namespace. A transaction that writes several namespaces commits them atomically.
- Isolation: read committed (default) or `LevelSnapshot` / `LevelRepeatableRead`, which read
  every namespace as of `Begin`. Under read committed a transaction's conflict check starts at
  its first write, so read-modify-write loops belong in a snapshot transaction; there a
  concurrent change is always a conflict, never a lost update. `LevelSerializable` is refused.
- All connections to one data root (or one memory group) share it, so pooling works; a file data
  root is still one process's at a time.

| DSN | Meaning |
|-----|---------|
| `kdb://memory:///catalog/namespace` | shared in-process database; every namespace in the same group (`isolate`) is part of one database |
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

Transactions across namespaces — every namespace gets its commit, or none does, including
across a crash at any point:

```go
res, err := c.CommitAcross(ctx, []client.Transaction{
    {Namespace: "bank/accounts", BaseVersion: acctHead, Writes: []client.DocWrite{{DocID: acct, JSON: debited}}},
    {Namespace: "bank/ledger",   Writes: []client.DocWrite{{DocID: entryID, JSON: entry}}}, // "" base: blind write
})
var cne *client.CrossNamespaceError
if errors.As(err, &cne) {
    // cne.Namespace refused; nothing was written anywhere. errors.Is(err, client.ErrConflict) etc. still work.
}
// res.Commits["bank/accounts"] is that namespace's new head; res.GroupID is the transaction id
// every participant commit carries.
```

The same over SQL, with a transaction whose statements name other namespaces by table
(`bank.ledger` is namespace `bank/ledger`; an unqualified table that is another namespace in the
same catalog is routed there too, and any other table means the transaction's own namespace):

```go
tx, err := c.Begin(ctx, "bank/accounts")        // BeginTx(..., client.TxOptions{Snapshot: true}) for one snapshot of every namespace
defer tx.Rollback(ctx)
tx.Exec(ctx, "UPDATE accounts SET balance = 70 WHERE owner = ?", "ada")
tx.Exec(ctx, "INSERT INTO bank.ledger (owner, debit) VALUES (?, ?)", "ada", 30)
_, err = tx.Commit(ctx)                          // both namespaces or neither
```

`BEGIN` / `COMMIT` / `ROLLBACK` sent as SQL text do the same on any wire session.

One `kdb-service` listener serves every namespace under its data root: `PutJSON`, `Commit`,
`Upsert`, leases, SQL, history and search all act on the namespace they name. A write to a
namespace that does not exist creates it; a read does not.

Each participant is checked (schema, unique keys, preconditions, conflicts) before any is
written; one refusal refuses the whole transaction. On the server the participating namespaces
must share one data root (`kdb-service --data-dir`), which is where the transaction's decision is
recorded. Reads of one namespace are read-committed as always; in-process callers that need a
consistent view of several namespaces use `server.NamespaceSet.Snapshot`. Design and guarantees:
[cross-namespace transactions](kdb-cross-namespace-transactions-plan.md).

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
| `errors.Is(err, client.ErrPreconditionFailed)` | your `PutIfAbsent`/`ReplaceIf` assertion did not hold | re-read and re-derive — **do not blind-retry**; `*PreconditionError.ActualHash` is what beat you |
| `errors.Is(err, client.ErrLockUnavailable)` | another session holds a lease on the document | wait, or report the holder from `*LockError` |
| unique collision (`UNIQUE_VIOLATION` in the message) | another document already holds that value | change the value; retrying is pointless |
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
| `--peer name=…,addr=…` (repeatable; `KDB_PEERS`) | replicate matching namespaces with another node - see [Peer sync and replication](#peer-sync-and-replication) |
| `--peer-create-namespaces` | let a peer pushing over sync v2 create a namespace this node does not hold |
| `--peer-retention-grace 168h` | how long a silent peer holds `history=none` truncation back |
| `--stream-allow-anonymous` | accept stream subscribers without credentials (the old behaviour; under `--rbac` it exposes every commit) |
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
| `unique` field enforced on write | ✅ | ✖ (metadata only) |

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
| **SQL** | `SELECT 1` (table-less, for connectivity probes); `SELECT` with `WHERE` on schema/indexed fields (including `IN (…)`, `IS NOT NULL`); `COUNT(*)` / `COUNT(col)`; `GROUP BY` with `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`; `INNER JOIN` (same catalog, two tables); `SELECT _doc …`; `AT VERSION` / `AT COMMIT` / `AT TIME`; `BEGIN` / `COMMIT` / `ROLLBACK` on embedded and network — see [SQL transactions](#sql-transactions) |
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

## Concurrent writers and readers

Several application instances can write through one `kdb-service` safely. Three primitives cover
it, in the order you should reach for them.

### 1. Unique constraints — "this natural key belongs to one document"

Declare a field `unique` in the schema; the commit path enforces it on **every** write verb
(`INSERT`, `Upsert`, `Commit`, `PutIfAbsent`). Two clients racing to claim the same email produce
exactly one winner; the loser gets `UNIQUE_VIOLATION` naming the document that already holds it.

| Rule | Behaviour |
|------|-----------|
| absent or `null` values | claim nothing — many documents may omit an optional unique field (SQL semantics) |
| number spelling | `1` and `1.0` collide (values canonicalise before comparison) |
| string case | compared **byte-wise** — case-insensitive uniqueness is something your schema must express, not a default |
| same document rewriting its own value | allowed |
| a swap or delete-then-recreate inside one transaction | allowed |
| two rows in one transaction claiming the same value | rejected |
| composite (multi-field) uniqueness | **not supported** — single-field only |

Turning an existing field unique is validated: if the stored data already contains duplicates the
migration is **rejected and rolled back**, so the schema and the namespace never disagree. On
startup the registry is rebuilt by scanning the namespace; a pre-existing duplicate is reported
(the service still starts, so you have the tools to fix it) and further writes that would compound
it are refused.

### 2. Conditional writes — insert-if-absent and compare-and-set

```go
// create exactly once
commit, err := c.PutIfAbsent(ctx, "myapp/users", docID, body)
if errors.Is(err, client.ErrPreconditionFailed) { /* someone else created it */ }

// compare-and-set against the value you read
body, hash, _ := c.GetJSONWithHash(ctx, "myapp/users", docID)
commit, err = c.ReplaceIf(ctx, "myapp/users", docID, updated, hash)

// or let the SDK run the read-modify-write loop
commit, err = c.CompareAndSwap(ctx, "myapp/counters", docID, 5, func(current []byte) ([]byte, error) {
    if current == nil {
        return []byte(`{"n":1}`), nil     // seed when absent
    }
    return bumped(current), nil            // return nil to abort with ErrAborted
})
```

`CompareAndSwap` **re-reads on every attempt** (recomputing from a stale value is the lost update
it exists to prevent) and retries only a lost race — a schema violation or unique collision is
returned immediately rather than burning attempts.

One behaviour to know: `ReplaceIf` compares the expected hash **literally**. A write whose content
is byte-identical to what is stored still fails if the hash you passed is stale — a compare-and-set
asserts that the state is still the one you read, not that your write would change anything.

### 3. Document leases — holding a document across round trips

For "a human has this record open in a form", where optimistic retry is the wrong shape:

```go
lease, err := c.AcquireLock(ctx, "myapp/users", docID, 60*time.Second)
if errors.Is(err, client.ErrLockUnavailable) {
    var le *client.LockError
    errors.As(err, &le)      // names the current holder
}
defer c.ReleaseLock(ctx, "myapp/users", docID)

lease, err = c.RenewLock(ctx, "myapp/users", docID, 60*time.Second)
// a CHANGED lease.Fence means the lease lapsed and was re-taken — your edit is stale
```

| Property | Value |
|----------|-------|
| default TTL | 30 s (asking for `0` means "the default", never "forever") |
| maximum TTL | 5 min |
| enforcement | every write path — including `Upsert` — refuses a document another session holds |
| stalled holder | a lease that expires is re-grantable; the original holder's commit is refused by a **fence check** rather than silently overwriting the new holder |
| disconnect | dropping the connection releases every lock and ends every session |
| your own commits | do **not** release a lease you took explicitly — only the implicit locks a commit takes |

Leases are advisory in one specific sense: they exclude *other sessions*, not your own subsequent
writes, and the server's clock decides expiry. The fence check at commit time is what makes the
answer safe.

### 4. Read-only replicas (unix)

Several processes may read one data directory while a writer is live:

```go
rt, err := embed.OpenReadOnlyFileRuntime("/var/lib/kdb", "myapp", "myapp/users", schema.None())
defer rt.Close()

// ... read ...
err = rt.Refresh()   // advance the view to whatever the writer has since made durable
```

| Holder | `.kdb.lock` (attach) | `.kdb.write.lock` |
|--------|----------------------|-------------------|
| writable runtime / service | shared | **exclusive** |
| read-only runtime | shared | — |
| `kdb-inspect` maintenance | **exclusive** | — |

So: many readers coexist, at most one writer exists, readers coexist with a *live* writer, and
maintenance (verify/repair/backup/restore) excludes everyone. A read-only runtime refuses every
write with `ErrReadOnly`, creates no files, and sees a **snapshot as of its open** — call
`Refresh()` on whatever cadence your freshness requirement demands, and treat staleness as an
explicit part of your contract. Unix only: the shared lock needs `flock(2)`.

---

## Peer sync and replication

Every node is a full, independent replica of the namespaces it holds. It accepts writes while
disconnected and reconciles on contact:
- **Fast-forward** when one side is simply ahead.
- **Auto-merge** when both sides changed *different* documents. The merge commit is identical on
  every node that makes it, so a mesh settles instead of merging its own merges forever.
- **Structured conflict** when both changed the *same* document and the policy won't pick a winner.

The design and its rationale are in [kdb-distributed-plan.md](kdb-distributed-plan.md).

### Keeping nodes in sync: `--peer`

A node listens with `--peer-addr` and keeps itself in step with other nodes with `--peer`. Only one
side of a pair needs `--peer`: it pushes its writes (right after each commit) and pulls the other's
(on every `interval` tick).

```bash
# the hub: only listens
./go/bin/kdb-service --data-dir /var/lib/kdb-hub --namespace site/berlin/orders \
  --peer-addr "tcps://0.0.0.0:9091?bind=true" --tls-cert hub.pem --tls-key hub.key --tls-ca ca.pem

# an edge node: listens too, and replicates everything under site/berlin/ with the hub
./go/bin/kdb-service --data-dir /var/lib/kdb-edge --namespace site/berlin/orders \
  --peer-addr "tcps://0.0.0.0:9091?bind=true" --tls-cert edge.pem --tls-key edge.key --tls-ca ca.pem \
  --peer "name=hub,addr=tcps://hub:9091,namespaces=site/berlin/*,interval=30s"
```

**`--peer` fields.** `--peer` is repeatable, and `KDB_PEERS` holds the same specs `;`-separated.

| Field | Meaning |
|---|---|
| `name` | how the peer appears in status, metrics and the control plane (required) |
| `addr` | the peer's `--peer-addr` (required) |
| `namespaces` | patterns separated by `\|`: `*` is one path segment, `**` is any depth, and `!` excludes. Default `**` |
| `mode` | `both` (default), `pull` or `push` |
| `interval` | the anti-entropy tick, default `30s`. Pulls happen at least this often |
| `user`, `password-env` | credentials. The password is read from the named environment variable, never from the flag |
| `create=true` | let a pull create namespaces this node doesn't hold yet |
| `bootstrap=snapshot` | an empty namespace starts from a snapshot of the peer's current state instead of its whole history |
| `writeback=true` | a filtered peer's projection takes writes and sends them to the source (see below) |
| `filter=` | keep only the documents of one namespace that match a SQL condition (see below). Always the last field, because the condition may contain commas |

**Connections.** Outbound connections use the node's own `--tls-*` settings, so a shared CA gives
node-to-node mTLS. Every node has a stable identity, stored in `NODE` in its data directory and
printed by `kdb node status` and `/healthz`. A data directory copied to make a *new* node must have
`NODE` deleted, and a peer that presents this node's own identity is refused.

### Partition by namespace

A namespace is the unit that replicates. The way to have a node hold only the data it needs is to
make that data its own namespaces: `tenant/<id>/…`, `site/<site>/…`. Then give each node the
matching `namespaces=` patterns. Cross-namespace transactions still work within a node.

When a group replicates to a node that lacks one of its namespaces, that group is reported as
incomplete there: its parts are visible, but the group can't be atomic on that node.

### A subset of one namespace: filtered peers

A peer with `filter=` keeps a *projection*: only the documents of one namespace that match a
KDB-SQL condition, and only those the peer's `user` may read.

```bash
kdb-service --peer "name=hq,addr=tcps://hq:4242,namespaces=orders,user=eu-site,password-env=HQ_PW,filter=region = 'EU'"
```

- **Where it lives.** The projection is its own local namespace, `orders.projection-<8 hex>` (a hash
  of the filter). It is not a peer of `orders`: it has its own commits, and it follows the source
  as documents enter the filter, change, leave it or are deleted.
- **Read-only by default.** A write to it is refused with an error that names the source.

**Writing to a projection (`writeback=true`).** With write-back on, the projection takes document
writes and deletes:
- **Local first.** A write commits locally at once and is readable there, so a site keeps working
  offline.
- **Sent on each sync.** Every sync first sends the waiting writes to the source, oldest first, and
  then pulls.
- **Applied only over what the projection saw.** The source applies a write only if each document
  it touches still has the content the projection had when the write was made. Nothing is
  overwritten blindly.
- **Conflicts and refusals.** If a document changed at the source in the meantime, or the source
  refuses the write (not authorized, a schema violation), the write is not applied. It goes to
  the projection's conflict queue with what was attempted (`GET /v1/ns/{projection}/conflicts`),
  and the projection holds the source's version again. There is nothing to merge: to keep the
  change, write it again over the current version, then dismiss the entry
  (`DELETE …/conflicts/{id}`).
- **Retries.** If the source can't decide right now (unreachable, busy, not the home), the write
  stays pending and goes again on the next sync. A resend of a write the source already applied
  is recognised, not applied twice.

A few things to keep in mind:
- **One identity.** The source sees every write-back as the peer's `user`, not as whoever wrote
  locally. Grant that user exactly what the site may change.
- **Only writes and deletes.** A projection can't take part in a cross-namespace transaction, and
  it takes no file or schema operations.
- **Leaving the filter.** A write that moves a document out of the filter is applied at the
  source, and the next pull removes it from the projection.

### Stream subscriptions (`--stream-addr`)

A stream subscriber (Go `stream.NewSubscriber`, Mode 1 read-only or Mode 2 write-back) is a
filtered projection that isn't stored. It gets the namespace's changes as `DeltaCommit` frames:
- **Only what it may read.** A document the subscriber may not read never reaches it. A document
  it held and may no longer read arrives as a delete.
- **Optional filter.** `SubscriberConfig.Filter` takes the same SQL condition as a filtered peer.
  A document that stops matching arrives as a delete.
- **Resume.** Reconnect with `ResumeFrom` set to the last position (`Connection.Position()`) and
  the subscriber is sent exactly what it missed:
  - Up to 64 commits behind, each commit is sent as it was made.
  - Further behind, or across a peer merge, it gets one frame holding the difference between its
    state and the head.

  A position the node doesn't have (never had, or truncated away under `history=none`) is refused
  at the handshake. Subscribe without one and reload.
- **No dropped frames.** A slow subscriber is caught up when it drains. Commits made from peer
  sync reach subscribers as reliably as local ones.
- **Filter and resume go together.** A resumed subscription must use the same filter as the
  position it resumes from. The node can't check this.

### Conflicts

Under `--peer-conflict-policy strict` (the default), a same-document divergence is recorded, not
applied. Three things happen:
- The local `main` is left alone.
- The peer's side is kept reachable on a `peers/<node>/branch/main` tracking branch.
- The conflict waits in the namespace's queue.

To work the queue:

```bash
kdb --data-dir /var/lib/kdb-edge conflicts site/berlin/orders
kdb --data-dir /var/lib/kdb-edge resolve site/berlin/orders <conflict-id> --take remote
```

Over the control plane, the same operations are `GET /v1/ns/{ns}/conflicts`,
`POST /v1/ns/{ns}/conflicts/{id}/resolve` with `{"choices":{"<docId>":{"take":"local"|"remote"}|{"body":"<json>"}}}`,
and `DELETE …/conflicts/{id}` to dismiss. A resolution is an ordinary merge commit, and it
replicates like any other write.

`--peer-conflict-policy last-write` makes the later write win on every node. Commits are always
stamped after everything they descend from, so a write made after seeing another one wins even
from a node whose clock runs behind.

The queue also records two things nothing can merge away:
- documents that replicated into a unique-key clash
- replicated schema or index definitions that can't be applied here

### Resolution chains: deciding conflicts automatically, per namespace

A **resolution chain** is a namespace's ordered list of rules for settling a same-document conflict
when nodes merge. Each rule either decides or passes to the next:

| Rule | Decides |
|---|---|
| `source-priority` | the value written by the higher-ranked node. `nodes` lists node ids, highest first; a node not listed ranks below all of them |
| `validity` | the value that passes the namespace's schema, when the other doesn't |
| `field-merge` | a field-by-field merge, when the two sides changed different top-level fields |
| `last-write` | the later write (always decides; must be last) |
| `queue` | nothing: the conflict is queued for the application or an operator, whatever `--peer-conflict-policy` says (must be last) |

A chain that runs out without deciding falls back to `--peer-conflict-policy`.

For example, "the AWS node is the source of truth, otherwise merge fields, otherwise ask":

```bash
curl -X PUT https://kdb:9443/v1/ns/site/berlin/orders/resolution -d '{
  "rules": [
    {"kind": "source-priority", "nodes": ["<aws node id>"]},
    {"kind": "field-merge"},
    {"kind": "queue"}
  ]}'
```

`GET …/resolution` shows the chain and its hash. `{"rules": []}` removes it.

A merge must come out the same on every node, so the chain is a replicated definition in
`_kdb/meta`, like a schema. Peers compare chain hashes at the start of each sync. **While the hashes
differ, neither node merges that namespace**:
- a divergence is queued;
- `main` is pushed only where it fast-forwards the peer, and the sync result reports
  `ResolutionMismatch`.

This clears on the next sync after the definition has replicated.

Resolvers registered on the Go API (`ConflictPolicyCustom`) now also receive the common base value
(`BaseDoc`) and which node wrote each side, in which commit and when (`ExistingOrigin`,
`IncomingOrigin`).

### Handing conflicts to your application: the resolver authority

When your application has the business rules that decide a conflict, end the chain with an
`authority` rule. It takes whatever the earlier rules left undecided and hands it to a
**resolver authority**: a service of yours, or a person, holding the `resolve` permission.

```json
{"rules": [
  {"kind": "field-merge"},
  {"kind": "authority", "pending": "hold", "node": "<node id>", "timeout": "24h"}
]}
```

| Field | Meaning |
|---|---|
| `pending` | What the namespace holds while it waits. `hold` (default): the conflict stays unmerged and queued, like `queue`. `provisional`: merge now by last write, and let the authority overrule it later. |
| `node` | The one node that tells the authority, so it hears about a conflict once. The home node or hub is the natural choice. Empty: every node tells it. `kdb node status` prints a node's id. |
| `timeout` | How long a conflict waits (a duration such as `24h`). Past it, the fallback becomes final: a held conflict is settled by last write, and a provisional value simply stands. Empty waits forever. |

**Why the authority decides after the merge, not during it.** Automatic rules run inside the merge,
so they must decide identically on every node. The authority's decision instead becomes an
ordinary commit, which replicates like any write. So your service can apply any logic it likes:
call other systems, look up which site is trusted, or ask a person.

**Learning about conflicts.** Choose one of two ways:
- **Webhook.** Start the notifying node with `--conflict-webhook https://resolver.internal/kdb` and
  `--conflict-webhook-secret` (or `$KDB_CONFLICT_WEBHOOK_SECRET`).
  - Each conflict is POSTed as `{"type":"kdb.conflict","node",...,"namespace",...,"conflict":{...}}`.
  - The body is signed with HMAC-SHA256: `X-KDB-Signature: sha256=<hex>`. Verify it over the raw
    body.
  - `X-KDB-Delivery` carries the conflict id, so you can deduplicate: delivery is at least once.
  - Anything other than a 2xx is retried every `--conflict-webhook-interval` (default 10s).
- **Polling.** `GET /v1/ns/{ns}/conflicts?authority=true&undelivered=true`, then
  `POST /v1/ns/{ns}/conflicts/{id}/ack` for each one you have taken. It won't be listed as
  undelivered again unless its report changes.

**What each conflict tells you.** Per document:
- both values (`items`: `localDoc`, `incomingDoc`);
- the value they both started from (`details[].base`);
- which node wrote each side, in which commit and when (`details[].localOrigin` /
  `incomingOrigin`: `nodeId`, `commit`, `timestampMicros`).

**Deciding.** Settle a conflict with
`POST /v1/ns/{ns}/conflicts/{id}/resolve {"choices":{"<docId>":{"take":"local"|"remote"}|{"body":"<json>"}}}`.
- For a `provisional` entry, `local` is the value the merge kept (confirm it) and `remote` is the
  value it displaced (overrule it).
- The resolution is a commit whose message is `kdb:resolve/1 <id>`. Every node that receives it
  closes its own copy of the conflict.
- A provisional decision is refused (409 `stale`) if the document changed after the merge. Decide
  again against its current value.

**Settling many at once.**
`POST /v1/ns/{ns}/conflicts/resolve-all {"take":"remote","filter":{"origin":"<node id>"},"dryRun":true}`
previews taking one side for every matching conflict ("the AWS node is right about all of these").
Drop `dryRun` to apply it. You can filter by `kind`, `peer` and `origin`. From the command line:

```bash
kdb --data-dir /var/lib/kdb resolve site/berlin/orders --all --take remote --origin <node id> --dry-run
```

**A client resolving its own conflicts.** Use the same endpoints:
- `GET /v1/ns/{ns}/conflicts?doc=<id>` lists every candidate version of one document;
- `POST .../resolve` settles it.

**Permissions.** In a namespace whose chain has an `authority` rule:
- settling, acknowledging and dismissing need `resolve` (`GRANT RESOLVE ON COLLECTION app.data TO
  resolver`, stored as the grant `resolve:app/data`);
- the resolution is also a write, so the principal needs `write` as well;
- a writer without `resolve` gets 403.

Other namespaces keep the old rule, where commit rights are enough.

**Across nodes and versions.**
- Peer sync protocol v1, which Kotlin peers speak, carries no chain hash. So a namespace with a chain
  never merges over v1: divergences are queued, and `main` is pushed only as a fast-forward.
- Kotlin reads the resulting history (the merge and `kdb:resolve/1` commits) like any other.
- The Go CLI's `kdb sync` and `kdb resolve` read the chain from the data directory's `_kdb/meta`.
- `kdb resolution <ns>` shows the chain.

### Reading beyond the filter: read-through

A filtered peer holds only the documents that match its filter. Add `readthrough=true` and a
client's read of any *other* document is answered from the source instead of "not found":

```bash
--peer "name=hq,addr=tcps://hq:4242,namespaces=orders,readthrough=true,filter=region = 'EU'"
```

- **Consistent.** The document is read as of the source commit the projection is at, so it is
  never newer than what the edge already holds. Once the projection syncs forward, so do its reads.
- **Verified.** The source sends a Merkle proof of what that commit's tree holds for the document,
  and the whole commit, which the edge checks hashes to the one it asked for. A wrong, stale or
  invented answer is refused, not served. The edge never needs the source's tree.
- **Bounded.** Fetched documents go into an in-memory cache of the 1024 most recently read. It is
  never committed, never replicated, and never makes a document part of the projection.
- **The source's read rights apply**, per document, with the peer's credentials.

Only point reads (a document by id, over the wire or the control plane) read through. Queries see
the projection alone.

### Embedding sync in your application: `syncnode`

An application that embeds KDB (through `embed.Host` and its own `server.NamespaceSet`) becomes a
sync node with `go/kdb/syncnode`, the same code `kdb-service` runs:

```go
node, err := syncnode.Open(host, set, primary, syncnode.Config{
	Peers:   []replication.PeerConfig{{Name: "cloud", Addr: "wss://api.example.com/kdb/sync",
		Namespaces: []string{"app/u/42"}, CreateLocal: true, Credentials: currentToken}},
	DataDir: dataDir,
})
set.SetOpener(node.Opener(nil)) // or call node.Prepare(rt) from your own opener
// ... open the namespaces you serve ...
node.Start()
defer node.Close()
```

- `host` may be nil: namespaces are then in memory, which is what tests use.
- The working set can change while it runs: `node.Replicator().AddNamespaces("cloud", "app/m/7")`
  when a user joins a match, `RemoveNamespaces` when they leave, `SetPeerNamespaces` to replace
  the set. Progress on namespaces that stay is kept.
- `Credentials` is called for every connection, so a token that expires can be refreshed.

### Peer sync over HTTP (WebSocket)

A node can serve peer sync from an HTTP server instead of a raw TCP port, on the same origin and
certificate as its API. That is what phones behind mobile networks and captive portals need.

- **In an application:** mount `node.Handler()` on your router, for example at `/kdb/sync`.
- **In `kdb-service`:** `--peer-http 0.0.0.0:8443` serves it at `/kdb/sync`. It is plain HTTP, so
  terminate TLS in front of it.
- **Dialling:** peers use `addr=ws://host/kdb/sync` or `wss://host/kdb/sync`.
- **Bearer token:** `token-env=VAR` on the peer spec, or `Token` / `Credentials` in Go. It is sent
  as `Authorization: Bearer ...` on the upgrade request, and in the sync hello. The node's auth
  engine authenticates from either.

### Pull-only and push-only peers

The auth engine can grant each direction of peer sync separately, per namespace. For example, a
phone may pull its cloud-authored `app/u/42/ro` but never push to it.

- **Registry engine (RBAC):**
  - `sync:<ns>` is both directions, as before.
  - `sync_pull:<ns>` grants pull only, and `sync_push:<ns>` grants push only (`GRANT sync_pull ON ...`).
- **Custom engine:** it answers `auth.PeerSyncAction` for both directions. To limit a peer to one
  direction, deny that action and answer `auth.PeerPullAction` or `auth.PeerPushAction`. Engines
  that only know `PeerSyncAction` keep working unchanged.
- **At hello:** the node tells the peer each namespace's access. The peer skips a direction it
  isn't granted instead of failing the namespace.
- **Every frame is checked:** fetches, snapshots and tree reads need pull; ref updates and pushed
  grafts need push. A grant revoked mid-session takes effect on the next frame.
- **Per-document checks:** `syncnode.Config.AuthorizePushedDocuments` also asks the engine about
  every document a push writes or deletes (`DocumentWriteAction` / `DocumentDeleteAction`), and
  refuses the page at the first denial.

### Read-modify-write on a replicating node: `PutJSON` with a precondition

On a node that syncs, peers write too: a replicated merge can land between your read and your
write. Writing with `embed.PutJSONDocument` bypasses the server's write gate, so it can fail with
"branch main moved", or overwrite a value it never saw. Use the runtime's gated write instead:

```go
for {
	body, hash, found, err := rt.ReadForUpdate(ns, id)
	// ... decide the new body from body ...
	expect := &server.Expect{ContentHash: hash}
	if !found {
		expect = &server.Expect{Absent: true}
	}
	_, err = rt.PutJSON(ns, id, newBody, expect, principal)
	var changed *server.PreconditionFailedError
	if errors.As(err, &changed) {
		continue // someone - a peer, another writer - changed it; decide again
	}
	break
}
```

- `PutJSON` replaces the whole document: fields the new body leaves out are removed.
- It goes through the same write gate as every other write, including peer ingest.
- The precondition is checked at the front of the gate, so it holds at the instant of the commit.
- `Expect{Absent: true}` is a create that must not overwrite. A nil `Expect` is an unconditional
  replace.

### Definitions by pattern, and peers that see only their own

With a namespace per user or per match, one definition per namespace does not scale, and sending
every definition to every peer leaks namespace names (which contain user ids).

**Patterns.** A resolution chain or a home can be defined for a pattern:
- `PUT /v1/ns/app%2Fu%2F*/resolution` from the control plane, or `Meta().SetResolution("app/u/*", chain)` in Go.
- It applies to every matching namespace, open now or later.
- A namespace's own definition overrides the pattern.
- Among patterns, the most specific wins: more literal segments, then fewer `**`, then name order.

  This is the same on every node, so chain hashes agree.

**Scoped peers.** `meta=scoped` on a peer spec (`ScopedMeta` in Go) makes the node take
definitions from that peer as a view instead of syncing `_kdb/meta` whole. The view is every
pattern definition, plus the definitions of the namespaces this node was granted.
- It arrives at the start of each session (`META_VIEW`), before any namespace syncs, so merges use
  the right chain.
- A scoped node must not also sync `_kdb/meta` whole with anyone: its copy is a subset.

### Many namespaces: idle close

Each open namespace holds about 40 KB of heap and 2 file descriptors, however little it stores. A
process with a namespace per user or per match should close the ones nobody is using. In
`syncnode`:

```go
syncnode.Config{Idle: &syncnode.IdleConfig{MaxOpen: 20000, IdleAfter: 30 * time.Minute}}
```

- The least recently used namespaces close first, and never while a write or sync is in flight.
- A closed namespace is still served to peers. A write, read or sync reopens it in a few milliseconds.
- At startup the namespaces on disk are known without being opened.
- `node.CloseIdle(time.Now())` sheds idle namespaces at a moment of your choosing, for example when a phone app goes to the background.

### Moving a namespace's home from your application

A single-home namespace (only its home accepts writes) can be moved by the application itself,
for example to resume a match on another device:

- **From the current home:** `node.Handover(ns, toNode, addr)`.
- **From the device that wants it:** `node.RequestHome(peer, ns, addr, reason, false)`. The current
  home's `Config.HandoverPolicy` decides.

  On yes, the home assigns the namespace to the requester and raises the fence. The requester
  syncs at once and can write when the call returns. Without a policy, every request is refused.
- **When the home is unreachable:** `RequestHome(..., force=true)` asks the node that the
  namespace's resolution chain names as its authority (`{"kind":"authority","node":"<id>"}`).
  That node may reassign the home without the old one.

  The old home's writes made after the move are refused by the fence wherever they arrive.
  Writes it made but never delivered are lost, which is the cost of not waiting for it.

A node may only ask for itself, and only for a namespace it may push to.

### Getting history back after a snapshot join: deepen

A node that joined with `bootstrap=snapshot` holds its peer's state without the history before it.
That is fast, but it has two costs:
- history below that point can't be read;
- the node **cannot sync with any node that has history of its own**. The other node can't store
  a commit whose parents it has never seen, and the sync reports `unrelated-history`.

**Deepen** fetches that history from a peer that has it:
- `deepen=true` on the peer spec does it after each sync:
  `--peer "name=hub,addr=tcps://hub:4242,bootstrap=snapshot,deepen=true"`.
- `POST /v1/ns/{ns}/deepen {"peer": "hub"}` does it on demand.
- So does `kdb deepen <namespace> <peer-addr>` (exit 3 while shallow roots remain).

Each fetched commit is verified against its hash, logged, and replayed like any other. If the peer's
own history also started from a snapshot, what it lacks stays shallow. Deepening from a node with
the whole history finishes the job.

Deepen is safe to interrupt. Run it again and it finishes, without fetching what it already has.

### Merging a history nobody else holds: allowUnrelated (graft)

Deepen needs a peer that still holds the history. When none does (the node the snapshot came from
is gone), a namespace can instead merge the two histories as they are. Set `allowUnrelated` in the
namespace's resolution chain, on the nodes that should merge:

```bash
curl -X PUT -H 'Authorization: Bearer ...' \
  http://node:7070/v1/ns/app%2Fdata/resolution \
  -d '{"allowUnrelated": true, "rules": [{"kind": "source-priority", "nodes": ["<hub id>"]}, {"kind": "last-write"}]}'
```

What happens:
- **The node without the history grafts the other's root.** It fetches the peer's state at its
  snapshot root, checks every body against the root's tree hash, and keeps it as a root of its own.
  That works whichever node dials: a pull grafts, and a push sends the root first (`GRAFT_PUSH`),
  so a hub that is only ever dialled still takes an edge's history.
- **The two heads merge with an empty base.** A document only one side holds is adopted. A
  document both hold with different values is a conflict for the chain's rules. Both nodes build
  the same merge commit.
- **It is durable.** The grafted state goes into the blob store, the root is recorded in the
  namespace's `meta.json` (`graftRoots`), and a restart or a replay of the log rebuilds it all.

Limits:
- **Needs `history=full` (tree objects).** A `history=none` namespace refuses with
  `unrelated-history` and a reason pointing at deepen.
- **A delete does not win.** With an empty base, a document one side deleted and the other still
  holds comes back.
- **The whole root state is held in memory** until it is verified (the tree hash is checked only
  once the last page is in).
- **The Kotlin CLI cannot read a grafted namespace.** It refuses the directory loudly, as it does a
  snapshot-rooted one; it never serves a wrong value.
- Nodes whose chains differ (including `allowUnrelated`) do not merge at all; the chain's hash is
  compared at every sync.

### Reading your own writes across replicas: sessions

A client that writes through one replica and then reads through another can, by default, read an
older value: the second replica may not have received the write yet. A **session** (Go client)
prevents that, and also ensures a session never reads something older than what it has already
read (Bayou's session guarantees):

```go
s := client.NewSession()
s.Upsert(ctx, "orders", id, body)        // remembers the commit
other := otherReplicaClient.NewSession()
other.Resume(s.Token())                  // the user moved to another replica
body, _, err := other.GetJSON(ctx, "orders", id)
```

A replica serves the read only once its main contains the session's newest commit. It waits up to 2
seconds for replication, and otherwise answers `BUSY` with a retry-after rather than an older value.
Only a Go `kdb-service` honours the token.

### Self-repair: scrub, and comparing with a peer

A **scrub** re-reads every live document and checks its body against the content hash the
namespace's tree records. If a body can't be read or doesn't match (bit rot, a bad sector), the
scrub fetches it by content hash from a peer, verifies it, and writes it back in a `kdb:repair/1`
commit.
- The content is the same, so the tree doesn't change and peers see a commit that changes nothing
  for them.
- The peer needn't be trusted: a body that doesn't hash to what the tree names is refused.
- A document no peer can supply stays as a `damaged` entry in the conflict queue. The next clean
  scrub clears it.

Running a scrub:
- **On a schedule:** `--scrub-interval 24h` scrubs every namespace, repairing from the `--peer`
  nodes. A scrub reads every body, so scale the interval to the data.
- **On demand:** `POST /v1/ns/{ns}/scrub` returns the report: 200 when clean, 409 when damage is
  left. `{"repair": false}` only reports.
- **From the command line:**

```bash
kdb --data-dir /var/lib/kdb scrub site/berlin/orders --peer tcp://hub:7401
```

It exits 3 when damage is left.

**Comparing with a peer.** `GET /v1/ns/{ns}/peers/{peer}/diff`, or
`kdb peer-diff <ns> <peer-addr>`, lists the documents two nodes hold differently. It compares subtree
hashes of the two trees, not documents, so equal trees cost one round trip and each level of
difference one more.

**A damaged log keeps its history.** A frame damaged in the middle of the delta log used to make
the next open drop every commit after it. Open now recognizes damage in place: the file is its
recorded length and its intact frames still end at the checkpoint's commit. It then opens from
the checkpoint, logs which frames are damaged, and only the bodies in those frames are unreadable
until a scrub repairs them.
- A log that was cut short or replaced is still distrusted, as before.
- Kotlin, which replays without checkpoints, refuses to open such a log rather than serve less than
  was committed.
- A log Go has repaired still holds the damaged frame, because repair appends and never rewrites.
  So Kotlin refuses that log too.

### Joining, catching up, and retention

**Joining.** A new node either fetches a peer's whole history or, with `bootstrap=snapshot`,
installs the peer's current documents and starts from there. The snapshot is verified against the
commit it claims, and it survives crashes like any other state. An empty node joining a peer that
itself only has recent history bootstraps from a snapshot automatically.

**Retention.** Under `history=none`, retention won't delete commits that an active peer hasn't
been seen to receive. That covers peers this node pushes to and peers that fetch from it.
`--peer-retention-grace` (default 7 days) is how long a silent peer holds history back. Past it,
the peer catches up by snapshot when it returns.

### Definitions travel with the data

Schemas (`CREATE TABLE`) and indexes (`CREATE INDEX`/`DROP INDEX`) are recorded in the reserved
namespace `_kdb/meta`. It replicates alongside every peer's namespaces unless a pattern excludes it,
and each node builds the indexes locally. Under `--rbac`, a peer therefore also needs `sync` on
`_kdb/meta`.

### Single-home namespaces (opt-in strong consistency)

By default every node that holds a namespace accepts its writes. In that mode a unique constraint
or a compare-and-set only holds on the node that checked it, because two nodes can each accept
`email=x` while apart. Where that matters, give the namespace a **home**: one node that alone
accepts its writes.

```bash
curl -X PUT -H "Authorization: Bearer $TOKEN" \
  -d '{"node":"<home node id>","addr":"tcp://home:9090"}' \
  http://node:7070/v1/ns/billing%2Finvoices/home
```

**Where writes go.** Every other node still holds and serves the namespace, and replicates the
home's writes. A write sent to one of them is refused with `NOT_HOME`, and the refusal names the
home's address. `client.NewRouter` in the Go SDK follows that redirect by itself, and afterwards
sends the namespace's writes straight to the home.

**Cross-namespace transactions.** A cross-namespace transaction that touches a namespace homed
elsewhere is refused: atomicity needs one node to decide every part.

**Moving the home.** Moving the home is the same `PUT` naming another node. Every assignment
raises a **fence**, and the home stamps its commits with it. A write the old home makes before it
hears it has been replaced is refused wherever it arrives: it's queued as a conflict, not applied.
`GET /v1/placement` lists every assigned home. There is no automatic failover; moving a home is an
operator's decision.

### Moving a namespace to another node

1. **Add the new node** with `--peer name=…,addr=<a node holding it>,namespaces=<ns>`. Add
   `bootstrap=snapshot` to start it from the current state rather than the whole history.
2. **Wait until it's caught up.** In `GET /v1/peers` on the new node, the namespace's `remoteMain`
   should equal the source's head, and `kdb_replication_last_success_seconds` should be recent.
3. **If the namespace is single-home,** `PUT /v1/ns/{ns}/home` naming the new node.
4. **Remove the old node's `--peer` entry** for the namespace, or stop the old node. Its data can
   be deleted once nothing lists it as a peer.

### Observing it

- **Metrics.** `/metrics` exposes:
  - `kdb_replication_last_success_seconds{peer}`
  - `kdb_replication_consecutive_failures{peer}`
  - `kdb_replication_commits_total{peer,namespace,direction}`
  - `kdb_conflicts_open{namespace,kind}`
- **Control plane.** `GET /v1/peers` shows each peer's progress, `POST /v1/peers/{name}/sync` syncs
  now, and `GET /v1/placement` lists single-home assignments.
- **One-shot sync.** `kdb sync <namespace> <addr>` runs a single sync from the CLI.

Peer connections are authenticated and authorized under `--rbac` (`sync` permission per
namespace). The classification rules are in [Flows §12](kdb-lld-flows.md#12-peer-sync-mode-3).

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
| `UNIQUE_VIOLATION` | another document already holds that value of a `unique` field | change the value — retrying is pointless. The message names the owning document |
| `PRECONDITION_FAILED` in a conflict report | your `PutIfAbsent`/`ReplaceIf` assertion did not hold | re-read and re-derive; `actualContentHash` is the value that beat you |
| `document ... is locked by session ...` | another session holds a lease | wait for expiry (default 30 s), or coordinate with the holder |
| `kdb: runtime is open read-only` | a write against a replica | write through the writer process or the service |
| `read-only data directory access requires flock(2)` | read-only open on a non-unix platform | not supported there |
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
