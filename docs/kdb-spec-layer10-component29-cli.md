# KDB Component Spec — Layer 10
## Component 29: CLI
### `dev.kdb.cli`

**File:** `kdb-spec-layer10-component29-cli.md`  
**Layer:** 10 — Tooling  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-cli` (JVM only)  
**Depends on:** Layer 6–8 (`HybridQueryEngine`, `TransactionEngine`, `PeerSyncClient`), Layer 9 transport (optional `sync` over TCP), `:kdb-jdbc` embedded runtime helpers

-----

## 1. Purpose

Provides the **git-style command-line interface** for KDB (master §11). Developers manage namespaces, write and read JSON documents, run SQL/hybrid queries, inspect history, and sync with peers — all through the same public engine APIs as JDBC and stream mode, with no private engine hooks.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.jdbc` | `openFileRuntime`, `EmbeddedKdbRuntime`, `NamespacePaths` |
| `dev.kdb.transaction` | `TransactionEngine`, commit builder |
| `dev.kdb.dag` | `CommitDag`, `head`, `log` traversal |
| `dev.kdb.document` | `KdbDocument`, `KdbOp`, `KdbCommit` |
| `dev.kdb.query.hybrid` | `HybridQueryEngine.execute` |
| `dev.kdb.peersync` | `peerSyncClient`, `PeerSession` (sync subcommands) |
| `dev.kdb.transport.tcp` | `defaultTcpWireTransport` (optional peer URIs) |
| `dev.kdb.error` | `KdbException` → exit code 1 + message on stderr |

-----

## 3. Public Interface

```kotlin
package dev.kdb.cli

/** Parsed global options before subcommand. */
data class CliConfig(
    val dataDir: String = System.getProperty("user.home") + "/.kdb",
    val nodeId: String = "local",
    val quiet: Boolean = false,
)

/** Opens or creates an embedded namespace workspace under [dataDir]. */
fun openCliRuntime(config: CliConfig, namespaceId: String): CliRuntime

class CliRuntime(
    val namespaceId: String,
    internal val embedded: dev.kdb.jdbc.EmbeddedKdbRuntime,
)

object KdbCli {
    fun run(args: Array<String>): Int  // 0 success, 1 error
}

// jvmMain entry
fun main(args: Array<String>)
```

Subcommands (v1 minimum):

| Command | Args | Behaviour |
|---|---|---|
| `init` | `<namespace>` | Create namespace metadata dir |
| `put` | `<namespace> <file\|json>` | Write document + commit |
| `get` | `<namespace> <docId>` | Print document JSON |
| `query` | `<namespace> <sql>` | Run hybrid SQL, print rows JSONL |
| `log` | `<namespace>` | Print commit hashes + messages |
| `status` | `<namespace>` | HEAD hash, doc count |
| `sync` | `<namespace> <peer-uri>` | Bidirectional peer sync over transport |
| `shell` | `<namespace>` | Interactive REPL; one `openCliRuntime` per session; `use` reopens another namespace |

-----

## 4. Data Structures

```kotlin
internal data class NamespaceMeta(
    val namespaceId: String,
    val createdAt: String,
)

internal sealed class CliCommand {
    data class Init(val namespace: String) : CliCommand()
    data class Put(val namespace: String, val payload: String) : CliCommand()
    data class Get(val namespace: String, val docId: String) : CliCommand()
    data class Query(val namespace: String, val sql: String) : CliCommand()
    data class Log(val namespace: String) : CliCommand()
    data class Status(val namespace: String) : CliCommand()
    data class Sync(val namespace: String, val peerUri: String) : CliCommand()
    data class Shell(val namespace: String) : CliCommand()
}
```

Interactive shell (`shell` subcommand): `CliSession` holds `CliConfig`, current `namespaceId`, and `CliRuntime`. `runShell` reads lines via `LineReader` (default `SystemLineReader`; `ListLineReader` in tests). Line verbs: `put`, `get`, `query`, `log`, `status`, `sync`, `use`, `help`/`?`, `exit`/`quit`. Session-scoped execution reuses `executePut` / `executeGet` / … from `CliCommands`; one-shot commands wrap the same helpers after a single `openCliRuntime`.

Workspace layout under `{dataDir}` (see [`kdb-spec-layer8-file-persistence-plan.md`](kdb-spec-layer8-file-persistence-plan.md)):

```
{dataDir}/ns/{namespaceId}/
  meta.json
  delta/          # sealed segments (KDBP-framed commits)
  wal/ ...
  sstable/ ...
```

`openCliRuntime` calls `openFileRuntime(dataDir, catalog, namespaceId)` — SERVER engine + delta replay on each open; `PersistingCommitDag` appends on commit. Separate CLI invocations with the same `--data-dir` see prior `put` data. The Go `kdb` binary uses the same delta layout; Kotlin and Go can share one workspace directory.

-----

## 5. Contracts

- **Preconditions:** Namespace id matches `^[a-zA-Z0-9._/-]+$`; JSON payloads must parse for inline `put`.
- **Postconditions:** `put` advances DAG head; `get` returns latest doc at HEAD; `query` uses `HybridQueryEngine` at HEAD.
- **Exit codes:** `0` success; `1` user error or `KdbException`; `2` usage error.
- **Output:** UTF-8 stdout; errors on stderr; `--quiet` suppresses informational lines only.
- **`put` stdout:** One JSON object per successful write: `{"docId":"<uuid>","docIdShort":"<8-hex>","commit":"<64-hex>"}`. `docId` is the document UUID for `get`; `docIdShort` is the first 8 hex nibbles of the UUID (no dashes), matching the minimum accepted `get` prefix length; `commit` is the new DAG head commit hash (distinct from `docId`).
- **`get` document id:** Full canonical UUID, 32 hex without dashes, or a **case-insensitive hex prefix** of the 32-nibble form. Require at least **8** hex digits unless the token parses as a full UUID. Resolve at **HEAD** via `scanDocuments`; **0** hits → not found; **1** hit → that document; **2+** hits → stderr lists candidate UUIDs (ambiguous prefix).
- **`put` document id:** If JSON has no `"id"` field, the CLI assigns a random UUID and injects `"id"` into the stored document body so `get` returns self-describing JSON.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `IllegalArgumentException` | Unknown subcommand, bad namespace, malformed UUID |
| `KdbException` | Engine failures (schema, conflict, not found) |
| `PeerSyncException` | Handshake rejected, sync failure |
| `TransportException` | TCP/WebSocket connect failure for `sync` |

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `init_createsMeta` | `init app/demo` | meta.json exists |
| 2 | `put_inlineJson` | `put app/demo '{"id":"1","v":1}'` | exit 0; stdout JSON with `docId` + `commit` |
| 3 | `putGet_survivesReopen` | put then `openFileRuntime` reopen | JSON contains `"v":1`; `get` by `docId` works |
| 3b | `put_autoId` | `put app/demo '{"v":1}'` (no id) | stdout `docId`; stored JSON contains injected `"id"` |
| 4 | `query_select` | `query app/demo "SELECT _doc"` | ≥1 JSONL row |
| 5 | `log_listsCommits` | after 2 puts | ≥2 lines |
| 6 | `status_showsHead` | after put | non-empty HEAD |
| 7 | `usage_unknownCommand` | `kdb foo` | exit 2 |
| 8 | `put_invalidJson` | `put ns not-json` | exit 1 |
| 9 | `sync_inMemoryPeer` | two runtimes, in-memory URI | heads converge (integration) |
| 10 | `help_text` | no args | usage on stderr, exit 2 |
| 11 | `shell_putQuery_exit` | shell + `put`, `query`, `exit` | exit 0; doc at HEAD after reopen |
| 12 | `shell_use` | `use` + `status` | output shows new namespace |
| 13 | `shell_unknownCommand` | bad verb + `exit` | stderr error; exit 0 |
| 14 | `shell_help` | `help` | usage fragment on stdout |

-----

## 8. Non-Goals

- GraalVM native-image packaging (follow-on).
- Cross-process **multi-writer** safety beyond the workspace lock (still one live holder per `dataDir`).
- WAL-only document recovery without delta replay (v1 reloads via delta segments).
- Full git parity (branch, merge UI, blame) — deferred commands return usage error.
- Auth / TLS on `sync`.

-----

## 9. Implementation Notes

- Reuse `openFileRuntime(dataRoot, catalog, namespaceId)` from `dev.kdb.jdbc.file`.
- Argument parsing: manual v1 (no Clikt) to limit dependencies; upgrade in v2.
- `put` generates random `KdbUuid` when JSON has no `id` field; `ensureIdInJson` injects `"id"` into the stored body; stdout prints `docId` and `commit` (see §5).
- `sync` uses `kdb-tcp://` or `memory://` hub for tests.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC |
|---|---|
| Parser + dispatch | 400 |
| Commands | 800 |
| Runtime wrapper | 150 |
| Tests | 450 |
| **Total** | **~1,800** |
