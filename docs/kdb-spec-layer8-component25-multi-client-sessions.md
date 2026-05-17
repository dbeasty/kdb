# Component 25 — Multi-client SQL sessions

## Conflict detection (optimistic)

Commits still run [`TransactionEngine.detectConflicts`](../kdb-transaction/src/commonMain/kotlin/dev/kdb/transaction/DefaultTransactionEngine.kt): each `Write`/`Delete` is checked against the transaction **base** tree vs DAG **HEAD** at commit time (`STRICT` policy rejects overlapping writers). This is git-style optimistic concurrency, not a held lock.

## Document write locks (pessimistic)

[`DocumentLockManager`](../kdb-transaction/src/commonMain/kotlin/dev/kdb/transaction/DocumentLockManager.kt) holds an **exclusive** lock per `(namespaceId, docId)` for one `sessionId` until:

- successful `TX_COMMIT` or failed commit path cleanup,
- `TX_ROLLBACK`,
- or session end.

Acquire points:

| Path | When |
|------|------|
| `TX_COMMIT` / `commitViaEngine` | All `docId`s in the transaction (re-entrant for same session) |
| `LockingTransactionBuilder` | Each `write` / `delete` buffered in `session.pending` |
| Hybrid SQL DML | Each document touched by DML ops when `documentLocks` + `writeSessionId` are set on `HybridQueryRequest` |

Reads (`SNAPSHOT`, `READ_COMMITTED`) do **not** take write locks.

`DocumentLockedException` (`KdbErrorCode.DOCUMENT_LOCKED`) is returned on the SQL wire as `SQL_RESULT.error`.

[`WriteCoordinator`](../kdb-server/src/main/kotlin/dev/kdb/server/WriteCoordinator.kt) still serializes commits on the server; locks block overlapping sessions **before** commit when possible.

## Index / JSON consistency

All successful commits through [`commitViaEngine`](../kdb-embed/src/commonMain/kotlin/dev/kdb/embed/EmbedWrites.kt) call `indexManager.writer.applyCommit` after the DAG commit. Peer sync hosts may register `PeerHostConfig.materializeCommit` to replay storage + indexes for pushed commits ([`materializeCommit`](../kdb-embed/src/commonMain/kotlin/dev/kdb/embed/EmbedOperations.kt)).

## Auxiliary caches

| Cache | Thread safety |
|-------|----------------|
| `JsonPath.compile` LRU | Platform lock around cache (`JsonPathCacheLock`) |
| `StorageVirtualViewRegistry.loaded` | Same `Mutex` as load path |
| SQL index stores | Per-store `Mutex`; updated only via `applyCommit` |
