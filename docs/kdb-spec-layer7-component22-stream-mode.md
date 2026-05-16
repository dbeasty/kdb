# KDB Component Spec — Layer 7
## Component 22: Stream Mode (Mode 1 + Mode 2)
### `dev.kdb.stream`

**File:** `kdb-spec-layer7-component22-stream-mode.md`  
**Layer:** 7 — Network Foundation  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-stream`  
**Depends on:** Layer 7 Component 21 (`WireCodec`, `WireMessage`), Layer 3 (`TransactionEngine`, `StorageAdapter`), Layer 5 (`IndexManager`, `IndexWriter`), Layer 6 (`HybridQueryEngine` — optional read path), Layer 4b (`EnlistmentManager` — browser catch-up, 11d)

-----

## 1. Purpose

Implements **Mode 1 (pure stream)** and **Mode 2 (write-back stream)** from master §8.1: subscribers receive `DeltaCommit` broadcasts, track position by last commit hash, apply **index hints** locally without rebuilding from documents, and (Mode 2) submit upstream transactions via `TransactionReplay` with `ConflictReport` handling.

This module owns the **coordinator/subscriber session** state machine on top of `WireCodec`. It does **not** implement full peer DAG sync (Mode 3 — Layer 8). Transport bytes flow through an injected `WireTransport` (implemented by Layer 9 adapters or in-memory test doubles).

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.wire` (21) | `WireCodec`, `WireMessage`, `DeltaCommitPayload`, `HandshakePayload`, `WireClientMode` |
| `dev.kdb.transaction` | `TransactionEngine`, `Transaction`, replay APIs |
| `dev.kdb.storage` | `StorageAdapter` — optional local doc cache for Mode 2 reads |
| `dev.kdb.index` | `IndexManager`, `IndexWriter`, `IndexHint`, `applyHints` |
| `dev.kdb.dag` | `CommitDag` — coordinator side only; subscribers have no DAG in Mode 1 |
| `dev.kdb.error` | `ConflictException`, `CompactionBoundaryException` |
| `dev.kdb.compaction` | `CompactionIntent` — subscriber handles notice (snapshot fetch) |
| `dev.kdb.storage.manager.enlistment` (11d) | `EnlistmentManager` — post-snapshot delta catch-up |

-----

## 3. Public Interface

```kotlin
package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.index.IndexManager
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow

/** Coordinator (publisher) side — backend or peer acting as hub. */
interface StreamCoordinator {
    suspend fun start(session: StreamSessionConfig)
    suspend fun stop()
    suspend fun publish(commit: PublishedCommit)
    val subscribers: Flow<SubscriberState>
}

/** Subscriber (Mode 1 or 2) side. */
interface StreamSubscriber {
    suspend fun connect(config: StreamSubscriberConfig): StreamConnection
    suspend fun disconnect()
    val events: SharedFlow<StreamEvent>
}

fun streamCoordinator(
    wire: WireCodec,
    transport: WireTransport,
    indexManager: IndexManager,
    dag: dev.kdb.dag.CommitDag,
): StreamCoordinator

fun streamSubscriber(
    wire: WireCodec,
    transport: WireTransport,
    indexManager: IndexManager,
    transactionEngine: TransactionEngine? = null,  // required for Mode 2
): StreamSubscriber

data class StreamSessionConfig(
    val namespaceId: String,
    val nodeId: String,
    val headProvider: suspend () -> KdbHash,
)

data class StreamSubscriberConfig(
    val namespaceId: String,
    val nodeId: String,
    val mode: StreamClientMode,
    val coordinatorUri: String,                   // opaque to wire — transport resolves
    val resumeFrom: KdbHash? = null,
)

enum class StreamClientMode {
    READ_ONLY,          // Mode 1 — no local DAG, no writes
    WRITE_BACK,         // Mode 2 — upstream writes via TransactionReplay
}

data class PublishedCommit(
    val commitHash: KdbHash,
    val parentHash: KdbHash,
    val operations: List<dev.kdb.document.KdbOp>,
    val indexHints: List<dev.kdb.index.IndexHint>,
    val timestampMicros: Long,
)

data class StreamConnection(
    val namespaceId: String,
    val mode: StreamClientMode,
    val position: () -> KdbHash?,
    suspend fun submitTransaction(transaction: dev.kdb.document.KdbTransaction): ReplayResult,
)

sealed class ReplayResult {
    data class Applied(val commitHash: KdbHash) : ReplayResult()
    data class Conflict(val report: dev.kdb.transaction.ConflictReport) : ReplayResult()
    data class Rejected(val reason: String) : ReplayResult()
}

sealed class StreamEvent {
    data class Connected(val negotiatedEncoding: PayloadEncoding) : StreamEvent()
    data class DeltaReceived(val commitHash: KdbHash, val hintCount: Int) : StreamEvent()
    data class PositionUpdated(val commitHash: KdbHash) : StreamEvent()
    data class CompactionWarning(val boundary: KdbHash) : StreamEvent()
    data class IceArchived(val originalHash: KdbHash, val location: String) : StreamEvent()
    data class Disconnected(val cause: Throwable?) : StreamEvent()
    data class Error(val throwable: Throwable) : StreamEvent()
}

/** Transport boundary — Layer 9 implements for TCP/WS; tests use memory channel. */
interface WireTransport {
    suspend fun connect(uri: String): WireConnection
}

interface WireConnection {
    suspend fun send(frame: ByteArray)
    fun incoming(): Flow<ByteArray>
    suspend fun close()
}

/** Apply index hints without document re-index (master §8.6). */
interface IndexHintApplier {
    suspend fun apply(namespaceId: String, hints: List<dev.kdb.index.IndexHint>)
}

fun defaultIndexHintApplier(indexManager: IndexManager): IndexHintApplier
// v1: routes each hint to IndexWriter.put/delete per IndexHintAction; extend IndexManager if needed

data class SubscriberState(
    val nodeId: String,
    val mode: StreamClientMode,
    val lastAck: KdbHash?,
)
```

-----

## 4. Data Structures

### Coordinator publish pipeline
1. Commit lands on coordinator DAG (`dag.appendCommit` — outside this module).
2. Index hints collected from `IndexWriter` / `DefaultIndexStack.drainHints()` during commit apply (or supplied by caller in `PublishedCommit`).
3. `publish(PublishedCommit)` → encode `WireMessage.DeltaCommit` → fan-out to all connections with matching namespace subscription.

### Subscriber receive pipeline (Mode 1)
1. Handshake `STREAM_READ_ONLY` + `resumeFrom` hash.
2. On `DeltaCommit`: validate parent chain against `position` (strict order v1); `IndexHintApplier.apply`; update `position`; send `PositionAck`.
3. On `CompactionNotice`: emit `StreamEvent.CompactionWarning`; if `position` below boundary → `CompactionBoundaryException` path (request snapshot via 11d / `SnapshotRequest`).
4. On `IceArchiveNotice`: emit event; if pinned version archived, surface to app.

### Subscriber write-back (Mode 2)
- `submitTransaction` encodes `TransactionReplay` with coordinator HEAD as `baseVersion` from last `DeltaReceived`.
- Coordinator runs `TransactionEngine.replay` (injected on coordinator factory — separate `StreamCoordinatorFactory` accepts engine).
- Response: `ConflictReport` message or implicit ack via subsequent `DeltaCommit` with result hash.

### Position tracking
`lastCommitHash: KdbHash?` — monotonic along single branch v1. Forked deltas rejected with `StreamDesyncException` (subscriber must snapshot-resync).

### Browser enlistment catch-up (11d integration)
After `EnlistmentManager.restoreSnapshot`, background task:
```
subscriber.connect(resumeFrom = anchorHash)
// receive deltas until coordinator HEAD
```
Documented hook: `StreamSubscriber.connect` with `resumeFrom` — aligns with 11d §9 background sync.

### In-memory test transport
`InMemoryWireTransport` pairs two `Channel<ByteArray>` ends for unit tests without Layer 9.

-----

## 5. Contracts

### `StreamCoordinator.publish`
- **Preconditions:** Active session; `commit.parentHash == headProvider()` before append (caller responsibility).
- **Postconditions:** All connected subscribers receive frame within one event-loop tick (best-effort); failed send removes subscriber from fan-out list.

### `StreamSubscriber.connect`
- **Preconditions:** Mode 2 requires non-null `transactionEngine` on factory.
- **Postconditions:** Handshake completes with `accepted=true`. Initial `PositionUpdated` if `resumeFrom` still known to coordinator (else coordinator sends catch-up burst or snapshot recommendation).

### `IndexHintApplier.apply`
- **Postconditions:** Index state reflects hints idempotently; duplicate hint for same `(docId, indexId, commitHash)` is no-op.

### `PositionAck`
Sent after successful hint application. Coordinator updates `SubscriberState.lastAck`.

### Ordering
v1: single-branch total order. Out-of-order parent → `StreamDesyncException`, subscriber disconnects and requires resync.

### Compaction safety
If subscriber `position` is below `CompactionNotice.boundary`, throw `CompactionBoundaryException` (Layer 0 error model) and emit event — matches Layer 6 coordinator semantics.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `StreamDesyncException` | Parent hash mismatch vs local position |
| `CompactionBoundaryException` | Position compacted away |
| `ConflictException` | Mode 2 replay failed (also in `ReplayResult.Conflict`) |
| `UnsupportedProtocolVersionException` | Handshake failed |
| `StreamNotConnectedException` | `submitTransaction` while disconnected |

```kotlin
class StreamDesyncException(
    val expectedParent: KdbHash,
    val actualParent: KdbHash,
) : KdbException("stream desync: expected parent $expectedParent, got $actualParent")

class StreamNotConnectedException : KdbException("stream not connected")
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `mode1_receiveDelta` | Publish one commit | Subscriber position updated; hints applied |
| 2 | `mode1_positionAck` | Receive delta | Coordinator sees `lastAck` |
| 3 | `mode2_replaySuccess` | Valid transaction at HEAD | `ReplayResult.Applied` |
| 4 | `mode2_replayConflict` | Conflicting write | `ReplayResult.Conflict` |
| 5 | `resumeFrom_catchUp` | 3 commits, resume at 1st | Receives 2nd and 3rd only |
| 6 | `outOfOrder_desync` | Delta with wrong parent | `StreamDesyncException` |
| 7 | `compactionNotice_boundary` | Notice below position | `CompactionBoundaryException` |
| 8 | `duplicateHints_idempotent` | Same hint twice | Index unchanged size, no error |
| 9 | `handshake_mode1` | READ_ONLY | No `transactionEngine` required |
| 10 | `handshake_rejectVersion` | Bad protocol version | Disconnect + error event |
| 11 | `coordinator_fanOut_twoSubs` | 2 subscribers | Both receive same delta |
| 12 | `iceNotice_forwarded` | IceArchiveNotice frame | `StreamEvent.IceArchived` |

-----

## 8. Non-Goals

- **Mode 3 full peer** — Layer 8 (`CommitFetch`, `CommitPush`, `DAGDiff`).
- **Local commit DAG on Mode 1 subscriber** — by design no DAG.
- **WebSocket/TCP** — Layer 9; inject `WireTransport`.
- **SQL query execution on subscriber** — app uses `HybridQueryEngine` locally if it maintains storage; stream only updates indexes + optional cache.
- **Automatic Mode 2 → Mode 3 upgrade mid-session** — master open question §15; v1 requires reconnect with `FULL_PEER` handshake (Layer 8).
- **Multi-namespace multiplexing** — one `StreamConnection` per namespace in v1.

-----

## 9. Implementation Notes

### Coordinator factory
Provide `streamCoordinator(..., transactionEngine: TransactionEngine)` for Mode 2 replay handling on incoming `TransactionReplay` messages.

### Correlation ids
Match `TransactionReplay` response `ConflictReport` to client via `correlationId` from wire header.

### Module layout
```
dev.kdb.stream
  StreamCoordinator.kt
  StreamSubscriber.kt
  IndexHintApplier.kt
  InMemoryWireTransport.kt
  StreamCoordinatorSession.kt
```

### Compaction coordinator bridge
Optional `WireCompactionCoordinator` implements `dev.kdb.compaction.CompactionCoordinator` using connected peers — replaces `InProcessCompactionCoordinator` when wire session active.

### KMP
`commonMain`; no platform sockets.

### Gradle
`:kdb-stream` → `:kdb-wire`, `:kdb-transaction`, `:kdb-index`, `:kdb-dag`, `:kdb-storage-manager` (enlistment types only).

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| Stream coordinator + fan-out | 550 |
| Stream subscriber + state machine | 600 |
| Mode 2 replay client/server | 450 |
| Index hint applier | 200 |
| In-memory transport + tests | 700 |
| **Total** | **~2,500** |
