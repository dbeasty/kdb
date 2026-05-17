# KDB Component Spec — Layer 8
## Component 23: Peer Sync Mode (Mode 3)
### `dev.kdb.peersync`

**File:** `kdb-spec-layer8-component23-peer-sync-mode.md`  
**Layer:** 8 — Advanced Sync + JDBC  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-peer-sync`  
**Depends on:** Layer 7 (`WireCodec`, `WireMessage`, `WireTransport`), Layer 2 (`CommitDag`), Layer 3 (`TransactionEngine`), Layer 7 Component 22 (`WireTransport`, `InMemoryWireTransport`)

-----

## 1. Purpose

Implements **Mode 3 (full peer)** from master §8.1: each node keeps a complete local commit DAG, discovers divergence via handshake + `DAGDiff`, exchanges commits with `CommitFetch` / `CommitPush`, and applies upstream transactions via `TransactionReplay` when replay is required.

This module owns the **bidirectional peer sync state machine** on top of `WireCodec`. It does **not** own transport sockets (Layer 9) or stream coordinator fan-out (Component 22).

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.wire` (21) | `WireCodec`, `WireMessage`, `HandshakePayload`, `WireClientMode.FULL_PEER` |
| `dev.kdb.stream` (22) | `WireTransport`, `WireConnection`, `InMemoryWireTransport` |
| `dev.kdb.dag` | `CommitDag`, `walk`, `commonAncestor`, `putCommit` |
| `dev.kdb.document` | `KdbCommit`, `KdbTransaction` |
| `dev.kdb.transaction` | `TransactionEngine`, `TransactionResult` |
| `dev.kdb.storage` | `StorageAdapter` |
| `dev.kdb.error` | `PeerSyncException`, `VersionNotFoundException` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.WireCodec
import dev.kdb.stream.WireTransport

/** Local peer — serves CommitFetch/Push and handles inbound wire frames. */
interface PeerSyncHost {
    suspend fun start(config: PeerHostConfig)
    suspend fun stop()
    suspend fun handleFrame(frame: ByteArray): ByteArray?
}

/** Remote sync client — pull/push against a peer URI. */
interface PeerSyncClient {
    suspend fun connect(config: PeerClientConfig): PeerSession
    suspend fun disconnect()
}

interface PeerSession {
    val namespaceId: String
    val remoteHead: KdbHash
    suspend fun pullMissing(): PeerSyncResult
    suspend fun pushCommits(commits: List<KdbCommit>): Int
    suspend fun syncBidirectional(): PeerSyncResult
}

fun peerSyncHost(
    wire: WireCodec,
    dag: CommitDag,
    storage: StorageAdapter,
    transactionEngine: TransactionEngine? = null,
): PeerSyncHost

fun peerSyncClient(
    wire: WireCodec,
    transport: WireTransport,
    dag: CommitDag,
    storage: StorageAdapter,
    transactionEngine: TransactionEngine? = null,
): PeerSyncClient

data class PeerHostConfig(
    val namespaceId: String,
    val nodeId: String,
    val transportHub: String,  // e.g. memory hub name for InMemoryWireTransport
)

data class PeerClientConfig(
    val namespaceId: String,
    val nodeId: String,
    val peerUri: String,
)

data class DagSyncPlan(
    val commonAncestor: KdbHash?,
    val localOnly: List<KdbHash>,
    val remoteOnly: List<KdbHash>,
)

data class PeerSyncResult(
    val appliedCommits: Int,
    val pushedCommits: Int,
    val finalHead: KdbHash,
    val plan: DagSyncPlan?,
)

fun computeSyncPlan(
    dag: CommitDag,
    localHead: KdbHash,
    remoteHead: KdbHash,
): DagSyncPlan
```

-----

## 4. Data Structures

### Sync algorithm (v1)
1. `FULL_PEER` handshake — exchange `localHeads`.
2. `computeSyncPlan(localHead, remoteHead)` via `dag.commonAncestor` + `walk`.
3. `CommitFetch` for each hash in `remoteOnly` (batched, `maxCommits` default 100).
4. `putCommit` each fetched commit (`requireParents = true`).
5. `CommitPush` for each commit in `localOnly` (payload = length-prefixed Layer 0 commit bytes).

### `CommitPush` payload (owned by `:kdb-wire`)
`[int32 count][repeat: int32 len, bytes]*` — each bytes is `KdbCommit.toPayloadBytes()`.

### In-memory test topology
`PeerSyncHost` registers on `InMemoryWireTransportHub`; client connects to `memory://{hub}`. Two hosts use distinct hub names linked by test harness `linkPeerHubs(a, b)` (test-only).

-----

## 5. Contracts

### `PeerSyncHost.handleFrame`
- **Postconditions:** Returns encoded response frame for request/response pairs (`CommitFetch`, `Handshake`); `null` for fire-and-forget.
- **CommitFetch:** Returns commits topologically from HEAD toward `sinceHash`, capped by `maxCommits`.
- **CommitPush:** Idempotent `putCommit`; returns ack frame with applied count.

### `PeerSyncClient.syncBidirectional`
- **Postconditions:** After success, `dag.head()` equals remote when histories were compatible; divergent histories merged only via fetched commits (merge replay deferred to `TransactionEngine.merge` in v2).

### `computeSyncPlan`
- **Postconditions:** `localOnly` commits are reachable from `localHead` but not from `commonAncestor`; same for `remoteOnly`.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `PeerSyncException` | Handshake rejected, namespace mismatch, push parent missing |
| `WireDecodeException` | Delegated from wire |
| `VersionNotFoundException` | `sinceHash` not in DAG during fetch |

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `handshakeFullPeer` | FULL_PEER handshake | `accepted=true`, heads exchanged |
| 2 | `fetchSinceGenesis` | empty DAG + fetch since null | empty list |
| 3 | `fetchLinearChain` | 3-commit chain, fetch since middle | 2 commits newest-first |
| 4 | `pushIdempotent` | push same commit twice | DAG size unchanged |
| 5 | `computePlanNoDivergence` | same head | empty `localOnly`/`remoteOnly` |
| 6 | `computePlanFork` | fork at v1 | correct ancestor + sides |
| 7 | `pullMissing` | remote ahead by 2 | local head matches remote |
| 8 | `pushLocalOnly` | local ahead by 1 | remote receives commit |
| 9 | `bidirectionalFork` | A and B diverged | both heads converge after sync |
| 10 | `namespaceMismatch` | wrong namespace in fetch | `PeerSyncException` |
| 11 | `maxCommitsCap` | 50 commits, max=10 | 10 returned |
| 12 | `rejectBadParent` | push orphan commit | `PeerSyncException` |

-----

## 8. Non-Goals

- Automatic Mode 2 → Mode 3 upgrade mid-session (master §15).
- Schema push replication (`SchemaPush`) — stub only.
- S3/archive bundle transfer during sync (use `SnapshotRequest` via stream/tier paths).
- Network TLS, WebSocket, TCP (Layer 9).

-----

## 9. NBNC Estimate

~4,500 lines (production + tests).
