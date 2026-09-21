# Distributed KDB — Detailed Implementation Plan

**Companion to:** [kdb-distributed-plan.md](kdb-distributed-plan.md). That document explains *what* is missing and *why*; this one says *how*, work item by work item.
**Branch:** `feat/distributed-sync`.
**Scope:** Go tree only (`go/kdb/...`). New wire frames are Go-only and additive, like 0x14–0x24 before them, so Kotlin keeps decoding everything it already decodes.
**Standing gate:** after each work item, `cd go && go build ./... && go vet ./... && go test -race ./kdb/...` must be green. Each phase ends with one commit, or several.

Status markers: ☐ not started · ◐ in progress · ☑ done (with the commit that landed it).

---

## Conventions every phase follows

1. **Tests first for defects.** Each Phase 0 defect gets a test that fails on `main` before its fix. The commit message names the test.
2. **No commit-hash format change** outside Phase 8. Everything below works inside the existing hashed payload: `ParentHashes`, `TransactionID`, `Timestamp`, `AuthorNodeID`, `Operations`, `DocumentTreeHash`, `SchemaHash`, `Message`.
3. **Additive wire only.** New fields are `omitempty`, and a new message gets a new opcode. An unknown kind is a decode error, never a silent downgrade (the rule `transaction_codec.go` already follows).
4. **A peer is just another writer.** Anything that changes a namespace's head goes through that runtime's write serialization and its post-commit hooks. There's exactly one path, `peersync.Ingest`, and every sync entry point calls it.
5. **Local state is not replicated state.** Conflict queues, peer watermarks, the node id and index snapshots are per node. Only what's inside commits and refs is shared.

---

## Phase 0 — Harden pairwise sync

Goal: two nodes can sync any history correctly, under concurrent local writes, with every local subsystem kept consistent.

### 0.1 One ingest path under the write gate ☑ (D2, D3, D6, D8)

**New file `peersync/ingest.go`:**

```go
// LocalNode is what peer sync needs from the runtime it feeds.
type LocalNode interface {
    // Exclusive runs fn under the runtime's write serialization (the server's writeGate).
    Exclusive(fn func() error) error
    // Advanced is called inside Exclusive, after the head moved, once per step in order.
    Advanced(step AdvanceStep) error
}

type AdvanceStep struct {
    Commit document.Commit  // the commit that is now reachable from main
    Applied []document.Op   // what was actually written to live storage for it
}

// Ingest stores commits, decides the head update, and applies it - the only function
// that moves a namespace's head on behalf of a peer.
func Ingest(env IngestEnv, commits []document.Commit, incomingHead codec.Hash) (IngestResult, error)
```

`IngestEnv` bundles the dag, the storage adapter, the namespace, `LocalNode`, `Persist`, `Materialize` and `ResolutionOptions`. Both `host.go`'s CommitPush and `client.go`'s PullMissing replace their inline bodies with one call to it.

**Inside `Ingest`:**

1. `PutCommit(requireParents)` each commit not already present, then `Persist`. This happens outside the gate: storing history is safe concurrently.
2. `LocalNode.Exclusive`:
   1. Read the head **inside** the gate. The head read before the gate is only a hint.
   2. Call `ResolveHeadUpdate`.
   3. **Fast-forward:** walk the fresh commits parents-first. For each one, `Materialize` it, then `SetHead` to it, then call `Advanced`.
      - If materializing a commit fails, call `DiscardPending`, leave the head at the last good commit, and return the error. The head never moves past storage.
   4. **Diverged:** call `resolveDivergedLocked`. Merged → `Advanced(merge step)`. Conflict → return the report.
3. A nil `LocalNode` gets a default that serializes on the existing per-namespace divergence lock, for embedded and in-memory callers. Existing tests keep working.

**Server wiring (`server/peersync_ingest.go`):** `serverLocalNode{rt}`.

- **`Exclusive`:** `rt.writeGate.acquire(ctx with WriteTimeout)`, plus the draining and fence checks from `admitWrite`, run as `system`.
- **`Advanced`:**
  - indexes: `prepareCommitForIndexes(step.Applied)` then `commitToIndexes(prepared, step.Commit.Hash)`
  - the unique registry (0.1b)
  - `rt.CommitListener(ns, step.Commit)`

**0.1b Unique keys on ingest.** Add `UniqueKeyRegistry.RebuildLenient(ns, store, tree, sch) []UniqueConstraintError`. It rebuilds, keeps the lowest doc id as owner of a contested key (deterministic), and returns the duplicates instead of failing.

- `Advanced` calls it only when `sch.HasUniqueConstraints()`, and records the duplicates on `rt.ReplicationConflicts` (a slice now; Phase 3 makes it durable).
- The O(n) cost per ingest batch is accepted for Phase 0 and documented.
- Follow-up **0.1c**, in Phase 3: incremental retract/claim using the pre-image read from storage before materializing.

**Tests** (`server/peersync_ingest_test.go`, `peersync/ingest_test.go`):

- `TestPeerFastForwardDoesNotLoseConcurrentLocalCommit`: a hook holds the gate mid-ingest while a local commit queues. After both finish, the local commit is reachable from main.
- `TestPeerIngestUpdatesIndexes`: `CREATE INDEX` on node B, then a peer push; an indexed query on B sees the pushed document.
- `TestPeerIngestFiresCommitListener`
- `TestPeerIngestUniqueDuplicateRecorded`
- `TestMaterializeFailureLeavesHeadAtLastGoodCommit`: the injected `Materialize` fails on the 3rd of 5 commits, so the head ends at commit 2 and storage agrees.

### 0.2 Parents-first paging and haves ☑ (D1)

**Wire:** `CommitFetchMessage` gains an additive `Haves []string` (hex, omitempty).

**Host `fetchCommits(since, haves, max)`:**

- Exclude everything reachable from every known hash in `{since} ∪ haves`.
- Walk from the head and emit **oldest-first**, capped at `max`.
- Unknown haves are ignored.

**Client:**

- `haves = exponentialAncestors(localHead)`: head, then ~1, ~2, ~4, … up to 32 entries, following first parents, plus every local branch head.
- `PullMissing` loops fetches until a page comes back empty or ends at the remote head. Each page goes through `Ingest`.
- `CommitsToPush` + `PushCommits` loops the same way.
- Page size stays 100 commits. Byte-sized pages arrive in Phase 2.

**Tests:**

- `TestPullMoreThanOnePage` (1,000 commits)
- `TestPushMoreThanOnePage`
- `TestPullDivergedDoesNotResendSharedHistory`: diverged by 5 on each side over a 500-commit shared base, so the fetch returns about 5 commits, not 505.

### 0.3 Namespace validation ☑ (D5)

The host refuses any CommitFetch or CommitPush whose `Namespace` differs from `cfg.NamespaceID`, replying with an error frame (0.4). The handshake refuses a `Namespaces` list that doesn't contain the served namespace.

**Test:** `TestPeerPushWrongNamespaceRefused`.

### 0.4 Error frame and unknown messages ☑ (D7, D12)

- **New `MsgPeerError` (0x25):** `PeerErrorMessage{Namespace, Code ErrorCode, Message string}`.
- **The host replies with it** for:
  - unknown message types
  - authorization failures after the handshake
  - namespace mismatch
  - ingest errors
- **It keeps the connection open** where that's safe; it currently drops it.
- **The client** turns a `PeerErrorMessage` into a `*peersync.Error` carrying the code.
- **A partial push** is reported honestly: the ack's `AppliedCommits` counts commits stored before the failure, and the error names the commit that failed.

**Tests:** `TestPeerUnknownFrameGetsErrorReply` (no 20s wait), `TestPeerHandlerErrorIsReportedNotDropped`.

### 0.5 Stubbed commits in fetch and push ☑ (D9)

Walks that meet a `StubbedEntry` send a **stub record** (`CommitStubDto{OriginalHash, ArchiveLocation}`) in a new additive `Stubs` field on CommitPush. The receiver calls `PutStub` (a new DAG method mirroring `StubCommit` for a commit it never had), so children validate.

**Test:** `TestSyncAcrossArchivedCommit`.

### 0.6 Cross-namespace parts from foreign hosts ☑ (D10, interim)

`txn_coordinator.go:375` stops treating a `kdb:xns/1` part whose `host=` isn't this node as committed-by-default. It's classified `ForeignGroupPart` and reads exactly as it does today, but it's **reported**: a `ForeignGroupParts()` accessor and a metric. That keeps behaviour, removes the silent claim, and is enough until Phase 6.4 does the full check.

**Test:** `TestForeignGroupPartIsFlagged`.

### 0.7 Authenticated stream ☑ (D11)

- **The stream handshake authenticates** with the same credentials fields as the peer handshake, and authorizes `auth.StreamSubscribeAction{Namespace}`. That's a new action; under `AllowAll` it's a no-op.
- **Write-back replays** run as the authenticated principal.
- **`--stream-allow-anonymous`** (default `false` when RBAC is on, `true` otherwise) keeps existing no-RBAC deployments working.

**Tests:** `TestStreamRejectsUnauthenticatedUnderRBAC`, `TestStreamWriteBackUsesPrincipal`.

**Phase 0 exit:** the existing e2e `test_multi_peer_sync.py` still passes. A new Go test brings two `KdbServerRuntime`s with peer listeners to convergence on a 2,000-commit divergent history with an index on each side, and both indexes agree.

---

## Phase 1 — Identity, causal timestamps, deterministic merge

### 1.1 Node identity ☐

- **`embed/node_identity.go`:** `LoadOrCreateNodeID(dataRoot string) (codec.UUID, error)` reads or creates `<dataRoot>/NODE`, written atomically with temp + rename + fsync.
  - If `<dataRoot>/txn/HOST` exists, it's adopted, so a cross-namespace host keeps its identity.
  - `NodeIDFromEnv` supports `KDB_NODE_ID` for tests and containers.
- **`KdbServerRuntime.NodeID codec.UUID`** is set by `kdb-service` at startup. Memory runtimes get a random one per process.
- **`runTransaction` stamps `tx.AuthorNodeID = rt.NodeID`** when it's non-zero. That's the server's own authorship. The client-supplied value was random noise anyway.
- **The peer host `NodeID`** comes from `rt.NodeID`, not `"kdb-service-go"`.
- **The handshake refuses a client whose `NodeID` equals the host's** ("two nodes share an identity; one was cloned without resetting NODE").
- **Surfaces:** `/healthz` gains `nodeId`.

**Tests:** `TestNodeIDPersistsAcrossOpen`, `TestNodeIDAdoptsTxnHost`, `TestPeerHandshakeRefusesOwnNodeID`, `TestServerStampsAuthorNodeID`.

### 1.2 Causal timestamps ☐

In `dag.appendCommitLocked`:

```go
ts := tx.Timestamp
for _, p := range parents { if pt := commitTs(p); pt >= ts { ts = pt + 1µs } }
```

This one line gives the HLC property that matters: **every commit's timestamp is strictly greater than every parent's.** Last-write-wins between two causally related writes then always picks the later one, whatever the clocks say.

- **Skew guard at ingest** (`peersync.Ingest`): a commit whose timestamp is more than `MaxClockSkew` (default 5m, from `IngestEnv`) past local wall time is refused with `ErrClockSkew`. Otherwise one bad clock would poison every descendant through the clamp.
- **The merge commit timestamp** is `max(parents)` (1.3). The clamp then makes it `max + 1µs`, which is deterministic because it depends only on the parents.

**Tests:**

- `TestCommitTimestampExceedsParents`: the tx timestamp is set in the past, and the commit timestamp is parent + 1.
- `TestLastWriteRespectsCausalityUnderClockSkew`: node A's clock is 10 minutes behind; A reads B's write and overwrites it, and after sync A's value wins on both nodes.
- `TestIngestRefusesFutureCommit`

### 1.3 Deterministic merge commits ☐ (D4)

Rewrite `mergeNonConflicting` to take a fully resolved `merged map[UUID]Op` over *all* touched docs, and to derive everything else from the pair of heads:

| Field | Value |
|---|---|
| parents | `P0, P1 = sort(localHead, incomingHead)` by hash bytes |
| ops | `merged` restricted to `touched(P1) ∪ overlapping`, sorted by doc id — the delta from P0's tree |
| storage writes on this node | `merged` restricted to `touched(remote) ∪ overlapping` — the delta from the *local* tree |
| tx id | `codec.DerivedUUID("kdb:merge/1:" + P0.Hex() + ":" + P1.Hex())` |
| author | `document.MergeAuthorNodeID` (a fixed sentinel) |
| timestamp | `max(ts(P0), ts(P1))`, which 1.2 clamps to max + 1µs |
| message | `"kdb:merge/1 policy=<name>"` |
| schema hash | equal on both parents → that value, else nil (Phase 5 resolves schema) |
| CAS | `AppendMergeCommitOnto(&localHead, tx, P0, P1, …)` |

**The Custom resolver is called canonically:** `ExistingDoc` is P0's side and `IncomingDoc` is P1's, not local and remote. The `ConflictResolver` doc comment gains the determinism contract. LWW is already symmetric through its hash tie-break.

**If the merge commit already exists** (another node made it and it arrived by sync), `appendCommitLocked` would rebuild the same hash. `putCommitLocked` then treats it as present, and the CAS still moves the head. So no special case is needed, and a test pins it.

**Tests:**

- `TestMergeCommitIsIdenticalOnBothSides`: A merges (a, b), B merges (b, a); the hashes are equal.
- `TestMergeReplayFromP0MatchesTree`: `MaterializeCommit` of the merge on a third node whose head is P0 passes the tree check, and likewise for P1 via fast-forward after it pulls P0's side.
- `TestCustomResolverSeesCanonicalOrder`
- **Property test** `TestMeshConvergesDeterministically`: 3 nodes, 200 seeded random writes and sync pairings, finally all-pairs sync until quiescent. Asserts equal heads and a bounded number of merge commits (≤ number of sync rounds).

**Phase 1 exit:** the property test is green across 50 seeds under `-race`.

---

## Phase 2 — Sync protocol v2

### 2.1 Messages (`wire/sync_v2_ops.go`, opcodes 0x26–0x2B) ☐

```
SyncHello     {NodeID, Protocol=2, Capabilities[], Namespaces[] (patterns), credentials}
SyncHelloAck  {Accepted, NodeID, Protocol, Capabilities[], Namespaces[] (granted, concrete), Reason}
RefAdvertise  {Namespace, Branches{name→hex}, Tags{name→hex}, HistoryFloorHex?, Shallow[]}
FetchRequest  {Namespace, Wants[], Haves[], MaxBytes, Cursor?}
PackPage      {Namespace, Commits[], Stubs[], NextCursor?, Done}
RefUpdate     {Namespace, Ref, OldHex, NewHex, Commits[] (pushed pack, parents-first)}
RefUpdateAck  {Namespace, Ref, Outcome (ff|merged|noop|conflict|rejected), HeadHex, Conflict?, Error?}
```

- **`Cursor`** is the hex of the last commit sent. The next page excludes the sender's reach from the cursor as well, so a reconnect can restart from the last cursor received.
- **`MaxBytes`** defaults to 4 MiB. Pages are filled until the next commit would exceed it; a single commit that is larger still goes out alone.
- **Capabilities:** `zstd`, `stubs`, `snapshot` (Phase 4), `filter` (Phase 7), `tags`, `branches`.

### 2.2 Host (`peersync/v2_host.go`) ☐

- **One connection serves many namespaces** through a `NamespaceResolver` interface. `server.NamespaceSet` implements it, returning a per-namespace `IngestEnv`.
- **Authorization is per namespace** (`PeerSyncAction{ns}`).
- **`RefAdvertise`** is sent for every granted namespace on hello.
- **`FetchRequest`** is served by `packFor(dag, wants, haves, cursor, maxBytes)`.
- **`RefUpdate`** goes through `Ingest` for branch `main`. Other branches run the same `ResolveHeadUpdate` against that branch's head. Only FF is supported off `main`; a divergent non-main branch gets `conflict`.
- **Tags:** a new tag is created. The same name at a different hash gets `conflict`, and the tag is never moved.

### 2.3 Client (`peersync/v2_client.go`) ☐

`SyncV2(conn, namespaces, mode) (map[string]NamespaceSyncResult, error)`:

1. hello
2. for each namespace:
   - **Pull:** compute wants (remote refs not present locally), send `FetchRequest` with haves, ingest pages until `Done`, then resolve each ref.
   - **Push:** for each local ref whose hash isn't known on the remote, send `RefUpdate` with the pack of commits from `CommitsToPush(localRef, remoteRef)`, paged.
3. **Fallback:** if the ack reports `Protocol < 2`, fall back to the v1 session.

### 2.4 Server listener ☐

`ListenPeerSync` dispatches by the first frame: `SyncHello` → v2 host over the runtime's `NamespaceSet` (or a solo set), and `Handshake` → the v1 host.

**Tests:**

- `TestV2SyncsAllBranchesAndTags`
- `TestV2MultipleNamespacesOneSession`
- `TestV2ResumeFromCursorAfterDisconnect`
- `TestV2PagesByBytes`
- `TestV2TagConflictReported`
- `TestV2ClientFallsBackToV1`
- `TestV2NegotiationSendsOnlyMissing` (counts commits on the wire)

**Measure:** a synthetic 100k-commit linear sync, v1 paging compared with v2. The result goes into `docs/benchmarks/`.

---

## Phase 3 — Replicator, conflict queue, CLI

### 3.1 Peer configuration (`service/peers.go`) ☐

```
--peer name=cloud,addr=tcps://cloud:4242,namespaces=site/*;shared/*,mode=bidirectional,interval=30s,user=..,password-env=..
KDB_PEERS="name=...;name=..."   (same grammar, ';'-separated entries use '|' inside namespaces)
```

Parsed into `replication.PeerConfig{Name, Addr, Patterns []string, Mode, Interval, Credentials}`. It's validated at startup; an unknown key is fatal, following the config convention.

### 3.2 Replicator (`go/kdb/replication/`) ☐

```go
type Replicator struct { ... }
func New(node NodeInfo, set NamespaceResolver, peers []PeerConfig, state *StateStore, dial Dialer) *Replicator
func (r *Replicator) Start(); Stop(); Status() []PeerStatus; SyncNow(peer string) error
func (r *Replicator) OnLocalCommit(ns string)   // wired to CommitListener (chained)
```

- **One goroutine per peer.** Triggers:
  - a local commit on a matching namespace (debounced by 50ms)
  - `interval`
  - `SyncNow`
- **Each cycle** dials, runs `SyncV2`, records the result, and closes. A persistent session comes later; notifications are an optimisation, and the interval tick is the correctness floor.
- **`StateStore`** keeps `<dataRoot>/peers/<peerName>.json`, holding `{peerNodeId, perNamespace{lastPulledRefs, lastPushedRefs, lastSuccess, lastError}, consecutiveFailures}`. It's written atomically, and losing it costs one full negotiation.
- **Backoff:** `min(interval × 2^failures, 10m)` with ±20% jitter.
- **Metrics:**
  - `kdb_replication_last_success_seconds{peer,ns}`
  - `kdb_replication_commits_total{peer,ns,direction}`
  - `kdb_replication_errors_total{peer}`
  - `kdb_replication_lag_commits{peer,ns}`

### 3.3 Durable conflict queue (`embed/conflict_queue.go`) ☐

- **Per namespace**, as `<nsRoot>/conflicts/<id>.json`, where the id is `sha256(localHead ‖ incomingHead)`, so it's idempotent.
- **An entry holds:** `{id, namespace, localHead, incomingHead, ancestor, policy, firstSeen, lastSeen, peer, items []ConflictItem, kind: divergence|unique}`.
- **`Ingest` records an entry** whenever it returns a conflict, and records unique duplicates from 0.1b.
- **Tracking ref:** on a conflict, `Ingest` also sets branch `peers/<peerNodeId>/main` to the incoming head, created if missing. Retention then keeps the remote side reachable, and resolution can find it.
- **`ResolveConflict(ns, id, choices map[docID]Choice{Local|Remote|Body(json)}, principal)`** builds a two-parent commit, parents `(localHead, incomingHead)` in local-first order. It's an authored resolution, not a deterministic merge, applied through `runTransaction` as an ordinary commit with merge parents. The entry is then deleted and the tracking ref removed.
- **Server and control:**
  - `GET /v1/ns/{ns}/conflicts`
  - `POST /v1/ns/{ns}/conflicts/{id}/resolve`
  - the `kdb_conflicts_open{ns}` gauge
- **Admin UI:** a Conflicts tab, following the Retention tab's pattern.

### 3.4 Control endpoints ☐

- `GET /v1/peers`: status
- `POST /v1/peers/{name}/sync`: sync now
- `/healthz` gains `replication: {peers: n, failing: m}`

### 3.5 CLI ☐

In `kdb-cli`, the Go CLI entry points, as far as its structure allows:

- `kdb node status`
- `kdb node peers`
- `kdb sync <ns> <addr>`: a one-shot `SyncV2`
- `kdb conflicts <ns>`
- `kdb resolve <ns> <id> --take local|remote`

**Tests:**

- `TestReplicatorConvergesTwoServices`: two in-process services configured only with each other as peers. Writes on each converge within 2s through the commit trigger.
- `TestReplicatorBackoffAndRecovery`
- `TestConflictQueuedAndResolvedReplicates`
- `TestPeerStatePersistsAcrossRestart`
- e2e `test_replicator.py`: real processes, with `--peer` only.

---

## Phase 4 — Snapshot bootstrap, shallow roots, peer-aware retention

### 4.1 Shallow roots in the DAG ☐

- `InMemoryCommitDag.AddShallow(hash)`, backed by a `shallow map[Hash]struct{}` that's persisted in the checkpoint as an additive field.
- `putCommitLocked(requireParents)` accepts a parent that's shallow.
- `Walk`, `CommonAncestor` and `IsAncestor` treat a shallow commit's parents as absent, the same way they treat stubs today. A shallow commit is inserted through `PutShallowCommit(c)`, which verifies the hash but not the parents.

### 4.2 Snapshot messages (0x0A / 0x0B, finally implemented) ☐

```
SnapshotRequest  {Namespace, At (ref or hex, default main), Cursor?}
SnapshotResponse {Namespace, Commit, TreeEntries[(docId, contentHash)] (first page only),
                  Bodies[(contentHash, json)] (paged by bytes), Refs, NextCursor?, Done}
```

**The receiver** requires an *empty* namespace (genesis head only) or an explicit `--reset`. For each body, it checks that `contentHash == sha256(record(docId, json))` and calls `PutDocument`. Then it runs `CommitTree`, requires the result to equal `Commit.DocumentTreeHash`, calls `PutShallowCommit`, sets the refs, persists, and runs a full index and unique rebuild.

**Capability:** `snapshot`. `SyncV2` falls back to a snapshot when:

- the remote's `HistoryFloorHex` isn't an ancestor of anything local, or
- ingest reports a missing parent below the floor.

### 4.3 Peer-aware truncation floor ☐

- **`embed` retention** takes an optional `PeerFloor func(ns) (codec.Hash, bool)`. The replicator provides it as the oldest `lastPushedRefs` among peers whose `lastSuccess` falls within `--peer-retention-grace` (default 7d).
- **`checkpointAndTruncate`** takes the older of the retention floor and the peer floor.
- **`Squash`** in a namespace with configured peers is refused until every peer's pushed ref descends from the squash boundary.
- **`CompactionNotice` (0x08)** goes out in the next `RefAdvertise` as `HistoryFloorHex`, not as a separate broadcast.

**Tests:**

- `TestSnapshotBootstrapEmptyNode`
- `TestSnapshotRejectsTamperedBody`
- `TestSyncFallsBackToSnapshotBelowFloor`
- `TestTruncationWaitsForActivePeer`
- `TestEvictedPeerRebootstraps`
- `TestShallowRootSurvivesRestart`

---

## Phase 5 — Replicated metadata (`_kdb/meta`)

### 5.1 The meta namespace ☐

- **Created on first start** in every `NamespaceSet`, with the Strict policy.
- **Document ids** are `DerivedUUID("schema:<ns>")`, `DerivedUUID("indexes:<ns>")` and `DerivedUUID("policy:<ns>")`, so two nodes creating the same definition independently produce the *same document*, and any disagreement shows up as a document conflict.
- **Writers:** `SetSchemaChecked`, index DDL and policy `Put` write the meta document alongside their local effect, through `CommitAcross` so the pair is atomic.

### 5.2 Reconciler ☐

`meta.Reconciler` subscribes to `_kdb/meta` commits through the `CommitListener` chain. For each changed document, it applies the change locally:

- `SetSchemaChecked`
- create or drop an index, then `rebuildIndex`
- `policy.Registry.Put`

It's idempotent, and a failure is recorded on the meta conflict queue instead of blocking ingest.

### 5.3 Schema migrations in data commits ☐

`MaterializeCommit` stops skipping `SchemaMigrationOp`. It applies the migration to the runtime's schema through a new `IngestEnv.ApplySchemaMigration` hook, so data never lands ahead of its schema.

**Tests:** `TestIndexCreatedOnAIsBuiltOnB`, `TestSchemaMigrationPropagatesBeforeData`, `TestMetaConflictQueued`.

---

## Phase 6 — Namespace sync sets

- **6.1** `Patterns` matching: `*` matches one path segment, `**` any depth, and a `!` prefix excludes. The intersection of what the client asks for and what the host grants is computed at hello (2.1 already carries patterns).
- **6.2** Namespaces are auto-created on the receiving side through `NamespaceSet.Resolve(ns, create=true)`, with the policy taken from `_kdb/meta` when it's present.
- **6.3** `RefAdvertise` for new namespaces created on the host mid-session: the next cycle picks them up. No push is needed.
- **6.4** Cross-namespace group completeness (the full D10 fix). When a replica ingests a group part, it checks whether it holds every namespace in the marker's `parts=`.
  - **Every part present:** it verifies all of them are present at the same group before exposing the heads. Otherwise it holds the part's namespace at its previous head until the rest arrive or a timeout expires, then exposes it and flags it.
  - **Some parts missing:** it flags `foreign-incomplete`.
- **6.5** The user guide gains a "partition by namespace" section.

**Tests:** `TestPatternSetReplicatesOnlyMatching`, `TestNamespaceAutoCreatedOnReplica`, `TestGroupHeldUntilComplete`, `TestGroupFlaggedWhenPartitionMissing`.

---

## Phase 7 — Filtered projection replicas

- **7.1 Filter language:** a KDB-SQL boolean expression over `_doc` paths and schema fields, parsed with the existing `sql` parser's expression grammar and evaluated per document. The capability is `filter`.
- **7.2 `SubscribeFiltered{Namespace, Filter, FromSourceHex?}`** gets a stream of `ProjectedDelta{SourceHex, Writes[], Deletes[]}`. The host computes each commit's effect on the filtered set:
  - a write that matches becomes a write
  - a write that no longer matches, to a document that previously matched, becomes a delete
  - a delete becomes a delete

  Per-document RBAC (`DocumentReadAction`) is applied on top.
- **7.3 Derived namespace `<ns>@<filterId>`** on the replica. `filterId` is `sha256(filter)[:8]`. The replica commits each delta locally with message `projection of <ns>@<SourceHex>`, and stores the last source hash in the namespace's meta.
  - **Resume:** a reconnect starts from that hash.
  - **Below the source floor:** the replica gets a filtered snapshot and a diff against the local projection.
- **7.4 Write-back:** a write to a derived namespace is queued as a pending `Transaction` (`<nsRoot>/outbox/`) and sent as `TransactionReplay` with the principal's credentials. A success is removed from the outbox, and the change comes back through the projection. A `ConflictReport` goes to the conflict queue.
- **7.5 Stream Mode 1/2 on top of this:** the `StreamHub` subscription becomes `SubscribeFiltered` with filter `TRUE` and no local persistence. It gains resume (it has none today).

**Tests:** `TestFilteredReplicaReceivesOnlyMatching`, `TestDocumentLeavingFilterIsDeleted`, `TestRBACHiddenDocumentNeverSent`, `TestFilteredReplicaResumesAfterOffline`, `TestOfflineWriteBackReplaysOnReconnect`.

---

## Phase 8 — Verifiable partial clone (gated)

**8.0 Measurement first.** `docs/benchmarks/<date>-partial-clone-model.md` compares a filtered projection with a modelled partial clone (tree metadata plus fetched bodies) on 50k documents × {1 KB, 64 KB, 1 MB}. Proceed only if the partial clone is less than half the projection's resident and disk cost on at least one realistic size **and** the workload needs replica-side writes into the source history.

If it proceeds:

- commit format v2 (op digest), a dual-format DAG, `ObjectFetch`
- golden fixtures in both trees
- a Kotlin decode path

This is a separate design review before any code.

---

## Phase 9 — Single-home ownership

- **9.1** A policy field `Consistency {MultiLeader (default), SingleHome}` in `policy.NamespacePolicy`, additive.
- **9.2** A home lease document in `_kdb/meta`: `DerivedUUID("home:<ns>")` → `{holder nodeId, expiresAt, fence}`.
  - **Claiming** is a CAS (`ExpectContentHash`) against the meta namespace *on the meta-home node*. The meta-home is configured with `--meta-home <peer name>`, or is this node when unset.
  - **Renewal** runs at TTL/3.
- **9.3** Write routing: a non-home node's `runTransaction` for a single-home namespace forwards the transaction to the home over the doc wire (`client.Commit` with the principal's token). A home that's unreachable gets `UnavailableError{retryAfter}`.
- **9.4** Fenced ingest: the home stamps `Message += " home=<node> fence=<n>"`. Ingest of a commit whose fence is older than the current lease's is stored but never fast-forwarded, and is queued as a conflict.
- **9.5** The multi-writer suite across 3 nodes.

**Tests:** `TestSingleHomeForwardsWrites`, `TestSingleHomeUniqueAcrossNodes`, `TestStaleFenceNotFastForwarded`, `TestHomeFailover`.

---

## Phase 10 — Placement and cross-node transactions

- **10.1** Placement documents in `_kdb/meta` (`placement:<ns>` → `{replicas[], home}`), and a `WhereIs(ns)` API.
- **10.2** Client SDK: on `UnavailableError{home=X}` or an explicit redirect, it reconnects to X and caches the placement.
- **10.3** Rebalance runbook and command: add a replica, wait for `lag=0`, move the lease, drop the old replica.
- **10.4** Cross-node `CommitGroup`: 2PC with the coordinator's `txn/` decision log as the durable record, and `Prepare`/`Decide` frames. Until it lands, `CommitAcross` refuses groups whose namespaces have different homes.

---

## Simulation harness (from Phase 1 onward)

`go/kdb/peersync/simtest/`: an in-process network of N nodes, each an `embed` memory runtime plus a `serverLocalNode`-equivalent, connected by the in-memory transport through a **seeded scheduler**. The scheduler can:

- deliver
- delay
- drop
- duplicate
- partition and heal
- skew clocks

**Invariants checked after quiescence:**

1. **Convergence:** equal refs on every pair that exchanged everything.
2. **Durability:** every locally acknowledged commit is reachable from some ref, a tracking ref, or a conflict entry.
3. **Integrity:** every stored commit re-hashes, and every head tree matches storage (`CommitTree` of an empty pending set equals the head's tree hash).
4. **Derived state:** index and unique-registry contents equal a fresh rebuild.

The `TEST_SEEDS` env var widens the run; CI uses 20 seeds.

---

## Progress log

This log is filled in as items land. Each entry gives the commit, what landed, and what deviated from this plan and why.

### Phase 0 — landed

`peersync.Ingest` = `StoreCommits` + `Adopt`, the one head-moving path used by the host's CommitPush and the client's pull. The server's `serverLocalNode` runs it under the write gate and feeds indexes, the unique registry, cross-namespace group checks and `CommitListener`. Tests are in `server/peersync_ingest_test.go`, `peersync/ingest_test.go`, `peersync/paging_test.go`, `wire/peer_sync_fields_test.go` and `server/stream_listen_test.go`.

**Deviations from the plan, and why:**

- **Fast-forward applies a net effect.** It no longer replays commit by commit. `fastForward` folds every newly reachable commit (parents first) into one final operation per document, applies them, and verifies the result against the incoming head's declared tree once.
  - Replay can follow only first parents. A fast-forward across a merge whose first parent is the *other* side's branch would check that side's commits against a tree that already holds this side's writes, and refuse a valid history. `TestIngestFastForwardAcrossMergeWithForeignFirstParent` pins this.
  - As a result, `HostConfig.MaterializeCommit`/`ClientConfig.MaterializeCommit` are no longer called. Setting either one now just means `ApplyToStorage`.
- **Tree mismatch restores pre-images.** This goes beyond `DiscardPending`. The mismatch is only discovered after `CommitTree` has committed the built tree, so pre-images are read before anything is staged and written back on failure.
- **`touchedDocsForRange` uses `commitsBetween`, not `Walk(head, &ancestor)`.** Walk prunes only at the ancestor's exact hash, so after a criss-cross it counts shared history as one side's edits.
- **Pull and push are store-then-decide.** Every page is stored, and only the final state is decided (`CommitPush.More`, and the pull loop calls `Adopt` once). Deciding per page merges half a divergent branch, then merges again for each later page. `TestDivergedPushMergesOnceNotPerPage` pins this.
- **The haves overshoot.** A diverged fetch overshoots the true common ancestor by up to the divergence length, because the haves are exponentially spaced. That's bounded and expected; before, the fetch went all the way to genesis.
- **D9 is only half closed.** Stubs now travel and children are stored, but adopting across an archived commit still needs a shared ancestor that the stub hides. That's Phase 4's shallow roots. Until then the error is `VersionNotFound` ("no common ancestor") instead of "missing parent".
- **Unique keys on ingest use `RebuildLenient`** (O(n) per ingest batch, only when the schema declares unique fields). Duplicates are kept in `ReplicationIssues()`, an in-memory bounded list that Phase 3's durable conflict queue replaces.
- **The stream hub's `AllowAnonymous` is an atomic setter,** because the hub is already accepting connections when `kdb-service` configures it.
