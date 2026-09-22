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

### 1.1 Node identity ☑

- **`embed/node_identity.go`:** `LoadOrCreateNodeID(dataRoot string) (codec.UUID, error)` reads or creates `<dataRoot>/NODE`, written atomically with temp + rename + fsync.
  - If `<dataRoot>/txn/HOST` exists, it's adopted, so a cross-namespace host keeps its identity.
  - `NodeIDFromEnv` supports `KDB_NODE_ID` for tests and containers.
- **`KdbServerRuntime.NodeID codec.UUID`** is set by `kdb-service` at startup. Memory runtimes get a random one per process.
- **`runTransaction` stamps `tx.AuthorNodeID = rt.NodeID`** when it's non-zero. That's the server's own authorship. The client-supplied value was random noise anyway.
- **The peer host `NodeID`** comes from `rt.NodeID`, not `"kdb-service-go"`.
- **The handshake refuses a client whose `NodeID` equals the host's** ("two nodes share an identity; one was cloned without resetting NODE").
- **Surfaces:** `/healthz` gains `nodeId`.

**Tests:** `TestNodeIDPersistsAcrossOpen`, `TestNodeIDAdoptsTxnHost`, `TestPeerHandshakeRefusesOwnNodeID`, `TestServerStampsAuthorNodeID`.

### 1.2 Causal timestamps ☑

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

### 1.3 Deterministic merge commits ☑ (D4)

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

### 2.1 Messages (`wire/sync_v2_ops.go`, opcodes 0x26–0x2B) ☑

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

### 2.2 Host (`peersync/v2_host.go`) ☑

- **One connection serves many namespaces** through a `NamespaceResolver` interface. `server.NamespaceSet` implements it, returning a per-namespace `IngestEnv`.
- **Authorization is per namespace** (`PeerSyncAction{ns}`).
- **`RefAdvertise`** is sent for every granted namespace on hello.
- **`FetchRequest`** is served by `packFor(dag, wants, haves, cursor, maxBytes)`.
- **`RefUpdate`** goes through `Ingest` for branch `main`. Other branches run the same `ResolveHeadUpdate` against that branch's head. Only FF is supported off `main`; a divergent non-main branch gets `conflict`.
- **Tags:** a new tag is created. The same name at a different hash gets `conflict`, and the tag is never moved.

### 2.3 Client (`peersync/v2_client.go`) ☑

`SyncV2(conn, namespaces, mode) (map[string]NamespaceSyncResult, error)`:

1. hello
2. for each namespace:
   - **Pull:** compute wants (remote refs not present locally), send `FetchRequest` with haves, ingest pages until `Done`, then resolve each ref.
   - **Push:** for each local ref whose hash isn't known on the remote, send `RefUpdate` with the pack of commits from `CommitsToPush(localRef, remoteRef)`, paged.
3. **Fallback:** if the ack reports `Protocol < 2`, fall back to the v1 session.

### 2.4 Server listener ☑

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

### 3.1 Peer configuration (`service/peers.go`) ☑

```
--peer name=cloud,addr=tcps://cloud:4242,namespaces=site/*;shared/*,mode=bidirectional,interval=30s,user=..,password-env=..
KDB_PEERS="name=...;name=..."   (same grammar, ';'-separated entries use '|' inside namespaces)
```

Parsed into `replication.PeerConfig{Name, Addr, Patterns []string, Mode, Interval, Credentials}`. It's validated at startup; an unknown key is fatal, following the config convention.

### 3.2 Replicator (`go/kdb/replication/`) ☑

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

### 3.3 Durable conflict queue (`embed/conflict_queue.go`) ☑

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

### 3.4 Control endpoints ☑

- `GET /v1/peers`: status
- `POST /v1/peers/{name}/sync`: sync now
- `/healthz` gains `replication: {peers: n, failing: m}`

### 3.5 CLI ☑

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

### 4.1 Shallow roots in the DAG ☑

- `InMemoryCommitDag.AddShallow(hash)`, backed by a `shallow map[Hash]struct{}` that's persisted in the checkpoint as an additive field.
- `putCommitLocked(requireParents)` accepts a parent that's shallow.
- `Walk`, `CommonAncestor` and `IsAncestor` treat a shallow commit's parents as absent, the same way they treat stubs today. A shallow commit is inserted through `PutShallowCommit(c)`, which verifies the hash but not the parents.

### 4.2 Snapshot messages (0x0A / 0x0B, finally implemented) ☑

```
SnapshotRequest  {Namespace, At (ref or hex, default main), Cursor?}
SnapshotResponse {Namespace, Commit, TreeEntries[(docId, contentHash)] (first page only),
                  Bodies[(contentHash, json)] (paged by bytes), Refs, NextCursor?, Done}
```

**The receiver** requires an *empty* namespace (genesis head only) or an explicit `--reset`. For each body, it checks that `contentHash == sha256(record(docId, json))` and calls `PutDocument`. Then it runs `CommitTree`, requires the result to equal `Commit.DocumentTreeHash`, calls `PutShallowCommit`, sets the refs, persists, and runs a full index and unique rebuild.

**Capability:** `snapshot`. `SyncV2` falls back to a snapshot when:

- the remote's `HistoryFloorHex` isn't an ancestor of anything local, or
- ingest reports a missing parent below the floor.

### 4.3 Peer-aware truncation floor ☑

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

### 5.1 The meta namespace ☑

- **Created on first start** in every `NamespaceSet`, with the Strict policy.
- **Document ids** are `DerivedUUID("schema:<ns>")`, `DerivedUUID("indexes:<ns>")` and `DerivedUUID("policy:<ns>")`, so two nodes creating the same definition independently produce the *same document*, and any disagreement shows up as a document conflict.
- **Writers:** `SetSchemaChecked`, index DDL and policy `Put` write the meta document alongside their local effect, through `CommitAcross` so the pair is atomic.

### 5.2 Reconciler ☑

`meta.Reconciler` subscribes to `_kdb/meta` commits through the `CommitListener` chain. For each changed document, it applies the change locally:

- `SetSchemaChecked`
- create or drop an index, then `rebuildIndex`
- `policy.Registry.Put`

It's idempotent, and a failure is recorded on the meta conflict queue instead of blocking ingest.

### 5.3 Schema migrations in data commits ☑

`MaterializeCommit` stops skipping `SchemaMigrationOp`. It applies the migration to the runtime's schema through a new `IngestEnv.ApplySchemaMigration` hook, so data never lands ahead of its schema.

**Tests:** `TestIndexCreatedOnAIsBuiltOnB`, `TestSchemaMigrationPropagatesBeforeData`, `TestMetaConflictQueued`.

---

## Phase 6 — Namespace sync sets ☑

- **6.1** `Patterns` matching: `*` matches one path segment, `**` any depth, and a `!` prefix excludes. The intersection of what the client asks for and what the host grants is computed at hello (2.1 already carries patterns).
- **6.2** Namespaces are auto-created on the receiving side through `NamespaceSet.Resolve(ns, create=true)`, with the policy taken from `_kdb/meta` when it's present.
- **6.3** `RefAdvertise` for new namespaces created on the host mid-session: the next cycle picks them up. No push is needed.
- **6.4** Cross-namespace group completeness (the full D10 fix). When a replica ingests a group part, it checks whether it holds every namespace in the marker's `parts=`.
  - **Every part present:** it verifies all of them are present at the same group before exposing the heads. Otherwise it holds the part's namespace at its previous head until the rest arrive or a timeout expires, then exposes it and flags it.
  - **Some parts missing:** it flags `foreign-incomplete`.
- **6.5** The user guide gains a "partition by namespace" section.

**Tests:** `TestPatternSetReplicatesOnlyMatching`, `TestNamespaceAutoCreatedOnReplica`, `TestGroupHeldUntilComplete`, `TestGroupFlaggedWhenPartitionMissing`.

---

## Phase 7 — Filtered projection replicas ◐

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

## Phase 8 — Verifiable partial clone (gated) — gate not passed, not built

**8.0 Measurement first.** `docs/benchmarks/<date>-partial-clone-model.md` compares a filtered projection with a modelled partial clone (tree metadata plus fetched bodies) on 50k documents × {1 KB, 64 KB, 1 MB}. Proceed only if the partial clone is less than half the projection's resident and disk cost on at least one realistic size **and** the workload needs replica-side writes into the source history.

If it proceeds:

- commit format v2 (op digest), a dual-format DAG, `ObjectFetch`
- golden fixtures in both trees
- a Kotlin decode path

This is a separate design review before any code.

---

## Phase 9 — Single-home ownership ◐

- **9.1** A policy field `Consistency {MultiLeader (default), SingleHome}` in `policy.NamespacePolicy`, additive.
- **9.2** A home lease document in `_kdb/meta`: `DerivedUUID("home:<ns>")` → `{holder nodeId, expiresAt, fence}`.
  - **Claiming** is a CAS (`ExpectContentHash`) against the meta namespace *on the meta-home node*. The meta-home is configured with `--meta-home <peer name>`, or is this node when unset.
  - **Renewal** runs at TTL/3.
- **9.3** Write routing: a non-home node's `runTransaction` for a single-home namespace forwards the transaction to the home over the doc wire (`client.Commit` with the principal's token). A home that's unreachable gets `UnavailableError{retryAfter}`.
- **9.4** Fenced ingest: the home stamps `Message += " home=<node> fence=<n>"`. Ingest of a commit whose fence is older than the current lease's is stored but never fast-forwarded, and is queued as a conflict.
- **9.5** The multi-writer suite across 3 nodes.

**Tests:** `TestSingleHomeForwardsWrites`, `TestSingleHomeUniqueAcrossNodes`, `TestStaleFenceNotFastForwarded`, `TestHomeFailover`.

---

## Phase 10 — Placement and cross-node transactions ◐

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

## Phase 16 — Embedding sync in an application (zolik gaps G1–G9)

**Source.** The zolik application embeds KDB through `embed.Host` and its own `NamespaceSet`. It needs every node (cloud, LAN server, phone) to be a sync node. Its review found nine gaps (G1–G9). Each was checked against main at 32a7c29, and the evidence held.

**Partitioning.** Zolik partitions by namespace:
- `zolik/u/<uid>` for a user's own data;
- `zolik/u/<uid>/ro` for cloud-authored data;
- `zolik/m/<id>` for a match.

It does not use filtered projections: a projection filters one namespace on a SQL WHERE and cannot bind the filter to the caller. **Not needed:** Phase 8 partial clone, projections, 2PC.

Slices, in dependency order. Each slice is one PR with its own tests.

### 16.1 — `syncnode`: sync as a library (G1) and a live namespace set (G5)

**G1.** `replication.New` has one caller, `service/service.go:827`. Everything a node needs is assembled inline in `service.go`:
- the retention floor (`:423`);
- the `MetaStore` (`:447`);
- authority expiry (`:454`);
- the conflict webhook (`:456`);
- projection setup inside the opener (`:465-506`);
- the peer listener (`:564`);
- the replicator (`:822-835`);
- commit notification (`:405`);
- the scrub loop.

An embedder cannot become a sync node without copying about 600 lines.

The new package `go/kdb/syncnode`:
- `Open(host *embed.Host, set *server.NamespaceSet, primary *server.KdbServerRuntime, cfg Config) (*Node, error)`.
- The node owns the meta runtime and `MetaStore`, authority expiry, the conflict webhook, the peer-floor and commit-notify plumbing, projection peers, the replicator, the scrub loop and listeners.
- `Node.Prepare(rt)` wires a runtime the application opened: commit listener, retention floor, projection settings, definitions. An application's opener calls it.
- `Node.Opener(...)` is a default opener that does the same for embedders with nothing extra to add.
- It exposes `Replicator()`, `Meta()`, `Conflicts(ns)`, `SyncNow(peer)`, `Listen(addr, tls)`, `Handler()` (16.2), `Start()` and `Close()`.
- `service.go` becomes its first caller with no behaviour change. The service e2e suites are the regression gate.
- It works on an in-memory `Host`.

**G5.** The peer namespace set is fixed at start: patterns come from `Config.Peers` only. A phone's set changes as its user joins matches and signs in or out. Restarting the replicator would drop watermarks and backoff.
- Add `Replicator.SetPeerNamespaces(peer, patterns)` and `AddNamespaces(peer, ns...)`.
- Each peer loop reads its patterns at the start of each cycle.
- Per-namespace state (watermarks, pending tips) is kept for namespaces that stay.

### 16.2 — Peer sync over an HTTP handler (G2)

The host side is TCP only (`server/peersync_listen.go:40,46`), although the client already dials WebSocket (`peersync/v2_client.go` `dial`). Phones must sync over the API's own origin on 443, behind the HTTPS load balancer, with a bearer token on the upgrade request.

- `Node.Handler() http.Handler` upgrades to WebSocket and runs the same per-connection handler as the TCP listener.
- The request's `Authorization` header (bearer or basic) goes into `auth.ConnectionContext`, so the pluggable `auth.Engine` authenticates it.
- The application mounts it on its own router, for example at `/kdb/sync`.
- The client's `ws://` / `wss://` peer addresses dial it, with headers on the upgrade.

### 16.3 — Pull versus push authorization (G3)

`auth.PeerSyncAction{Namespace}` is the only peer action. It is checked once per namespace at hello and on each frame's namespace, never per direction. A phone must pull `u/<uid>/ro` but never push to it.

- Add `auth.PeerPullAction` and `auth.PeerPushAction`.
- `PeerSyncAction` stays as "both", for compatibility and for existing policies.
- The v2 host authorizes:
  - pull at hello, at refs and at every read frame;
  - push at `REF_UPDATE` and `GRAFT_PUSH`, and on the v1 host's commit push.
- Optional per-document check: `DocumentWriteAction` on each document a push ingests, when the engine is configured to ask.
- The registry engine maps both new actions onto its existing peer permission with a direction, so current role definitions keep working.

### 16.4 — A gated, preconditioned whole-document write (G7)

`embed.PutJSONDocument` bypasses the server write gate that peer ingest runs under. A replicated merge landing between an application's read and its write causes "branch main moved" errors or, worse, a write based on a pre-merge read.

- `KdbServerRuntime.PutJSON(ns, id, body, Expect{ContentHash, Absent})`: whole-document swap semantics (delete and write in one transaction), through the gate.
- It fails with a typed `PreconditionFailedError` carrying the current hash when the expectation does not hold.
- The application retries its read-modify-write on that error.

### 16.5 — Metadata by pattern, and meta sync scoped to what the peer may see (G4)

`_kdb/meta` holds one document per namespace (`SetResolution`, `AssignHome`) and replicates whole to every peer. With per-user and per-match namespaces that is millions of documents, and a privacy leak: namespace names contain user ids.

- **Pattern definitions:** `SetResolution("zolik/u/*", chain)`, home rules by pattern, and a per-namespace document overriding its pattern.
- **Resolution is deterministic:** exact name first, then the most specific pattern (the longest literal prefix, then lexicographic order). Both sides of a chain-hash comparison resolve identically.
- **Scoped meta sync:** when a peer syncs `_kdb/meta`, the host sends a per-peer *view* containing only the definitions that are patterns, or that name namespaces the peer may pull (16.3).
- **This needs a meta format version.** Pattern documents are new document kinds. An old node ignores them, and a new node refuses to merge a chain it cannot resolve.

### 16.6 — Many namespaces: idle close, and a measurement first (G6)

The cloud holds about two namespaces per user plus one per match. Phones hold tens to hundreds.

- **Measure first:** open latency, RSS, file descriptors and disk for 1k, 10k and 50k small namespaces (500k by extrapolation).
- **LRU idle close in `NamespaceSet`:** a bounded number of open runtimes and an idle timeout. It closes through `Host.CloseNamespace`, never while a write, sync or transaction holds the runtime, and reopens through the opener on the next `Resolve`.
- **Gate:** if the per-namespace cost is too high even with idle close, the fallback (finished matches folded into per-user archive namespaces) is an application decision. It is recorded, not built here.

### 16.7 — Application-driven handover (G8)

Phase 9 has manual handover only (control plane). Resuming a match on another device means moving its home from phone A to the cloud, or from the cloud to phone B.

- `Node.Handover(ns, toNode)`: in-process, by the current home, with the fence bump the control plane does.
- **`HOME_REQUEST` over the sync connection.** The would-be home asks the current home, and the current home's application hook (`HandoverPolicy`) approves or refuses. For zolik the hook asks whether the match is idle and the requester is a seated player. On approval the current home hands over and the requester's next pull adopts the new assignment.
- **Forced handover:** a node the namespace's chain names as its authority may reassign the home when the current home is unreachable. The fence makes the old home's late writes unadoptable.

### 16.8 — gomobile packaging (G9)

- A CI job runs `gomobile bind` for Android (and for iOS on a macOS runner) on a tiny package that imports `syncnode`, `replication`, `peersync` and the WebSocket transport.
- It records the binary size so growth is visible.

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

### Phase 1 — landed

- **Node identity.** `embed.LoadOrCreateNodeID` writes `<dataRoot>/NODE`, adopting `txn/HOST` if it exists. The coordinator now takes `NODE` as its host id when it has to create one. `KDB_NODE_ID` overrides it.
  - `server.SetProcessNodeID` is called by `kdb-service` before any namespace opens.
  - `KdbServerRuntime.NodeID` authors every commit through `authored()`.
  - The peer host introduces itself by node id and refuses a peer presenting its own id. `/healthz` prints `node_id`.
- **Causal timestamps** landed in `dag.causalTimestampLocked`, applied in `appendCommitLocked`: every commit Go builds is at least 1µs past its parents. Ingest refuses commits more than `MaxClockSkew` (default 5m) in the future.
  - **Deviation:** there's no separate HLC state. The parents already carry the "max seen" timestamp a clock would track, so clamping to them is the whole mechanism.
- **Deterministic merges.** `mergeNonConflicting` builds the merge with:
  - hash-ordered parents
  - `DerivedUUID("kdb:merge/1:P0:P1")` as the tx id
  - `MergeAuthorNodeID` as author
  - `max(parent timestamps)` as timestamp
  - the fixed message `kdb:merge/1`
  - schema hash passed through when both parents agree

  Its operations are the delta from the *canonical* first parent. When that parent is the remote head, they're the local side's final writes, with overlaps taking the resolution.
  - The Custom resolver now sees Existing = first parent's side and Incoming = second's. Its determinism contract lives on that ordering.
- **The property test catches the old behaviour.** `TestMeshConvergesDeterministically` (20 seeds by default, `TEST_SEEDS` to widen; run green at 50 under `-race`) fails on every seed when the merge tx id is put back to random.
- **Deviation: no separate simulation harness package yet.** The in-process `pullInto`/`exchange` helpers in `peersync/determinism_test.go` are that harness's first cut. The package split waits until Phase 3 needs it across packages.

### Phase 2 — landed

**Wire:** eight messages at 0x26–0x2D (`wire/sync_v2_ops.go`), one more than planned. REFS_REQUEST/REFS_RESULT is a request/response pair, and the initial refs ride on SYNC_HELLO_ACK.

**`peersync` pieces:**
- `V2Host` and `SyncV2`
- `NamespaceProvider`
- `ApplyRef`, one decision function for every ref kind, shared by host and client
- `MissingCommitsFrom`, byte-paged
- `MatchNamespace`/`SelectNamespaces`, the pattern grammar Phase 6 also uses

**Server side:** the listener picks v1 or v2 from the first frame, and `KdbServerRuntime.PeerNamespaces()`/`PeerIngestEnv()` expose the process's namespace set.

**Deviations, and why:**
- **No server-side cursor.** Paging is stateless: each page is "missing from wants, excluding haves", and the client names the tips of what it has received as further haves. A resume after disconnect is a new session that passes `ExtraHaves` = the previous session's `ReceivedTips` (`TestV2ResumeAfterInterruption`). Stateless means a host holds nothing per session, which matters once a hub serves many edges.
- **Commit payloads are base64 `[]byte` in v2 frames,** not the integer-array `jsonByteArray` that v1 keeps for Kotlin's decoder. v2 is Go-only, and the integer form is three to four times larger.
- **Only `main` merges. Side branches only fast-forward** (a diverged side branch is a conflict), and **tags never move.** Only `main` has live documents behind it; merging someone else's side branch is their decision.
- **Namespaces are granted one by one at hello.** A pattern is a request, and a principal with rights to some of what it matches gets exactly those. Every later frame re-authorizes, so revocation takes effect mid-session.
- **`CreateOnPush` (off by default)** lets a peer push into a namespace this node doesn't hold, but only a literal (wildcard-free) name.
- **Not measured yet:** the 100k-commit v1-vs-v2 comparison. It needs an isolated machine (see the benchmark-isolation note), so it's recorded as outstanding rather than run under load.

**Known limitation, carried to Phase 4:** replicated side branches and tags are held in the DAG and survive a clean restart through the checkpoint, but not a crash. The delta log records only commits on `main`, in the order `main` reached them — that's what keeps replay correct (commit `2aec036`).

### Phase 3 — landed

**Replicator.** `go/kdb/replication` holds:
- `ParsePeer`/`ParsePeers`, the `--peer` / `KDB_PEERS` grammar. Passwords come only from an environment variable named in the spec, never from the flag.
- `StateStore`: `<dataRoot>/replication/peers/<name>.json`.
- `Replicator`: one loop per peer, triggered by a debounced commit, the interval tick, or `SyncNow`. Backoff doubles per failure, is capped, and has jitter. An interrupted pull's received tips are kept as `PendingTips` and passed back as `ExtraHaves`, so the next attempt resumes.

**Conflict queue.** It's `peersync.ConflictQueue`, not an `embed` type as planned: every writer to it is in peersync, and `embed` doesn't need it.
- Stored at `<dataRoot>/replication/conflicts/<escaped ns>/<id>.json`.
- Keyed by what the conflict is about: `(ns, ref, peer)` for a divergence, so a conflict that persists is one entry whose `Seen` counter grows.
- A refused ref update records an entry and points `peers/<peer>/<kind>/<name>` at the incoming side. A later clean update of that ref with that peer clears both.
- Unique duplicates and foreign cross-namespace parts moved here from Phase 0's in-memory `ReplicationIssues`, which is gone. Unique-duplicate entries clear themselves once a rebuild no longer finds the duplicate.

**Resolution.** `peersync.ResolveConflict` merges the tracked side through `Adopt`, driven by a new `ResolutionOptions.Choose` hook. That hook is explicitly local/remote, unlike the canonical-order Custom resolver, because it's an operator's decision rather than a policy. `KdbServerRuntime.ResolveConflict` and `DismissConflict` authorize as a commit.

**Surfaces.**
- `kdb-service`: `--peer`, `KDB_PEERS`, `--peer-create-namespaces`.
- Every runtime's commits notify the replicator: the primary through the chained listener, the others from the namespace opener. The replicator stops first on shutdown.
- Outbound connections use the node's own TLS settings, which gives node-to-node mTLS when `--tls-ca` is set.
- Control plane:
  - `GET /v1/peers`, `POST /v1/peers/{name}/sync`
  - `GET /v1/ns/{ns}/conflicts`, `POST .../conflicts/{id}/resolve`, `DELETE .../conflicts/{id}`
- `/metrics`:
  - `kdb_conflicts_open{namespace,kind}`
  - `kdb_replication_last_success_seconds{peer}`
  - `kdb_replication_consecutive_failures{peer}`
  - `kdb_replication_commits_total{peer,namespace,direction}`
- CLI: `kdb node status`, `kdb sync <ns> <addr> [--pull|--push]`, `kdb conflicts <ns>`, `kdb resolve <ns> <id> --take local|remote`.

**e2e.** `kdb-integration/e2e/test_replicator.py` covers two real processes configured only with `--peer`: writes travel both ways, and replication resumes after `kill -9` and restart.

**Not done, deliberately:**
- **The admin UI Conflicts tab.** The API it would sit on is complete.
- **0.1c, incremental unique-key maintenance on ingest.** It's still a lenient full rebuild per ingest batch, and only when the schema declares unique fields.
- **Persistent held-open sessions.** A remote commit reaches this node on the next interval tick, not immediately. Push is immediate because it's commit-triggered. The interval is the knob.

### Phase 4 — landed

**Shallow roots.** `dag.PutShallowCommit`/`MarkShallow`/`IsShallow`/`ShallowRoots`/`Horizon`/`CommitCount`. Traversals stop at a shallow root as they do at a stub. Its operations are never evicted, because no log holds them. `RefsOf` advertises the horizon, meaning shallow roots plus commits whose parent was truncated away, as `Shallow`.

**Snapshot transfer.**
- **Frames:** new Go-only SNAPSHOT_FETCH/SNAPSHOT_PAGE (0x2E/0x2F). The old 0x0A/0x0B are left alone: they're shared with Kotlin, and their opaque payload would have hidden a Go-only format.
- **Host:** pages the tree at a commit in id order, by bytes, through a new optional `storage.TreeWalker`, implemented by the mem adapter, `ServerEngine` and `MultiplexAdapter`.
- **Receiver:** `peersync.InstallSnapshot` requires an empty namespace. It verifies the commit's own hash, installs page by page (committing a tree each page, so memory isn't the whole namespace), and requires the final tree to be the declared one, undoing everything otherwise. It then admits the commit as a shallow root.
- **When `SyncV2` uses it:** automatically when the local namespace is empty and the peer advertises a horizon, or on request (`PreferSnapshot`; the peer config `bootstrap=snapshot`).

**Durability on a file runtime** (`embed/snapshot_install.go`). None of this changes an on-disk format.
- `CanInstallSnapshot` refuses up front if checkpoints are disabled or the namespace has any commit.
- `PersistSnapshot` does three things, in order:
  1. `ServerEngine.MaterializeLiveBodies` writes the bodies to the blob store (history=full included).
  2. It writes a checkpoint claiming `through=-1`, carrying the shallow root's payload.
  3. It adds `shallowRoots` to `meta.json`.
- Open reads the marker. It points cold reads at the blob store, re-marks the roots, and refuses with `SnapshotCheckpointMissingError` rather than silently replaying an empty namespace.
- `saveCheckpoint` always carries shallow roots' payloads, so their operations survive after they stop being a branch head.

**Two fixes found along the way.** Both mattered before this phase, and more after it:
- `OSByteStore.WriteSnapshot` was a plain overwrite. It's now temp file, fsync, rename, directory fsync. The checkpoint had been a disposable cache; for a bootstrapped namespace it's the only record.
- `meta.json` writes are atomic, for the same reason.

**Peer-aware retention.** `EmbeddedKdbRuntime.SetPeerRetentionFloor` widens the history=none truncation window to keep everything newer than the floor. That makes it a floor on retention, never a ceiling. The floor is the older of two sources:
- `Replicator.PeerFloor`: for peers this node pushes to, the last successful sync of that namespace within `--peer-retention-grace` (default 7 days).
- `KdbServerRuntime.InboundPeerFloor`: for peers that fetch from this node, the last time each one finished a fetch. The host records it through `V2HostConfig.OnCaughtUp`, persisted under `replication/inbound/`.

**Tests:**
- `peersync/snapshot_test.go`: bootstrap; tampered body refused and undone; automatic snapshot from a shallow peer.
- `server/snapshot_restart_test.go`: clean restart; crash replaying the log tail onto the bootstrap checkpoint; a lost checkpoint refused; a namespace with history refused.
- `embed/peer_floor_test.go`, plus the floor-source tests.
- e2e `test_new_node_bootstraps_from_a_snapshot_and_survives_kill9`.

**Deviations and gaps:**
- **CompactionNotice (0x08) is not sent.** The floor travels as the advertised horizon instead, which a peer reads on every hello.
- **Squash isn't guarded against peers.** `dag.Squash` is only reached from the compaction evaluator, which the Go engine doesn't run. The guard belongs with whoever wires that evaluator up.
- **In a running process, truncation can delete the segments behind commits the DAG still holds in memory.** Until a restart makes them a horizon, fetching them fails with "operations unavailable" instead of being advertised. The peer floor makes this unlikely for known peers.
- **Replicated side branches and tags still survive only a clean restart,** as recorded in Phase 2.
- **Historical reads below a shallow root under the replay history strategy fail.** The objects strategy, which is the default, is unaffected.

### Phase 5 — landed

`server.MetaStore` over the reserved namespace `_kdb/meta`.
- **One document per definition,** with a derived id, so two nodes making the same definition agree for free:
  - `kdb:meta/schema/<ns>` holds the schema's bytes as hex.
  - `kdb:meta/index/<ns>/<name>` holds the CREATE INDEX statement, or `dropped:true` as a tombstone, so a node knows to drop an index rather than merely not knowing about it.
- **Recording.** `CREATE TABLE` (through `SetSchemaChecked` on the wire path) and index DDL record into it, as long as the change isn't the store itself applying a replicated definition (`metaApplying`). A definition identical to the stored one writes nothing.
- **Reconciling.** Every commit `_kdb/meta` takes, local or replicated, kicks a background reconcile. Startup runs one synchronously, and a namespace opened later gets its definitions through `ApplyTo` before it joins the set.
- **Failures.** A definition that can't apply here (data violates the schema, an index won't build) becomes a `meta-apply` entry in that namespace's conflict queue and clears once it applies.
- **Schema migrations inside replicated commits** now reach the schema. `serverLocalNode.Advanced` applies each `SchemaMigrationOp` in commit order.

**Deviations, and why:**
- **Reserved, not ordinary.** `_kdb/meta` joins the `NamespaceSet` through a new `AddSystem`, not `Add`. `Get`/`Resolve` never return it, so SQL, the document wire and cross-namespace commits can't reach it; only `PeerNamespaces` sees it. The plan hadn't said how to keep a system namespace off the wire, and `ValidateNamespaceID` rightly refuses reserved names through the opener.
- **Policy documents are not replicated yet.** `policy.Registry` has no durable store in the Go server to mirror, so there's nothing to reconcile against.
- **The replicator adds `_kdb/meta` to every peer's patterns** unless a pattern excludes it by name. Under RBAC, a peer then also needs `sync` on `_kdb/meta`.
- **Found along the way, and fixed as a consequence:** `CREATE TABLE` on the Go server set the schema in memory only, so it didn't survive a restart. The metadata namespace is now its durable record (`TestSchemaSurvivesRestartThroughMeta`).

### Phase 6 — landed

- **6.1–6.3 were already in place from Phase 2:** pattern matching, per-namespace grants, and a pull with `CreateLocal` opening namespaces through the process's opener. Phase 6 adds the server-level test `TestNamespaceAutoCreatedOnReplica`.
- **6.4 follows each foreign cross-namespace group as its parts arrive** (`trackForeignGroupPart`).
  - While any part is missing, every arrived part is flagged in its namespace's conflict queue. The flag says whether the missing part's namespace isn't replicated here at all (the group can never be whole) or just hasn't arrived yet.
  - When the last part lands, the flags on all parts clear.
  - **Deviation:** parts are *not* held back from readers until their group is complete, as the plan had proposed. That would make one namespace's availability depend on another's replication, which multi-leader replication exists to avoid. The flag tells a reader what they may be looking at instead.
- **6.5:** the user guide's "Peer sync and replication" section is rewritten around `--peer`, partitioning by namespace, conflicts, snapshots, retention and `_kdb/meta`. The flags table covers the new flags.

### Phase 7 — landed (7.1–7.5)

**Filter.** `sql.ParseFilter` makes a WHERE expression usable on its own, and it's evaluated with `sql.EvalPredicate` against each document.

**Protocol.** PROJECT_FETCH/PROJECT_PAGE (0x30/0x31), pull-based. A fetch names the source commit the replica last reached.
- **Delta:** when that commit is an ancestor of the target, the host sends the net effect since then. A document that now matches is a write. One deleted, no longer matching, or no longer readable is a delete.
- **Snapshot:** on the first fetch, or when the replica's position is unrelated, the host sends a paged, filtered snapshot marked `Reset`.
- **Consistent target:** `AtHex` pins the target across a transfer's pages.
- **Authorization:** read on the namespace (`StreamSubscribeAction`), then document by document (`DocumentReadAction`). A projection is a filtered read, so it needs no sync rights. A document the principal can't read goes out as a delete.

**Replica.** The projection lives in `<ns>.projection-<sha256(filter)[:8]>`. Each page is one local commit whose message is `kdb:projection/1 source=<hex> complete=<bool>`. The last complete one is where the next transfer starts, so an interrupted transfer is simply redone from there, which is safe because pages carry final states. A `Reset` transfer deletes, at its end, every local document it didn't deliver.

`KdbServerRuntime.ProjectionOf` makes the namespace read-only to everything but the projection itself: client writes, cross-namespace commits and peer pushes all get `ProjectionReadOnlyError`. The service's opener recognises projection names, so the namespace is read-only from the moment it opens.

**Replicator.** A `filter=` peer runs projection cycles instead of sync. The filter is always the last field of the spec, because it may contain commas, and it requires exactly one literal namespace.

**Tests:**
- `server/projection_test.go`: only matching documents arrive; entering and leaving the filter; deletes; a no-op resync; an out-of-filter change only moves the recorded position; RBAC-hidden documents never sent; client writes refused.
- `sql/filter_test.go`.
- e2e `test_filtered_peer_keeps_only_matching_documents`.

**7.4, offline write-back (landed later; see "Phase 7.4 — landed" below).**

**7.5 (landed later; see "Phase 7.5 — landed" below).**

### Phase 8 — gate evaluated, not built

Step 8.0 was done as a model, grounded in a fresh measurement of the document trie (~165 B per entry) and the commit-graph figure already on record (~519 B per commit). See [benchmarks/2026-09-21-partial-clone-model.md](benchmarks/2026-09-21-partial-clone-model.md).

A partial clone holds a strict superset of a projection: every tree entry and the whole graph, plus the same fetched bodies. Its cost is therefore the projection's plus a fixed ~138 MB for a 50k-document, 250k-commit source, whatever the selectivity or body size. The gate required it to cost less than half the projection's, so it cannot pass.

Commit format v2 is not introduced. The write need Phase 8 might have served belongs to Phase 7.4 (write-back).

### Phase 9 — landed (manual handover; automatic failover not built)

**The assignment.** A namespace's single-home assignment `{node, addr, fence}` is a `kind:"home"` definition in `_kdb/meta`, made with `MetaStore.AssignHome` or `PUT /v1/ns/{ns}/home`, and read with `GET` on the same path. Every assignment, including returning a namespace to multi-leader, raises the fence past the last one this node knows of.

**Enforcement:**
- **Writes elsewhere:** on a node that isn't the home, `admitWrite` refuses every non-system write with `NotHomeError`. The error names the home's address and classifies as the new wire code `NOT_HOME`. Replication is unchanged: the other nodes still receive the home's writes and serve reads.
- **Stamping:** the home stamps every commit it makes with `kdb:home/1 node=<id> fence=<n>`.
- **Fence check:** ingest checks every commit a head move would adopt (the new `IngestEnv.CheckCommit`, set only on single-home namespaces). A commit stamped with an older fence than the namespace's current one is refused with `StaleFenceError`. It stays stored, the head doesn't move, and the refusal is queued as a conflict. That's the write a former home made after it was replaced.
- **Global uniqueness:** unique constraints and compare-and-set are global on a single-home namespace, because only one node's registry ever decides.

**Deviations, and why:**
- **No lease, and no automatic failover.** The plan's lease with a CAS on a meta-home node would put a linearizable coordination point back into a system built to have none, and done properly it's consensus. Assignment is explicit, and a handover is an operator action. The fence is what makes a handover safe: a stale home's writes are refused wherever they land, not trusted until a lease runs out.
- **Redirect, not forwarding (9.3).** A non-home node refuses with the home's address instead of forwarding the write. Server-to-server forwarding would need the home to trust a node acting for a principal it never authenticated. A redirect keeps authentication end to end; the client follows it (Phase 10.2).
- **Concurrent assignments on two nodes** are a same-document conflict in `_kdb/meta`, surfaced for an operator.

**Tests:** `server/home_test.go`. Writes are refused away from the home and carry the home's address; the home's writes are stamped and replicate. A handover makes the former home's in-flight write unadoptable, and makes the former home refuse writes once it learns.

### Phase 10 — landed (10.1–10.3, and 10.4's refusal; cross-node 2PC not built)

- **10.1 Placement.** There's no separate placement document: a namespace's home is its placement, since every node that holds it serves reads. `MetaStore.Placement` and `GET /v1/placement` list the assignments.
- **10.2 Client redirects.** The Go SDK's `client.NotHomeError` carries the home's address, parsed from the `NOT_HOME` refusal. `client.Router` retries a refused write once at the home and remembers the home per namespace. Reads stay on the first connection. Test: `client/router_test.go`.
- **10.3 Rebalancing** is a documented runbook in the user guide ("Moving a namespace to another node"), built from pieces that already exist: `--peer` (optionally with `bootstrap=snapshot`), the lag signals, `PUT /v1/ns/{ns}/home`, and removing the old peer.
- **10.4 Cross-node transactions.** A cross-namespace transaction touching a namespace homed elsewhere is refused (`TestCrossNamespaceCommitRefusedAcrossHomes`). That isn't new code: `admitWrite`, which `CommitAcross` runs per part, refuses any part not homed here.
  - **Not built:** 2PC across nodes. It needs a coordinator reachable from every home and a participant protocol over the wire. It is also the one piece that would make the system depend on several nodes being up to commit anything, and nothing so far has needed that. Until a workload does, a transaction that has to span homes should move those namespaces to one home first.

### Review fixes — landed

An adversarial review of phases 0–10 found eight defects. Each has a regression test.

1. **A merge echoed a node's own write back as a change** and re-applied it over a newer local write.
2. **Criss-cross merges** (two nodes merging the same pair) disagreed or reported false conflicts.
   - **Fix for 1 and 2:** `peersync/merge_resolution.go` replaces the op-based resolver with a value-based one.
     - For each document either side wrote since the merge base, it compares the values at the two heads.
     - It traces each value to the commit that introduced it. Through a merge, that is the parent that already held it.
     - A side has changed a document only if its value's origin is neither zero nor an ancestor of the other side's.
     - One side changed: take it. Both changed: a conflict, resolved by policy (LastWrite compares the origins' timestamps, then hashes).
   - **New rule:** a merge commit's ops contain *every* document the parents differ on, with its merged value. Applying them on top of either parent gives the merged tree.
   - Tests: `TestMergeEchoOfOwnWriteIsNotAChange`, `TestCrissCrossMergesAgreeWithoutConflict`.
3. **A snapshot replica stopped following main** once a side branch on the peer reached below its shallow root.
   - Pull now runs per ref, main first.
   - A failure on any other ref is recorded in `NamespaceSyncResult.RefErrors` and doesn't stop the rest of the sync.
   - Test: `TestSnapshotReplicaFollowsMainDespiteBranchBelowRoot`.
4. **A crash between logging a peer commit and logging the merge** made replay move main onto the peer's branch.
   - **Replay rule:** a logged commit is applied only if the current head is one of its parents. Otherwise it is only stored.
   - Test: `TestReplayDoesNotMoveMainOntoAPeerBranchCutOffFromItsMerge`.
5. **Race on `metaApplying`:** a flag that suppressed Meta recording while applying Meta could hide a concurrent local DDL.
   - Removed. Meta apply calls the local-only variants (`createIndexLocal`, `dropIndexLocal`) directly.
   - Meta tracks the document versions it has seen and skips re-applying them.
   - Local DDL records into Meta and rolls back if the record fails: CREATE INDEX drops the index, CREATE TABLE restores the previous schema.
6. **A handover refused the old home's own acknowledged writes** once they replicated after the new fence was known.
   - `Home.Since` is the handover point. `AssignHome` marks the home as stopping, takes the writeGate, and records the head as `Since`.
   - The new home refuses writes (`UnavailableError`) until it holds `Since`.
   - A replicated commit passes the fence check if any of these holds:
     - it is a merge commit
     - the current home authored it
     - it carries the current fence stamp
     - it is an ancestor of `Since`
   - Cross-namespace parts are now authored by the node, so this author check covers them too.
   - Test: `TestHandoverOnOldHomeKeepsItsWrites`.
   - **Known consequence:** a failover assigned on a node *other* than the old home can't know what the old home acknowledged but never replicated. Those writes arrive later as stale-fence conflicts. That is the intended behaviour for writes acknowledged by a home that has since been replaced.
7. **Snapshot durability.**
   - If `SnapshotInstalled` (the checkpoint plus the meta.json marker) fails, `InstallSnapshot` moves main back and undoes the document writes. Test: `TestSnapshotNotMadeDurableLeavesNamespaceEmpty`.
   - A crash after the checkpoint but before the marker used to leave a DAG with horizon commits whose bodies cold reads never looked for. On open, any horizon commit now switches the engine to external bodies.
8. **Retention floor.**
   - A peer's `LastSync` is now the time its sync *started*. The inbound `OnCaughtUp` passes the time of the session's hello, so commits made during a long transfer stay above the floor.
   - `foreignGroups` is memory only. After a restart, an arrived part is recognised by the flag it left in the durable conflict queue, so a group can still complete.
   - **Not changed:** commit listeners fire before the delta log is fsynced, as they always have. A crash can lose a commit a listener already saw. Peers get it again on the next sync, because the refs they compare are durable.

### Phase 7.4 — landed (offline write-back to filtered projections)

**Configuration.** A filtered peer with `writeback=true` makes its projection writable. The service knows which projections are write-back from the peer config, before any sync runs, so a node that starts offline takes writes straight away.

**The DAG is the outbox.** There is no separate outbox log to keep consistent with the data.
- **A local write** on a write-back projection is an ordinary client commit. Its message is `kdb:writeback-local/1 <doc>=<base content hash|-> …`, where each base is read under the write gate from the tree the commit lands on.
- **A decision** is a later system commit, `kdb:writeback/1 commit=<local> outcome=<applied|conflict|refused>`.
- **Pending** means the local commits above the local write the newest decision names. Decisions are made in order, so that is the whole rule, and a local write made while a decision was in flight sits below the decision but above the write it names. The projection's history is a single line, so walking the first parent is exact.
- **The cache.** An in-memory list caches the result and is rebuilt from the DAG after any error, or after a restart.

**Sync order.** Before each pull, `SyncProjection` sends every pending write in order as PROJECT_WRITE (0x32), and the source answers PROJECT_WRITE_RESULT (0x33).
- **What the source commits.** For each document it replaces the value with the write's final state (delete+write). Every op carries a precondition: `ExpectContentHash(base)`, or `ExpectAbsent` if the projection didn't hold the document. A guarded op is judged by its precondition alone, so the write lands on whatever head it reaches the gate at, and never overwrites a change it didn't see.
- **Idempotent resend.** The source transaction id is `DerivedUUID("kdb:writeback/1:" + local commit hash)`, and the source checks for an existing commit with that id first. A resend after a lost reply therefore answers `applied`. The engine's own idempotency check is not enough: it only matches a retry onto the same parents.
- **Outcomes:**
  - `applied`: the local value already is the source's.
  - `conflict` (a precondition failed) or `refused` (authorization, schema, a read-only source, ResourceExhausted, an unsupported op): the reply carries the source's current state of each document, as far as the principal may read it. The replica records a `write-back` conflict entry with the attempted and current bodies. The decision commit then puts each document back to the source's state, or removes it if that state is absent, unreadable or outside the filter. The decision and its effect land in one commit.
  - Any other failure is a PEER_ERROR. The write stays pending and the cycle stops.
- **Pulled pages skip pending documents.** Otherwise a pull would overwrite a local value before the source decides on it. Client writes, pulled pages and decisions are serialized by one mutex, so the pending set can't change between the check and the commit. A pulled page may skip a document whose source value changed; that write then conflicts, and the conflict reply restores the source's value. The projection doesn't depend on a later delta for it.

**Authorization.** The source authorizes a write-back as the sync session's principal: `StreamSubscribeAction` on the namespace, `TxCommitAction`, then the ordinary per-document checks in `admitWrite`. The replica's local writers are authorized locally as usual. The source never learns who wrote at the site, only the peer's user.

**Restrictions.**
- Only `WriteOp` and `DeleteOp` are accepted (`ErrProjectionWriteOp`).
- A projection can't take part in a cross-namespace transaction. Each local transaction reaches the source on its own, so a group could not arrive there as one.
- A dependent chain of local writes (a second edit of a document before the first is decided) cascades: if the first is rejected, the second's base no longer matches and it is rejected too. Both attempts are kept in the conflict queue.

**Two defects found on the way, fixed in 11d5c9f.** A `WriteOp` merges into the document it lands on.
- **Projection pages** carry final states, so a key removed at the source used to stay in the projection. Pages now write delete+write.
- **`_kdb/meta` records** had the same problem: an index created again after a drop stayed `dropped:true`. They now write delete+write too.

**Also fixed (ed8a6b8).** A local DDL change could race the meta reconciler, which applied a definition it had read just before the change. For example, a DROP INDEX was undone by the CREATE the reconciler had read a moment earlier. Local definition changes now run under the reconciler's lock (`MetaStore.Local`).

**Tests:**
- `server/writeback_test.go`: applied (update, create, delete); conflict with the source's value put back, including a pull in between that must not overwrite the pending write; offline then reconnect, with dependent writes in order; idempotent resend after the source moved on; pending list rebuilt from the DAG with a write made during a decision; refusal not retried; leaving the filter; unsupported ops refused.
- `server/projection_test.go` `TestProjectionFollowsKeyRemoval`; `server/meta_test.go` `TestIndexRecreatedAfterDropReachesPeer`.
- `replication/config_test.go` `TestParseWriteBackPeer`; `wire` `TestRoundTripProjectWrite`.
- e2e `test_writeback_peer_sends_local_writes_to_source`.

### Phase 7.5 — landed (stream subscriptions as unstored projections)

**What was wrong with the stream hub.** It was more broken than "has no resume":
- **Resume did nothing.** A subscriber's `LocalHeads` was recorded and then ignored. A reconnecting subscriber got only commits made after it reconnected, the first one failed its parent check (`DesyncError`), and it could never recover.
- **Slow subscribers were stuck for good.** One that fell behind lost frames (`DroppedFrames`) and was then in the same state.
- **Replication broke it.** The service published each commit against its first parent, so every peer merge whose first parent was the peer's side desynced every subscriber.
- **No read checks.** Every subscriber got every commit's full operations; only `StreamSubscribeAction` was checked.

**Now.** A subscription is followed like a projection that is not stored:
- **Publish only wakes.** `Publish` pokes each subscriber's own goroutine (one pending poke is enough). It carries no content, and the service passes the commit only for the listener signature.
- **Catch-up from the DAG.** The goroutine brings the subscriber from its position to the head.
  - **Per-commit** when the head's first-parent line reaches the position within 64 commits and every commit still has its ops: each commit goes as it was made.
  - **Otherwise one frame** named for the head. It carries the difference between the two trees (`embed.DiffCommits`, final bodies and deletes), with `ParentHash` set to the subscriber's position. This covers a merge that put the position on a side branch, a long absence, and archived ops.
  - Either way the client's existing parent check holds frame to frame. The frame format is unchanged, so existing Go and Kotlin clients work as they are.
- **Visibility.**
  - **Filter:** an optional `Filter` on the handshake, JSON `filter` omitted when nil, same grammar as projections.
  - **Read checks:** each written document is checked with `DocumentReadAction` for the handshake's principal. A write the subscriber may not see, or that doesn't match, becomes a delete. Ops naming no document pass only on an unfiltered subscription.
  - **Anonymous hubs** (`--stream-allow-anonymous`) skip the per-document check, keeping that flag's documented meaning.
- **Resume.**
  - A `LocalHeads` position must be on this node's main, as the head or an ancestor. Anything else is refused at the handshake with a message saying to subscribe fresh.
  - With no position, the subscriber starts at the head.
  - The subscriber's goroutine starts only after the ack is sent, so no delta can overtake it.

**Not changed.** The in-memory `stream.Coordinator` (tests and embedding) still publishes what it is given. Index hints were never sent by the service and still aren't.

**Limits.**
- A tree-diff frame is as large as the difference and isn't paged, so a subscriber far enough behind for that to matter should resubscribe fresh.
- A resume under a different filter than the position was built with can't be detected.

**Tests:** `server/stream_resume_test.go`, with a recording subscriber that applies every frame and checks the parent chain:
- resume over TCP sends exactly the missed commits
- an unknown position is refused
- a 74-commit absence arrives as one frame, deletes included
- six rounds of peer merges end with the subscriber holding the merged tree; a mutation shows the tree-diff path is taken
- a filter plus a hidden document: only readable matching documents, and leaving the filter arrives as a delete

The fan-out tests were rewritten for coalescing: 300 commits behind a stuck subscriber arrive, once it drains, in far fewer frames and end at the head.

---

## Proposed next phases (10.5, 11–15) — landed (see each entry)

These come from the research survey and gap analysis in [kdb-distributed-self-healing-research.md](kdb-distributed-self-healing-research.md). That document carries the rationale, the citations and the full work items. The phases are listed here so the plan shows the sequence.

| Phase | Scope | Depends on |
|---|---|---|
| 10.5 | Resolution policies and an application resolver authority:<ul><li>`DocumentConflict` gains base + origins</li><li>replicated per-namespace/collection resolution chains (`source-priority`, `validity`, `field-merge`, `last-write`, `queue` / `authority`)</li><li>a `ConflictResolve` permission, conflict stream/webhook, `kdb:resolve/1` resolution commits, `hold`/`provisional` pending modes</li><li>operator bulk take-theirs/ours</li></ul> | 5 (`_kdb/meta`), 9 (home = resolver node) |
| 11 | Merge unrelated histories: opt-in `AllowUnrelated`, with the empty tree as the base | 10.5 |
| 12 | `TREE_NODES` / `OBJECT_FETCH` frames; trie `NodeHash`/`Children`/`Proof` | — |
| 13 | Scrub and self-repair from peers in the maintenance scheduler; `integrity.Repair` fetches missing commits from peers | 12 |
| 14 | Edge lazy fill: projection read-through with inclusion proofs, bounded hoard, filter widening by tree diff, deepen | 12 |
| 15 | Gated on measurement: Bloom/RIBLT negotiation, φ-accrual suspicion, changed-docID Bloom per commit, session head tokens | — |

### Phase 10.5, slice 1 — landed (resolution chains)

**Landed:**
- **Chains:** `peersync/resolution_chain.go` adds the `source-priority`, `validity`, `field-merge`, `last-write` and `queue` rules. Each is evaluated per document in canonical parent order, so it is symmetric.
- **Order of decision:** `resolveConflicting` now tries `Choose`, then the chain, then the policy, **per document**. Before, `Choose` was all-or-nothing. An operator's choice for one document now stands while the chain settles the rest, and a report names only what is still undecided.
- **Base:** it comes from `CommonAncestor(p0, p1)` in canonical order. With a criss-cross, the local/incoming order would let two nodes pick different bases.
- **Resolver input:** `transaction.DocumentConflict` gains `ExistingOrigin`/`IncomingOrigin` ({node, commit, timestamp}), and `BaseDoc` is now filled.
- **Replication:** the chain is replicated as `_kdb/meta` kind `resolution` (`MetaStore.SetResolution`), with `GET`/`PUT /v1/ns/{ns}/resolution`.
- **Hash check:** `NamespaceRefs.ResolutionHash` travels in the hello ack. The client compares hashes, and on a mismatch it pulls with strict resolution and pushes `main` only as a fast-forward. So neither side builds a merge under a chain the other lacks. The Go-only v2 frames mean there's no Kotlin counterpart.

**Superseded by slice 2 below.** Still not built: node labels for `source-priority`, and a chain dry-run preview over the queued conflicts.

**Known limit:** the chain hash doesn't cover the schema that `validity` checks against. Two nodes that have received different schema versions can judge differently until the schema has replicated.

### Phase 10.5, slice 2 — landed (resolver authority)

**The rule.** A terminal `authority` rule with three settings:
- `pending`: `hold` (queue unmerged) or `provisional` (merge by last write now).
- `node`: the one node that notifies the authority.
- `timeout`: after it, the fallback becomes final.

**Every node learns of a provisional decision from the merge itself.** Its message is `kdb:merge/1 provisional=<ids>`, part of the deterministic merge content. Every node that adopts the merge records the same `provisional` entry, with its id derived from the merge. So it doesn't matter whether the notifying node made the merge or fetched it.

**Closing entries everywhere.** The authority's decision is an ordinary commit with message `kdb:resolve/1 <id>`, and every node that adopts it closes that entry. A held divergence is instead **swept** once `main` contains its incoming side. That covers a settlement arriving from a different peer than the one the divergence was recorded against, which the per-peer clear cannot see.

**Delivery.**
- A signed webhook (`ConflictWebhook`), or poll-with-ack.
- The queue keeps a `delivered` flag across unchanged re-sightings and an `OnRecord` hook.
- Delivery is at least once, keyed by the conflict id.

**Permission.** A new RBAC kind, `resolve` (`auth.ConflictResolveAction`). It applies only to namespaces whose chain names an authority; other namespaces keep the commit-rights check.

**v1.** The host is strict whenever the namespace has a chain, and the v1 client pushes only fast-forwards (`FastForwardPushOnly`).

**Surfaces:**
- control plane: `?authority`, `?undelivered` and `?doc` filters; `ack`; `resolve-all` with dry run; 403 and 409 mapped;
- CLI: `resolve --all`, `resolution`, and richer `conflicts`. The CLI reads the chain from `_kdb/meta`;
- `kdb-service` flags: `--conflict-webhook*`, plus a 30s expiry loop.

**Tests:**
- peersync scenarios;
- server end-to-end tests over real sync (hold, provisional overrule/confirm, stale refusal, permissions, bulk, poll/ack, webhook retry and signature, notifying-node filter, v1, timeouts);
- control-plane tests;
- a CLI scenario;
- a Python e2e test against two `kdb-service` processes with a real webhook receiver;
- Kotlin interop: the Kotlin CLI replays Go's provisional-merge and resolve history to identical documents, and grants are tested case for case in both trees.

**Traps found:**
- `codec.UUID` and `codec.Hash` have no JSON marshalers, so `transaction.ConflictOrigin` needed its own. Without it, origins went over the API and into the durable queue as raw structs.
- A test mutating a recorded entry's `Items` slice aliased the queue's copy: the queue stores caller slices.
- Go↔Kotlin *peer sync* is broken independently of this work: v2 hello isn't decodable by Kotlin, and the CommitPushAck frames mismatch. The interop surface that works, and is tested, is Kotlin reading Go-written data.

**Not built:** node labels for `source-priority` (would need label assignments covered by the chain hash to stay deterministic), and an embedded-Go convenience beyond `ConflictQueue.OnRecord` plus the async API.

### Phases 12–13 — landed (anti-entropy frames, scrub and self-repair)

**Merkle views.** `DocumentTree.Node(prefix, limit)` gives the trie's subtree hash, 16 child hashes and small-subtree entries. Compressed leaves are folded, so the hashes are exactly the tree hash's own. `document.DiffTrees` finds the documents two node sources hold differently, one batched round trip per level.

**Wire (Go-only).** `TREE_NODES`, `OBJECT_FETCH` and their results (0x34–0x37), behind a `repair` capability. `peersync.RepairSession` provides `Nodes`, `Diff` and `Fetch`. Fetch returns only bodies that hash to what was asked for.

**Scrub.** `server.Scrub(BodyFetcher)` walks the head's tree, verifies every body, fetches damaged ones through `Replicator.FetchBodies` (the peers, in order), and rewrites them in one `kdb:repair/1` commit. The tree is unchanged. What can't be repaired becomes a `damaged` queue entry.

**Surfaces:**
- service: `--scrub-interval`
- control: `POST /v1/ns/{ns}/scrub`, `GET /v1/ns/{ns}/peers/{peer}/diff`
- CLI: `kdb scrub`, `kdb peer-diff`

**A durability bug found by the scrub tests.** A byte flipped in the middle of a delta segment made the next open **silently roll the namespace back** past the damage, and the next close checkpointed the loss. The chain of steps:
1. The segment's listing stops at the damaged frame, so the checkpoint looked stale.
2. The full replay treated the newest-segment damage as a torn tail and stopped there.

The fix, in `checkpointMatchesLog`: damage in place is told apart from a replaced or truncated log using the physical size and the last intact commit, read past the damage with `delta.ScanSkippingCorrupt` / `storage.DamageTolerantReader`. The checkpoint is then kept. The cold loader also indexes past damaged frames, so damage costs only its own frame's bodies.

**Kotlin had the same bug.** Kotlin replay now refuses newest-segment damage that intact frames follow (`CorruptFrameException.intactFramesAfter`).

**Tests:**
- trie diff against a brute-force diff;
- frame round trips;
- real on-disk corruption repaired from a peer and durable across restarts;
- a forged body refused;
- CLI and control plane;
- a Python e2e with a real service repairing itself on a schedule;
- Kotlin: a scanner unit test, and interop tests that Kotlin refuses a damaged newest segment rather than rolling back, and that on a Go-repaired log it serves exact values or refuses.

**Not built:**
- live-tree-vs-declared-root checking (the engine resolves trees by hash, so a mismatch has no reachable cause today);
- repair of damaged *historical* versions (only the live tree is scrubbed);
- Kotlin opening from checkpoints, which is what would let it read a Go-repaired log.

### Phase 14 — landed in part (projection read-through)

**Landed:**
- **Proofs:** `DocumentTree.Proof` and `VerifyTreeProof`, Merkle inclusion and absence proofs of about ceil(log16 n) × 16 × 32 bytes.
- **Wire:** `DOC_FETCH`/`DOC_FETCH_RESULT` (0x38/0x39), requiring read rights only.
- **Verifying client:** `RepairSession.FetchDocs`, whose `verifyDocFetch` refuses the whole answer on any inconsistency.
- **`server.ReadThrough`:** a bounded LRU hoard. An entry is valid only at the source position it was read at.
- **Client reads:** `KdbServerRuntime.ReadDocument` for client point reads, and a `readthrough=true` peer option.

**Deliberately not built, and why:**
- **Filter widening by tree diff.** A projection's namespace is keyed by its filter (`ProjectionNamespace`), so a wider filter is a *new* projection and must start from a snapshot anyway. What a diff could save is the case where the source cannot relate the projection's position to its head. There, a Merkle diff of a *filtered subset* against the whole source tree would name every source document outside the filter as a difference, which is not cheaper than the reset.
- **Deepen (fetching history below a shallow root).** Valuable, because it would also let a snapshot-rooted node sync with independent peers again (Phase 11), provided the snapshot's source still holds the history. But the deepened commits have to be persisted without the delta-log replay treating them as adopted, and the shallow-root markers (`meta.json`) have to be retired in step with the checkpoint. That is its own piece of durability work.

### Phase 14, deepen — landed

**`peersync.Deepen` / `DeepenAll`** fetch a shallow root's ancestors (`FETCH_REQUEST`, paged, parents first), verify and store them, and unshallow the root. Commits at the peer's own horizon become the new shallow roots.

**Durability, in order:**
1. The marker names the new horizons.
2. The fetched commits and the root are logged and waited on.
3. `dag.Unshallow` runs, invalidating generations.
4. The marker is rewritten without the root.

**Engine changes:**
- Replay admits a marker-named root without its parents.
- `meta.json` gains `bodiesExternal`, set once and never cleared: snapshot bodies are only in the blob store.
- Open invalidates generations for deepened namespaces, since history arrives after its descendants.

**Surfaces:**
- the `deepen=true` peer option;
- `POST /v1/ns/{ns}/deepen`;
- `kdb deepen`.

**It partly unblocks Phase 11.** A snapshot-rooted node that deepens from a peer holding the history syncs with independent nodes again. The graft, which needs a durable non-live tree, is still needed only when no reachable peer holds that history.

**Tests:**
- the unrelated-history case resolved;
- durability across restart;
- a partial deepen surviving a replay and then finished from a full node;
- an interrupted deepen finished by rerunning it;
- the replicator option, the CLI, and a three-service Python e2e with a kill -9.

### Phase 15 — measured, then built what the measurements justified

Measurements are in `go/kdb/server/measure_phase15_test.go` (`KDB_MEASURE=1`).

**M1: have/want overshoot** (2,000 shared commits, each side diverged by d):

| Divergence d | Before (exponential haves) | After (peer's last-known head as a have) |
|---|---|---|
| 1 | 0 | 0 |
| 10 | 6 | 0 |
| 100 | 28 | 0 |
| 1,000 | 24 | 0 |
| 5,000 | 2,001 (the whole shared history) | 0 |

The overshoot never exceeded about d, so Bloom/IBLT reconciliation was not justified. The fix is the replicator's recorded `RemoteMain` (the peer's main as of the last clean sync), sent as an extra have. It pins the merge base exactly.

**M2: projection delta noise** (5,000 documents, 2,000 updates):

| Filter matches | Before | After |
|---|---|---|
| 1% | 15 writes, **1,634 deletes** (99% of entries were deletes of documents never held) | 15 writes, 0 deletes |
| 10% | — | 177 writes, 0 deletes |
| 50% | — | 815 writes, 0 deletes |

The fix is Cimbiosys-style move-out: the source sends a delete only for a document that matched the filter at the replica's position. It checks the filter against the document's value at `from`, ignoring read rights, so a lost read right still deletes. Exact, with no protocol change.

**Changed-document-id Bloom filter per commit: not built.** Projection filters are content predicates, so knowing which ids a commit touched cannot tell whether it is relevant. The premise does not hold.

**φ-accrual: built as health ordering.** The replicator observes sync outcomes, not heartbeats. Repair requests (`FetchBodies`) now ask peers in health order: fewest consecutive failures, then most recent success. A dead first peer no longer costs every repair its timeout.

**Session tokens: built.** `DocumentGet.MinCommit` (wire, Go-only), `KdbServerRuntime.AwaitCommit` (bounded wait, then `ReplicaBehindError` mapped to BUSY with a retry-after), and `client.Session` with a portable `Token`/`Resume`. Read-your-writes and monotonic reads across replicas are tested end to end.

**Also:** `NamespaceSyncResult.Received` counts every commit a pull received, so overshoot is observable.

### Phase 16.1–16.2 — landed (syncnode, live namespace sets, peer sync over HTTP)

**16.1 (G1, G5), PR #88:**
- `go/kdb/syncnode` owns everything `service.go` assembled inline: the meta runtime and `MetaStore`, authority expiry, the webhook, the retention floor, commit notification, projection peers, the replicator, scrub and listeners.
- `Prepare`, `Opener`, `Start`, `StopSync` and `Close` handle the lifecycle.
- `kdb-service` is its first caller, with no behaviour change: Python e2e 44 + 8 passed.
- `Replicator.SetPeerNamespaces`, `AddNamespaces` and `RemoveNamespaces` take effect on the next cycle and keep per-namespace progress.

**16.2 (G2):**
- `ws.Upgrade` turns a request on an application's HTTP server into a KDB WebSocket connection, with the listener's own RFC 6455 validation.
- `server.ServePeerSyncConnection` serves peer sync on any accepted connection with its `ConnectionContext`.
- A v2 hello without credentials authenticates from that context.
- `Node.Handler()` maps `Authorization: Bearer|Basic` to credentials.
- Client side:
  - the replicator dials `ws://` / `wss://` addresses over WebSocket;
  - `PeerConfig.Token` (`token-env=`) rides the upgrade as a bearer header and in the hello;
  - `PeerConfig.Credentials` refreshes per connection.
- `kdb-service --peer-http`.

**Tests:**
- `syncnode`: sync through an `httptest` server beside an API route; a missing token refused; a token only on the upgrade header; a plain GET answered with 400.
- `test_peer_http.py`: processes, with the phone killed by `kill -9`.

### Phase 16.3 — landed (pull versus push authorization, G3)

- **Actions:** `auth.PeerPullAction` and `auth.PeerPushAction`.
  - `AuthorizePeer` and `PeerDirections` treat an allowed `PeerSyncAction` as both directions, so every existing engine is unchanged. A directional engine denies it and answers the new actions.
  - Registry kinds are `sync_pull` and `sync_push` (underscores, because the Kotlin GRANT parser reads the kind as an identifier). `sync` still grants both.
- **v2 host:**
  - Hello grants each namespace in either direction and records its access in `NamespaceRefs.Access` (`pull` or `push`, JSON `access`, omitted when both). The client skips the other direction instead of failing.
  - Every frame re-authorizes its direction: fetch, snapshot, tree and object reads need pull; `REF_UPDATE` and `GRAFT_PUSH` need push; refs need either.
- **v1 host:** `COMMIT_FETCH` needs pull and `COMMIT_PUSH` needs push. The handshake needs either.
- **Optional per-document check:** `AuthorizeDocuments` (`syncnode.Config.AuthorizePushedDocuments`).

**Tests:** grant semantics; a phone's tampered write to its read-only namespace never reaches the cloud while its own namespace still pushes; a locked document refuses its push.

### Phase 16.4 — landed (gated, preconditioned whole-document write, G7)

`KdbServerRuntime.PutJSON(ns, id, body, *Expect, principal)` replaces a document with Delete+Write in one transaction, through the write gate.
- `Expect{ContentHash}` or `Expect{Absent}` is checked inside the gate, at the live head. A failure is `*PreconditionFailedError` carrying the current hash.
- The transaction is then re-based on the live head, so a merge that touched other documents is not a conflict.
- `ReadForUpdate` returns a document together with its content hash.

**Tests:** replace semantics; stale, fresh and absent expectations; 8×10 concurrent read-modify-writes with retry and no lost updates; a peer merge between read and write is seen, while an unrelated one does not get in the way.

### Phase 16.5 — landed (definitions by pattern, scoped meta views, G4)

**Patterns:**
- A definition's `Namespace` may be a pattern (`*` one segment, `**` any number). Pattern documents carry `"v": 2` (`MetaFormatPatterns`). A node skips definitions newer than it understands; one from before patterns never applies them, because no namespace is named `*`.
- Resolution (`metaIndex.effective`): a namespace's own definition per key (kind and name), then the most specific matching pattern. Specificity is more literal segments, then fewer `**`, then name order.
- Applied-state tracking is per document per namespace.
- `SetResolution` and `AssignHome` accept patterns (a pattern home has no handover point). `ReadResolutionChain` and `Placement` resolve them too.

**Scoped views:**
- `MetaStore.View(canSee)` returns every pattern plus the definitions of the namespaces `canSee` admits.
- `META_VIEW` (0x3C/0x3D, capability `metaview`) serves it for the session's granted namespaces.
- `V2ClientConfig.MetaView` asks for it right after hello, before any namespace syncs.
- `PeerConfig.ScopedMeta` (`meta=scoped`): the replicator always excludes the meta namespace for that peer, whatever its patterns, and `syncnode` adopts the view (`AdoptView`: same ids and bodies, one commit, changed documents only).

**Tests:**
- specificity, overrides, namespaces opened later, pattern homes, validation;
- the view contains nothing of another user's;
- a scoped phone gets only the pattern, its chain hash matches the cloud's, and a divergent write merges.

### Phase 16.6 — landed (many namespaces: measured, then idle close, G6)

**Measured** (`embed/measure_many_namespaces_test.go`, `KDB_MEASURE=1`, file host, macOS/APFS). Each namespace holds one small document:

| | 1,000 | 2,000 | 5,000 |
|---|---|---|---|
| open + first write | 12.9 ms | 14.2 ms | 16.3 ms |
| heap while open | 43.6 KB | 41.8 KB | 39.3 KB |
| RSS while open | 75.5 KB | 77.9 KB | 77.6 KB |
| file descriptors while open | 2 | 2 | 2 |
| disk (allocated) | 8.0 KB | — | — |
| close | 17.6 ms | 18.3 ms | 19.3 ms |
| cold reopen | 8.0 ms | 7.6 ms | 7.6 ms |

Costs per namespace are flat with count, so 500k extrapolates linearly:
- **All open:** about 20 GB of heap and 38 GB of RSS. Not viable, so idle close is required.
- **Cap of 20k open:** about 0.8 GB of heap.
- **Disk:** about 4 GB.

The open and close times are fsync-bound on macOS.

**Found and fixed on the way: a descriptor leak.**
- The host's byte store is shared by every namespace and kept a file handle per segment until the whole host closed. Closing 300 namespaces left 900 descriptors open, so a process that opens and closes namespaces over time would have run out.
- Fix: `Host.CloseNamespace` now releases them (`storio.HandleReleaser`, implemented by `OSByteStore`, `PrimaryWithReplicas` and `FileBackedPlatformIO`). Descriptors after close: 0.
- What remains after close is fixed-size (zstd encoder pools, runtime state) plus map capacity from the peak number of open namespaces: nothing per closed namespace.

**Idle close:**
- `NamespaceSet.StartIdleClose(IdlePolicy{MaxOpen, IdleAfter, MinIdle, Pinned, Close})` / `CloseIdle(policy, now)`.
  - Least recently used first, never under `MinIdle`, never while busy (a write queued or a group publishing).
  - The set stays aware of closed namespaces (`KnownNamespaces`), so peers are still served them, and `Resolve` reopens through the opener.
  - `MetaStore.Forget(ns)` on close means a reopened runtime gets its definitions applied afresh.
- `syncnode`:
  - `Config.Idle` (a host is required) never closes the primary.
  - At `Open` it seeds known namespaces from disk.
  - `Node.CloseIdle(now)` runs a sweep on demand.

**Gate:** per-namespace layout with idle close is viable, so the archive-folding fallback is not needed at these costs.

**Tests:** LRU over the cap with pinning; age; busy left open; reopen on resolve; a file-backed cloud that closes both user namespaces, serves one to a phone by reopening it with its chain, and after a restart knows the namespaces on disk without opening them.

### Phase 16.7 — landed (application-driven handover, G8)

- **`HOME_REQUEST` (0x3E/0x3F, capability `home`):** the would-be home asks over the sync connection. The host requires a push grant, and the request must be for the session's own node.
  - The current home asks `server.HandoverPolicy` (`syncnode.Config.HandoverPolicy`; nil refuses everything). The policy sees who asks, their principal, the reason and the assignment being replaced.
  - On yes the home calls `AssignHome`, which bumps the fence and sets the handover point.
  - A node that isn't the home refuses and names the home.
- **Forced handover:** allowed only for the node the namespace's chain names as its authority (`Authority().Node`).
- **API:**
  - `syncnode.Node.Handover` and `ForceHandover` are in-process.
  - `RequestHome` asks a peer. On a grant it syncs and reconciles definitions synchronously, so the caller can write when it returns. (Definitions arriving by sync are otherwise applied in the background, which a first version of the test caught.)

**Tests:**
- the phone is refused writes while the cloud is home;
- the policy's refusal reaches the phone; a grant lets the phone write while the cloud is refused;
- a second phone is told who the home is, then force-requests from the authority;
- the dark phone's offline write is refused by the fence when it returns;
- a request for another node, or to a node without a policy, is refused.

### Phase 16.8 — landed (gomobile packaging, G9)

- **`go/mobile/kdbsync`** is a gomobile-shaped facade: strings, bools and errors only. It wraps a file host plus `syncnode`:
  - `Open`, `AddPeer` (a refreshable token via `SetToken`; `meta=scoped`), `Start`
  - `AddNamespaces` / `RemoveNamespaces`, `SyncNow`
  - `Put` (the gated `PutJSON`), `Get`
  - `RequestHome`, `Close`

  It is tested end to end against a cloud serving `Node.Handler()` over WebSocket.
- **`syncnode.Open` now adopts the data root's identity** (its `NODE` file) when the primary still has the process default, as `kdb-service` does. Two hosts in one process, such as a device and the cloud in a test, are then two nodes.
- **CI:**
  - `mobile-android` (ubuntu, NDK): `gomobile bind -target=android/arm64,android/amd64`
  - `mobile-ios` (macOS): `-target=ios`
  - Sizes go to the job summary.
- **Sizes measured locally:** Android arm64 `libgojni.so` is 27.4 MB (the `.aar` is 9.3 MB compressed); the iOS arm64 static framework is 35 MB (before linking into the app).

**Finding:** binding `go/kdb/embed` directly, as the gap report assumed still worked, fails on main.
- Some exported functions return three values (`NamespaceMarker`).
- `GroupParticipant.Lock` returns a function (since f049228, the cross-namespace commit protocol).

gomobile can bind neither. Mobile apps should bind a facade: `kdbsync`, or their own over `syncnode`. Reshaping `embed`'s core API for the binding tool was not done.

### Phase 11 — landed (graft: merging unrelated histories)

Opt-in per namespace: `ResolutionChain.AllowUnrelated` (json `allowUnrelated`, part of the chain's hash, so two nodes that disagree never merge).

- **Engine:** `storage.ForeignTreeStore.StoreForeignTree(ns, tree, bodies)`. Every body is checked against the tree's content hashes. Bodies and one full tree object go to the blob store, which is flushed and synced, then the tree is cached. The live tree is untouched. Refused (`ErrForeignTreeNeedsObjects`) unless the history strategy is `objects`. Also implemented for the memory adapter and the multiplex.
- **Graft (`peersync.Graft`):** fetches a root's state page by page (`SNAPSHOT_FETCH AtHex=root`), checks the commit hash and rebuilds the tree to its declared hash, then commits in durability order:
  1. store the tree;
  2. write `meta.json` `graftRoots`;
  3. `PutShallowCommit`;
  4. log the root.

  The pull grafts when a page fails on one of the peer's shallow roots (`unsharedRoot`) and the chain allows it, then stores the page again.
- **Push side:** before pushing a ref, the client probes each of its own shallow roots under it (a `FETCH_REQUEST` for the root and its parents; any commit back means the peer can store it). If the peer lacks it, the client sends `GRAFT_PUSH` (0x3A/0x3B, capability `graft`), which the host stages and grafts on the last page. Without `allowUnrelated`, the ref is skipped with a reason instead of failing on the peer's missing-parent error.
- **Merge:** with no common ancestor and `allowUnrelated`, `resolveDivergedLocked` merges with an empty base. Candidates are every op of both sides' histories plus every document in any shallow root's tree they reach. The base commit is recorded as the zero hash. Genesis is fixed per namespace, so independent databases always share an ancestor; histories are unrelated only through a shallow root.
- **Replay:** grafted roots are side history. A replay of the log with no checkpoint rebuilds the namespace; it admits `graftRoots` shallow (`applyCommitsTopologically`), and open marks them shallow. `RecordShallowRoots` (deepen) keeps each root's kind.

**Tests:**
- peersync: both directions converge; the unrelated merge is identical on both nodes; refused without the flag; a tampered state is rejected; push skip; push graft.
- server (file-backed): restart, replay with no checkpoint, a push graft to a host that is only dialled, and a refusal under `history=none`.
- wire round trip.
- Python e2e `test_graft.py`: B stopped, C grafts, `kill -9` of C.
- Kotlin interop `TestKotlinReadsAGoNamespaceThatGraftedAnUnrelatedHistory`: Kotlin refuses loudly.

**Not done:**
- Kotlin replay of grafted (and snapshot-rooted) namespaces. That needs shallow roots and Go's rule that only commits extending main are applied.
- Candidates miss documents written only in pruned (stubbed) history.

