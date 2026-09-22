# Distributed KDB — Gap Analysis and Implementation Plan

**Status:** design. Nothing in this document is implemented yet.
**Written:** 2026-09-21, against `main` at `0563595`.
**Scope:** the Go engine (`go/kdb/...`), which is what we deploy. Kotlin parity is noted where a wire or format change forces it.
**Question this answers:** what does KDB still need so that several instances can sync with each other, and so that an instance can hold only the part of the data it needs?

---

## 0. Summary

KDB already has most of what a distributed database needs. Content-addressed commits, a commit DAG, verifiable Merkle document trees, a two-parent merge primitive, conflict policies, and a working pairwise sync protocol are all built. It is structurally close to git, and git is a working distributed database for files.

What's missing is mostly the layer on top of those pieces:

1. **No node starts a sync.** `kdb-service` accepts inbound peer sync (`--peer-addr`) but has no outbound client, no peer list and no replication loop. Two services today converge only through an outside driver (`kdb-e2e-helper relay`).
2. **Merges don't converge.** Two nodes that resolve the same divergence produce *different* merge commits, because of a random tx id, a random author, `TimestampNow()`, and parents ordered local-first (`peersync/conflict_detection.go:317-335`). In a mesh this never settles.
3. **The protocol is a single-namespace, `main`-only, 100-commit, one-shot exchange.** It has no paging, no negotiation, no branch or tag replication, no resume, and no fallback when history was truncated.
4. **Nodes have no identity.** `AuthorNodeID` is a random UUID per write, and the host hard-codes `"kdb-service-go"`. There's no stable node id, no clock that orders events across nodes, and no membership.
5. **Partial replication only works at namespace granularity, and only by accident.** Within a namespace, every commit hashes the root of a tree over *all* documents, so a subset replica can't verify or replay commits. Document ids are random, so an id-prefix range selects a random shard, not a meaningful subset.
6. **Ingesting from a peer bypasses the local write path.** Indexes, the unique-key registry, stream subscribers and schema never see commits that arrive by sync.

The plan, in order: harden pairwise sync (Phase 0). Make convergence deterministic (Phase 1). Build a proper sync protocol v2 (Phase 2). Put a replicator into `kdb-service` (Phase 3). Add snapshot bootstrap (Phase 4). Replicate metadata (Phase 5). Then take partial replication in three steps of increasing cost: namespace sync sets (Phase 6), filtered projection replicas (Phase 7), and optionally a verifiable partial clone that needs a commit format change (Phase 8). Ownership for strong consistency (Phase 9) and cross-node placement (Phase 10) come last and are opt-in.

The one decision that shapes everything: **the default model stays multi-leader and eventually consistent, as the spec intends** (`kdb-spec.md` §1.3, §8.2: "Peers are equal"). Strong consistency is an opt-in per namespace, through ownership, not consensus.

---

## 1. Critical concepts

These are the ideas any distributed KDB has to get right. Each one is stated against what KDB has today.

### 1.1 Replication unit

This is the smallest piece of data that can be replicated on its own. In KDB it is the **namespace**: each namespace has its own DAG, its own tree and its own storage engine (`storage/engine/multiplex.go:22-26`). A namespace can be replicated whole or not at all, because every commit names a root hash over all of its documents (`document/kdb_commit.go:21-55`).

**Consequence:** "take only the part you need" is cheap along namespace lines and expensive across them. The cheapest correct partitioning strategy is to *model partitions as namespaces*: `tenant/acme`, `site/berlin/orders` and so on. Cross-namespace transactions (PRs #67–#74) already make that model workable for writes that span partitions.

### 1.2 Identity and verification

Content addressing means a receiver never has to trust a sender about content:

- `PutCommit` recomputes the commit hash (`dag/in_memory_commit_dag.go:351-384`).
- `MaterializeCommit` rebuilds the tree and rejects a mismatch (`embed/materialize.go:58-64`).

This is KDB's strongest distributed property, and every design choice below keeps it. The one exception is a filtered replica (§1.8), where it can't hold.

**Node identity** is a separate concept and it's missing. A stable per-node id is needed for:

- authorship
- per-peer watermarks
- membership
- ownership leases
- HLC tie-breaks

### 1.3 Causality and ordering

The DAG already records causality exactly: a commit happens-after its parents. So there's no need for vector clocks. The DAG *is* the vector clock, the same way it is in git.

What's missing is a **cross-node total order for tie-breaking**. `LastWrite` resolution compares wall-clock timestamps (`conflict_detection.go:266-279`), so a node with a fast clock always wins. A **hybrid logical clock** fixes this. It can live inside the existing `Timestamp` microsecond field, with no format change (§Phase 1).

### 1.4 Convergence

A multi-leader system is correct only if every replica that has seen the same set of commits ends up in the same state. That requires:

- **Deterministic resolution.** The same inputs give the same winner. This is already true for LWW, via the hash tie-break.
- **Deterministic merge commits.** The same pair of heads gives a byte-identical merge commit. This is **not true today**, and it's the most important correctness gap in this document. Without it, A merges (A,B)→M1, B merges (A,B)→M2, and they sync and merge (M1,M2)→M3 and M4. That repeats forever.
- **Commutative application.** Replaying the merge from either parent gives the same tree. The tree check in `MaterializeCommit` already enforces this.

### 1.5 Conflict semantics

Conflicts are detected per document; there's no field-level merge and no CRDTs (`conflict_detection.go:340-345`). The policies behave as follows:

- **Strict:** surfaces a `ConflictReport` and leaves the head unmoved.
- **LastWrite:** picks a winner deterministically.
- **Custom:** calls a resolver.
- **AppendOnly:** only merges cleanly, because its writes are new documents.

This is the right model, and the spec's principle is "conflicts surface to the application". Two things are missing:

- A **durable conflict queue**. A Strict conflict today lives only in the response to one sync call. The next sync recomputes it, but nothing records it, exposes it or lets an operator resolve it.
- A **resolution commit** that an application can write to settle a reported conflict. Mechanically this is a merge commit with the chosen bodies.

### 1.6 Anti-entropy and the sync protocol

Two replicas have to find out what each is missing, cheaply, and transfer it resumably. Git's approach is to advertise refs, negotiate have/want, send what's missing, and update refs. It fits KDB directly, because KDB's objects are git-shaped.

Today's protocol sends `CommitFetch(since=myHead, max=100)`. That breaks as soon as the heads have diverged by more than 100 commits (§2.2).

### 1.7 History horizon (shallow replicas)

Under `history=none`, commits below the retention floor are deleted (`dag/checkpoint.go:55-94`). A peer that is further behind than the floor can't catch up by replaying commits. It needs a **snapshot**: the tree at the floor plus the live bodies. It must then be able to treat that commit as a **shallow root** whose parents are known to be absent.

`SnapshotRequest`/`SnapshotResponse` (0x0A/0x0B) are defined on the wire and not implemented. Retention must also respect peers: a namespace shouldn't truncate history that a known, still-active peer hasn't received yet. That is spec §6.4's `CompactionIntent`, which nothing sends.

### 1.8 Partial replication

This covers any replica holding less than a whole namespace. There are three distinct designs, which differ in what they trade away:

| Design | Holds | Verifiable | Can write | Format change |
|---|---|---|---|---|
| **Namespace sync set** | chosen namespaces, whole | yes | yes, full peer | none |
| **Filtered projection** | the documents that match a predicate, as its *own* locally authored history | no, trusts the source | only via write-back to the source | none |
| **Partial clone** (lazy bodies) | full commit graph and tree hashes, and only the bodies it has fetched | yes | yes | commit v2, cross-language |

The partial clone looks the most elegant, but it saves less than it seems to. The live tree costs about 5 KB per document *regardless of body size* (see the trie-memory measurement). So a partial clone of 200k small documents still carries most of a full replica's memory. It only pays off for large bodies and attachments. That's why it's last, and gated.

### 1.9 Consistency model

- **Default:** asynchronous multi-leader, eventually consistent. Every node accepts writes, and divergence is merged or reported.
- **Guarantees that are node-local under multi-leader, and must be documented as such:**
  - **Unique constraints** (`transaction/unique_registry.go`). Two nodes can each accept `email=x` for different documents, and sync then surfaces a conflict only if they're the *same* document id. A derived id from a natural key (`codec/derived_uuid.go`) turns that into a same-document conflict, which is the recommended pattern.
  - **CAS preconditions and leases** hold only against the local node.
- **Opt-in:** single-home ownership per namespace (§Phase 9). One node holds a fenced write lease and the others forward writes to it. That restores global uniqueness and CAS for that namespace without a consensus protocol in the data path.

### 1.10 Topology and membership

KDB's spec says peer discovery is the application's job (§1.2). This plan keeps that and adds:

- **Static peer configuration**
- **A per-peer replication state machine**
- **Persisted watermarks**

Topology then follows from configuration:

- hub-and-spoke (edge ↔ cloud)
- mesh (multi-site)
- chain (edge → region → cloud)

Gossip and mDNS are optional later work.

### 1.11 Security across nodes

- **Node-to-node authentication:** mTLS with node certificates, in addition to today's user/password/token handshake.
- **Authorization:** `PeerSyncAction` exists per namespace (`peersync/host.go:125-163`). Per-document RBAC (`auth/types.go:56-78`) is ignored by sync. That's fine for full peers, but it's exactly what filtered projections must enforce.
- **Known hole:** the stream handshake is unauthenticated. Anyone can subscribe and read every delta, and write-back replays run as an anonymous principal (`server/stream_listen.go:288-303`). Fix this in Phase 0.
- **Encryption at rest** (Layer 14, design only) is per-namespace DEK. Commit-level sync re-encrypts locally, so it's unaffected. Raw segment replication would need a shared DEK. That's one reason this plan replicates commits, not segments.

---

### 1.12 Self-healing and merging foreign state

This section was added after Phases 0–10 landed. It is the outcome of a research survey; see [kdb-distributed-self-healing-research.md](kdb-distributed-self-healing-research.md) for sources.

- **The trie is already a Merkle tree.** Exposing its interior hashes enables tree-diff anti-entropy, scrub-and-repair by content hash from any peer, and inclusion proofs.
- **Phase 8 reframed.** An edge that fetches documents outside its projection *with an inclusion proof* (about 2.4 KB at 1M documents) gets verified reads without holding the full trie. The space objection to partial clone does not apply to it.
- **Merging a database with no shared history** needs an empty-tree base and application-driven resolution. Merge-time rules must be deterministic and replicated. Business-logic resolution happens *after* the merge, as an ordinary commit by a resolver authority.

## 2. What exists — audit

### 2.1 Inventory

| Piece | State | Where |
|---|---|---|
| Content-addressed commit with parents, ops (full bodies), tree root, schema hash | done, verified on receipt | `document/kdb_commit.go:8-100` |
| Merkle trie over doc ids, canonical, order-independent | done; interior hashes are memory-only and not exposed | `document/document_tree_trie.go` |
| Multi-parent commits, `AppendMergeCommit` (CAS on head), `CommonAncestor`, `IsAncestor` with generation pruning | done | `dag/ancestry.go` |
| Pairwise peer sync: handshake, fetch, push, divergence classification, auto-merge of disjoint doc sets, LWW/Custom/Strict | done, `main` only, single namespace | `peersync/` |
| Peer sync listener in `kdb-service` (TCP, TLS, RBAC) | inbound only | `server/peersync_listen.go`, `service/service.go:454` |
| Outbound sync / replicator | **absent** | only `cmd/kdb-e2e-helper` relay |
| Stream fan-out, Mode 1/2 | server side only; no resume backfill; unauthenticated; one namespace, no filter | `server/stream_listen.go`, `stream/` |
| S3 replica / archive of sealed segments | done; a backup mechanism, not a replication one — the open segment is never shipped and no follower reads it | `storage/io/primary_replicas.go` |
| Same-disk read-only attach | done (unix); `Refresh()` has no production caller | `embed/readonly*`, `embed/runtime.go:174` |
| Stable node id | **absent** (`HOST` file exists only for cross-namespace txns) | `embed/txn_decision_log.go:309-338` |
| HLC / cross-node clock | **absent** (wall-clock µs only) | `codec/primitives.go:107` |
| Snapshot transfer, DagDiff, SchemaPush, CompactionNotice | wire structs only | `wire/types.go:30-90` |
| Membership, leader election, consensus, placement, routing | **absent** | — |

### 2.2 Defects in today's pairwise sync (Phase 0 targets)

These are real bugs in shipped code, found by reading it. Each one gets a reproducing test before its fix.

| # | Defect | Where | Effect |
|---|---|---|---|
| D1 | `fetchCommits` walks newest-first and cuts at 100; the receiver gets the newest 100 | `peersync/host.go:290-310` | more than 100 missing commits → "missing parent", sync can never complete |
| D2 | Fast-forward is an unconditional `SetHead`, not serialized with `writeGate` | `conflict_detection.go:149` | a local commit that lands between the head read and `SetHead` is dropped from `main` |
| D3 | Materialization mutates live storage outside the write gate | `host.go`, `embed/materialize.go` | readers can see a half-applied commit; this races local writers |
| D4 | Merge commits are non-deterministic (random tx and author, `TimestampNow`, local-first parents) | `conflict_detection.go:317-335` | a mesh never converges (§1.4) |
| D5 | Host ignores `m.Namespace` and serves its single DAG | `host.go` | a peer syncing namespace X can write into namespace Y |
| D6 | Materialize errors swallowed on the host | `host.go:267` | the DAG head moves while storage is behind; the replica silently corrupts |
| D7 | Unknown message types get no reply | `host.go:285-286` | the caller hangs for 20s, then fails confusingly |
| D8 | Peer-ingested commits skip `CommitListener`, index maintenance and the unique-key registry | server wiring | stream subscribers, indexes and uniqueness go stale on every replica |
| D9 | Stubbed (archived) commits omitted from fetch and push | `host.go:307`, `sync_plan.go:60` | a peer that lacks the stub rejects the children |
| D10 | A cross-namespace part from another host is treated as committed | `embed/txn_coordinator.go:375` | atomicity across a group is lost on replicas |
| D11 | Stream handshake unauthenticated; write-back runs as anonymous principal | `server/stream_listen.go:288-303` | data exposure and an RBAC bypass |
| D12 | Push failing halfway leaves the earlier commits stored with no report | `host.go` | not corrupting (orphans are harmless), but the result is misreported |

---

## 3. Design

### 3.1 Node identity

- **A `NODE` file at the data root** holds a UUIDv4 generated on first open. The existing `txn/HOST` id is migrated into it, so there's one identity per data root rather than two.
- **Exposed** via `/healthz`, `kdb node status` and the handshake (`NodeID` is already a field).
- **Every local commit** sets `AuthorNodeID = nodeID`. The field is already in the hashed payload, so this is a behaviour change, not a format change. The random per-write UUID carried no information anyway.
- **Backups and restores keep the id.** A *clone* (§Phase 4 bootstrap) generates a new one. Two live nodes with the same id is a misconfiguration, and the handshake refuses it.

### 3.2 Hybrid logical clock (no format change)

Commit `Timestamp` stays microseconds since the epoch, but it's issued by an HLC:

- `ts = max(wall_now_µs, last_issued + 1, max_seen_remote + 1)`.
- `max_seen_remote` advances on every commit ingested from a peer.
- A remote timestamp more than `--max-clock-skew` (default 5 min) ahead of local wall time is **refused**, not adopted. Otherwise one bad clock poisons the whole mesh.

Why it helps: every commit's timestamp is then greater than its causal predecessors', so LWW ("later timestamp wins") respects causality. Ties still break on commit hash. Old commits remain valid; they just weren't issued with the guarantee.

### 3.3 Deterministic merge commits

A merge of heads `{X, Y}` must be a pure function of `{X, Y}` and the resolution inputs:

| Field | Today | Deterministic |
|---|---|---|
| `ParentHashes` | `[local, incoming]` | `[min(X,Y), max(X,Y)]`, ordered by hash |
| `TransactionID` | random | `DerivedUUID("kdb:merge/1" ‖ P0 ‖ P1)` |
| `AuthorNodeID` | random | the fixed sentinel `MergeAuthorID` (all-zero v8) |
| `Timestamp` | `TimestampNow()` | `max(ts(X), ts(Y))`, not bumped by the HLC; see note |
| `Operations` | the remote side's touched docs | the ops that take `P0`'s tree to the merged tree, sorted by doc id |
| `SchemaHash` | nil | the resolved schema (Phase 5); equal schemas pass through |
| `Message` | fixed string | fixed string plus the policy used |

Because the first parent is chosen by hash instead of "whichever side I am", both nodes compute the same ops, tree, hash and commit. Replay from `P0` stays valid, and `MaterializeCommit`'s tree check keeps holding.

**Consequences:**

- **Merges are idempotent across the mesh.** If A and B both merge the same pair, sync finds the merge commit already present and fast-forwards.
- **Merge-of-merge still happens under 3+ concurrent writers,** but it terminates. Each merge strictly reduces the number of distinct heads among nodes that have exchanged commits.
- **Custom resolvers must be deterministic,** meaning a pure function of `(base, local, remote)` ordered canonically. That's a documented requirement. A resolver that can't meet it must use Strict and resolve through the conflict queue (§3.6), which creates an ordinary authored commit.

**Note on timestamps:** the merge timestamp must not come from the local HLC, because two nodes' clocks would disagree. `max(parents)` is deterministic, and the next locally authored commit's HLC reading moves past it.

**Rejected alternative:** detecting "same merge, different hash" after the fact. It's more complex and still leaves duplicate histories behind.

### 3.4 Sync protocol v2

It's git's smart protocol, adapted. A session carries **many namespaces and all refs**:

```
Handshake    { nodeId, protocol=2, capabilities[], namespaces[] }
RefAdvertise { per namespace: branches{name→hash}, tags{name→hash},
               shallowRoots[], historyFloor, policyRevision }
Negotiate    { per ns: want[] (remote heads I lack),
               have[] (my heads + exponentially spaced ancestors) }
             → Ack { common[] }   // repeat until common ancestors found or have exhausted
Pack         { per ns: commits in topological (parents-first) order,
               paged by bytes, not count; each page has a resumable cursor }
RefUpdate    { per ns, per ref: old→new (CAS), outcome }
```

**Design points:**

- **Parents-first order and byte-sized pages** fix D1 structurally. A receiver can `PutCommit(requireParents)` each page as it arrives, and a broken connection resumes from the cursor.
- **Negotiation replaces `SinceHash=myHead`.** That scheme walks to genesis whenever the sender doesn't know the receiver's head, which is the diverged case. The have-list is exponentially spaced (head, head~1, ~2, ~4, …) plus generation numbers (`dag/generations.go`). Finding a common ancestor then takes O(log n) round trips.
- **All branches and tags replicate.** Only `main` does today. Each branch is resolved independently with the same `ResolveHeadUpdate`. Tags are immutable, so a tag that already exists with a different hash is a conflict, reported and never overwritten.
- **Only the side that owns a ref updates it.** A push proposes `RefUpdate`s, and the receiver decides: fast-forward, merge, or report. This keeps "peers are equal" without either side forcing the other's head.
- **Capability negotiation** covers encoding, zstd pack compression, snapshot support (Phase 4), filters (Phase 7), and a partial-clone objects capability (Phase 8). Encoding moves to `EncodingKdbBinary`, which the ack already advertises and the host doesn't honour.
- **Every frame type gets a reply.** Unknown types get an `Error{unsupported}` (fixes D7).
- **Compatibility:** v1 messages 0x03/0x04 remain served. A v2 client falls back when the handshake reports protocol 1.

### 3.5 Ingest pipeline: one write path

Every commit from a peer goes through the **same gate and the same post-commit hooks as a local write**:

```
page received → verify hash, PutCommit(requireParents)       (outside gate)
             → enter writeGate
             → resolve ref update (FF / deterministic merge / report)
             → materialize into storage (tree check)
             → persist to delta log (same PersistAsync path)
             → update unique registry, indexes, schema
             → publish to CommitListener (stream subscribers)
             → leave gate; ack
```

This fixes D2, D3, D6 and D8 together. A peer is just another writer.

**Unique-key conflicts on ingest:** under multi-leader a replicated commit can violate a local unique constraint. For example, peer P created `email=x` on doc 1 while this node created it on doc 2. The commit **is accepted**, because history is never refused. The violation is then recorded in the conflict queue (§3.6) as a `UniqueConflict`. Refusing it would make the two replicas permanently unable to converge.

### 3.6 Durable conflict queue

- **Stored per namespace**, under `<nsRoot>/conflicts/`. It's local state, not replicated: each node reports what it saw.
- **An entry holds:** the two heads, the ancestor, the conflicting doc ids with base/local/remote content hashes, the policy, and the time first seen.
- **The diverged branch stays diverged.** `main` stays at the local head, and the remote head is kept as a **tracking ref** `peers/<nodeId>/<branch>`. Like git's remote-tracking branches, it keeps the remote's commits reachable, so retention can't drop them.
- **Resolution** comes through API, SQL or UI: `ResolveConflict(ns, entryId, choices{docId → local|remote|body})` produces an ordinary authored two-parent commit, which then replicates. This is spec §15's open question "Conflict resolution UX contract", answered.
- **Exposed as:**
  - metrics: `kdb_conflicts_open`
  - a control endpoint: `/v1/ns/{ns}/conflicts`
  - an admin UI tab

### 3.7 Replicator (in `kdb-service`)

```
--peer name=cloud,addr=tls://cloud:4242,namespaces=site/*,mode=bidirectional
--peer name=edge2,addr=tls://10.0.0.7:4242,namespaces=*,mode=pull
```

Peers are configured by flag, config file or env, and later can be changed live through `/v1/peers`. For each (peer, namespace-set), a **replication task** runs:

- **Triggers:**
  - a local commit (`CommitListener`, debounced about 50ms), which pushes;
  - a remote notification (a `RefAdvertise` pushed over a held-open session), which pulls;
  - a periodic anti-entropy tick (default 30s), which runs a full negotiation, is always correct, and catches whatever notifications missed.
- **State,** persisted in `<dataRoot>/peers/<peerNodeId>.json`: last acknowledged refs in each direction, the last success, and a consecutive-failure count. This is the **per-peer watermark** that retention consults (§3.8).
- **Backoff:** exponential with jitter, capped. The circuit opens after N failures, and the control plane shows it.
- **Modes:**
  - `pull`: a read replica, or edge-from-cloud
  - `push`: edge-to-cloud, write-only upstream
  - `bidirectional`
- **Lag metrics:** commits behind, and seconds since the last sync per (peer, ns).

The replicator is a **client of the protocol**. It holds no state that sync correctness depends on. Deleting `peers/` costs one full negotiation, nothing more.

### 3.8 Retention that respects peers

- **The floor is capped by active peers' watermarks.** A namespace under `history=none` (or a squash) computes its truncation floor as `min(retention floor, oldest ack of any peer active within --peer-retention-grace)`. The grace defaults to 7 days. A peer that has been silent longer than that is **evicted from the floor calculation** and must bootstrap by snapshot (Phase 4) when it returns. Without the grace, one dead laptop would pin history forever.
- **`Squash` rewrites commit identity** (`dag/in_memory_commit_dag.go:781-845`). In a replicated namespace, it's refused unless every peer in the replication set has acknowledged past the squash window. Otherwise peers holding the originals diverge from a history that no longer exists. This is spec §6.4's `CompactionIntent`, done as a precondition instead of a broadcast.
- **`CompactionNotice` (0x08) is sent** when the floor moves. Peers then know to snapshot rather than negotiate below it.

### 3.9 Snapshot bootstrap and shallow roots

- **`SnapshotRequest{ns, at: ref}` returns a stream.** The stream holds:
  - the commit at the floor (or at a requested ref)
  - its tree as `(docId, contentHash)` pairs
  - the live bodies (deduplicated by content hash, zstd, paged)
  - the ref set
- **The receiver:**
  1. writes bodies through the existing document-body blob path (`storage/engine/document_bodies.go`), which already exists for `history=none`
  2. rebuilds the trie and checks it against the commit's `DocumentTreeHash`, so the snapshot is fully verified
  3. records the commit as a **shallow root** in a local `shallow` list
- `PutCommit(requireParents)` treats a parent listed as shallow the same as a stub. Walks stop there and `CommonAncestor` treats it as a horizon. This generalizes the existing stub handling instead of adding a new concept.
- **The same mechanism covers:**
  - fresh clones: much faster than replaying all history, even under `full`
  - recovery of evicted peers
  - Layer 4b's browser "repair from peer" path (`EnlistmentManager.repairFromPeer`)

### 3.10 Replicated metadata

Today, schema, index definitions, policy and RBAC are local per node:

- the index catalog is `catalog.json`
- schema is registered on open by `syncEmbedSchema`
- `SchemaMigrationOp` is skipped by `MaterializeCommit`

They move into a **system namespace per database, `_kdb/meta`**. It's an ordinary replicated namespace with the Strict policy, holding one document per schema, index definition and namespace policy. It replicates by the same protocol, so it's versioned, auditable and verified. Appliers watch it and reconcile the local catalogs.

**Policy per concern:**

- **Schema** is additionally carried in data commits (`SchemaHash`, `SchemaMigrationOp`). The receiver applies the migration op during ingest instead of skipping it, so data never arrives ahead of its schema.
- **RBAC grants stay local by default.** Who may read on the edge is a local decision. They can be opted into `_kdb/meta` with `--replicate-grants`.

### 3.11 Namespace sync sets (partial replication, step 1)

A peer's `namespaces=` pattern set (§3.7) *is* partial replication at namespace granularity: `site/berlin/*`, `tenant/acme/*`, `!audit/*`. Supporting work:

- **The handshake advertises the pattern set,** so each side knows the intersection. The host authorizes each namespace individually (`PeerSyncAction`).
- **Namespaces are created on first sync,** with the policy fetched from `_kdb/meta`.
- **Cross-namespace groups** (`kdb:xns/1` marker) whose parts span a namespace a replica doesn't hold are **visible as partial**. The replica records the group as `foreign-incomplete` instead of claiming atomicity it can't check (fixes D10 for this case). When it does hold every part, it checks the group decision before exposing any part (full fix for D10).
- **Documentation:** a data-modeling guide, "choose your partition key as your namespace". This is the recommended route to "an instance holds only what it needs", because it keeps full verification and full write capability at zero format cost.

### 3.12 Filtered projection replicas (partial replication, step 2)

For subsets that don't line up with namespaces, for example "orders where `region='EU'`" or "documents this user may read":

- **The replica subscribes with a filter:** a KDB-SQL `WHERE` over schema fields and `_doc` paths, plus the principal's per-document RBAC (`DocumentReadAction`, which is finally enforced).
- **The source evaluates the filter per commit.** It sends a **projected delta**: the matching writes, plus *deletes for documents that stopped matching*. It also sends the source commit hash it corresponds to, so the stream resumes from a known point.
- **The replica is not a peer of the source namespace.** It writes the projection into a local **derived namespace** (`<ns>@<filterId>`) with its own locally authored commits (`AuthorNodeID = replica`, `Message = "projection of <ns>@<sourceHash>"`).
  - It's verifiable internally but only *trusts* the source for content, since the TLS-authenticated peer vouches for it (§1.2 exception).
  - It can be queried and indexed like any namespace, and it survives offline periods. On reconnect it resumes from the last projected source hash. If that's below the source's floor, it re-snapshots the filtered set.
- **Writes** go as Mode-2 write-back (`TransactionReplay`) to the source, with the principal's credentials. They're applied at the source and return through the projection. Offline writes queue locally as pending transactions and replay on reconnect, with conflicts surfaced through the same `ConflictReport`.
- **This absorbs stream Mode 1/2.** A filter of `TRUE` is the existing full-namespace stream, which gains resume/backfill (it has none today), authentication, and a durable local copy. Mode 1 becomes "projection with no local persistence".
- **Filter changes** (for example a user gaining a permission) are handled as a re-snapshot of the new filtered set, diffed against the local projection.

This is where the edge and browser use cases land (Layer 4b enlistments, Zolik). It's also what spec §8.6's `indexHints` were for.

### 3.13 Verifiable partial clone (partial replication, step 3, gated)

This needs **commit format v2**:

- The hashed payload holds each op as `(docId, contentHash)`.
- Bodies are separate objects addressed by content hash, which is already how content hashes are defined (`kdb_document.go:16-19`).
- A replica can then verify every commit and every tree with no bodies at all. The leaf hash is `sha256(uuid ‖ contentHash)`, so bodies never enter the tree.
- Bodies are fetched on demand with `ObjectFetch{hashes[]}`. That's the "object by hash" API that doesn't exist today; `IngestDeltaSegment` is a stub.

**Why gated:**

- **It's a cross-language wire and hash contract change,** with golden tests in both trees, a dual-format DAG, and a migration story. The format must stay dual forever, because v1 commits are immutable.
- **It saves body bytes, not tree overhead.** The live trie costs about 5 KB per document regardless of body size.
- **Build it only on evidence.** That means a workload with large bodies or attachments and a measured win over §3.12 projections. Phase 8 starts with that measurement.

### 3.14 Ownership: opt-in strong consistency

A namespace policy `consistency: multi-leader | single-home` controls this.

- **Under `single-home`:**
  - one node holds a **home lease** for the namespace. The lease is a document in `_kdb/meta` with `holder`, `expiresAt` and a monotonic `fence`, and it replicates to everyone.
  - Non-home nodes **forward** writes to the home over the SQL/doc wire. They keep serving reads, stale by their replication lag, or fresh with `ReadYourWrites` via a home round-trip.
  - The home stamps each commit's `Message` with `home=<node> fence=<n>`. A commit carrying a stale fence is fenced off at ingest: it's accepted into the DAG but reported, never fast-forwarded.
- **Handover:** the holder releases the lease, or it expires and another node claims it. Claiming uses the lease document's CAS. The CAS resolves at whichever node is authoritative for `_kdb/meta`; in the simplest deployment that's a designated **meta-home**.
- **Out of scope:** true leaderless consensus (Raft over `_kdb/meta`) is a non-goal unless a deployment needs automatic failover with no designated meta-home. It would slot in behind the same lease interface later.

What single-home buys: global unique constraints, global CAS and leases (existing machinery, now at one node), and no merge conflicts. What it costs: writes need the home reachable, which is exactly the trade an offline-first namespace must not make. Hence it's per namespace.

### 3.15 Placement and routing (scale-out)

This is the only part of "distributed database" that isn't about replicas, and it builds on everything above:

- **Placement:** a `_kdb/meta` document per namespace lists its replica set and home.
- **Routing:** any node answers "where does `ns` live", and clients are redirected or proxied. The Go client SDK caches the placement.
- **Rebalancing:** add a replica (Phase 4 snapshot + Phase 3 replication), wait until caught up, move the home lease, then drop the old replica.
- **Cross-node transactions:** `CommitGroup` today assumes one host. Groups spanning homes on different nodes need the decision log to become a coordinator reachable across nodes, which is two-phase commit with the existing `txn/` decision log as the durable coordinator record. Until that exists, a cross-namespace transaction whose namespaces have different homes is **refused** with a clear error instead of being non-atomic.

---

## 4. Phases

Each phase ends green on `cd go && go test -race ./... && go vet ./...`, plus its named e2e tests in `kdb-integration/e2e`.

**Wire-format changes land in Go with golden fixtures.** Kotlin keeps its read side compatible, following the precedent of the cross-namespace work.

### Phase 0 — Harden pairwise sync

**Fixes D1–D3 and D5–D12,** through the ingest pipeline (§3.5) and namespace validation. It also authenticates the stream handshake and runs write-back as the authenticated principal. D1 gets a stop-gap: parents-first paging over the existing `CommitFetch`, looping until the head is reached. The structural fix is Phase 2.

**Tests** (each written to fail first):

- sync of 1,000 commits
- concurrent local write during a fast-forward
- namespace mismatch refused
- materialize failure leaves the head unmoved
- unknown frame gets an error reply
- stream subscriber sees peer-ingested commits
- index and unique registry updated on ingest
- unauthenticated stream refused

**Exit:** two `kdb-service` processes plus the relay helper converge on a 10k-commit divergent history with indexes, uniqueness and subscribers consistent on both.

### Phase 1 — Identity, clock, deterministic merge

- `NODE` file (migrating `txn/HOST`), `AuthorNodeID`, and the handshake id check (§3.1).
- HLC with skew refusal (§3.2).
- Deterministic merge commits (§3.3). The determinism requirement for Custom resolvers is documented.

**Tests:**

- A and B independently merge the same pair and get byte-identical commits.
- 3-node ring with concurrent writers reaches a single head within a bounded number of rounds (property test, seeded).
- A skewed clock is refused.
- LWW respects causality under a 10-minute clock offset.

**Exit:** a 3-node mesh under random writes and partitions reaches one head per branch after partitions heal (deterministic simulation, §5).

### Phase 2 — Protocol v2

- Handshake v2, `RefAdvertise`, `Negotiate`, byte-paged `Pack` with cursors, `RefUpdate` (§3.4).
- Multi-namespace sessions, all branches and tags, binary encoding, zstd.
- v1 served for compatibility.

**Tests:**

- Negotiation round trips are O(log n) on 100k-commit histories.
- Resume after a connection is killed mid-pack.
- Tag conflict reported.
- A v1 client works against a v2 host.

**Measure:** bytes and time to sync 100k commits, compared with Phase 0.

### Phase 3 — Replicator and conflict queue

- `--peer` config, the replication task, watermarks, backoff, lag metrics, and `/v1/peers` (§3.7).
- Durable conflict queue, tracking refs, `ResolveConflict`, and the admin UI tab (§3.6).
- `kdb push/pull/fetch/sync/status` and `kdb node peers` in the CLI (spec §11).

**Exit:** two services configured only with `--peer`, with no relay helper, converge continuously. A Strict conflict appears in the queue, is resolved through the API, and the resolution replicates.

### Phase 4 — Snapshot bootstrap and retention

- `SnapshotRequest/Response`, shallow roots (§3.9), and a peer-aware truncation floor with grace.
- Squash refused in replicated namespaces until peers have acknowledged (§3.8). `CompactionNotice` sent.

**Tests:**

- A peer behind a `history=none` floor bootstraps by snapshot.
- A clone of a 200k-document namespace by snapshot, timed against replay.
- An evicted peer returns and re-bootstraps.
- Truncation waits for an active peer.

### Phase 5 — Replicated metadata

- `_kdb/meta`, reconcilers for schema, index catalog and policy (§3.10).
- `SchemaMigrationOp` applied on ingest.
- Optional grant replication.

**Exit:** an index created on node A exists and is used on node B. A schema migration on A arrives on B before any data that depends on it.

### Phase 6 — Namespace sync sets

- Pattern sets in the replicator and handshake, namespace auto-create, cross-namespace group completeness on replicas (§3.11).
- The data-modeling guide in `kdb-user-guide.md`.

**Exit:** an edge node configured with `site/berlin/*` holds exactly those namespaces, and a cross-namespace group that spans a namespace it lacks is flagged, never presented as atomic.

### Phase 7 — Filtered projection replicas

- Filter subscriptions, projected deltas with un-match deletes, derived namespaces, and resume by source hash (§3.12).
- Offline write-back queue; per-document RBAC enforced on the projection.
- Stream Mode 1/2 reimplemented on top of it.

**Exit:**

- A replica filtered on `region='EU'` goes offline, receives nothing, writes locally, reconnects, and converges.
- A document leaving the filter disappears from the replica.
- A document the principal can't read never arrives.

### Phase 8 — Verifiable partial clone (gated)

**Step 0 is a measurement:** on a large-body workload, compare a §3.12 projection with a modelled partial clone. Proceed only on a clear win.

**Then:** commit v2 op digests, `ObjectFetch`, lazy body loading on read, a dual-format DAG, and golden fixtures in both trees.

### Phase 9 — Ownership

- `consistency: single-home`, the home lease with a fence in `_kdb/meta`, write forwarding, fenced ingest, and handover (§3.14).

**Exit:** the multi-writer suite (`server/multiwriter_test.go`: unique race, CAS counter, lease fencing) passes with the writers spread across *three nodes* of a single-home namespace.

### Phase 10 — Placement and cross-node transactions

- Placement documents, routing and redirects in the client SDK, rebalancing (§3.15).
- Cross-node `CommitGroup` via 2PC over the decision log. Until that lands, cross-home groups are refused.

```
Phase 0 ─► Phase 1 ─► Phase 2 ─► Phase 3 ─┬─► Phase 4 ─► Phase 6 ─► Phase 7 ─► (Phase 8, gated)
                                          └─► Phase 5 ─┘
                                                        Phase 5 + 3 ─► Phase 9 ─► Phase 10
```

Phases 0–3 give a working multi-node, eventually consistent KDB with whole-namespace replicas. Phases 4–7 give edge and partial replicas. Phases 9–10 turn it into a scale-out system with optional strong consistency.

---

## 5. Testing strategy

Distributed bugs don't show up in unit tests, so testing is a track of its own:

- **Deterministic simulation harness** (starts in Phase 1).
  - N in-process nodes over the in-memory wire transport.
  - A seeded scheduler that delivers, delays, drops and reorders frames, and partitions and heals links.
  - Clocks with injected skew.
  - Invariants checked after each run:
    - **convergence:** all nodes that exchanged everything have identical refs per branch
    - **no lost commits:** every authored commit is reachable from some ref or listed in the conflict queue
    - **every stored commit verifies**
    - **the tree on each node matches its head**
  - A failing seed is a reproducible bug.
- **Process-level e2e** in `kdb-integration/e2e`. Real `kdb-service` processes, killed with `kill -9` mid-pack. The known flaky crash-recovery test is a warning here: use generous deadlines and log the seed.
- **Compatibility:**
  - golden fixtures for every new frame and for the deterministic merge commit
  - a v1↔v2 protocol matrix
  - Kotlin can decode v2 frames
- **Benchmarks:**
  - sync throughput
  - negotiation cost against history size
  - snapshot against replay bootstrap
  - replication lag under write load

  Follow the isolation rules. Check `uptime` first, never run concurrently with other heavy jobs, and pin `HistoryStrategy`.

---

## 6. Decisions to confirm

1. **Default consistency is multi-leader.** Single-home is opt-in per namespace, and there's no consensus protocol in the data path (§1.9, §3.14). *Recommended; it matches the spec's "peers are equal".*
2. **Partitions are namespaces.** The recommended partial-replication route is §3.11 plus §3.12, and commit v2 (§3.13) is gated on measurement. *Recommended.*
3. **Replicated history is never refused.** Unique-key violations and Strict conflicts from peers are accepted into the DAG and queued, not rejected (§3.5). The alternative, refusing, makes two replicas permanently unable to converge.
4. **Custom conflict resolvers must be deterministic** (§3.3). This is a new contract on an existing API.
5. **RBAC grants don't replicate by default** (§3.10).
6. **Kotlin parity:** Go leads, and Kotlin keeps decode compatibility, as with cross-namespace transactions. A full Kotlin replicator isn't planned.
7. **Peer discovery stays static configuration.** mDNS and gossip are out of scope (spec §15).
