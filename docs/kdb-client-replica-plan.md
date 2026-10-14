# Client replica (memory plan Track D)

Status: **design; owner decisions recorded in §10.** Written 2026-10-13 against `origin/main` 0399daf.
No code yet. File references are relative to `go/kdb/` unless they start with a repo-level directory.

Inputs: `kdb-memory-cassandra-plan.md` Track D (fixed constraints), `kdb-distributed-plan.md` §3.11–3.14,
`kdb-spec-layer12-component63-ts-client-sdk.md` §11/§13.6, the attachments plan on `feat/attachments-model`
(PR #214), and a survey of the code. No separate offline-sync design exists in `docs/` or in a live session,
so this document is the first.

## 1. What already exists

| Piece | What it is | Consequence for the replica |
|---|---|---|
| `@kdb/embed` (`packages/kdb-embed`, `go/wasm/embed`) | **The Go engine compiled to WASM**, fronted by `server.FrameHost`. Durable through `PersistentFs` (`packages/kdb-embed/src/fs.ts`), a TS implementation of the `fs` object Go's js/wasm runtime calls, written behind to IndexedDB in 64 KiB chunks. | Its storage layer is a whole-file-tree image held in memory, so a native replica reuses its IndexedDB backend's transaction discipline, not its shape (§5). There is no TS commit codec, trie or DAG to reuse. |
| `go/mobile/kdbsync` | A full Go node for gomobile (`bind -target=android|ios`): `embed.OpenFileHost` + `syncnode.Node`, peers over `ws://`/`wss://`/`tcp://`, bearer token, `SyncBoth`. | The same role, as a full Go node on device. It stays available for apps that accept gomobile; the Kotlin replica is the native alternative. |
| peersync v2 (`peersync/`, `wire/sync_v2_ops.go`) | FETCH/PACK_PAGE with zstd packs, SNAPSHOT_FETCH, SQUASH_HEAD + TREE_NODES + OBJECT_FETCH, PROJECT_FETCH/PAGE/WRITE/WRITE_RESULT, REF_UPDATE, HOME_REQUEST. JSON envelope over TCP or WebSocket (`/kdb/sync`). | Everything a replica needs is already a peersync message, *including* filtered bootstrap and write-back (projections). |
| `peersync.SyncProjection` + `server/writeback.go` | A KDB-SQL filter per namespace; host filters per commit and per principal (`DocumentReadAction`); local projection namespace `<ns>.projection-<hash>`; write-back with a content-hash precondition, idempotent tx ids, the projection's DAG as outbox. | Filtered replica + offline write queue + per-document read rights are built. Two host-side costs need fixing (§4.3). |
| `replication.Replicator` / `syncnode.Node` | Peer config (`catchup=`, `Filter`, `WriteBack`, `PreferSnapshot`), interval sync, backoff, conflict queue. | The replica is a `syncnode.Node` with a client-shaped configuration, not a new loop. |
| Kotlin `kdb-peer-sync` | **v1 only** (HANDSHAKE, COMMIT_FETCH, COMMIT_PUSH); Kotlin's replayer refuses `shallowRoots` (#209). No Android target in Gradle. | A Kotlin-native replica means writing a v2 client *and* shallow-root support in Kotlin's storage. See §10 D2. |
| `@kdb/client` | Binary wire frames (#202), WS/TCP, zstd *decode* on Node only. | Not the replica: it stays the thin RPC client. The replica exposes the same `Client`/`KdbOperations` surface (§11.3 of Component 63). |

**Route (owner, 2026-10-13): native replicas in TS and in Kotlin.** Both speak peersync v2 directly, with the
same messages a Go node uses. Neither embeds the Go engine. That rules out the obvious shortcut (the Go engine
already inside `@kdb/embed`'s WASM, or `kdbsync` via gomobile). In exchange, the browser bundle stays small, and
neither platform depends on Go's runtime or a 31 MB WASM. The cost is a second (TS) and third (Kotlin) peersync
client, so **parity is enforced by golden vectors generated from Go** and by live interop tests against a Go node
in every PR.

## 2. Which clients first

**TS first (browser and Node), then Kotlin (JVM and Android).** TS has no commit codec, trie or DAG today. Kotlin
has all three (`kdb-document`, multiplatform, hash-identical to Go), but its peer sync is v1, and its replayer
refuses shallow roots. Doing TS first fixes the replica's semantics against the Go host. Kotlin then ports the
same design, reusing its existing codec and trie.

## 3. Shape of the replica

A replica is deliberately **much less than a node**. That is what makes two native implementations affordable:

- **It never merges.** In full mode it pushes its local commits first (REF_UPDATE); the host merges them
  (`outcome: merged`); then it pulls, which is now a fast-forward. A fast-forward is all the replica ever applies:
  topo-sort the fetched range, keep each document's last op (merge commits carry every document their parents
  disagree on), apply, and require the built tree hash to equal the target commit's `documentTreeHash`. This
  mirrors `peersync/ingest.go` `fastForward`/`netEffect`/`applyAndVerify`. A Strict conflict on push comes back as
  a `ConflictReport` and is the app's to resolve (§7).
- **It keeps no history** beyond its head and its unpushed commits. Its root is a shallow root: a snapshot,
  squash or fast-forward target. Older states come from the host.
- **It serves nothing.** No host side, no listener, no deepen, no graft, no backup.
- **It runs no SQL.** Local reads are get-by-id, ordered scans, id ranges and a predicate callback, with optional
  secondary indexes on JSON paths declared by the app. KDB-SQL stays on the server (open decision D10).

### 3.1 Components (TS, new package `@kdb/replica`)

| Component | Port of | Verified by |
|---|---|---|
| Canonical codec subset: `CommitPayload` encode/decode, ops (Write, Delete; attachment ops later), `DocumentBody` | `codec/wirecodec.go`, `document/wire.go`, `document/kdb_commit.go`; extends `@kdb/client`'s `wire/hash.ts` | Golden commit vectors from Go (`go/cmd/kdb-tsfixtures` gains `commits.json`): bytes and hash |
| SHA-256, **synchronous**, pure TS | - | NIST vectors. WebCrypto is async per call, and the trie needs ~30 hashes per insert (leaf folding), so a promise per hash is too slow. Node uses `node:crypto` when present. |
| Document trie: 16-ary nibble Merkle, leaf `0x00‖uuid‖contentHash`, internal `0x01‖16×32B`, single-entry subtrees folded | `document/document_tree_trie.go` | Golden tree vectors (sets of ids and content hashes → root); random differential test against a Go helper |
| Persistent trie storage: nodes keyed by nibble prefix, written per commit, LRU-cached | - | Reopen tests; budget test |
| peersync v2 client: SYNC_HELLO, REFS, FETCH/PACK_PAGE, SNAPSHOT_FETCH/PAGE, SQUASH_HEAD, TREE_NODES, OBJECT_FETCH, REF_UPDATE, PROJECT_*, PEER_ERROR; JSON envelope | `peersync/v2_client.go`, `squash.go`, `snapshot.go`, `projection.go` | Live interop against a Go node |
| Framing and transports | Reuse `@kdb/client`'s `wire/frame.ts` and its WebSocket/TCP transports | Existing tests |
| zstd **decode** for packs | Node: `zlib.zstdDecompressSync` (as `@kdb/client` does). Browser: a TS port of the pure-Kotlin decoder in `kdb-compression` (the "JS zstd" the plan names) | Go zstd golden vectors (`go/kdb/interop/zstd_physical_golden_test.go`). The replica advertises `zstd-pack` only when it has a decoder; pushes go uncompressed (the host accepts both). |
| Replica store | Modelled on `@kdb/embed`'s `FsBackend` (atomic `apply` batch; IndexedDB backend with strict durability) but **key-value and paged**, not a whole-file-tree image | Crash and reload tests, as `persist.test.ts` does |
| Replica engine: modes, bootstrap, catch-up, push, projections, write-back, conflict events, poll loop | `replication/replicator.go` behaviour | Live interop |

### 3.2 API

```ts
const db = await openReplica({
  store: indexedDbStore("app"),      // or memoryStore(), fileStore(dir) on Node
  peer: { url: "wss://api.example.com/kdb/sync", token: () => auth.currentToken() },
  namespaces: [
    { ns: "app/u/42", mode: "full" },                                              // §4.1
    { ns: "orders", mode: "filtered", filter: "region = 'EU'", writeBack: true },  // §4.2
  ],
  catchUp: "auto",                   // "replay" | "squash" | "auto"
  memory: { cacheBytes: 32 << 20 },  // §5
});
const orders = db.namespace("orders");
await orders.put(id, { region: "EU", total: 12 });
await orders.get(id); orders.scan({ from, to, where: d => d.total > 10 });
db.syncNow(); db.status(); db.on("change", …); db.on("conflict", r => …); await db.close();
```

### 3.3 Kotlin (`kdb-replica`, JVM and Android)

Same design and API shape. It reuses `kdb-document` (commit codec, trie, content hash), `kdb-codec`,
`kdb-compression` (zstd) and `kdb-wire` framing. New code: the v2 client messages in `kdb-wire`, and the replica
engine.

**Storage is the Kotlin embed runtime** (owner, D12): `FileEmbeddedRuntime` in `kdb-jdbc/.../file/`, the
Go-compatible delta log. A Kotlin replica's data directory is therefore readable by Go tools and the Kotlin CLI,
and the Go↔Kotlin physical-format goldens cover it. What that needs first:

- **Shallow roots in Kotlin's replayer.** Every replica root is a shallow root (snapshot, squash). Kotlin refuses
  namespaces whose marker lists `shallowRoots` (#209, `NamespaceMarker.kt`). The Kotlin replayer must learn Go's
  rule (`embed/delta_replay.go applyCommitsTopologically`): a logged shallow root is admitted without applying its
  ops or moving main, and the state comes from the checkpoint. Squash's durability is a checkpoint, so Kotlin must
  also *write* a checkpoint that Go and Kotlin both read. This is the largest Kotlin item and lands as its own PR
  before the Kotlin replica.
- **Android**: `kdb-jdbc` is a plain JVM module (`java.nio.file`, API 26+) and `kdb-compression` uses zstd-jni,
  which ships Android binaries. Both need an Android build check; the pure-Kotlin zstd codec is the fallback.
- **Memory**: the Kotlin runtime's resident structures (it has none of Go's Track A work) bound how large a
  namespace a Kotlin replica can hold. Measure in the Kotlin PR, and use filtered mode for large namespaces.

## 4. What the replica holds

Two modes, chosen per namespace. They differ because a peer that holds real source commits must receive every
document those commits touch: a v1 commit carries its bodies inside its hash.

### 4.1 `full`: the whole namespace, real commits

- Same hashes as the cloud. Every commit and tree is verified. Local writes are real commits pushed by
  REF_UPDATE, merged on the host by the namespace policy.
- **Requires a pull grant on the whole namespace.** FETCH/SNAPSHOT/SQUASH have no per-document filtering
  (`v2_host.go:246-370`). That is correct, not a gap: a pull grant means "may read the namespace". Per-user
  namespaces (`app/u/42`), the data-modelling advice in `kdb-distributed-plan.md` §3.11, are the main case.
- **Bootstrap**: SNAPSHOT_FETCH (unfiltered, cursor-paged, C5) when the namespace is empty, installed as a
  shallow root. Then FETCH/PACK_PAGE.
- **Catch-up**: `catchup=auto` by default (C7): SQUASH_HEAD → TREE_NODES diff → OBJECT_FETCH → shallow root.
  Fast-forward only; a replica with local unpushed commits falls back to replay (it pushes first, then pulls).
- **Auto rule** (the Go rule, `squash.go:84-91`): squash when the backlog is ≥ 1000 commits and ≥ 2× the changed
  documents; otherwise FETCH/PACK_PAGE and fast-forward. When the host refuses squash, replay.
- The replica keeps no history below its root. As-of reads go to the host.

### 4.2 `filtered`: a projection, per-document read rights

- PROJECT_FETCH with a KDB-SQL filter. The host evaluates the filter **and** `DocumentReadAction` per document;
  documents the principal cannot read never arrive, and documents that stop matching (or lose read access) arrive
  as deletes.
- Held locally as documents plus the last complete source commit (`atHex`); a native replica needs no locally
  authored commits for it (Go's `ProjectionTarget` keeps them only because a node stores everything as commits).
  Pages are applied atomically, each with its final states, so an interrupted transfer resumes from the last
  complete source.
- **Bootstrap = filtered snapshot**: PROJECT_FETCH with no `fromHex` is a reset, a cursor-paged
  (`after`/`next`) filtered snapshot. An id-range filter is a predicate on `_id`
  (`_id >= '…' AND _id < '…'`); the host change in §4.3 makes that a seek, not a scan.
- **Catch-up**: PROJECT_FETCH from the last complete source commit returns the net effect, which the host must
  compute the C7 way (§4.3). If `fromHex` is not an ancestor (below a floor, or squashed away), the host sends a
  reset, which the replica applies as a diff against what it holds.
- **Filter change** (a new filter or new rights): a new filter is a new projection namespace. A rights change is
  handled because every delta re-evaluates read rights.

### 4.3 Host-side fixes the filtered mode needs (Go peersync; coordinate first)

Both are in `peersync/projection.go`, are internal to the host, and change no message.

1. **Reset is O(namespace) per page.** `projectPage` walks the whole tree, collects every id after the cursor
   and sorts them, *for every page*: the O(D²/page) shape C5 removed from `snapshotPage`. Use C5's
   `snapshotPageSeek` cursor seek in the trie, and recognise an `_id` range in the filter to bound the walk.
2. **Delta is a replay, recomputed per page.** The incremental path calls `commitsBetween(at, from)` and
   `netEffect` on *every page*, which is O(backlog) per page and is replay-shaped, not C7-shaped. Replace it with
   `document.DiffTrees(fromTree, atTree)`, cursor-ordered by id: O(changed documents), independent of how many
   commits the replica missed. This is the projection's equivalent of squash.

The squash, replay-allocation and attachments sessions all work in `peersync/`; these changes touch only the
projection paths, and I will confirm with them before starting.

### 4.4 Mixed

A replica can hold namespaces in both modes on one connection. The `SYNC_HELLO` advertises the full-mode
patterns; projection requests are authorized one by one, as today.

## 5. Storage and memory bounds

This section is the TS replica's store; Kotlin uses its embed runtime (§3.3).

**Store**: a small key-value interface (`get`, `getMany`, ordered `scan`, atomic `apply(batch)`), with a memory
backend (tests), an IndexedDB backend (browser) and a file backend (Node; JVM/Android in Kotlin). Keys:

| Prefix | Value | Notes |
|---|---|---|
| `m/` | replica metadata: node id, per-namespace mode, head, root, last projection source, schema version | resident |
| `d/<ns>/<uuid>` | document body + content hash | id-ordered, so scans and id ranges are key ranges |
| `t/<ns>/<nibble prefix>` | trie node: 16 child hashes, or a folded leaf | written per apply; enough to recompute a root and to answer TREE_NODES diffs locally |
| `c/<ns>/<hash>` | head commit payload, plus unpushed local commits | the push queue for full mode |
| `o/<ns>/<seq>` | write-back outbox entries | filtered mode |
| `i/<ns>/<index>/<key>/<uuid>` | app-declared secondary index entries on a JSON path | D10 |

- **Atomicity**: one apply (a pack page's fast-forward, a snapshot page, a projection page, a local write) is one
  backend transaction. The head moves in the same transaction as the documents and trie nodes. On IndexedDB this is
  one `readwrite` transaction with `durability: "strict"`, as `@kdb/embed`'s backend does.
- **Memory bound**: nothing is loaded whole. Two LRU caches (trie nodes and document bodies) share
  `memory.cacheBytes` (default 32 MiB). A fetch in progress holds at most one page (≤ 4 MiB, the FETCH default) plus
  the documents it touches. Snapshot install builds the trie bottom-up from id-ordered pages, so it never holds
  the namespace. A squash diff walks TREE_NODES level by level. Peak memory is O(cache + page), independent of
  namespace size: this is decision 1 of the memory plan, applied on the client.
- **Quota**: the browser can evict storage. `openReplica` calls `navigator.storage.persist()` and reports the
  result. An evicted replica is detected on open (metadata missing) and re-bootstraps. `status()` warns ahead of
  time that unsynced local writes would be lost when persistence was denied.
- **Encryption at rest**: later; per-store, matching the "toggle, whole store" scope of the engine's encryption.
- **Format**: versioned by a schema number in `m/`. A replica is a cache of the host, so an incompatible upgrade
  may re-bootstrap after pushing pending writes. It never has to migrate.

## 6. Live updates

peersync has no head announcement. The replicator syncs on an interval (30 s) and on local commits.

- **v1 (no protocol change)**: poll. A cheap REFS_REQUEST every N seconds while the page is visible and online,
  with backoff when hidden or offline, and an immediate sync on `online`/`visibilitychange`. A REFS round trip
  for unchanged refs is a few hundred bytes.
- **v2 (proposed, codes reserved)**: `REFS_WATCH 0x50` / `REFS_CHANGED 0x51`. The client asks the host to send
  a REFS_CHANGED frame on the open sync connection when a watched namespace's head moves. The client then
  FETCHes (or PROJECT_FETCHes). The host side is a hook on the namespace's commit notifier (the one the stream
  listener already uses), coalesced per namespace. For projections, the host only says "the source moved"; the
  filter is evaluated on the next PROJECT_FETCH, so the watch leaks nothing beyond "something changed".

Codes 0x50–0x57 are reserved for the replica by the B4 session (2026-10-13). Bump `nextFreeMessageCode` in
`wire/message_roundtrip_test.go` when adding them.

## 7. Local writes and write-back

| Mode | Local write | Offline | Conflict |
|---|---|---|---|
| `full`, multi-leader | A real local commit: canonical payload, parent = local head, tree hash recomputed, `authorNodeId` = the replica's id, HLC timestamp | Unpushed commits are the queue (`c/`), a chain on the last host-confirmed head | REF_UPDATE → host merges by policy. A Strict conflict returns a `ConflictReport` (`refUpdateAck.conflictBytes`) and is kept with the local commits; `db.on("conflict")` raises it. The app resolves by writing a new local commit on top of the host's head (with `rebase` helpers: keep mine, take theirs, per document) or by discarding its commits. |
| `filtered` + `writeBack` | Applied to the local projection at once (optimistic), and appended to the outbox with its `baseHash` | The outbox (`o/`), oldest first, each entry with a tx id fixed when it is written, so a resend after a lost reply is idempotent (the role `WriteBackTxID` plays in Go) | PROJECT_WRITE applies each doc with a `baseHash` precondition. `conflict`/`refused` return `current[]`; `ResolveWrite` resets those docs to the source state and keeps the attempt. The replica turns that into the same `ConflictReport` shape for `db.on("conflict")`, so the app has one conflict type. |
| `single-home` namespace | **Only through write-back.** A full-mode replica of a single-home namespace is read-only locally (its commits would be fenced or refused with NOT_HOME). | Queued in the projection outbox; a NOT_HOME or unreachable home is a PEER_ERROR, so the write stays pending | Same as filtered. |

- So a single-home namespace is held in `filtered` mode (the filter can be `TRUE`). `openReplica` enforces this
  from the namespace policy it reads through META_VIEW (as `ScopedMeta` does) and refuses `mode: "full"` with a
  clear error.
- **Ordering across namespaces**: a cross-namespace transaction written on the replica is pushed as its
  `CommitGroup` parts; projection write-back is per namespace. A group spanning a projection is refused locally
  rather than half-applied. This matches the distributed plan's rule for groups the replica cannot check.
- **Durability of the queue**: a local write is acknowledged only after its store transaction commits, so a pending
  write survives reload and crash.
- **Clock**: the host refuses commits whose timestamp is skewed (`checkSkew`). The replica keeps an HLC seeded
  from the host's commit timestamps it has seen, so a phone with a wrong clock still produces acceptable commits.

## 8. Auth and read rights

- **Credentials**: a bearer token in `SYNC_HELLO.token`, fetched from the app's callback on every (re)connect.
  An expired token makes the host refuse the hello; the replica asks the callback again and backs off.
- **Grants**: `full` requires the namespace pull grant (and push for writes). `filtered` requires
  `StreamSubscribeAction` and filters per document by `DocumentReadAction`; write-back runs as the principal with
  per-document write checks (`AuthorizeDocuments`).
- **Rule that keeps this safe**: per-document read restrictions are only meaningful in `filtered` mode. A
  principal with document-level restrictions must not hold a namespace pull grant. The design doc for RBAC
  (`kdb-rbac-plan.md`) should say this explicitly, and `openReplica` reports a host refusal of `full` mode as
  "use filtered".
- **Local data**: the replica holds only what the principal could read. Logging out deletes the replica
  (`db.destroy()`) unless the app opts to keep it.

## 9. Attachments (with the B4 session's PR 5)

Agreed with the B4 session on 2026-10-13 and recorded in `docs/kdb-attachments-plan.md` (PR 5):

- Refs (`AttachmentPutOp`/`AttachmentDeleteOp`, `AttachmentTreeHash`) travel in commits, so `full` mode gets them
  through FETCH/PACK_PAGE with no extra work. The engine verifies the attachment root as it does the tree.
- **Bytes are lazy on the replica.** The replica stores refs and fetches bytes on first read or by a prefetch
  policy (`attachments: "lazy" | "eager" | (ref) => boolean`), with a blob cache under its own budget. Blobs it
  wrote and the host has not yet confirmed are pinned.
- **BLOB_FETCH 0x42/0x43**, per entry `(docId, commit, hash, offset, maxBytes)`: a ranged and resumable read
  (each chunk carries the total size), batched, streamed in request order and paged by bytes, with per-entry
  errors ("not retained", "forbidden"). Authorized by read rights on `docId` at `commit`, so lazy fetch of any
  readable old ref works.
- **Squash/snapshot**: TREE_NODES gains a tree selector, so squash diffs the attachment table with the same
  mechanism. Missing blobs are *not* fetched before install on a replica (lazy), unlike a node.
- The native replica must decode op kinds 5/6 and payload field 10, and keep the attachment table as a second
  trie (same hashing, ref hash as leaf content) to verify `AttachmentTreeHash`. Golden vectors come from #214's
  `AttachmentCommitGoldenTest`.
- **Two gaps to settle with B4** (raised in §10 D7):
  1. **Projections carry no attachment refs.** `ProjectPageMessage.writes` is `{id, body}`. A filtered replica
     needs `{id, body, attachment?: {hash, contentHash, size, mime}}`, plus the source commit (`atHex`, already
     there) for BLOB_FETCH's `commit`.
  2. **Upload from a replica.** A host adopting a pushed commit fetches missing blobs from the *sender*, but a
     replica serves nothing. A replica must upload first: the 0x44+ client messages (ATTACHMENT_BEGIN/CHUNK/END)
     on the sync connection, or a blob-push message on the sync protocol. Then it pushes the commit or
     PROJECT_WRITE that references them. Resumable for mobile.

## 10. Decisions

**Decided (owner, 2026-10-13):**
- **D1 TS route: native TS replica** (not the Go engine in WASM).
- **D2 Kotlin route: Kotlin-native v2 client** (not gomobile `kdbsync`).
- **D3 Filtered mode = projections** (PROJECT_FETCH/PROJECT_WRITE). A filtered replica cannot hold real source
  commits without receiving documents it may not read. "One pack format" therefore means "no new client protocol":
  whole-namespace replicas use FETCH/PACK_PAGE, snapshot and squash exactly as nodes do. The verifiable partial clone
  (commit format v2, `kdb-distributed-plan.md` §3.13) stays gated.
- **D5 Live updates: poll first, `REFS_WATCH 0x50 / REFS_CHANGED 0x51` later.**

**Proceeding on the recommendation unless the owner objects:**
- **D4** Host fixes to projections (§4.3) in PR 3: a cursor seek for reset and `DiffTrees` for delta. Go peersync
  projection paths only, no wire change, coordinated with the peersync sessions first.
- **D6** Single-home namespaces are filtered/write-back only on a replica; full mode is refused.
- **D7** Ask B4 to add attachment refs to PROJECT_PAGE and to accept blob upload on a sync session; the replica's
  attachment PR follows B4's PR 5.
- **D9** The replica keeps no history (head plus unpushed commits).

- **D10 Local queries (TS):** get by id, ordered scans, id ranges, a predicate callback, and app-declared
  secondary indexes on JSON paths (equality and range). No local SQL.
- **D11 Browser zstd:** a decode-only TS port of the pure-Kotlin decoder in `kdb-compression`, checked against the
  Go golden vectors. Until it lands, browsers do not advertise `zstd-pack`.
- **D12 Kotlin store: the Kotlin embed runtime** (§3.3), which first needs shallow roots in Kotlin's replayer.

## 11. PR plan

Each PR is tested against a real Go node. A `go/cmd/kdb-replicahost` helper runs a file host with the peer
WebSocket and TCP listeners, auth on, and test hooks (seed N commits, write a document, read the head). It prints
`ready ws://…/kdb/sync` and is spawned by the TS tests in the style of `packages/kdb-client/test/interop.test.ts`.
The Kotlin tests drive the same helper. A Go-side test in `go/kdb/integration` checks the host's view: hashes
equal, conflicts reported, unreadable documents never sent.

| PR | Scope | Tests |
|---|---|---|
| 1 | `@kdb/replica` core: codec subset, sync SHA-256, trie, KV store (memory + IndexedDB), fixture generator for commit and trie vectors | Golden commit/trie/content-hash vectors from Go; store atomicity; trie reopen |
| 2 | v2 client + full mode: hello/auth, snapshot bootstrap, FETCH/PACK_PAGE fast-forward, squash catch-up, `catchUp=auto`, local commits pushed by REF_UPDATE, poll loop, zstd on Node | Against `kdb-replicahost`: bootstrap; catch-up by replay and by squash (asserts which route ran); push + host merge + pull; reopen; bad token; two replicas converge; memory stays under budget on a namespace larger than it |
| 3 | Filtered mode + write-back + conflicts + single-home, plus the projection host fixes (§4.3) | Filter and read rights; un-match deletes; id range; offline writes queue and apply on reconnect; conflict event and resolution; NOT_HOME keeps the write pending; host reset cost O(page) |
| 4 | Browser: zstd decoder (D11), Chromium smoke page, `navigator.storage.persist` | Golden zstd vectors; browser page test |
| 5a | Kotlin shallow roots: replay admits logged shallow roots; checkpoint written and read in Go's format; #209's refusal lifted | Go writes a squashed namespace, Kotlin opens it, and the reverse (`go/kdb/integration` runCli pattern) |
| 5b | Kotlin `kdb-replica` (JVM, Android target): v2 messages in `kdb-wire`, engine over `FileEmbeddedRuntime` | Interop test against `kdb-replicahost` under `./gradlew build` (skipped without Go, as the TS tests skip) |
| 6 | Attachments (after B4 PR 5), TS then Kotlin | Lazy fetch, ranged resume, upload then push, a filtered replica gets only its documents' blobs |
| 7 | `REFS_WATCH`/`REFS_CHANGED` | A change is visible on the replica without a poll tick |
