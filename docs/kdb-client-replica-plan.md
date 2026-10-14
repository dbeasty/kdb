# Client replica (memory plan Track D)

Status: **design, awaiting owner decisions (§10).** Written 2026-10-13 against `origin/main` 0399daf.
No code yet. File references are relative to `go/kdb/` unless they start with a repo-level directory.

Inputs: `kdb-memory-cassandra-plan.md` Track D (fixed constraints), `kdb-distributed-plan.md` §3.11–3.14,
`kdb-spec-layer12-component63-ts-client-sdk.md` §11/§13.6, the attachments plan on `feat/attachments-model`
(PR #214), and a survey of the code. No separate offline-sync design exists in `docs/` or in a live session,
so this document is the first.

## 1. What already exists (and decides most of the design)

| Piece | What it is | Consequence for the replica |
|---|---|---|
| `@kdb/embed` (`packages/kdb-embed`, `go/wasm/embed`) | **The Go engine compiled to WASM**, fronted by `server.FrameHost`. Durable through `PersistentFs` (`packages/kdb-embed/src/fs.ts`), a TS implementation of the `fs` object Go's js/wasm runtime calls, written behind to IndexedDB in 64 KiB chunks. | "The TS embed's storage layer" is Go's own delta log, checkpoints and graph files on a TS filesystem. There is no TS commit codec, trie, DAG or storage engine to reuse, and none is needed. |
| `go/mobile/kdbsync` | A full Go node for gomobile (`bind -target=android|ios`): `embed.OpenFileHost` + `syncnode.Node`, peers over `ws://`/`wss://`/`tcp://`, bearer token, `SyncBoth`. | The Android/iOS replica already exists in its "whole namespace" form. It lacks only the projection, catch-up and conflict options. |
| peersync v2 (`peersync/`, `wire/sync_v2_ops.go`) | FETCH/PACK_PAGE with zstd packs, SNAPSHOT_FETCH, SQUASH_HEAD + TREE_NODES + OBJECT_FETCH, PROJECT_FETCH/PAGE/WRITE/WRITE_RESULT, REF_UPDATE, HOME_REQUEST. JSON envelope over TCP or WebSocket (`/kdb/sync`). | Everything a replica needs is already a peersync message, *including* filtered bootstrap and write-back (projections). |
| `peersync.SyncProjection` + `server/writeback.go` | A KDB-SQL filter per namespace; host filters per commit and per principal (`DocumentReadAction`); local projection namespace `<ns>.projection-<hash>`; write-back with a content-hash precondition, idempotent tx ids, the projection's DAG as outbox. | Filtered replica + offline write queue + per-document read rights are built. Two host-side costs need fixing (§4.3). |
| `replication.Replicator` / `syncnode.Node` | Peer config (`catchup=`, `Filter`, `WriteBack`, `PreferSnapshot`), interval sync, backoff, conflict queue. | The replica is a `syncnode.Node` with a client-shaped configuration, not a new loop. |
| Kotlin `kdb-peer-sync` | **v1 only** (HANDSHAKE, COMMIT_FETCH, COMMIT_PUSH); Kotlin's replayer refuses `shallowRoots` (#209). No Android target in Gradle. | A Kotlin-native replica means writing a v2 client *and* shallow-root support in Kotlin's storage. See §10 D2. |
| `@kdb/client` | Binary wire frames (#202), WS/TCP, zstd *decode* on Node only. | Not the replica: it stays the thin RPC client. The replica exposes the same `Client`/`KdbOperations` surface (§11.3 of Component 63). |

**The central observation.** The plan's constraint "one pack format, no separate client sync protocol" is met
most strongly by not writing a second implementation at all: the replica *is* the Go node, compiled to WASM for
the browser and Node and to gomobile for Android/iOS, speaking peersync v2 unchanged. The plan's "start from the
JS zstd" predates the Go→WASM decision for `@kdb/embed` (Component 63 §13.6); the WASM already contains Go's zstd,
so no JS zstd is needed. A native TS reimplementation would duplicate the commit codec, the trie, the DAG,
squash, snapshot install, projection apply and conflict handling, and would drift (the same reason §11.2 chose
WASM).

## 2. Which clients first

**Recommendation: TS (browser and Node) first, through `@kdb/embed`; Android/iOS second, through `kdbsync`;
Kotlin-native last and only on evidence (§10 D2).**

- TS first: `@kdb/embed` has the storage and the engine, and is missing exactly one thing to sync, a transport.
  Go's `net` does not exist under js/wasm, so `transport/ws` cannot dial from the browser or Node. The work is a
  `stream.Transport` backed by the JS `WebSocket` (§3.1).
- Android/iOS second: `kdbsync` works today; it gains the options in §4–§6 and a small Kotlin/Swift facade.
- Kotlin-native: v2 client, shallow roots in the Kotlin replayer, projections and write-back in Kotlin. That is
  most of the Go peersync client again. Only worth it if a JVM/Android app cannot take a gomobile AAR.

## 3. Shape of the replica

### 3.1 TS: `openReplica` in `@kdb/embed`

```ts
const db = await openReplica({
  name: "app",                       // IndexedDB database / directory name
  peer: { url: "wss://api.example.com/kdb/sync", token: () => auth.currentToken() },
  namespaces: [
    { ns: "app/u/42", mode: "full" },                                    // §4.1
    { ns: "orders", mode: "filtered", filter: "region = 'EU'", writeBack: true }, // §4.2
  ],
  catchUp: "auto",                   // default; "replay" | "squash" | "auto"
  memory: { budgetBytes: 64 << 20 }, // §5
});
db.client          // the ordinary @kdb/client Client (KdbOperations) over the local engine
db.syncNow(); db.status(); db.on("conflict", r => …); db.on("change", …); await db.close();
```

- **Go side** (`go/wasm/embed`): `open` gains a `replica` option. It builds a `syncnode.Node` over the same
  `embed.Host`, with `replication.PeerConfig`s whose transport is `transport/jsws` (new, `//go:build js`): a
  `stream.Transport` whose `Connect` calls back into TS to open a `WebSocket`, with frames carried exactly as
  the existing loopback does. One WebSocket message is one frame, as `transport/ws` already requires. No wire
  change.
- **Token refresh**: `Credentials` is already a callback in `PeerConfig` (`kdbsync.go:87`); `jsws` asks TS for the
  current token on every connect. In the browser, `WebSocket` cannot set an `Authorization` header, so the token
  goes in `SYNC_HELLO.token` (already supported, `v2_host.go:374`).
- **Node**: the same API. Storage is a `PersistentFs` over a real directory, or the Node `fs` that `wasm_exec`
  wires by default. A Node service that can run a native binary should just run `kdb-service` with `--peer`; the
  replica is for code that must stay in-process.
- **Workers**: in the browser, the engine runs in a dedicated worker; the main thread holds only the `Client`
  over a `MessagePort` loopback. Sync, checkpoints and zstd then never block the UI. OPFS sync access handles
  (§5) are only available in a worker anyway.

### 3.2 Android/iOS: `kdbsync`

`AddPeer` gains the options it does not expose today: `filter`, `writeBack`, `catchUp`, `preferSnapshot`
(all already in `replication.PeerConfig`), plus a conflict callback and a change callback (gomobile
interfaces). A thin Kotlin module (`kdb-replica-android`, an AAR wrapping the gomobile output) gives it a Kotlin
API matching the TS one. This is PR 5.

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
- History below the shallow root is absent; `deepen` can fetch it on demand.

### 4.2 `filtered`: a projection, per-document read rights

- PROJECT_FETCH with a KDB-SQL filter. The host evaluates the filter **and** `DocumentReadAction` per document;
  documents the principal cannot read never arrive, and documents that stop matching (or lose read access) arrive
  as deletes.
- Kept in the local `<ns>.projection-<hash>` namespace with locally authored commits that name the source
  commit they reach. Locally queryable and indexable like any namespace.
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

The engine's own bounds (Tracks A–C) apply unchanged, because it is the same engine: off-heap graph, trie
slab and pages, bounded body cache, chunked adoption, squash's O(diff) install. What the replica adds is the
**filesystem underneath**, which is where the browser loses today.

| Platform | Storage | Today | Needed |
|---|---|---|---|
| Browser | `PersistentFs` → IndexedDB | **whole file tree resident in memory**; the README says memory ≈ data × 2 | **Paged `PersistentFs`**: metadata resident, 64 KiB chunks loaded on read, an LRU of clean chunks under `memory.budgetBytes`, dirty chunks pinned until their `fsync` transaction commits. Then an **OPFS** backend (sync access handles, in the worker) as the fast path where available, with IndexedDB as the fallback. |
| Node | real directory | fine | nothing |
| Android/iOS | app data dir via `kdbsync` | fine | nothing |

- **Budget defaults**: 64 MiB browser engine + 32 MiB fs page cache; set by `memory.budgetBytes`, split by the
  existing memory guard. The engine's history mode defaults to `NONE` on a replica: the replica keeps the head,
  and history comes from the cloud on demand (`deepen`, as-of reads through the cloud).
- **Quota**: the browser can evict storage. `openReplica` calls `navigator.storage.persist()` and reports the
  result. An evicted replica is detected on open (marker missing) and re-bootstraps; local unsynced writes are
  lost in that case, and `status()` says so ahead of time when persistence was denied.
- **Encryption at rest**: the store-level toggle (`kdb-spec-layer14`) applies unchanged, since it is the same
  engine. The key comes from the app. Not in the first PRs.

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
| `full`, multi-leader | A real local commit through the write gate | Commits accumulate in the local DAG; nothing else to queue | REF_UPDATE → host merges by policy. A Strict conflict returns a `ConflictReport` (`refUpdateAck.conflictBytes`) and lands in the local conflict queue; `db.on("conflict")` raises it, and the app resolves with `ResolveConflict` (existing). |
| `filtered` + `writeBack` | A local commit on the projection namespace (optimistic: visible at once) | The projection's DAG is the outbox (`PendingWrites`), oldest first, idempotent tx ids | PROJECT_WRITE applies each doc with a `baseHash` precondition. `conflict`/`refused` return `current[]`; `ResolveWrite` resets those docs to the source state and keeps the attempt. The replica turns that into the same `ConflictReport` shape for `db.on("conflict")`, so the app has one conflict type. |
| `single-home` namespace | **Only through write-back.** A full-mode replica of a single-home namespace is read-only locally (its commits would be fenced or refused with NOT_HOME). | Queued in the projection outbox; a NOT_HOME or unreachable home is a PEER_ERROR, so the write stays pending | Same as filtered. |

- So a single-home namespace is held in `filtered` mode (the filter can be `TRUE`). `openReplica` enforces this
  from the namespace policy in `_kdb/meta` (the replica reads its view through META_VIEW, as `ScopedMeta` does)
  and refuses `mode: "full"` with a clear error.
- **Ordering across namespaces**: a cross-namespace transaction written on the replica is pushed as its
  `CommitGroup` parts; projection write-back is per namespace. A group spanning a projection is refused locally
  rather than half-applied. This matches the distributed plan's rule for groups the replica cannot check.
- **Durability of the queue**: it is the engine's log, so it inherits `fsync`. A pending write survives reload
  and crash, as the existing `persist` tests prove for commits.

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
- **Two gaps to settle with B4** (raised in §10 D7):
  1. **Projections carry no attachment refs.** `ProjectPageMessage.writes` is `{id, body}`. A filtered replica
     needs `{id, body, attachment?: {hash, contentHash, size, mime}}`, plus the source commit (`atHex`, already
     there) for BLOB_FETCH's `commit`.
  2. **Upload from a replica.** A host adopting a pushed commit fetches missing blobs from the *sender*, but a
     replica serves nothing. A replica must upload first: the 0x44+ client messages (ATTACHMENT_BEGIN/CHUNK/END)
     on the sync connection, or a blob-push message on the sync protocol. Then it pushes the commit or
     PROJECT_WRITE that references them. Resumable for mobile.

## 10. Open decisions for the owner

**D1. Implementation route for TS.** *Recommended: (a).*
(a) The replica is the Go engine in `@kdb/embed`'s WASM, plus a JS-WebSocket transport. One implementation,
peersync unchanged, inherits squash, snapshot, projections, conflicts and attachments as they land.
Cost: a 4.7 MB brotli download, and browser memory depends on the paged fs (§5).
(b) A native TS replica: commit codec, trie, DAG, v2 client and a storage engine in TS. Small bundle, but it is
a third implementation of peersync, and the plan's "one pack format" then has to be enforced by golden tests
instead of by construction.

**D2. Kotlin/Android route.** *Recommended: (a) now, (b) only on demand.*
(a) Android/iOS via `kdbsync` (gomobile) with a Kotlin facade. No Kotlin peersync work.
(b) Kotlin-native v2 client in `kdb-peer-sync`: v2 messages, zstd packs, snapshot, squash, projections, write
back, plus shallow roots in Kotlin's replayer (it refuses them today, #209) and an Android target for the
storage modules. Several thousand lines, and a second client implementation to keep in step.
(c) Both: (a) for apps, plus a read-only Kotlin v2 pull (FETCH/PACK_PAGE + snapshot) to lift the v1 limit
for JVM tooling.

**D3. Filtered mode = projections (PROJECT_*).** *Recommended: yes.* A filtered replica cannot hold real source
commits without receiving documents it may not read, so the filtered path is the existing projection messages
rather than FETCH/PACK_PAGE. This reads "one pack format" as "no new client protocol", which projections satisfy,
rather than "only FETCH/PACK_PAGE". The verifiable alternative (commit format v2, `kdb-distributed-plan.md` §3.13)
stays gated.

**D4. Host fixes to projections (§4.3).** *Recommended: yes, in PR 2.* A cursor seek for reset and `DiffTrees`
for delta. These touch Go peersync, but only the projection paths, with no wire change.

**D5. Live updates.** *Recommended: poll in PR 1, `REFS_WATCH 0x50/0x51` in a later PR.* Or poll only, and
leave push to the app (for example a web-push "sync now").

**D6. Single-home namespaces on the replica** are filtered/write-back only, and full mode is refused.
*Recommended: yes.* The alternative is a full replica forwarding writes online-only, which gives up offline
writes.

**D7. Attachments gaps (§9).** *Recommended: ask B4 to add refs to PROJECT_PAGE in PR 5, and to allow blob
upload on the sync connection* (ATTACHMENT_BEGIN/CHUNK/END accepted on a peersync session). The replica's
attachment PR then follows B4's PR 5.

**D8. Browser storage engine.** *Recommended: a paged IndexedDB `PersistentFs` first (PR 4), OPFS second.* Or
OPFS-only (worker-only, simpler and faster, but Safari's OPFS sync handles are recent).

**D9. Replica history default.** *Recommended: `NONE`* (head only, history fetched on demand). `FULL` keeps
every version the replica has seen, which mostly matters for offline as-of reads.

## 11. PR plan

Each PR is tested against a real Go node: a `go/cmd/kdb-replicahost` helper (a file host with `--peer-http`,
auth on, a seeded namespace, printing `ready ws://…/kdb/sync`), spawned by `node --test` in the style of
`packages/kdb-client/test/interop.test.ts`. A Go-side test in `go/kdb/integration` checks the cloud's view
(hashes equal, conflicts queued, documents absent where not readable).

| PR | Scope | Tests |
|---|---|---|
| 1 | `transport/jsws` (js-only), the `replica` option in `go/wasm/embed`, `openReplica` in `@kdb/embed` with `full` mode, poll loop, `status`/`change` events, token callback | Node replica bootstraps from the Go host and catches up; local writes push; reopen keeps state; bad token refused; two replicas converge through the host |
| 2 | Snapshot bootstrap and `catchup=auto` defaults; `filtered` mode; projection host fixes (§4.3) | Long-offline full replica catches up by squash (asserts SQUASH_HEAD used and commits not replayed); filtered replica: filter, read rights, un-match deletes, id-range; reset cost O(page) (allocation test on the host) |
| 3 | Write-back, conflict events, single-home rules | Offline writes queue → reconnect → applied; a conflict surfaces as a `ConflictReport` and resolves; NOT_HOME keeps the write pending; full-mode refused for single-home |
| 4 | Paged `PersistentFs` (+ memory budget), worker host, OPFS backend | Resident bytes stay under budget while syncing a namespace larger than the budget; crash-recovery and reload tests from `persist.test.ts` pass on the paged fs; Chromium page test |
| 5 | `kdbsync` options + Kotlin facade (per D2) | Go test: device in filtered and full mode against a cloud; Kotlin facade unit tests under `./gradlew build` |
| 6 | Attachments (after B4 PR 5) | Lazy fetch, ranged resume after a dropped connection, upload then push, filtered replica receives only its documents' blobs |
| 7 | `REFS_WATCH`/`REFS_CHANGED` (if D5) | Change visible on the replica without a poll tick |
