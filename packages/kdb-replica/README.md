# @kdb/replica

A client-side KDB replica for browsers and Node. It holds namespaces locally, syncs them with a KDB
node over peer sync v2 (the same messages nodes use), and keeps working offline. The design is in
[docs/kdb-client-replica-plan.md](../../docs/kdb-client-replica-plan.md).

```ts
import { openReplica, IndexedDbStore } from "@kdb/replica";

const db = await openReplica({
  store: await IndexedDbStore.open("my-app"),
  peer: { url: "wss://api.example.com/kdb/sync", token: () => auth.currentToken() },
  namespaces: [
    { ns: "app/u/42" },                                                      // the whole namespace
    { ns: "shop/orders", mode: "filtered", filter: "region = 'EU'", writeBack: true },
  ],
});
await db.put("app/u/42", id, { theme: "dark" }); // local, durable, pushed in the background
await db.get("app/u/42", id);
db.on("change", (e) => render(e.ids));
db.on("conflict", (e) => askUser(e.report));
```

**Status:** whole-namespace replicas, filtered replicas with write-back, live updates (REFS_WATCH) and
browsers (IndexedDB, zstd packs decoded in TS).

| Part | What it is |
|---|---|
| `openReplica` / `Replica` | Local reads and writes, sync rounds (poll, after a write, `syncNow()`), `change` / `conflict` / `sync` events, `status()`. |
| Sync (`syncNamespace`) | Pushes local commits (REF_UPDATE; the host merges, the replica never does). Pulls with the messages nodes use: a snapshot (SNAPSHOT_FETCH) for an empty namespace, then either replay (FETCH/PACK_PAGE) or squash (SQUASH_HEAD + TREE_NODES + OBJECT_FETCH), chosen like Go's `catchup=auto`. |
| Filtered (`ProjectionNamespace`, `syncProjection`) | PROJECT_FETCH: the documents matching a filter that the token may read, kept by delta (or reset) transfers that resume when interrupted. With `writeBack`, local writes apply at once, queue in a durable outbox, and go to the source by PROJECT_WRITE. A conflict or refusal puts the source's state back and raises `conflict`. A single-home source defers writes until its home is reachable. |
| Attachments | `attachments`, `readAttachment` (lazy BLOB_FETCH, every chunk verified against the content hash; fetched bytes are an LRU cache bounded by `blobCacheBytes`, default 256 MiB - see `blobCache()` / `evictBlobs()`), `putAttachment` / `deleteAttachment` (full mode, and filtered mode with write-back: bytes upload before the commit or write-back that references them). Each commit's attachment root is checked like its tree hash. |
| Live updates | While `live` (the default), the connection stays open and the host reports head moves (REFS_WATCH), so remote writes arrive without a poll. Polling continues every 60 s as a fallback. |
| `zstdDecompress` (internal) | A TS port of kdb-compression's zstd decoder, so browsers take compressed packs. Node uses its native decoder. |
| `decodeCommit` / `buildCommit` / `genesisCommit` | The canonical `CommitPayload` codec. Hashes are SHA-256 of the payload bytes, as in Go. |
| `TrieEdit` / `TrieReader` | KDB's 16-ary Merkle document trie, stored node by node so it is never resident. |
| `MemoryStore`, `IndexedDbStore`, `CachedStore` | An ordered key-value store with atomic, durable batches, and a byte-bounded read cache. |
| `ReplicaNamespace` | One namespace: fast-forward (verifying the target's tree hash), resumable convergence by snapshot or diff, local commits, reads and scans. |

## Browser smoke test

`test/browser/smoke.html` runs the replica in a real browser against a Go node. Run `go run ./cmd/kdb-replicahost
-dir $(mktemp -d) -serve -seed shop/browser:20 -seed shop/browser-eu:5` in `go/`, serve the repository root over
HTTP, and open the page with `?ws=<the host's ws URL>`.

## Compatibility

Every hash is checked against vectors from the Go code a node runs (`test/golden`, written by
`cd go && go run ./cmd/kdb-tsfixtures`). CI regenerates them and fails if they drift.

## Tests

The suite includes a live interop run against a real Go node (`go/cmd/kdb-replicahost`, built on
first use; skipped without a Go toolchain).


```bash
npm install && npm test
```
