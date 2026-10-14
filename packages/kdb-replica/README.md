# @kdb/replica

A client-side KDB replica for browsers and Node. It holds namespaces locally, syncs them with a KDB
node over peer sync v2 (the same messages nodes use), and keeps working offline. The design is in
[docs/kdb-client-replica-plan.md](../../docs/kdb-client-replica-plan.md).

```ts
import { openReplica, IndexedDbStore } from "@kdb/replica";

const db = await openReplica({
  store: await IndexedDbStore.open("my-app"),
  peer: { url: "wss://api.example.com/kdb/sync", token: () => auth.currentToken() },
  namespaces: [{ ns: "app/u/42" }],
});
await db.put("app/u/42", id, { theme: "dark" }); // local, durable, pushed in the background
await db.get("app/u/42", id);
db.on("change", (e) => render(e.ids));
db.on("conflict", (e) => askUser(e.report));
```

**Status: whole-namespace replicas** (plan PR 2). Filtered replicas with write-back come next.

| Part | What it is |
|---|---|
| `openReplica` / `Replica` | Local reads and writes, sync rounds (poll, after a write, `syncNow()`), `change` / `conflict` / `sync` events, `status()`. |
| Sync (`syncNamespace`) | Pushes local commits (REF_UPDATE; the host merges, the replica never does). Pulls with the messages nodes use: a snapshot (SNAPSHOT_FETCH) for an empty namespace, then either replay (FETCH/PACK_PAGE) or squash (SQUASH_HEAD + TREE_NODES + OBJECT_FETCH), chosen like Go's `catchup=auto`. |
| `decodeCommit` / `buildCommit` / `genesisCommit` | The canonical `CommitPayload` codec. Hashes are SHA-256 of the payload bytes, as in Go. |
| `TrieEdit` / `TrieReader` | KDB's 16-ary Merkle document trie, stored node by node so it is never resident. |
| `MemoryStore`, `IndexedDbStore`, `CachedStore` | An ordered key-value store with atomic, durable batches, and a byte-bounded read cache. |
| `ReplicaNamespace` | One namespace: fast-forward (verifying the target's tree hash), resumable convergence by snapshot or diff, local commits, reads and scans. |

## Compatibility

Every hash is checked against vectors from the Go code a node runs (`test/golden`, written by
`cd go && go run ./cmd/kdb-tsfixtures`). CI regenerates them and fails if they drift.

## Tests

The suite includes a live interop run against a real Go node (`go/cmd/kdb-replicahost`, built on
first use; skipped without a Go toolchain).


```bash
npm install && npm test
```
