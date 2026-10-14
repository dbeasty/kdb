# @kdb/replica

A client-side KDB replica for browsers and Node. It holds namespaces locally, syncs them with a KDB
node over peer sync v2 (the same messages nodes use), and keeps working offline. The design is in
[docs/kdb-client-replica-plan.md](../../docs/kdb-client-replica-plan.md).

**Status: core only.** This release has the parts every later slice builds on:

| Part | What it is |
|---|---|
| `decodeCommit` / `buildCommit` | The canonical `CommitPayload` codec. Hashes are SHA-256 of the payload bytes, as in Go. |
| `TrieEdit` / `TrieReader` | KDB's 16-ary Merkle document trie, stored node by node so it is never resident. |
| `MemoryStore`, `IndexedDbStore`, `CachedStore` | An ordered key-value store with atomic, durable batches, and a byte-bounded read cache. |
| `ReplicaNamespace` | A namespace that fast-forwards through a commit range (verifying the target's tree hash), makes local commits, and serves reads and scans. |

Peer sync (bootstrap, catch-up, push, projections, write-back) comes in the next slices.

## Compatibility

Every hash is checked against vectors from the Go code a node runs (`test/golden`, written by
`cd go && go run ./cmd/kdb-tsfixtures`). CI regenerates them and fails if they drift.

## Tests

```bash
npm install && npm test
```
