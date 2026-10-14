/**
 * @kdb/replica - a client-side KDB replica (docs/kdb-client-replica-plan.md).
 *
 * This first slice is the replica's core: the canonical commit codec, KDB's document trie, the
 * key-value stores, and a namespace that fast-forwards through commits and makes local ones.
 * Every hash is byte-identical to Go's, which test/golden (written by `go run ./cmd/kdb-tsfixtures`)
 * checks. Peer sync arrives in the next slice.
 */

export { toHex, fromHex, uuidBytes, uuidString, randomUuid } from "./bytes.ts";
export { sha256, sha256Portable } from "./sha256.ts";
export {
  buildCommit,
  contentHash,
  decodeCommit,
  decodeCommitList,
  encodeCommitList,
  encodeCommitPayload,
  type Commit,
  type CommitFields,
  type Op,
} from "./commit.ts";
export { DecodeError } from "./codec.ts";
export { CachedStore, MemoryStore, prefixEnd, type Batch, type KvStore } from "./store.ts";
export { IndexedDbStore } from "./idb.ts";
export { TrieEdit, TrieReader, foldLeaf, leafHash } from "./trie.ts";
export {
  NotFastForwardError,
  ReplicaNamespace,
  TreeMismatchError,
  netEffect,
  topoSort,
  type NamespaceMeta,
} from "./namespace.ts";
