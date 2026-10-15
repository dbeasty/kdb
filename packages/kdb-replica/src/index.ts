/**
 * @limidus/kdb-replica - a client-side KDB replica (docs/kdb-client-replica-plan.md).
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
export { ReplicaBusyError, type TreeNodeView } from "./namespace.ts";
export { PeerConnection, RemoteError, ProtocolError, type ConflictReport, type NamespaceRefs } from "./peer.ts";
export { syncNamespace, type CatchUp, type NamespaceSyncResult } from "./sync.ts";
export {
  openReplica,
  Replica,
  type ChangeEvent,
  type ConflictEvent,
  type ReplicaOptions,
  type ReplicaStatus,
} from "./replica.ts";
export {
  ProjectionNamespace,
  ReadOnlyProjectionError,
  projectionNamespace,
  type OutboxEntry,
  type WriteBackDoc,
} from "./projection.ts";
export { SingleHomeError, syncProjection, type ProjectionSyncResult } from "./sync.ts";
export {
  AttachmentEdit,
  AttachmentError,
  ContentHasher,
  attachmentContentHash,
  refHash,
  setHash,
  type Attachment,
  type AttachmentRef,
} from "./attachments.ts";
export { AttachmentRootMismatchError } from "./namespace.ts";
export { BlobStore, BlobUnavailableError, DEFAULT_BLOB_CACHE_BYTES, type BlobCacheStats } from "./blobs.ts";
export { parsePackedAttachmentTable } from "./attachments.ts";
