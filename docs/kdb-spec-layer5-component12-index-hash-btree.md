# KDB Component Spec — Layer 5
## Component 12: Index — Hash + B-tree
### `dev.kdb.index.hash` · `dev.kdb.index.btree`

**File:** `kdb-spec-layer5-component12-index-hash-btree.md`  
**Layer:** 5 — Index Implementations  
**Status:** Implementation-ready  
**Gradle modules:** `:kdb-index-hash`, `:kdb-index-btree`  
**Depends on:** Layer 0 (Codec, Error Model), Layer 1 (Document), Layer 2 (Schema, DAG), Layer 3 (Index Layer Core, Storage Adapter), Layer 4a (SSTable / MemTable / LSM patterns via `LsmBlobStore` or dedicated index segment layout)

-----

## 1. Purpose

Implements durable, version-aware `IndexStore` backends for `IndexType.HASH` and `IndexType.BTREE` — the two index kinds selected by `inferIndexType()` in Component 8 (`StringType`, `BoolType`, `UuidType`, `EnumType` → HASH; numeric and timestamp types → BTREE).

**Hash index** provides O(1) exact equality on a single schema field (including unique constraints). **B-tree index** provides ordered range scans, `ORDER BY` support, and composite keys (multi-field indexes declared via schema or `CREATE INDEX`).

Both implementations persist index segments through the namespace `StorageAdapter` (content-addressed blobs + optional MemTable overlay for hot writes), reuse Layer 4a LSM building blocks where practical, and honour historical reads via commit-stamped entries and DAG ancestry checks — matching the contracts already exercised by `MemoryIndexStore` in `:kdb-index`.

-----

## 2. Dependencies

| Module | Interfaces / types used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid`, `KdbValue`, `encodeToBytes` / `decodeFromBytes` for on-disk index records |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode`, `IndexCorruptionException` |
| `dev.kdb.document` | `KdbCommit` (commit hash validation) |
| `dev.kdb.dag` | `CommitDag.isAncestor`, `CommitDag.hasCommit` |
| `dev.kdb.schema` | `KdbFieldType`, `SchemaField` |
| `dev.kdb.index` (Component 8) | `IndexStore`, `IndexDescriptor`, `IndexEntry`, `IndexKey`, `compareIndexKeys`, `IndexStoreFactory`, `IndexType` |
| `dev.kdb.storage` (Component 9) | `StorageAdapter.putBlob` / `getBlob`, `StorageEngineConfig` |
| `dev.kdb.storage.sstable` (Layer 4a — optional) | `SsTableWriter`, `SsTableReader`, `MergedIterator` — for immutable index segment files |
| `dev.kdb.storage.memtable` (Layer 4a — optional) | `MemTable` pattern for mutable index write buffer |

-----

## 3. Public Interface

```kotlin
package dev.kdb.index.hash

import dev.kdb.dag.CommitDag
import dev.kdb.index.*
import dev.kdb.storage.StorageAdapter

/** Factory for HASH indexes. Registered in [CompositeIndexStoreFactory]. */
fun interface HashIndexStoreFactory {
    fun create(descriptor: IndexDescriptor, dag: CommitDag, storage: StorageAdapter): IndexStore
}

fun hashIndexStoreFactory(dag: CommitDag, storage: StorageAdapter): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.HASH) {
            "HashIndexStoreFactory expected HASH, got ${descriptor.type}"
        }
        DefaultHashIndexStore(descriptor, dag, storage)
    }

/**
 * Open-addressing hash table with append-only version log.
 * Unique indexes reject duplicate keys at [put] time.
 */
class DefaultHashIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
) : IndexStore {
    // IndexStore methods — see §5
}

/** On-disk root pointer for a hash index (content-addressed). */
data class HashIndexManifest(
    val indexId: KdbUuid,
    val tableGeneration: Long,
    val bucketSegmentHash: KdbHash,
    val tombstoneLogHash: KdbHash?,
)
```

```kotlin
package dev.kdb.index.btree

import dev.kdb.dag.CommitDag
import dev.kdb.index.*
import dev.kdb.storage.StorageAdapter

fun interface BtreeIndexStoreFactory {
    fun create(descriptor: IndexDescriptor, dag: CommitDag, storage: StorageAdapter): IndexStore
}

fun btreeIndexStoreFactory(dag: CommitDag, storage: StorageAdapter): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.BTREE) {
            "BtreeIndexStoreFactory expected BTREE, got ${descriptor.type}"
        }
        DefaultBtreeIndexStore(descriptor, dag, storage)
    }

/**
 * LSM-backed ordered index. Keys sorted via [compareIndexKeys].
 * Supports composite keys when [IndexDescriptor.fields] has length > 1.
 */
class DefaultBtreeIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
) : IndexStore {
    // IndexStore methods — see §5
}

data class BtreeIndexManifest(
    val indexId: KdbUuid,
    val generation: Long,
    val memtableOverlayHash: KdbHash?,
    val levelSegmentHashes: List<KdbHash>,   // L0 newest first
)
```

```kotlin
package dev.kdb.index

import dev.kdb.dag.CommitDag
import dev.kdb.storage.StorageAdapter

/**
 * Dispatches [IndexStoreFactory.create] to hash, btree, full-text, or vector
 * factories. Layer 5 index modules register here at node bootstrap.
 */
class CompositeIndexStoreFactory(
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val hashFactory: IndexStoreFactory = hashIndexStoreFactory(dag, storage),
    private val btreeFactory: IndexStoreFactory = btreeIndexStoreFactory(dag, storage),
    // fulltext + vector wired by Components 13–14
) : IndexStoreFactory {

    override fun create(descriptor: IndexDescriptor): IndexStore =
        when (descriptor.type) {
            IndexType.HASH -> hashFactory.create(descriptor)
            IndexType.BTREE -> btreeFactory.create(descriptor)
            IndexType.FULLTEXT, IndexType.VECTOR ->
                error("Register full-text/vector factories before creating ${descriptor.type}")
        }
}
```

-----

## 4. Data Structures

### `HashIndexManifest` / `BtreeIndexManifest`
Content-addressed roots stored under a stable namespace key derived from `indexId` (not field name — survives renames during schema migration). Generation monotonically increases on flush.

### `IndexRecord` (wire, both types)
Layer 0 record shape (registered in module-local `KdbTypeRegistry`):

| Field | Type | Notes |
|---|---|---|
| `docId` | `Uuid` | Document id |
| `keyBytes` | `Bytes` | Canonical `IndexKey` encoding (see §9) |
| `commitHash` | `Bytes` (32) | Commit that wrote this entry |
| `seq` | `Int64` | Monotonic write sequence within index generation |

### `VersionedBucket` (hash, in-memory)
`Map<IndexKey, LinkedHashMap<KdbUuid, IndexRecord>>` plus tombstone set per `docId` keyed by superseding commit.

### `BtreeNode` (btree, on-disk)
Fixed fan-out B-tree node: separator keys, child segment hashes, leaf flag. Leaf values are `(IndexKey, docId, commitHash)` sorted by `compareIndexKeys` then `docId`.

### `IndexKey` encoding (canonical bytes)
Deterministic order-preserving byte encoding shared by hash and btree (must match `compareIndexKeys` ordering):

```
[typeTag:1][payload…]
  STRING  → 0x01 + utf8
  INT32   → 0x02 + big-endian int
  INT64   → 0x03 + big-endian long
  FLOAT64 → 0x04 + ieee754 bits (total order via bits pattern)
  BOOL    → 0x05 + 0/1
  TIMESTAMP → 0x06 + epoch micros
  UUID    → 0x07 + 16 bytes
  COMPOSITE → 0x08 + concat(child encodings)
  NULL    → 0x00
```

-----

## 5. Contracts

### Shared `IndexStore` semantics (both implementations)

| Method | Preconditions | Postconditions |
|---|---|---|
| `put` | `entry.commitHash` exists in DAG. If `descriptor.unique`, no other `docId` may hold the same key at HEAD after write. | `lookup(entry.key)` includes `entry.docId` at HEAD; historical `atCommit` sees entry iff commit is ancestor. |
| `delete` | — | Latest HEAD lookup excludes `docId`; older commits unchanged. |
| `bulkLoad` / `rebuild` | Entries represent one consistent snapshot commit. | Replaces all prior data; `isValid(thatCommit)` true. |
| `lookup` | HASH: exact key. BTREE: exact key (point query). | Throws `IndexTypeMismatchException` if wrong store type. |
| `range` | BTREE only; HASH throws `IndexTypeMismatchException`. | Keys in `[from, to]` inclusive per `compareIndexKeys`; obeys `limit` and `ascending`. |
| `search` / `nearestNeighbours` | — | Always throw `IndexTypeMismatchException`. |
| `snapshot` / `restoreSnapshot` | Browser enlistment eviction path. | Round-trip preserves logical index state at HEAD. |
| `isValid` | — | True when manifest generation matches replayed DAG head or rebuild completed. |

### `DefaultHashIndexStore.put` (unique)
If `descriptor.unique` and another `docId` already maps to `entry.key` at HEAD, throw `UniqueIndexViolationException` before persisting.

### Historical reads (`atCommit != null`)
Replay write log filtered with `dag.isAncestor(entry.commitHash, atCommit)`. Deletes visible only if delete commit is ancestor of `atCommit` and no later write to same `docId` is also ancestral.

### Flush / durability
Mutable state may live in memory; `put`/`delete` append to WAL-style index log segment stored via `StorageAdapter.putBlob`. Background flush compacts into `HashIndexManifest` / `BtreeIndexManifest`. Crash recovery replays log from last manifest generation.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `UniqueIndexViolationException` | HASH/BTREE unique index; second document with same key at HEAD. |
| `IndexTypeMismatchException` | `range` on HASH; `lookup` with `CompositeKey` on single-field hash index. |
| `IndexCorruptionException` | Manifest decode failure, orphan segment hash, key order violation in btree node. |
| `IndexRebuildException` | `rebuild` cannot read documents from `StorageAdapter`. |

```kotlin
class UniqueIndexViolationException(
    message: String,
    val namespaceId: String,
    val fieldName: String,
    val key: IndexKey,
    val existingDocId: KdbUuid,
    val incomingDocId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `hash_putLookup_exact` | HASH index; put `StringKey("a")` → doc1. | `lookup(StringKey("a"))` == `[doc1]`. |
| 2 | `hash_unique_rejectsDuplicate` | Unique HASH; put doc1 key K, put doc2 key K. | `UniqueIndexViolationException`; doc1 still sole owner. |
| 3 | `hash_delete_historicalPreserved` | Put at H1, delete at H2. `lookup(atCommit=H1)`. | Still returns doc. HEAD returns `[]`. |
| 4 | `btree_range_inclusive` | Insert Int32 keys 1,3,5,7. Range [3,5]. | Doc ids for 3 and 5 only, ascending. |
| 5 | `btree_composite_order` | Composite `(name, age)` keys; range from `("bob",0)` to `("bob",99)`. | Correct subset and order. |
| 6 | `btree_descendingLimit` | Keys 1..10. `range(ascending=false, limit=3)`. | Three highest keys' docs. |
| 7 | `bulkLoad_replacesPrior` | Put 100 entries, `bulkLoad` 10 different entries. | Only 10 visible at HEAD. |
| 8 | `snapshot_roundTrip` | Build index with 50 entries; `snapshot` → new store `restoreSnapshot`. | Identical lookups at HEAD. |
| 9 | `manifest_survivesReopen` | Flush index, close factory, reopen with same storage blobs. | Lookups match pre-close. |
| 10 | `hash_range_throws` | HASH store `range(...)`. | `IndexTypeMismatchException`. |
| 11 | `corruptManifest_throws` | Truncate manifest bytes. Open index. | `IndexCorruptionException`. |
| 12 | `rebuild_fromStorage_scan` | Empty index; 20 docs in storage at commit H. `rebuild(entries)`. | All 20 keys queryable. |

-----

## 8. Non-Goals

- **Full-text tokenisation** — Component 13.
- **Vector ANN / HNSW** — Component 14.
- **SQL parsing or index selection** — Component 15.
- **Cross-namespace indexes** — v1 out of scope.
- **GPU-accelerated index build** — future; CPU path only here.
- **Automatic index type migration** when schema field type changes — Component 8 `syncSchema` drops and recreates stores; this module does not migrate data in place.

-----

## 9. Implementation Notes

### Module split
`:kdb-index-hash` (~800 NBNC) and `:kdb-index-btree` (~3,500 NBNC) keep hash logic small and btree LSM complexity isolated. Both depend on `:kdb-index` (interfaces only).

### Physical storage key namespace
Index blobs use a dedicated prefix in `StorageAdapter` separate from document blobs, e.g. hash `index/{namespaceId}/{indexId}/gen/{n}/…` — exact path built via `SegmentNameBuilder` patterns from Layer 4a 10g.

### Reuse Layer 4a SSTable
B-tree immutable levels may be written with `SsTableWriter` using encoded `IndexKey` as sort key and `(docId, commitHash)` as value. Hash buckets may use simpler append-only segment files (not necessarily SSTable) for v1.

### HEAD fast path
When `atCommit == null`, maintain a materialised `HEAD` map updated synchronously on `put`/`delete` to avoid full log replay — mirror `MemoryIndexStore` semantics with persistence underneath.

### Composite indexes
`IndexDescriptor.fields` lists field names in order; `IndexWriter` (Component 8) must construct `CompositeKey` parts when `fields.size > 1`. Extend `DefaultIndexWriter.matchingStores` in a follow-up if not already emitting composite keys for multi-field schema indexes (v1 may ship single-field btree only; composite documented as required for `CREATE INDEX (a,b)` from Component 15).

### KMP
Pure `commonMain`. No `expect/actual`. All I/O via `StorageAdapter`.

### `CompositeIndexStoreFactory`
Lives in `:kdb-index` or a thin `:kdb-index-factories` module to avoid circular deps — prefer `:kdb-index` default factory extension function that accepts optional fulltext/vector factories.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `IndexKey` canonical encoding | 120 |
| `DefaultHashIndexStore` + manifest | 450 |
| Hash flush / recovery | 200 |
| `DefaultBtreeIndexStore` + nodes | 1,400 |
| Btree LSM levels + compaction hook | 900 |
| `CompositeIndexStoreFactory` | 80 |
| Exceptions + wire registry | 80 |
| Unit + integration tests (both modules) | 1,200 |
| **Total** | **~4,430** |
