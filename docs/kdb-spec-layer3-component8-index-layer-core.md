# KDB Component Spec — Layer 3
## Component 8: Index Layer — Core
### `dev.kdb.index`

**File:** `kdb-spec-layer3-component8-index-layer-core.md`
**Layer:** 3 — Write Path
**Depends on:** Layer 0 (BSON Codec, Error Model), Layer 1 (Document + Commit Model, JSON Functions Engine), Layer 2 (Schema Engine, Commit DAG)

---

## 1. Purpose

The Index Layer Core is the registry, lifecycle, and consistency manager for all KDB indexes. It does not implement any specific index algorithm — that is the job of Layer 5 (B-tree, full-text, vector). It defines the `IndexStore` interface that all index implementations satisfy, manages the mapping from namespace+schema to a set of live `IndexStore` instances, drives index writes after every committed transaction, and provides the read path that the SQL Query Engine calls to locate documents.

A key responsibility is version awareness: every index operation is stamped with a commit hash, enabling historical checkout queries to produce consistent historical index state without materialising a separate index per commit.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash`, `KdbTimestamp`, `BsonDocument` |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode`, `KdbResult`, `IndexCorruptionException` |
| `dev.kdb.document` | `KdbDocument`, `KdbCommit`, `KdbOp`, `DocumentTree` |
| `dev.kdb.json` | `kdbJsonGet`, `JsonValue` |
| `dev.kdb.schema` | `KdbSchema`, `SchemaField`, `KdbFieldType`, `SchemaDiff` |
| `dev.kdb.dag` | `CommitDag`, `CommitDiff` |
| `dev.kdb.storage` (Component 9) | `StorageAdapter` (document body reads during rebuild) |

---

## 3. Public Interface

```kotlin
package dev.kdb.index

import dev.kdb.codec.*
import dev.kdb.document.*
import dev.kdb.error.*
import dev.kdb.json.*
import dev.kdb.schema.*
import dev.kdb.dag.*
import dev.kdb.storage.StorageAdapter

// ── Index type ────────────────────────────────────────────────────────────────

enum class IndexType {
    HASH,       // exact equality; O(1); implemented in Layer 5
    BTREE,      // range, ordering, composite; implemented in Layer 5
    FULLTEXT,   // tokenised keyword + fuzzy; implemented in Layer 5
    VECTOR,     // ANN semantic search (HNSW); implemented in Layer 5
}

// ── Index descriptor ──────────────────────────────────────────────────────────

/** Immutable description of one index. Created by the Index Registry from schema. */
data class IndexDescriptor(
    val indexId: KdbUuid,
    val namespaceId: String,
    val fieldName: String,              // "" for multi-field (composite) indexes
    val fields: List<String>,           // ordered field list for composite indexes
    val type: IndexType,
    val unique: Boolean,
    val schemaVersion: Int,             // schema version that introduced this index
    val createdAtHash: KdbHash,         // commit hash at which this index became active
)

// ── Index entry ───────────────────────────────────────────────────────────────

/** A single entry written into an index. */
data class IndexEntry(
    val docId: KdbUuid,
    val key: IndexKey,
    val commitHash: KdbHash,            // the commit that produced this entry
)

/** Typed key variant per field type. */
sealed class IndexKey {
    data class StringKey(val value: String)          : IndexKey()
    data class Int32Key(val value: Int)              : IndexKey()
    data class Int64Key(val value: Long)             : IndexKey()
    data class Float64Key(val value: Double)         : IndexKey()
    data class BoolKey(val value: Boolean)           : IndexKey()
    data class TimestampKey(val epochMillis: Long)   : IndexKey()
    data class UuidKey(val id: KdbUuid)              : IndexKey()
    data class VectorKey(val embedding: FloatArray)  : IndexKey()
    data class CompositeKey(val parts: List<IndexKey>) : IndexKey()
    object NullKey                                   : IndexKey()
}

fun indexKeyFromJsonValue(value: JsonValue?, fieldType: KdbFieldType): IndexKey

// ── Index store (implemented by each index type in Layer 5) ───────────────────

interface IndexStore {

    val descriptor: IndexDescriptor

    // Write

    /** Insert or update the index entry for a document at this commit. */
    suspend fun put(entry: IndexEntry)

    /** Remove the index entry for a document (called on KdbOp.Delete). */
    suspend fun delete(docId: KdbUuid, atCommit: KdbHash)

    /** Bulk-load entries during index rebuild. More efficient than repeated put(). */
    suspend fun bulkLoad(entries: List<IndexEntry>)

    // Read

    /** Exact lookup by key. Returns all matching document IDs. */
    suspend fun lookup(key: IndexKey, atCommit: KdbHash? = null): List<KdbUuid>

    /** Range scan. Both bounds are inclusive when non-null. */
    suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
        ascending: Boolean = true,
    ): List<KdbUuid>

    /** Full-text search. Only valid for FULLTEXT indexes. */
    suspend fun search(query: String, atCommit: KdbHash? = null, limit: Int = Int.MAX_VALUE): List<KdbUuid>

    /** Vector ANN search. Only valid for VECTOR indexes. */
    suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash? = null,
    ): List<RankedResult>

    // Lifecycle

    /** Called during engine start if the index is new or schema has changed. */
    suspend fun rebuild(entries: List<IndexEntry>)

    /** Clear all data — called when the namespace is dropped or index is removed. */
    suspend fun clear()

    /** True if the index has valid data at the specified commit. */
    suspend fun isValid(atCommit: KdbHash): Boolean

    /** Serialise internal state for snapshot (browser/eviction use). */
    suspend fun snapshot(): ByteArray

    /** Restore from snapshot produced by [snapshot]. */
    suspend fun restoreSnapshot(data: ByteArray)
}

data class RankedResult(val docId: KdbUuid, val score: Float)

// ── Index registry ────────────────────────────────────────────────────────────

/** Owns all IndexStore instances for one namespace. One registry per namespace. */
interface IndexRegistry {

    val namespaceId: String

    /** All currently active indexes for this namespace. */
    val indexes: List<IndexStore>

    /** Retrieve a specific index by field name and type. */
    fun get(fieldName: String, type: IndexType): IndexStore?

    /** Retrieve a specific index by descriptor ID. */
    fun getById(indexId: KdbUuid): IndexStore?

    /**
     * Synchronise the registry with a new schema.
     * - Creates new IndexStore instances for fields added in [newSchema].
     * - Marks removed index fields for async rebuild/teardown.
     * - Returns a [SchemaSyncResult] describing what changed.
     */
    suspend fun syncSchema(
        oldSchema: KdbSchema,
        newSchema: KdbSchema,
        storeFactory: IndexStoreFactory,
        dag: CommitDag,
        storage: StorageAdapter,
    ): SchemaSyncResult
}

data class SchemaSyncResult(
    val created: List<IndexDescriptor>,
    val removed: List<IndexDescriptor>,
    val unchanged: List<IndexDescriptor>,
    val rebuilding: List<IndexDescriptor>,   // async rebuild scheduled
)

// ── Index writer ──────────────────────────────────────────────────────────────

/**
 * Drives index writes after a transaction commits.
 * Called by the layer above (Transaction Engine / Storage Manager) once a
 * [KdbCommit] is durably in the DAG.
 */
interface IndexWriter {

    /**
     * Apply a committed transaction's operations to all registered indexes.
     *
     * For each [KdbOp.Write]: extracts schema field values from the new document
     * and calls [IndexStore.put] on every relevant index.
     * For each [KdbOp.Delete]: calls [IndexStore.delete] on every index.
     * [KdbOp.FileWrite] and [KdbOp.SchemaMigration] are handled separately (schema sync).
     */
    suspend fun applyCommit(
        commit: KdbCommit,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
    )

    /**
     * Rebuild all indexes for a namespace from scratch by replaying the
     * document tree at [fromCommit].
     * Called when an index is first created on an existing namespace, or after
     * snapshot restore fails integrity check.
     */
    suspend fun rebuildAll(
        fromCommit: KdbHash,
        dag: CommitDag,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
        onProgress: ((rebuilt: Int, total: Int) -> Unit)? = null,
    )
}

// ── Index reader (used by SQL Query Engine, Layer 5) ─────────────────────────

interface IndexReader {

    /** Exact equality lookup, optionally at a historical commit. */
    suspend fun lookupExact(
        registry: IndexRegistry,
        fieldName: String,
        key: IndexKey,
        atCommit: KdbHash? = null,
    ): List<KdbUuid>

    /** Range query, optionally at a historical commit. */
    suspend fun lookupRange(
        registry: IndexRegistry,
        fieldName: String,
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
        ascending: Boolean = true,
    ): List<KdbUuid>

    /** Full-text query. */
    suspend fun lookupFullText(
        registry: IndexRegistry,
        fieldName: String,
        query: String,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<KdbUuid>

    /** Vector ANN query. */
    suspend fun lookupVector(
        registry: IndexRegistry,
        fieldName: String,
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash? = null,
    ): List<RankedResult>
}

// ── Index store factory (implemented per index type in Layer 5) ───────────────

fun interface IndexStoreFactory {
    fun create(descriptor: IndexDescriptor): IndexStore
}

// ── Index manager (top-level facade) ─────────────────────────────────────────

/**
 * Top-level facade owned by the Storage Manager. One instance per node.
 * Manages all per-namespace [IndexRegistry] instances.
 */
interface IndexManager {

    /** Get or create the registry for a namespace. */
    fun registryFor(namespaceId: String): IndexRegistry

    /** Release a registry and all its indexes (called on namespace drop). */
    suspend fun releaseRegistry(namespaceId: String)

    val writer: IndexWriter
    val reader: IndexReader
}

fun indexManager(storeFactory: IndexStoreFactory): IndexManager

// ── Index hint (used by stream protocol to pre-compute browser index updates) ──

/**
 * Pre-computed index update shipped with a [DeltaCommit] to allow Mode 1/2
 * browser clients to update their local indexes without re-evaluating documents.
 */
data class IndexHint(
    val indexId: KdbUuid,
    val fieldName: String,
    val type: IndexType,
    val action: IndexHintAction,
    val docId: KdbUuid,
    val key: IndexKey?,   // null for DELETE hints
    val commitHash: KdbHash,
)

enum class IndexHintAction { PUT, DELETE }

fun IndexHint.toBson(): BsonDocument
fun IndexHint.Companion.fromBson(bson: BsonDocument): IndexHint

// ── Exceptions ────────────────────────────────────────────────────────────────

class IndexNotFoundException(
    message: String,
    val namespaceId: String,
    val fieldName: String,
    val type: IndexType,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

class IndexTypeMismatchException(
    message: String,
    val fieldName: String,
    val expectedType: IndexType,
    val actualType: IndexType,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

class IndexRebuildException(
    message: String,
    val namespaceId: String,
    val indexId: KdbUuid,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
```

---

## 4. Data Structures

### `IndexDescriptor`
Immutable record that fully identifies an index. Created by `IndexRegistry.syncSchema`. Stored alongside the index data so it can be validated on reload.

### `IndexKey`
Typed sum type covering all supported KDB field types plus a `CompositeKey` for multi-field indexes and `NullKey` for nullable fields that are absent. `VectorKey` holds a float array (embeddings). Structural equality on `CompositeKey` compares each part in order.

### `IndexEntry`
The unit of write for all index types. The `commitHash` allows version-aware indexes to tag each entry with the commit that produced it, enabling historical reads without separate index snapshots.

### `IndexHint`
Wire-format hint included in `DeltaCommit` messages. Allows browser Mode 1/2 clients to update local indexes without computing document diffs. The `IndexWriter.applyCommit` path produces these hints as a side-product of writing to indexes.

---

## 5. Contracts

### `IndexStore.put`
**Preconditions:** `entry.commitHash` must be a committed hash (not pending). Inserting the same `docId` + `commitHash` twice is idempotent.
**Postconditions:** `lookup(entry.key)` returns `entry.docId` for queries at `entry.commitHash` or any later commit (until a `delete` supersedes it).

### `IndexStore.delete`
**Postconditions:** `lookup` for the deleted `docId`'s key at `atCommit` and later returns a result set that excludes this docId. Earlier commits (historical reads) are unaffected.

### `IndexStore.rebuild`
**Preconditions:** All `IndexEntry` values represent the full document set at a single commit.
**Postconditions:** All prior entries are replaced. `isValid(entries.first().commitHash)` returns true.

### `IndexRegistry.syncSchema`
**Invariant:** After `syncSchema` returns, `registry.indexes` is exactly the set implied by `newSchema`. Removed indexes' stores are cleared asynchronously; the registry stops routing queries to them immediately.

### `IndexWriter.applyCommit`
**Preconditions:** `commit` is already durably appended to the DAG before this is called (the Transaction Engine commits first).
**Postconditions:** All indexes in `registry` reflect the state of every document affected by `commit.operations`.

### `IndexReader` methods
**Thread safety:** All `IndexReader` methods are safe to call concurrently from multiple coroutines. They are read-only and do not mutate registry or store state.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `IndexNotFoundException` | `IndexReader` is asked for a field/type combo that has no registered store. |
| `IndexTypeMismatchException` | A query operation (e.g. `range`) is applied to an index whose type does not support it (e.g. `HASH`). |
| `IndexCorruptionException` (from Error Model) | `isValid` returns false; detected during startup integrity check or during a read. |
| `IndexRebuildException` | `rebuildAll` fails mid-way (storage read error, deserialization failure). |

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `putAndLookupExact_hashIndex` | Put `IndexEntry(docId=A, key=StringKey("alice"), ...)`. Lookup exact `StringKey("alice")`. | Returns `[A]`. |
| 2 | `putAndRange_btreeIndex` | Insert 5 docs with `Int32Key` values 1–5. Range query [2, 4]. | Returns doc IDs for values 2, 3, 4 in ascending order. |
| 3 | `delete_removesFromLookup` | Put entry for docId A, then delete A at a later commit. Lookup at latest commit. | Returns `[]`. Historical lookup at the earlier commit still returns `[A]`. |
| 4 | `historicalRead_atOlderCommit` | Insert at commit H1, delete at H2. Lookup `atCommit = H1`. | Returns `[A]`. Lookup `atCommit = H2` returns `[]`. |
| 5 | `syncSchema_createsNewIndex` | Schema gains a new indexed field. Call `syncSchema`. | `SchemaSyncResult.created` has one entry; new `IndexStore` in registry. |
| 6 | `syncSchema_removesDroppedIndex` | Schema drops an indexed field. Call `syncSchema`. | `SchemaSyncResult.removed` has one entry; registry no longer routes queries to it. |
| 7 | `applyCommit_updatesAllIndexes` | Schema has 3 indexed fields. Commit writes a document with all 3 fields set. | All 3 IndexStore instances contain an entry for the new docId. |
| 8 | `applyCommit_deleteOp_removesEntries` | A commit contains `KdbOp.Delete(docId)`. | All IndexStore instances call `delete(docId, commitHash)`. |
| 9 | `rebuildAll_fromExistingNamespace` | Namespace has 50 documents at `HEAD`. Call `rebuildAll`. | All index stores contain correct entries for all 50 documents. `onProgress` called with increments summing to 50. |
| 10 | `indexHintProduced_withApplyCommit` | `applyCommit` called after a write commit. Caller collects hints. | One `IndexHint` per indexed field per written document; action = PUT. Delete ops produce DELETE hints. |
| 11 | `nullKey_skippedForRequired` | Schema field is `required = true` but document missing the field (schema error path). | `IndexStore.put` is not called for that field (schema validation upstream prevents this; test verifies index is not poisoned). |
| 12 | `compositeKey_orderMatters` | Two docs with same field-1 value but different field-2 values. Composite range query. | Correct ordering by composite key; both docs present; order matches composite sort. |

---

## 8. Non-Goals

- **B-tree, full-text, or vector algorithm implementations** — those are Layer 5 components. This component only defines the `IndexStore` interface they implement.
- **SQL query planning** — the SQL DSL/planner (Layer 5) decides which index to use; the Index Layer Core provides the read interface but does not choose among indexes.
- **Index compression or physical layout** — each `IndexStore` implementation is responsible for its own physical storage format.
- **Cross-namespace index joins** — out of scope for v1.
- **Index creation DDL via SQL** — `CREATE INDEX` SQL syntax is a Layer 5 / JDBC concern.

---

## 9. Implementation Notes

### Version-aware reads

The simplest correct approach for historical reads: every `IndexEntry` carries the commit hash at which it was written. For a lookup `atCommit = H`, the index returns only entries whose `commitHash` is an ancestor of `H` (or `H` itself) and which have not been superseded by a later delete/write also ancestral to `H`. This requires the `IndexStore` implementation to track deletion tombstones per commit hash.

For the hot path (latest HEAD), `atCommit = null` resolves to the current HEAD and the index can use a simpler non-versioned read path if the implementation chooses to optimise it.

### `IndexHint` production

`IndexWriter.applyCommit` iterates operations. For each `KdbOp.Write`, it extracts field values via `kdbJsonGet`, constructs `IndexKey` instances, calls `IndexStore.put`, and emits an `IndexHint` per index entry written. These hints are collected and returned via the `applyCommit` result (or passed to a hint sink injected at construction) so the Wire Protocol layer can include them in `DeltaCommit` messages.

### Schema sync and rebuild scheduling

`IndexRegistry.syncSchema` is synchronous on the "which indexes exist" question and asynchronous on the "rebuild new indexes" work. The registry immediately routes queries to new (but empty) stores. The rebuild runs in a coroutine. Queries against a rebuilding index return partial results until rebuild completes; the `IndexStore.isValid` flag lets callers detect this.

### Kotlin Multiplatform

Pure `commonMain`. No `expect/actual`. All physical I/O is inside the `IndexStore` implementations (Layer 5), which themselves delegate to the `StorageAdapter` (Component 9) for raw byte storage.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `IndexKey` hierarchy + `indexKeyFromJsonValue` | 150 |
| `IndexDescriptor`, `IndexEntry`, `RankedResult` | 80 |
| `IndexStore` interface | 80 |
| `IndexRegistry` interface + implementation | 300 |
| `IndexWriter` interface + implementation | 350 |
| `IndexReader` interface + implementation | 200 |
| `IndexHint` + BSON serialisation | 100 |
| `IndexManager` interface + implementation | 150 |
| Exception classes | 60 |
| Unit tests | 600 |
| **Total** | **~2,070** |
