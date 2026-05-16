# KDB Component Spec — Layer 1, Component 3

# Document + Commit Model

# Package: `dev.kdb.document`

# Spec version: aligned with master spec v0.5

-----

## 1. Purpose

Defines the canonical in-memory and serialisable representations of a KDB document, a commit, a commit DAG node, and the operations that flow through the transaction engine. This module is the shared vocabulary of the entire engine — every higher layer passes documents and commits using these types. It provides content-addressable hashing of documents and commit trees, which underpins the git-like version model described in master spec §6.

-----

## 2. Dependencies

|Module         |Interfaces Used                                                                                                                                                                                                                                                                             |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`dev.kdb.codec`|`KdbUuid`, `KdbHash`, `KdbTimestamp`, `BsonDocument`, `BsonValue`, `BsonBinary`, `BsonDateTime`, `BsonNull`, `BsonString`, `BsonInt64`, `BsonArray`, `BsonDocument.toBytes()`, `BsonDocument.fromBytes()`, `BsonDocument.fromJson()`, `BsonDocument.toJson()`, `BsonDocument.toPrettyJson()`|
|`dev.kdb.error`|`KdbException`, `KdbResult`, `KdbErrorCode`, `kdbRunCatching`                                                                                                                                                                                                                               |

No other KDB modules. This module has no dependency on Layer 1 Component 4 (JSON Functions Engine) — JSON merging at the document level is a simple root-level key merge only, not a JSONPath operation.

-----

## 3. Public Interface

```kotlin
package dev.kdb.document

import dev.kdb.codec.*
import dev.kdb.error.*

// ── Document ──────────────────────────────────────────────────────────────────

/**
 * A KDB document: a stable-identity wrapper around a JSON string.
 * [json] is the canonical source of truth, always exactly as stored.
 * [bson] and [contentHash] are computed lazily.
 */
data class KdbDocument(
    val id: KdbUuid,
    val json: String,
) {
    val bson: BsonDocument by lazy { BsonDocument.fromJson(json) }
    val contentHash: KdbHash by lazy { computeContentHash(this) }

    /** Return new document with [patchJson] merged (shallow root-level merge). */
    fun merge(patchJson: String): KdbDocument

    /** Return new document with [newJson] as the full body. ID preserved. */
    fun withJson(newJson: String): KdbDocument

    companion object {
        fun fromJson(json: String): KdbDocument
        fun fromJson(id: KdbUuid, json: String): KdbDocument
        fun fromBson(bson: BsonDocument): KdbDocument
    }
}

/** Encode document to BSON storage form. Injects `_kdb_id` (BinData/UUID subtype). */
fun KdbDocument.toBson(): BsonDocument

/** SHA-256 of the canonical BSON encoding of [doc]. */
fun computeContentHash(doc: KdbDocument): KdbHash

// ── Operations ────────────────────────────────────────────────────────────────

/**
 * Atomic unit of change within a transaction.
 * The sealed hierarchy is fixed — new op types require a spec change.
 */
sealed class KdbOp {
    /** Full document replacement (insert or overwrite). [patch] is complete replacement JSON. */
    data class Write(
        val docId: KdbUuid,
        val patch: String,
    ) : KdbOp()

    /** Logical document deletion. Does not purge history. */
    data class Delete(
        val docId: KdbUuid,
    ) : KdbOp()

    /**
     * Raw file blob write, keyed by path within the namespace.
     * [blobHash] refers to content stored in the storage adapter.
     */
    data class FileWrite(
        val path: String,
        val blobHash: KdbHash,
    ) : KdbOp()

    /**
     * Schema migration op, embedded in a transaction for atomicity.
     * [migrationId] is a stable identifier for idempotent replay.
     * [migrationPayload] is an opaque JSON string owned by the Schema Engine (Layer 2).
     */
    data class SchemaMigration(
        val migrationId: KdbUuid,
        val migrationPayload: String,
    ) : KdbOp()
}

// ── Transaction (pre-commit) ──────────────────────────────────────────────────

/**
 * An in-flight transaction. Not persisted until committed to a Commit.
 * [resultVersion] is null until the transaction has been committed.
 */
data class KdbTransaction(
    val id: KdbUuid,
    val baseVersion: KdbHash,        // commit hash this was built against
    val operations: List<KdbOp>,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val resultVersion: KdbHash? = null,
)

// ── Commit ────────────────────────────────────────────────────────────────────

/**
 * An immutable, content-addressed node in the commit DAG.
 * [hash] is the SHA-256 of the canonical BSON encoding of this commit (excluding [hash] itself).
 * [parentHashes] is empty only for a root commit.
 * Merge commits have exactly two parent hashes.
 */
data class KdbCommit(
    val hash: KdbHash,
    val parentHashes: List<KdbHash>,
    val namespaceId: String,
    val transactionId: KdbUuid,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val operations: List<KdbOp>,
    val documentTreeHash: KdbHash,   // hash of the full materialised document tree at this commit
    val schemaHash: KdbHash?,        // null if namespace has no schema
    val message: String = "",
)

/** Compute the canonical hash of a commit (excluding the hash field itself). */
fun computeCommitHash(commit: KdbCommit): KdbHash

// ── Commit serialisation ──────────────────────────────────────────────────────

fun KdbCommit.toBson(): BsonDocument
fun KdbCommit.toBytes(): ByteArray
fun KdbCommit.Companion.fromBson(bson: BsonDocument): KdbCommit
fun KdbCommit.Companion.fromBytes(bytes: ByteArray): KdbCommit

// ── Operation serialisation ───────────────────────────────────────────────────

fun KdbOp.toBson(): BsonDocument
fun KdbOp.Companion.fromBson(bson: BsonDocument): KdbOp

// ── Document tree ─────────────────────────────────────────────────────────────

/**
 * The full materialised set of live documents at a given commit.
 * Immutable snapshot. Stored and loaded by the storage adapter.
 * [entries] maps document ID → content hash (not the document itself).
 * Actual document bodies are stored separately, keyed by content hash.
 */
data class DocumentTree(
    val treeHash: KdbHash,
    val entries: Map<KdbUuid, KdbHash>,  // docId → contentHash
) {
    val size: Int get() = entries.size
    fun contains(docId: KdbUuid): Boolean
    fun hashFor(docId: KdbUuid): KdbHash?

    /** Return a new tree with [docId] pointing to [contentHash]. */
    fun with(docId: KdbUuid, contentHash: KdbHash): DocumentTree

    /** Return a new tree with [docId] removed. No-op if absent. */
    fun without(docId: KdbUuid): DocumentTree

    companion object {
        val EMPTY: DocumentTree

        /** Recompute treeHash from entries (SHA-256 of sorted BSON encoding). */
        fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree
    }
}

fun DocumentTree.toBson(): BsonDocument
fun DocumentTree.Companion.fromBson(bson: BsonDocument): DocumentTree

// ── Branch + Tag pointers ─────────────────────────────────────────────────────

/** A named mutable pointer to a commit hash. Equivalent to a git branch. */
data class KdbBranch(
    val name: String,
    val namespaceId: String,
    val headHash: KdbHash,
    val createdAt: KdbTimestamp,
    val updatedAt: KdbTimestamp,
)

/** A named immutable pointer to a commit hash. Survives compaction. */
data class KdbTag(
    val name: String,
    val namespaceId: String,
    val commitHash: KdbHash,
    val createdAt: KdbTimestamp,
    val message: String = "",
)

// ── Stub commit ───────────────────────────────────────────────────────────────

/**
 * Replaces an ice-archived commit in the live DAG.
 * Accessing a stub via a history query returns IceStorageException.
 */
data class CommitStub(
    val originalHash: KdbHash,
    val archiveLocation: String,
    val stubbedAt: KdbTimestamp,
)

// ── Exceptions ────────────────────────────────────────────────────────────────

class DocumentDecodeException(
    message: String,
    val docId: KdbUuid? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.BSON_DECODE_ERROR
}

class CommitDecodeException(
    message: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.BSON_DECODE_ERROR
}
```

-----

## 4. Data Structures

### KdbDocument

|Field        |Type          |Notes                                                    |
|-------------|--------------|---------------------------------------------------------|
|`id`         |`KdbUuid`     |Stable document identity, never changes across versions  |
|`json`       |`String`      |Canonical UTF-8 JSON, exactly as stored — source of truth|
|`bson`       |`BsonDocument`|Lazy; computed from `json` on first access               |
|`contentHash`|`KdbHash`     |Lazy; SHA-256 of BSON encoding of the document body      |

The BSON storage encoding injects a `_kdb_id` field (BinData, UUID subtype 0x04) so documents are self-identifying when read back from storage without a separate index lookup.

### KdbOp discriminator BSON encoding

|Op type          |BSON field `"t"` value|
|-----------------|----------------------|
|`Write`          |`"w"`                 |
|`Delete`         |`"d"`                 |
|`FileWrite`      |`"f"`                 |
|`SchemaMigration`|`"m"`                 |

### KdbCommit

`parentHashes` invariants:

- `[]` — root commit (first commit in namespace)
- `[h]` — normal linear commit
- `[h1, h2]` — merge commit (order: local parent first, incoming parent second)

`documentTreeHash` is the `treeHash` of the `DocumentTree` produced after applying all `operations` to the parent commit’s tree.

### DocumentTree

The tree is a flat map of `docId → contentHash`. It does not store document bodies — those are stored separately by content hash, enabling deduplication across commits. The `treeHash` is a deterministic SHA-256 of the BSON encoding of the sorted entries (sorted by `docId.toString()` lexicographically).

-----

## 5. Contracts

### `KdbDocument.fromJson(json)`

- **Pre:** `json` is a valid JSON object string (root must be `{}`).
- **Post:** Returns a `KdbDocument` with a freshly generated `KdbUuid`.
- **Guarantee:** `doc.json` is exactly the input string — no normalisation.

### `KdbDocument.fromJson(id, json)`

- **Pre:** `json` is a valid JSON object string; `id` is a valid `KdbUuid`.
- **Post:** Returns a `KdbDocument` with the provided `id`.
- **Guarantee:** Round-trip `fromBson(doc.toBson())` returns a document with identical `id` and `json`.

### `KdbDocument.merge(patchJson)`

- **Pre:** `patchJson` is a valid JSON object string.
- **Post:** Returns a new `KdbDocument` with `id` preserved. Root-level keys from `patchJson` overwrite corresponding keys in `this.json`. Nested structure of `this.json` beyond root is untouched unless its root key is overwritten.
- **Not a deep merge** — this is root-level only. Deep JSON operations belong to the JSON Functions Engine (Component 4).

### `computeContentHash(doc)`

- **Post:** Returns SHA-256 of `doc.toBson().toBytes()` (i.e. the BSON encoding including the injected `_kdb_id` field).
- **Guarantee:** Two documents with identical `id` and `json` always produce the same `contentHash`.

### `computeCommitHash(commit)`

- **Pre:** All fields of `commit` except `hash` must be populated.
- **Post:** SHA-256 of canonical BSON encoding of the commit with `hash` field omitted.
- **Guarantee:** Deterministic — same inputs always produce the same hash.

### `DocumentTree.build(entries)`

- **Post:** `treeHash` is SHA-256 of the BSON encoding of entries sorted by `docId.toString()` ascending.
- **Guarantee:** Same entries in any order → same `treeHash`.

### `DocumentTree.with(docId, contentHash)`

- **Post:** Returns new `DocumentTree` with updated entry. `treeHash` is recomputed.
- **Guarantee:** Original tree is unchanged (immutable).

### `KdbOp` round-trip

- **Guarantee:** `KdbOp.fromBson(op.toBson()) == op` for all op types.

### `KdbCommit` round-trip

- **Guarantee:** `KdbCommit.fromBson(commit.toBson()).hash == commit.hash`.

-----

## 6. Error Cases

|Exception                         |When Thrown                                                                                                                |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------|
|`DocumentDecodeException`         |`fromBson` receives a BSON document missing `_kdb_id` or with malformed `_kdb_id`, or the JSON body cannot be reconstructed|
|`CommitDecodeException`           |`KdbCommit.fromBson` or `fromBytes` encounters missing required fields, unknown op type discriminator, or malformed hashes |
|`BsonDecodeException` (from codec)|Propagated when `BsonDocument.fromBytes` fails during deserialisation of commit or document bytes                          |

Neither `KdbDocument.merge` nor `KdbDocument.withJson` throw — invalid patch JSON results in a `DocumentDecodeException` wrapped in a `KdbResult.Failure` at the call site (callers should use `kdbRunCatching`).

-----

## 7. Test Cases

|# |Name                              |Input                                             |Expected                                                                    |
|--|----------------------------------|--------------------------------------------------|----------------------------------------------------------------------------|
|1 |`fromJson_assignsFreshId`         |Valid JSON object string                          |Document returned; `id` is a valid non-nil UUID; `json` exactly equals input|
|2 |`fromJson_withId_preservesId`     |Known UUID + valid JSON                           |`doc.id == providedId`                                                      |
|3 |`toBson_roundTrip`                |Any `KdbDocument`                                 |`fromBson(doc.toBson())` produces doc with same `id` and `json`             |
|4 |`contentHash_deterministic`       |Two documents built from same `(id, json)`        |`contentHash` values are equal                                              |
|5 |`merge_rootLevelOverwrite`        |`doc` with `{"a":1,"b":2}`, patch `{"b":99,"c":3}`|Result `json` parses to `{"a":1,"b":99,"c":3}`                              |
|6 |`merge_preservesNestedJson`       |`doc` with `{"a":{"x":1,"y":2}}`, patch `{"z":3}` |`{"a":{"x":1,"y":2},"z":3}` — nested untouched                              |
|7 |`documentTree_withAndWithout`     |`EMPTY.with(id, hash).without(id)`                |`treeHash == DocumentTree.EMPTY.treeHash`                                   |
|8 |`documentTree_hashDeterministic`  |Same entries inserted in different order          |`treeHash` values are equal                                                 |
|9 |`commitHash_deterministic`        |Same commit data built twice                      |`computeCommitHash` returns same hash both times                            |
|10|`op_roundTrip_allTypes`           |One of each `KdbOp` subtype                       |`fromBson(op.toBson()) == op` for all types                                 |
|11|`fromBson_missingKdbId_throws`    |BSON document without `_kdb_id` field             |`DocumentDecodeException`                                                   |
|12|`mergeCommit_twoParents`          |`KdbCommit` with `parentHashes` of length 2       |Serialises and deserialises correctly; `parentHashes.size == 2`             |
|13|`commitStub_preservesOriginalHash`|`CommitStub` with known `originalHash`            |Hash survives BSON round-trip unchanged                                     |
|14|`fromJson_arrayRoot_throws`       |JSON string `"[1,2,3]"` (array, not object)       |`DocumentDecodeException`                                                   |

-----

## 8. Non-Goals

- **No JSONPath operations** — deep path access, array indexing, and mutation belong to Component 4 (JSON Functions Engine). `merge` is root-level only.
- **No storage** — this module defines structures and serialisation only. No reading from or writing to any storage adapter.
- **No schema validation** — schema field checking belongs to Component 5 (Schema Engine, Layer 2).
- **No index maintenance** — index updates on write belong to Component 8 (Index Layer, Layer 3).
- **No conflict detection** — transaction replay logic belongs to Component 7 (Transaction Engine, Layer 3).
- **No DAG traversal** — commit graph walking (log, diff, merge-base) belongs to Component 6 (Commit DAG, Layer 2).
- **No branch/tag persistence** — `KdbBranch` and `KdbTag` are data structures only; their storage belongs to the storage adapter interface (Component 9, Layer 3).
- **No zstd compression** — applied by the storage tier manager (Component 18, Layer 6).

-----

## 9. Implementation Notes

### Canonical BSON encoding for hashing

For both `computeContentHash` and `computeCommitHash`, the BSON encoding must be canonical and deterministic:

- Fields in `BsonDocument` must be in a defined order (insertion order is fine if construction order is fixed in code — always build the document in the same field order).
- Do not rely on `LinkedHashMap` ordering surviving serialisation differently on JS vs JVM. Explicitly build the BSON document field by field in a fixed sequence.

### Content hash includes the `_kdb_id` field

`computeContentHash` hashes the full BSON storage form (including `_kdb_id`). This means two documents with the same JSON body but different IDs have different content hashes, which is correct — they are different documents.

### Lazy BSON computation

`bson` and `contentHash` are `by lazy` delegates. On Kotlin/JS, `lazy` is not thread-safe by default — use `LazyThreadSafetyMode.NONE` on JS (single-threaded) and `LazyThreadSafetyMode.SYNCHRONIZED` on JVM/Native. Use `expect/actual` if needed, or unconditionally use `SYNCHRONIZED` (acceptable overhead for these one-time computations).

### JSON merge implementation

`merge` should:

1. Parse `this.json` into a `Map<String, Any?>` using the BSON codec’s JSON parser.
1. Parse `patchJson` similarly.
1. Apply patch keys over base map (shallow).
1. Re-serialise to JSON string.

Do not use string concatenation or regex. Parse both sides fully.

### DocumentTree hash stability

The treeHash sorts entries by `docId.toString()` (UUID string representation, lowercase hex with hyphens) before encoding. This guarantees stability across platforms regardless of map insertion order.

### Kotlin Multiplatform boundaries

- `SHA-256`: use `kotlinx-crypto` or expect/actual. Do not call `java.security.MessageDigest` directly in `commonMain`.
- `LinkedHashMap`: available in `commonMain` via Kotlin stdlib.
- `by lazy`: available in `commonMain`.
- All serialisation in this module is pure Kotlin with no platform-specific calls.

### Estimated size

~1,800 NBNC lines.

-----

## 10. Estimated Lines

|Sub-component                                                              |NBNC lines|
|---------------------------------------------------------------------------|----------|
|`KdbDocument` data class + companion + extensions                          |200       |
|`KdbOp` sealed hierarchy + BSON serialisation                              |250       |
|`KdbTransaction`                                                           |80        |
|`KdbCommit` + hash + BSON serialisation                                    |350       |
|`DocumentTree` + hash + BSON serialisation                                 |300       |
|`KdbBranch`, `KdbTag`, `CommitStub`                                        |120       |
|SHA-256 expect/actual (commonMain stub + jvmMain/jsMain/nativeMain actuals)|150       |
|Exceptions                                                                 |80        |
|Unit tests                                                                 |270       |
|**Total**                                                                  |**~1,800**|

-----

## Session Instructions (for next spec or implementation session)

> **Note added per project convention:** All component specs must be saved as files for download. When generating specs, produce one `.md` file per component and present them for download before the session ends.

When implementing this component, paste the master spec (kdb-spec-v0_5.md or later) plus this file and say:

```
You are implementing KDB, a portable embedded database engine in Kotlin Multiplatform.
This document is the master architecture spec. The attached component spec is your implementation contract.
Implement Component 3: Document + Commit Model in Kotlin Multiplatform (commonMain).
All dependencies are in Section 17 — treat those interfaces as already existing.
Produce production-quality Kotlin. No placeholders.
```

After implementation, extract the public interface (Section 3 of this spec) and paste it into Section 17 → Layer 1 of the master spec, then mark `[x] 3. Document + Commit Model` in the checklist.