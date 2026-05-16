# KDB Component Spec — Layer 1, Component 3

# Document + Commit Model

# Package: `dev.kdb.document`

# Spec version: aligned with Layer 0 codec spec v0.2 and master spec v0.5+

-----

## 1. Purpose

Defines the canonical in-memory and serialisable representations of a KDB document, a commit, a commit DAG node, and the operations that flow through the transaction engine. This module is the shared vocabulary of the entire engine — every higher layer passes documents and commits using these types. It provides content-addressable hashing of documents and commit trees, which underpins the git-like version model described in master spec §6.

Structured persistence and hashing use **Layer 0’s typed value model** (`KdbValue`) and **canonical binary encoding** (`encodeToBytes` / `decodeFromBytes` per `kdb-spec-layer0-codec.md`). BSON is not part of the public contract.

-----

## 2. Dependencies

**Depends on:** Layer 0 — Type System & Codec (`kdb-spec-layer0-codec.md`), Error Model (`kdb-spec-layer0-error-model.md` — codec exceptions renamed to `KdbDecodeException` / `KdbEncodeException`; stable numeric codes unchanged).

|Module         |Interfaces Used |
|---------------|----------------|
|`dev.kdb.codec`|`KdbUuid`, `KdbHash`, `KdbTimestamp`, `KdbValue`, `KdbType`, `KdbTypeRegistry`, `encodeToBytes`, `decodeFromBytes`, `KdbValue.fromJson`, `KdbValue.toJson`, named record schemas registered under `dev.kdb.document.*` (see §4.1)|
|`dev.kdb.error`|`KdbException`, `KdbResult`, `KdbErrorCode`, `KdbDecodeException`, `kdbRunCatching`|

No other KDB modules. This module has no dependency on Layer 1 Component 4 (JSON Functions Engine) — JSON merging at the document level is a simple root-level key merge only, not a JSONPath operation.

-----

## 3. Public Interface

```kotlin
package dev.kdb.document

import dev.kdb.codec.*
import dev.kdb.codec.schema.*
import dev.kdb.error.*

// ── Layer 0 registry ───────────────────────────────────────────────────────────

/**
 * Builtin schemas for document/commit/op/tree wire shapes.
 * Frozen before any encode/decode; includes every `dev.kdb.document.*` record,
 * enum, fixed, and union type referenced in this spec.
 */
fun KdbDocumentWireRegistry(): KdbTypeRegistry

// ── Document ──────────────────────────────────────────────────────────────────

/**
 * A KDB document: a stable-identity wrapper around a JSON string.
 * [json] is the canonical source of truth for the application-visible body,
 * always exactly as stored. Identity lives in [id]; do not require `_kdb_id`
 * inside [json] (the engine may omit it — unlike BSON storage, identity is not injected into the JSON text).
 *
 * [contentHash] is computed lazily from the canonical Layer 0 binary encoding
 * of record `dev.kdb.document.DocumentBody` (see §4.1).
 */
data class KdbDocument(
    val id: KdbUuid,
    val json: String,
) {
    val contentHash: KdbHash by lazy { computeContentHash(this) }

    /** Typed storage/hash payload: `DocumentBody` record (`id` + `json` fields per §4.1). */
    fun toDocumentBodyValue(): KdbValue

    /** Return new document with [patchJson] merged (shallow root-level merge). */
    fun merge(patchJson: String): KdbDocument

    /** Return new document with [newJson] as the full body. ID preserved. */
    fun withJson(newJson: String): KdbDocument

    companion object {
        fun fromJson(json: String): KdbDocument
        fun fromJson(id: KdbUuid, json: String): KdbDocument

        /** Decode from typed storage record produced by [toDocumentBodyValue]. */
        fun fromDocumentBodyValue(value: KdbValue): KdbDocument
    }
}

/** SHA-256 of `encodeToBytes(doc.toDocumentBodyValue(), DocumentBodyType, registry)`. */
fun computeContentHash(doc: KdbDocument): KdbHash

/** Resolved `KdbType.Ref("dev.kdb.document.DocumentBody")` from [KdbDocumentWireRegistry]. */
val DocumentBodyType: KdbType

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
 * [hash] is the SHA-256 of the canonical Layer 0 binary encoding of the commit **payload**
 * (record `dev.kdb.document.CommitPayload` — all fields except [hash]; see §4.1).
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

/** Encode payload only (fields other than [KdbCommit.hash]) as `CommitPayload` bytes. */
fun KdbCommit.toPayloadBytes(): ByteArray

fun KdbCommit.toBytes(): ByteArray   // preferred persisted form = payload bytes + optional framing TBD by storage layer

fun KdbCommit.Companion.fromPayloadBytes(bytes: ByteArray): KdbCommit   // fills [hash] via computeCommitHash
fun KdbCommit.Companion.fromBytes(bytes: ByteArray): KdbCommit

/** Resolved ref types from [KdbDocumentWireRegistry]. */
val CommitPayloadType: KdbType
val KdbOpWireType: KdbType

// ── Operation serialisation ───────────────────────────────────────────────────

/** Union wire encoding — branch index then branch record (§4.2). */
fun KdbOp.toKdbValue(): KdbValue
fun KdbOp.Companion.fromKdbValue(value: KdbValue): KdbOp

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

        /** Recompute treeHash from entries (SHA-256 of sorted canonical encoding — §4.3). */
        fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree
    }
}

fun DocumentTree.toKdbValue(): KdbValue
fun DocumentTree.Companion.fromKdbValue(value: KdbValue): DocumentTree

val DocumentTreeWireType: KdbType

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
    /** Same numeric legacy as BSON decode (Layer 0 typed codec decode). */
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

class CommitDecodeException(
    message: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}
```

-----

## 4. Data Structures

### 4.1 Named Layer 0 schemas (`dev.kdb.document`)

Register these in `KdbDocumentWireRegistry()` before encode/decode. Field IDs must remain stable.

**Fixed:** `dev.kdb.document.Hash32` — `FIXED(32)`, SHA-256 raw digest.

**Record `dev.kdb.document.DocumentBody`** — canonical input to `computeContentHash`:

|Field ID|Name|Type|
|--------|-----|-----|
|1|`id`|`UuidVal` (logical `uuid` on `FIXED(16)` — same semantics as Layer 0)|
|2|`json`|`STRING`|UTF-8 JSON object text; must parse as a JSON object `{}` (same payload as [KdbDocument.json])|

**Record `dev.kdb.document.CommitPayload`** — hashed by `computeCommitHash` (all columns below; **no** `hash` field):

|Field ID|Name|Type|
|--------|-----|-----|
|1|`parentHashes`|`ARRAY` of `Ref("dev.kdb.document.Hash32")`|
|2|`namespaceId`|`STRING`|
|3|`transactionId`|`UuidVal`|
|4|`timestamp`|`TimestampVal` (micros)|
|5|`authorNodeId`|`UuidVal`|
|6|`operations`|`ARRAY` of `KdbOpWire` union type|
|7|`documentTreeHash`|`Ref("dev.kdb.document.Hash32")`|
|8|`schemaHash`|`Nullable(Ref("dev.kdb.document.Hash32"))`|Absent/`null` when the namespace has no schema|
|9|`message`|`STRING`|

The persisted `KdbCommit` row still carries `hash`; hashing applies only to `CommitPayload`.

### KdbDocument

|Field        |Type          |Notes                                                    |
|-------------|--------------|---------------------------------------------------------|
|`id`         |`KdbUuid`     |Stable document identity, never changes across versions  |
|`json`       |`String`      |Canonical UTF-8 JSON object text — source of truth for body|
|`contentHash`|`KdbHash`     |Lazy; SHA-256 of Layer 0 binary `DocumentBody` record      |

Identity is carried explicitly as record field `id`, not injected into JSON keys.

### 4.2 `KdbOp` wire union (`dev.kdb.document.KdbOpWire`)

Encoded as Layer 0 `UNION` (`INT8` branch index + payload):

|Branch|`Op`|Payload record|
|-----|-----|----------------|
|0|`Write`|`docId`: `UuidVal`, `patch`: `STRING`|
|1|`Delete`|`docId`: `UuidVal`|
|2|`FileWrite`|`path`: `STRING`, `blobHash`: `Ref("dev.kdb.document.Hash32")`|
|3|`SchemaMigration`|`migrationId`: `UuidVal`, `migrationPayload`: `STRING`|

### KdbOp discriminator (reference)

Legacy BSON discriminator `"t"` / `"w"|"d"|"f"|"m"` is **not** used on the wire; union ordinal above is authoritative.

### KdbCommit

`parentHashes` invariants:

- `[]` — root commit (first commit in namespace)
- `[h]` — normal linear commit
- `[h1, h2]` — merge commit (order: local parent first, incoming parent second)

`documentTreeHash` is the `treeHash` of the `DocumentTree` produced after applying all `operations` to the parent commit’s tree.

### DocumentTree

The tree is a flat map of `docId → contentHash`. It does not store document bodies — those are stored separately by content hash, enabling deduplication across commits.

### 4.3 DocumentTree wire encoding (`treeHash`)

`treeHash` is SHA-256 of `encodeToBytes(tree.toKdbValue(), DocumentTreeWireType, registry)` where `tree.toKdbValue()` is an `ARRAY` of **records** `{ docId: UuidVal, contentHash: Ref("dev.kdb.document.Hash32") }`, sorted by **`docId.toString()`** ascending (lower-case UUID with hyphens). `DocumentTreeWireType` is that array type (registered under `dev.kdb.document`).

-----

## 5. Contracts

### `KdbDocument.fromJson(json)`

- **Pre:** `json` is a valid JSON object string (root must be `{}`).
- **Post:** Returns a `KdbDocument` with a freshly generated `KdbUuid`.
- **Guarantee:** `doc.json` is exactly the input string — no normalisation.

### `KdbDocument.fromJson(id, json)`

- **Pre:** `json` is a valid JSON object string; `id` is a valid `KdbUuid`.
- **Post:** Returns a `KdbDocument` with the provided `id`.
- **Guarantee:** Round-trip `fromDocumentBodyValue(doc.toDocumentBodyValue())` returns a document with identical `id` and `json`.

### `KdbDocument.merge(patchJson)`

- **Pre:** `patchJson` is a valid JSON object string.
- **Post:** Returns a new `KdbDocument` with `id` preserved. Root-level keys from `patchJson` overwrite corresponding keys in `this.json`. Nested structure of `this.json` beyond root is untouched unless its root key is overwritten.
- **Not a deep merge** — this is root-level only. Deep JSON operations belong to the JSON Functions Engine (Component 4).

### `computeContentHash(doc)`

- **Post:** Returns SHA-256 of `encodeToBytes(doc.toDocumentBodyValue(), DocumentBodyType, registry)` using `registry = KdbDocumentWireRegistry()`.
- **Guarantee:** Two documents with identical `id` and `json` always produce the same `contentHash`.

### `computeCommitHash(commit)`

- **Pre:** All fields of `commit` except `hash` must be populated.
- **Post:** SHA-256 of canonical binary encoding of `CommitPayload` (Layer 0 record), omitting `hash`.
- **Guarantee:** Deterministic — same inputs always produce the same hash.

### `DocumentTree.build(entries)`

- **Post:** `treeHash` matches §4.3 (sorted array-of-records encoding).
- **Guarantee:** Same entries in any order → same `treeHash`.

### `DocumentTree.with(docId, contentHash)`

- **Post:** Returns new `DocumentTree` with updated entry. `treeHash` is recomputed.
- **Guarantee:** Original tree is unchanged (immutable).

### `KdbOp` round-trip

- **Guarantee:** `KdbOp.fromKdbValue(op.toKdbValue()) == op` for all op types.

### `KdbCommit` round-trip

- **Guarantee:** Payload decode + hash recompute: `fromPayloadBytes(commit.toPayloadBytes())` yields equal logical commit (including `hash`).

-----

## 6. Error Cases

|Exception                         |When Thrown                                                                                                                |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------|
|`DocumentDecodeException`         |Invalid `DocumentBody` bytes/value, missing fields, non-object JSON in `body`, or UUID/hash malformed                     |
|`CommitDecodeException`           |`fromPayloadBytes` / `fromBytes` encounters truncated bytes, unknown union branch, or malformed hashes                     |
|`KdbDecodeException` (from codec)|Malformed binary (`decodeFromBytes`), including truncated records / bad union discriminant                                  |

Neither `KdbDocument.merge` nor `KdbDocument.withJson` throw — invalid patch JSON results in a `DocumentDecodeException` wrapped in a `KdbResult.Failure` at the call site (callers should use `kdbRunCatching`).

-----

## 7. Test Cases

|# |Name                              |Input                                             |Expected                                                                    |
|--|----------------------------------|--------------------------------------------------|----------------------------------------------------------------------------|
|1 |`fromJson_assignsFreshId`         |Valid JSON object string                          |Document returned; `id` is a valid non-nil UUID; `json` exactly equals input|
|2 |`fromJson_withId_preservesId`     |Known UUID + valid JSON                           |`doc.id == providedId`                                                      |
|3 |`documentBody_roundTrip`          |Any `KdbDocument`                                 |`fromDocumentBodyValue(toDocumentBodyValue())` preserves `id` and `json`   |
|4 |`contentHash_deterministic`       |Two documents built from same `(id, json)`        |`contentHash` values are equal                                              |
|5 |`merge_rootLevelOverwrite`        |`doc` with `{"a":1,"b":2}`, patch `{"b":99,"c":3}`|Result `json` parses to `{"a":1,"b":99,"c":3}`                              |
|6 |`merge_preservesNestedJson`       |`doc` with `{"a":{"x":1,"y":2}}`, patch `{"z":3}` |`{"a":{"x":1,"y":2},"z":3}` — nested untouched                              |
|7 |`documentTree_withAndWithout`     |`EMPTY.with(id, hash).without(id)`                |`treeHash == DocumentTree.EMPTY.treeHash`                                   |
|8 |`documentTree_hashDeterministic`  |Same entries inserted in different order          |`treeHash` values are equal                                                 |
|9 |`commitHash_deterministic`        |Same commit data built twice                      |`computeCommitHash` returns same hash both times                            |
|10|`op_roundTrip_allTypes`           |One of each `KdbOp` subtype                       |`fromKdbValue(toKdbValue()) == op` for all types                            |
|11|`fromDocumentBody_badUuid_throws` |Corrupt UUID field in wire record                  |`DocumentDecodeException` or `KdbDecodeException`                           |
|12|`mergeCommit_twoParents`          |`KdbCommit` with `parentHashes` of length 2       |Serialises and deserialises correctly; `parentHashes.size == 2`             |
|13|`commitStub_preservesOriginalHash`|`CommitStub` with known `originalHash`            |Hash survives typed-binary round-trip unchanged                             |
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

### Canonical binary encoding for hashing

Layer 0 already guarantees deterministic bytes per `(KdbValue, KdbType, frozen registry)`. Implementations **must**:

- Register all §4.1 schemas in a single frozen registry singleton used by hash functions.
- Build `RecordVal` maps with explicit field IDs matching the tables — never infer field order from Kotlin reflection alone.

### Content hash includes document id

`computeContentHash` hashes `DocumentBody` (`id` + `body`), so two documents with the same JSON body but different IDs produce different content hashes.

### Lazy `contentHash`

`contentHash` is a `by lazy` delegate. On Kotlin/JS use `LazyThreadSafetyMode.NONE`; on JVM/Native prefer `SYNCHRONIZED` or explicit freezing after construction.

### JSON merge implementation

`merge` should:

1. Parse `this.json` with `KdbValue.fromJson(...)` using a **JSON-object** schema (`STRING`-keyed map type per Layer 0 extended JSON rules), **or** use the JSON Functions Engine / a dedicated recursive parser — BSON-backed parsers are not required.
2. Parse `patchJson` similarly.
3. Apply patch keys over base map (shallow).
4. Re-serialise with `toJson`.

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
|`KdbOp` sealed hierarchy + Layer 0 union/value encoding                       |280       |
|`KdbTransaction`                                                           |80        |
|`KdbCommit` + hash + `CommitPayload` binary encode/decode                     |380       |
|`DocumentTree` + hash + sorted array wire encoding                           |320       |
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