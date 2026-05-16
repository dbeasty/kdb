# KDB — Component Spec: Commit DAG

**Layer:** 2  
**Component:** 6  
**Package:** `dev.kdb.dag`  
**File:** `kdb-spec-layer2-component6-commit-dag.md`  
**Status:** Implementation-ready

-----

## 1. Purpose

The Commit DAG maintains the directed acyclic graph of commits for one namespace — the equivalent of a git repository’s object graph. It provides commit storage and retrieval, linear and merge-commit creation, branch and tag management, ancestor resolution, topological traversal, diff between any two commits, and the compaction-safe stub mechanism. Every operation that changes namespace content produces a commit through this module. All historical queries (checkout, log, diff) are answered by traversing the DAG this module manages.

-----

## 2. Dependencies

|Module                                                |Interfaces used                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`dev.kdb.codec` (Layer 0 — Type System & Codec)       |`KdbUuid`, `KdbHash`, `KdbTimestamp`, `KdbValue`, `KdbType`, `encodeToBytes`, `decodeFromBytes` — per `kdb-spec-layer0-codec.md`. BSON is not a public interchange format.                                                                                                                                                                                                                                                                                                   |
|`dev.kdb.error` (Layer 0 — Error Model)               |`KdbException`, `KdbErrorCode`, `VersionNotFoundException`, `IceStorageException`, `CompactionBoundaryException`, `KdbResult`, `kdbRunCatching`                                                                                                                                                                                                                                                                                                                               |
|`dev.kdb.document` (Layer 1 — Document + Commit Model)|`KdbCommit`, `KdbTransaction`, `KdbOp`, `DocumentTree`, `KdbBranch`, `KdbTag`, `CommitStub`, `computeCommitHash`, `KdbCommit.toCommitPayloadValue()`, `KdbCommit.toPayloadBytes()`, `KdbCommit.toBytes()`, `KdbCommit.Companion.fromPayloadBytes()`, `KdbCommit.Companion.fromBytes()`, `DocumentTree.build()`, `DocumentTree.toKdbValue()`, `DocumentTree.Companion.fromKdbValue()`, `KdbDocumentWireRegistry()`, `CommitPayloadType`, `DocumentTreeWireType` (see `KdbDocumentWire.kt`) |
|`dev.kdb.schema` (Layer 2 — Schema Engine)            |`KdbSchema`, `KdbSchema.NONE`, `KdbSchema.isNone`, `KdbSchema.toBytes()`, `KdbSchema.Companion.fromBytes()`, `SchemaEngine.computeSchemaHash()`                                                                                                                                                                                                                                                                                                                               |

-----

## 3. Public Interface

```kotlin
package dev.kdb.dag

// ── DAG store interface ────────────────────────────────────────────────────────

/**
 * Primary interface for all commit DAG operations within one namespace.
 * Implementations provide in-memory (tests) and persistent (storage adapter) backends.
 */
interface CommitDag {

    // ── Identity ──────────────────────────────────────────────────────────────

    val namespaceId: String

    // ── Commit read ───────────────────────────────────────────────────────────

    /** Returns the commit for [hash], or null if not present. */
    suspend fun getCommit(hash: KdbHash): KdbCommit?

    /** Returns the commit for [hash]; throws [VersionNotFoundException] if absent. */
    suspend fun getCommitOrThrow(hash: KdbHash): KdbCommit

    /** Returns the stub for [hash] if this commit has been archived to ice. */
    suspend fun getStub(hash: KdbHash): CommitStub?

    /** True if [hash] is present as a full commit (not a stub). */
    suspend fun hasCommit(hash: KdbHash): Boolean

    /** True if [hash] is present as a stub. */
    suspend fun hasStub(hash: KdbHash): Boolean

    // ── Commit write ──────────────────────────────────────────────────────────

    /**
     * Persist [commit].
     * Idempotent: storing the same hash twice is a no-op.
     * Throws [DagConsistencyException] if any parent hash is unknown and [requireParents] is true.
     */
    suspend fun putCommit(commit: KdbCommit, requireParents: Boolean = true)

    /**
     * Replace a full commit with a [CommitStub] (ice archival).
     * The original commit bytes are discarded; only the stub is retained.
     * Throws [DagConsistencyException] if [hash] is not currently a full commit.
     */
    suspend fun stubCommit(hash: KdbHash, archiveLocation: String): CommitStub

    // ── Document tree ─────────────────────────────────────────────────────────

    suspend fun getDocumentTree(treeHash: KdbHash): DocumentTree?
    suspend fun getDocumentTreeOrThrow(treeHash: KdbHash): DocumentTree
    suspend fun putDocumentTree(tree: DocumentTree)

    // ── HEAD + branch ─────────────────────────────────────────────────────────

    /** Current HEAD commit hash for the default branch of this namespace. */
    suspend fun head(): KdbHash

    /** Set HEAD of [branchName] to [hash]. */
    suspend fun setHead(branchName: String, hash: KdbHash)

    suspend fun getBranch(name: String): KdbBranch?
    suspend fun getBranchOrThrow(name: String): KdbBranch
    suspend fun listBranches(): List<KdbBranch>
    suspend fun createBranch(name: String, fromHash: KdbHash): KdbBranch
    suspend fun deleteBranch(name: String)

    // ── Tags ──────────────────────────────────────────────────────────────────

    suspend fun getTag(name: String): KdbTag?
    suspend fun getTagOrThrow(name: String): KdbTag
    suspend fun listTags(): List<KdbTag>
    suspend fun createTag(name: String, commitHash: KdbHash, message: String = ""): KdbTag
    suspend fun deleteTag(name: String)

    // ── Traversal ─────────────────────────────────────────────────────────────

    /**
     * Topologically ordered ancestors of [from] (BFS, most-recent first).
     * Stops at [limit] commits or at [until] (exclusive).
     * Skips stubs — they appear as [TraversalEntry.Stubbed].
     */
    suspend fun walk(
        from: KdbHash,
        until: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<TraversalEntry>

    /**
     * All commit hashes reachable from [from] not reachable from [exclude].
     * Used during peer sync to identify commits to send.
     */
    suspend fun commitsSince(from: KdbHash, exclude: Set<KdbHash>): List<KdbHash>

    // ── Ancestor resolution ───────────────────────────────────────────────────

    /**
     * Returns the most-recent common ancestor of [hashA] and [hashB].
     * Returns null if the two commits share no common ancestor (disjoint histories).
     */
    suspend fun commonAncestor(hashA: KdbHash, hashB: KdbHash): KdbHash?

    /**
     * True if [ancestor] is reachable from [descendant] in the DAG.
     */
    suspend fun isAncestor(ancestor: KdbHash, descendant: KdbHash): Boolean

    // ── Diff ─────────────────────────────────────────────────────────────────

    /**
     * Compute the document-level diff between [fromHash] and [toHash].
     * Each entry describes whether a document was added, removed, or modified.
     */
    suspend fun diff(fromHash: KdbHash, toHash: KdbHash): CommitDiff

    // ── Commit factory ────────────────────────────────────────────────────────

    /**
     * Build and store a new linear commit from [transaction] applied at [parentHash].
     * Computes the new [DocumentTree] by applying all write/delete operations.
     * Validates the commit hash via [computeCommitHash].
     * Returns the new [KdbCommit] on success.
     */
    suspend fun appendCommit(
        transaction: KdbTransaction,
        parentHash: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    /**
     * Build and store a merge commit with two parents.
     * [primaryParent] is the base branch; [mergedParent] is the branch being merged in.
     */
    suspend fun appendMergeCommit(
        transaction: KdbTransaction,
        primaryParent: KdbHash,
        mergedParent: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    // ── Compaction ────────────────────────────────────────────────────────────

    /**
     * Returns all commit hashes that are safe to compact (squash) below [boundary].
     * Safe means: not tagged, not a branch point, not referenced by [peerHeads].
     */
    suspend fun compactableBefore(
        boundary: KdbHash,
        peerHeads: Set<KdbHash>,
    ): List<KdbHash>

    /**
     * Replace a contiguous run of [squashHashes] with a single synthetic root commit.
     * All tags, branches, and peer references must be above [boundary] — this is
     * verified before squashing.
     * Returns the new synthetic root [KdbCommit].
     */
    suspend fun squash(
        squashHashes: List<KdbHash>,
        boundary: KdbHash,
        syntheticTree: DocumentTree,
        syntheticSchemaHash: KdbHash?,
        message: String = "compaction",
    ): KdbCommit
}

// ── Traversal result ──────────────────────────────────────────────────────────

sealed class TraversalEntry {
    /** A full commit was successfully loaded. */
    data class Full(val commit: KdbCommit) : TraversalEntry()
    /** This hash has been archived; the stub is all that remains. */
    data class Stubbed(val stub: CommitStub) : TraversalEntry()
}

// ── Diff result ───────────────────────────────────────────────────────────────

data class CommitDiff(
    val fromHash: KdbHash,
    val toHash: KdbHash,
    val entries: List<DiffEntry>,
) {
    val added: List<DiffEntry.Added>
    val removed: List<DiffEntry.Removed>
    val modified: List<DiffEntry.Modified>
    val isEmpty: Boolean
}

sealed class DiffEntry {
    /** Document present in [toHash] but not [fromHash]. */
    data class Added(val docId: KdbUuid, val contentHash: KdbHash) : DiffEntry()
    /** Document present in [fromHash] but not [toHash]. */
    data class Removed(val docId: KdbUuid, val contentHash: KdbHash) : DiffEntry()
    /** Document present in both but with different content hashes. */
    data class Modified(
        val docId: KdbUuid,
        val fromContentHash: KdbHash,
        val toContentHash: KdbHash,
    ) : DiffEntry()
}

// ── Checkout reference ────────────────────────────────────────────────────────

/**
 * Resolves a human-supplied reference string to a commit hash.
 * Supports: hex hash prefix, branch name, tag name, ISO 8601 timestamp.
 */
suspend fun CommitDag.resolveRef(ref: CommitRef): KdbHash?
suspend fun CommitDag.resolveRefOrThrow(ref: CommitRef): KdbHash

sealed class CommitRef {
    /** Full or prefix hex hash (min 7 chars). */
    data class ByHash(val hex: String)           : CommitRef()
    /** Branch name. Resolves to branch HEAD. */
    data class ByBranch(val name: String)        : CommitRef()
    /** Tag name. */
    data class ByTag(val name: String)           : CommitRef()
    /** Walk history to find the most recent commit at or before this timestamp. */
    data class ByTime(val timestamp: KdbTimestamp) : CommitRef()
}

// ── In-memory implementation (for tests and schema-less namespaces) ────────────

fun inMemoryCommitDag(namespaceId: String): CommitDag

// ── Exceptions ────────────────────────────────────────────────────────────────

class DagConsistencyException(
    message: String,
    val namespaceId: String,
    val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class BranchNotFoundException(
    message: String,
    val namespaceId: String,
    val branchName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class TagNotFoundException(
    message: String,
    val namespaceId: String,
    val tagName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class CompactionSafetyException(
    message: String,
    val namespaceId: String,
    val blockerHash: KdbHash,
    val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}
```

-----

## 4. Data Structures

### `CommitDag` interface

The primary abstraction. All DAG operations are `suspend` because storage adapters (RocksDB, IndexedDB) are async. The interface is stateless except for the underlying storage. One `CommitDag` instance corresponds to one namespace.

### `TraversalEntry`

Sealed class returned by `walk()`. `Full` carries the loaded `KdbCommit`. `Stubbed` signals that this commit has been ice-archived and only its `CommitStub` remains — the caller decides whether to surface `IceStorageException` or skip.

### `CommitDiff`

Computed by comparing the `DocumentTree.entries` maps of two commits. Each `DiffEntry` is identified by `KdbUuid` (document ID). Content equality is determined by `KdbHash` comparison — no document bytes are loaded during diff.

### `CommitRef`

Sealed class representing a user-facing reference to a commit. `ByHash` accepts a hex prefix (min 7 chars) and performs a prefix scan. `ByTime` resolves by walking history and finding the latest commit whose `timestamp ≤ ref.timestamp`.

### `inMemoryCommitDag`

Pure in-memory implementation backed by `LinkedHashMap`. Required for tests and for namespaces that have `history = NONE` (they still need a DAG interface but with no persistence). Must be thread-safe for coroutine use.

-----

## 5. Contracts

### `putCommit(commit, requireParents)`

- **Idempotent:** calling with the same hash twice is a no-op — the second call returns without error.
- **Parent verification:** if `requireParents = true` (default), all `commit.parentHashes` must already be present in the DAG (as full commits or stubs). Throws `DagConsistencyException` if any parent is missing.
- **Root commits** (`parentHashes.isEmpty()`): always accepted regardless of `requireParents`.
- **Hash integrity:** `computeCommitHash(commit)` must equal `commit.hash`. Throws `DagConsistencyException` if not.

### `appendCommit` / `appendMergeCommit`

- **Atomicity:** the commit is not visible via `getCommit` until it has been fully written to storage. The HEAD update happens after successful commit storage.
- **Document tree:** `newDocumentTree` must be pre-computed by the caller (Transaction Engine). This module stores it and references it from the commit; it does not re-derive it.
- **Schema hash:** may be null for schema-less namespaces. Must equal `SchemaEngine.computeSchemaHash(schema)` for namespaced ones.

### `walk(from, until, limit)`

- Breadth-first, most-recent-first topological order.
- `until` is exclusive — the walk stops when `until` would be the next entry.
- Stubs encountered during traversal appear as `TraversalEntry.Stubbed` and do not halt the walk unless they are the only path forward (no other parent branches exist).
- Never returns cycles (DAG invariant).

### `commonAncestor(hashA, hashB)`

- Uses the standard lowest-common-ancestor algorithm: BFS from both heads simultaneously, tracking visit sets.
- Returns the first hash in both visit sets (the deepest common ancestor).
- Returns `null` if no common ancestor exists (e.g. two independently initialised namespaces before any shared history).

### `diff(fromHash, toHash)`

- O(n) in the number of entries in the two `DocumentTree` maps.
- Does not load document content — works on `KdbHash` identity only.
- If `fromHash == toHash`, returns an empty diff immediately.

### `squash(squashHashes, boundary, syntheticTree, ...)`

- **Pre-squash safety check:** verifies that no tag, no branch HEAD, and no hash in `peerHeads` falls within `squashHashes`. Throws `CompactionSafetyException` on any violation.
- **Post-squash:** the synthetic root commit has `parentHashes = emptyList()`. All `squashHashes` are removed from the DAG.
- **Tags:** tags pointing to squashed commits are preserved — they redirect to the synthetic root.

### `resolveRef(ref)`

- `ByHash`: scans the commit store for all hashes with the given prefix. Returns `null` if no match, throws `DagConsistencyException` if ambiguous (multiple matches).
- `ByBranch`: delegates to `getBranch(name)?.headHash`.
- `ByTag`: delegates to `getTag(name)?.commitHash`.
- `ByTime`: walks from HEAD backwards; returns the first commit with `timestamp ≤ ref.timestamp`.

-----

## 6. Error Cases

|Exception                    |When thrown                                                                                     |
|-----------------------------|------------------------------------------------------------------------------------------------|
|`VersionNotFoundException`   |`getCommitOrThrow()`, `getDocumentTreeOrThrow()` when hash is absent.                           |
|`IceStorageException`        |`getCommitOrThrow()` when hash exists as a stub. Passes `stub.archiveLocation`.                 |
|`CompactionBoundaryException`|A caller tries to use a hash that has been squashed below the compaction boundary.              |
|`DagConsistencyException`    |`putCommit` with missing parents; hash integrity failure; `stubCommit` on a non-existent commit.|
|`BranchNotFoundException`    |`getBranchOrThrow`, `deleteBranch` for unknown branch names.                                    |
|`TagNotFoundException`       |`getTagOrThrow`, `deleteTag` for unknown tag names.                                             |
|`CompactionSafetyException`  |`squash` when a tag, branch, or peer head would be lost.                                        |

-----

## 7. Test Cases

### TC-01 — Append and retrieve a linear commit

**Input:** create an in-memory DAG; append a commit with one write operation at the root.  
**Expected:** `getCommit(hash)` returns the stored commit; `head()` returns the new hash.

### TC-02 — `putCommit` is idempotent

**Input:** append the same commit twice.  
**Expected:** second call is a no-op; DAG is unchanged; no exception.

### TC-03 — Missing parent is rejected

**Input:** attempt to store a commit whose `parentHashes` contains an unknown hash with `requireParents = true`.  
**Expected:** `DagConsistencyException`.

### TC-04 — Branch creation and HEAD tracking

**Input:** append three commits on `main`; create branch `feature` at commit 2; append one commit on `feature`.  
**Expected:** `getBranch("main").headHash` → commit 3; `getBranch("feature").headHash` → commit 4; `commonAncestor(commit3, commit4)` → commit 2.

### TC-05 — Tag survives squash

**Input:** append commits 1–5; tag commit 2 as `"v1.0"`; squash commits 1–3 into a synthetic root; call `getTag("v1.0")`.  
**Expected:** tag is present and points to the synthetic root commit.

### TC-06 — Diff between two linear commits

**Input:** commit A (docTree: `{uuid1→h1, uuid2→h2}`); commit B (docTree: `{uuid1→h1_modified, uuid3→h3}`).  
**Expected:** `diff(A, B).modified` contains uuid1; `.added` contains uuid3; `.removed` contains uuid2.

### TC-07 — Empty diff for identical commits

**Input:** `diff(hashX, hashX)`.  
**Expected:** `CommitDiff.isEmpty == true`, `entries` is empty.

### TC-08 — `walk` respects `limit`

**Input:** linear DAG of 10 commits; `walk(head, limit = 3)`.  
**Expected:** exactly 3 `TraversalEntry.Full` entries returned, most-recent first.

### TC-09 — `walk` stops at stub

**Input:** commits [1, 2, 3]; stub commit 1; `walk(from=3)`.  
**Expected:** entries are `[Full(3), Full(2), Stubbed(stub_for_1)]`.

### TC-10 — `resolveRef` by time

**Input:** commits with timestamps T1 < T2 < T3; `resolveRef(ByTime(T2 + 1 second))`.  
**Expected:** resolves to commit 2’s hash.

### TC-11 — `squash` blocked by tag

**Input:** commit 1 tagged; attempt to squash commits [1, 2].  
**Expected:** `CompactionSafetyException`.

### TC-12 — Merge commit has two parents

**Input:** branch `main` at commit 3; branch `feat` at commit 4 (diverged from commit 2); `appendMergeCommit(primaryParent=3, mergedParent=4, ...)`.  
**Expected:** merge commit has `parentHashes = [hash3, hash4]`; `commonAncestor(hash3, hash4) == hash2`.

-----

## 8. Non-Goals

- **Does not execute transactions** — transaction application (write/delete operations on documents) is the Transaction Engine’s job (Component 7). This module stores the resulting commit.
- **Does not validate document content** — document validation is the Schema Engine’s job.
- **Does not implement conflict resolution** — conflict detection and replay are handled by the Transaction Engine.
- **Does not manage storage persistence** — the `CommitDag` interface is backed by a storage adapter (RocksDB, IndexedDB, mmap). The adapter is provided via constructor injection. This module only defines the interface and the in-memory implementation.
- **Does not implement the wire protocol** — DAG exchange between peers is handled by the Peer Sync module (Component 21).
- **Does not implement index maintenance** — the Index Layer (Component 8) is responsible for keeping indexes consistent with commits.
- **Does not perform garbage collection** — GC beyond `squash` (removing unreachable orphan commits) is the Compaction Engine’s responsibility (Component 19, Layer 6).

-----

## 9. Implementation Notes

### Layer 0 encoding for commits and document trees

Persist and verify commits using `KdbCommit.toPayloadBytes()` / `KdbCommit.fromPayloadBytes()` (aliases `toBytes` / `fromBytes` in `:kdb-document`). Always check `computeCommitHash(commit) == commit.hash`. Materialised trees must match Layer 1 rules: `DocumentTree.build` computes `treeHash` from the canonical `encodeToBytes` payload (`DocumentTreeWireType`, `KdbDocumentWireRegistry()`). Do not add a BSON encoding path alongside this — storage adapters hold opaque bytes.

### In-memory implementation structure

Use two `LinkedHashMap` stores: one for `KdbHash → KdbCommit`, one for `KdbHash → CommitStub`. Branch and tag stores are simple `MutableMap<String, KdbBranch/KdbTag>`. Protect with a `Mutex` for coroutine safety.

### Ancestor resolution algorithm

Use the standard two-pointer BFS approach:

1. BFS from `hashA`, collect all ancestors into set `A`.
1. BFS from `hashB`, stop as soon as a hash is found in `A`.
1. The first such hash is the most-recent common ancestor.

For the full LCA (needed for merge base), use a colour-marking DFS variant. Keep it simple in v1 — the two-pointer BFS is sufficient for the peer sync use case.

### Hash prefix resolution

For `resolveRef(ByHash(prefix))`, maintain a sorted index of all stored hashes as hex strings. Use binary search for prefix scan. A minimum prefix length of 7 characters is enforced to avoid excessive ambiguity.

### `walk` BFS order

Use a `PriorityQueue` keyed on `KdbTimestamp` (newest first) to produce a topological order that favours recency across merge branches. This matches git’s `--date-order` traversal.

### `diff` implementation

`diff` is a pure map comparison:

```
val fromEntries = getDocumentTreeOrThrow(fromCommit.documentTreeHash).entries
val toEntries   = getDocumentTreeOrThrow(toCommit.documentTreeHash).entries
added    = toEntries.keys - fromEntries.keys
removed  = fromEntries.keys - toEntries.keys
modified = (fromEntries.keys ∩ toEntries.keys).filter { fromEntries[it] != toEntries[it] }
```

No document content is loaded. O(n + m) where n, m are the tree sizes.

### Compaction and stub interaction during `walk`

When `walk` encounters a stub, it adds a `TraversalEntry.Stubbed` entry and stops that branch of the BFS. If the stub is the only path forward (root of a squashed history), the walk terminates naturally. Callers that need to surface `IceStorageException` must check for `Stubbed` entries themselves.

### `appendCommit` / `appendMergeCommit` atomicity

The persistent storage adapter must write the `DocumentTree`, then the `KdbCommit`, then update the branch HEAD — in that order. A crash between steps 2 and 3 leaves an orphan commit that does not affect HEAD; the compaction engine can collect it. A crash between steps 1 and 2 leaves an orphan tree. This is acceptable for v1 (no WAL required at this layer).

### Kotlin Multiplatform

All code in `commonMain`. The `CommitDag` interface uses `suspend` throughout. The in-memory implementation uses `kotlinx.coroutines.sync.Mutex`. Do not use `java.util.*` collection types — use Kotlin stdlib only.

-----

## 10. Estimated Lines

|Section                                                              |Est. lines|
|---------------------------------------------------------------------|----------|
|`CommitDag` interface                                                |150       |
|`TraversalEntry`, `CommitDiff`, `DiffEntry`, `CommitRef` data classes|120       |
|`inMemoryCommitDag` implementation                                   |500       |
|`appendCommit` / `appendMergeCommit` logic                           |200       |
|`walk` BFS implementation                                            |150       |
|`commonAncestor` + `isAncestor`                                      |150       |
|`diff` implementation                                                |100       |
|`resolveRef` + prefix scan                                           |150       |
|`squash` + safety checks                                             |200       |
|`compactableBefore`                                                  |100       |
|Branch + tag CRUD                                                    |150       |
|Stub management                                                      |100       |
|Exceptions                                                           |80        |
|Tests                                                                |500       |
|**Total**                                                            |**~2,650**|