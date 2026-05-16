# KDB Component Spec — Layer 3
## Component 7: Transaction Engine
### `dev.kdb.transaction`

**File:** `kdb-spec-layer3-component7-transaction-engine.md`
**Layer:** 3 — Write Path
**Depends on:** Layer 0 (Type System & Codec, Error Model), Layer 1 (Document + Commit Model, JSON Functions Engine), Layer 2 (Schema Engine, Commit DAG)

---

## 1. Purpose

The Transaction Engine is the single write gateway for KDB. It owns the full lifecycle of a transaction: building it from caller-supplied operations, validating each operation against the current namespace schema, detecting conflicts against the target HEAD commit, and either committing a new entry to the Commit DAG or surfacing a typed `ConflictReport` for the application to resolve. It enforces the four conflict policies (APPEND_ONLY, LAST_WRITE, STRICT, CUSTOM) and implements the transaction replay path used during peer sync.

The Transaction Engine does not own persistence. It delegates document-tree reads and writes to a `StorageAdapter` interface (defined in Component 9) and commits to the `CommitDag` (Component 6). It is the only component allowed to call `CommitDag.appendCommit` or `CommitDag.appendMergeCommit`.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` (Layer 0 codec) | `KdbUuid`, `KdbHash`, `KdbTimestamp`, `KdbValue`, encoding helpers |
| `dev.kdb.error` (Error Model) | `KdbException`, `KdbErrorCode`, `ConflictReport`, `ConflictItem`, `ConflictOperationType`, `KdbResult` |
| `dev.kdb.document` (Document + Commit Model) | `KdbDocument`, `KdbOp`, `KdbTransaction`, `KdbCommit`, `DocumentTree` |
| `dev.kdb.json` (JSON Functions Engine) | `kdbJsonMerge`, `kdbJsonGet`, `JsonValue` |
| `dev.kdb.schema` (Schema Engine) | `KdbSchema`, `SchemaEngine.validate`, `SchemaEngine.checkFieldValue` |
| `dev.kdb.dag` (Commit DAG) | `CommitDag`, `CommitDag.appendCommit`, `CommitDag.appendMergeCommit`, `CommitDag.getDocumentTree` |
| `dev.kdb.storage` (Storage Adapter — Component 9) | `StorageAdapter` (read document bodies, write document bodies) |

---

## 3. Public Interface

```kotlin
package dev.kdb.transaction

import dev.kdb.codec.*
import dev.kdb.document.*
import dev.kdb.error.*
import dev.kdb.schema.*
import dev.kdb.dag.*
import dev.kdb.storage.StorageAdapter

// ── Conflict policy ───────────────────────────────────────────────────────────

enum class ConflictPolicy {
    /** Every write succeeds regardless of concurrent modifications. */
    APPEND_ONLY,
    /** Incoming write wins; no conflict surfaced. */
    LAST_WRITE,
    /** Any concurrent modification on the same document produces a ConflictReport. */
    STRICT,
    /** Application-provided resolver is called per conflicting document. */
    CUSTOM,
}

// ── Custom resolver ───────────────────────────────────────────────────────────

/** Called once per conflicting document when [ConflictPolicy.CUSTOM] is active.
 *  Returns the document that should be committed, or null to abort the entire transaction. */
fun interface ConflictResolver {
    suspend fun resolve(conflict: DocumentConflict): KdbDocument?
}

data class DocumentConflict(
    val docId: KdbUuid,
    val operationType: ConflictOperationType,
    /** The document as it exists at target HEAD. Null if deleted. */
    val existingDoc: KdbDocument?,
    /** The document produced by the incoming operation. Null for deletes. */
    val incomingDoc: KdbDocument?,
    /** The base document the transaction was built against. Null for inserts. */
    val baseDoc: KdbDocument?,
)

// ── Transaction builder ───────────────────────────────────────────────────────

/** Fluent builder; collects operations before submission. Thread-safe. */
class TransactionBuilder(
    val namespaceId: String,
    val baseVersion: KdbHash,
    val authorNodeId: KdbUuid,
    val schema: KdbSchema = KdbSchema.NONE,
) {
    fun write(docId: KdbUuid, patchJson: String): TransactionBuilder
    fun writeDocument(document: KdbDocument): TransactionBuilder
    fun delete(docId: KdbUuid): TransactionBuilder
    fun fileWrite(path: String, blobHash: KdbHash): TransactionBuilder
    fun schemaMigration(migration: SchemaMigration): TransactionBuilder
    fun build(timestamp: KdbTimestamp = KdbTimestamp.now()): KdbTransaction
}

// ── Transaction engine ────────────────────────────────────────────────────────

interface TransactionEngine {

    /** Current conflict policy for this engine instance. */
    val conflictPolicy: ConflictPolicy

    /** Optional custom resolver; only consulted when [conflictPolicy] is CUSTOM. */
    val customResolver: ConflictResolver?

    /**
     * Commit a transaction against the namespace DAG.
     *
     * - Loads the document tree at [transaction.baseVersion].
     * - Applies all operations, validating schema fields on every Write.
     * - Detects conflicts between the base tree and [targetHead] per [conflictPolicy].
     * - On success: appends a new commit to the DAG and returns it.
     * - On conflict: returns [TransactionResult.Conflict] without mutating the DAG.
     *
     * [targetHead] defaults to the current branch HEAD if null.
     */
    suspend fun commit(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        targetHead: KdbHash? = null,
        message: String = "",
    ): TransactionResult

    /**
     * Replay a transaction that was built against [transaction.baseVersion] onto
     * [replayTarget]. Used during peer sync to integrate remote transactions.
     *
     * Replay semantics:
     * - Each Write/Delete is re-evaluated against [replayTarget].
     * - Conflict policy and custom resolver apply identically to [commit].
     * - A SchemaMigration op in the replayed transaction is re-validated
     *   against the schema at [replayTarget].
     */
    suspend fun replay(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        replayTarget: KdbHash,
        message: String = "",
    ): TransactionResult

    /**
     * Produce a merge commit from two diverged branch tips.
     * Transactions from [mergedHead] are replayed onto [primaryHead] in topological
     * order. Conflicts accumulate; all-or-nothing: if any conflict is unresolvable
     * under the current policy, returns [TransactionResult.Conflict].
     */
    suspend fun merge(
        primaryHead: KdbHash,
        mergedHead: KdbHash,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        message: String = "",
    ): TransactionResult

    /**
     * Validate a transaction's operations against the given schema without committing.
     * Returns all schema violations found. Empty list means the transaction is schema-valid.
     */
    suspend fun validate(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
    ): List<OperationViolation>
}

// ── Transaction result ────────────────────────────────────────────────────────

sealed class TransactionResult {
    /** Transaction committed successfully. */
    data class Success(
        val commit: KdbCommit,
        /** New document tree hash after all operations applied. */
        val newTreeHash: KdbHash,
    ) : TransactionResult()

    /** One or more operations conflicted under the active policy. No commit produced. */
    data class Conflict(
        val report: ConflictReport,
        val conflictingOps: List<OperationConflict>,
    ) : TransactionResult()

    /** Schema validation failed before conflict check. No commit produced. */
    data class SchemaError(
        val violations: List<OperationViolation>,
    ) : TransactionResult()
}

// ── Conflict and violation detail types ──────────────────────────────────────

data class OperationConflict(
    val opIndex: Int,
    val op: KdbOp,
    val type: ConflictOperationType,
    val existingDoc: KdbDocument?,
    val incomingDoc: KdbDocument?,
)

data class OperationViolation(
    val opIndex: Int,
    val op: KdbOp,
    val violations: List<FieldViolation>,
)

// ── Write result for a single document operation ──────────────────────────────

sealed class DocWriteOutcome {
    data class Written(val newDoc: KdbDocument, val contentHash: KdbHash) : DocWriteOutcome()
    data class Deleted(val docId: KdbUuid) : DocWriteOutcome()
    data class Conflicted(val conflict: OperationConflict) : DocWriteOutcome()
    data class SchemaRejected(val violation: OperationViolation) : DocWriteOutcome()
}

// ── Factory ───────────────────────────────────────────────────────────────────

fun transactionEngine(
    conflictPolicy: ConflictPolicy,
    customResolver: ConflictResolver? = null,
): TransactionEngine

fun TransactionBuilder(
    namespaceId: String,
    dag: CommitDag,
    authorNodeId: KdbUuid,
    schema: KdbSchema = KdbSchema.NONE,
): TransactionBuilder  // async factory — reads current HEAD from dag

// ── Exceptions ────────────────────────────────────────────────────────────────

class TransactionBaseNotFoundException(
    message: String,
    val transactionId: KdbUuid,
    val missingHash: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class TransactionSchemaException(
    message: String,
    val transactionId: KdbUuid,
    val violations: List<OperationViolation>,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

class MergeBaseNotFoundException(
    message: String,
    val primaryHead: KdbHash,
    val mergedHead: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}
```

---

## 4. Data Structures

### `ConflictPolicy`
Enum with four values. Stored in namespace policy config and passed to `TransactionEngine` at construction time. The engine is stateless with respect to policy — create separate instances for different namespaces if policies differ.

### `DocumentConflict`
Passed to `ConflictResolver.resolve`. Contains all three document states: base (what the transaction was built against), existing (what is currently at HEAD), and incoming (what the transaction wants to write). A CUSTOM resolver can inspect all three and return any `KdbDocument`, or return `null` to abort.

### `TransactionResult`
Sealed class with three branches. `Success` carries the committed `KdbCommit` and the new `DocumentTree` hash. `Conflict` carries a `ConflictReport` (suitable for wire serialisation and surfacing to callers) plus structured `OperationConflict` list for programmatic resolution. `SchemaError` carries per-operation violations and is returned before conflict detection runs.

### `OperationConflict`
Links back to the operation by index and type, carrying both the existing and incoming document for programmatic inspection. One entry per conflicting operation.

### `OperationViolation`
Links back to a `KdbOp.Write` by index, carrying the full `FieldViolation` list from `SchemaEngine.validate`.

---

## 5. Contracts

### `commit`

**Preconditions:**
- `transaction.baseVersion` must be a commit hash present in `dag` (or `KdbHash.ROOT` for the first commit).
- All `KdbOp.Write` patch JSON must be valid JSON.
- `authorNodeId` must be set on the transaction.

**Postconditions:**
- On `Success`: a new `KdbCommit` exists in `dag` with `parentHash = targetHead`, `transactionId = transaction.id`, and `documentTreeHash` reflecting all applied operations.
- On `Conflict`: DAG is unchanged. No partial writes.
- On `SchemaError`: DAG is unchanged. Schema validation runs before conflict detection.

**Atomicity:** All operations in a transaction succeed or fail together. The DAG write is the commit point; if it succeeds, all document tree writes are durably associated with the new commit hash.

**Idempotency:** Submitting the same `transaction.id` twice to a `CommitDag` that already contains a commit with that `transactionId` returns `Success` with the existing commit rather than re-committing.

### `replay`

**Preconditions:**
- `replayTarget` must exist in `dag`.
- `transaction.baseVersion` need not exist in `dag` (it is from a foreign peer).

**Behaviour:** Each operation is applied as if the transaction had originally been built against `replayTarget`. Document reads use `replayTarget` as the base tree. Conflict detection compares `replayTarget` state, not `transaction.baseVersion` state.

### `merge`

**Preconditions:**
- Both `primaryHead` and `mergedHead` must exist in `dag`.
- A common ancestor must be reachable from both (discovered via `CommitDag.commonAncestor`).

**Postconditions:**
- On `Success`: a merge commit exists in the DAG with `parentHashes = [primaryHead, mergedHead]`.
- Operations from `mergedHead`'s branch are replayed in topological order (ancestor-first).
- Conflicts from any replayed transaction propagate to the overall `TransactionResult.Conflict`.

### `validate`

Pure read — no writes to DAG or storage. Returns immediately if schema is `KdbSchema.NONE`.

---

## 6. Error Cases

| Exception | When thrown |
|---|---|
| `TransactionBaseNotFoundException` | `transaction.baseVersion` is not in `dag` and is not `KdbHash.ROOT`. |
| `TransactionSchemaException` | Thrown only when the caller chooses to throw rather than inspect `TransactionResult.SchemaError`. The engine never auto-throws on schema errors; it returns the result type. |
| `MergeBaseNotFoundException` | `commonAncestor` returns null — the two branches share no history. |
| `DagConsistencyException` (from DAG) | The DAG detects an internal inconsistency during commit append. Propagated as-is. |
| `IceStorageException` (from DAG) | A required ancestor commit is archived. Propagated as-is. |

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `commitSingleWrite_appendsCommit` | Single `KdbOp.Write` on an empty namespace. | `TransactionResult.Success`; DAG has one commit; `documentTreeHash` references the written doc. |
| 2 | `commitWithSchemaValidation_valid` | Write that satisfies all required schema fields. | `TransactionResult.Success`. |
| 3 | `commitWithSchemaValidation_invalid` | Write missing a `required = true` field. | `TransactionResult.SchemaError` with one `OperationViolation`. DAG unchanged. |
| 4 | `strictPolicy_detectsConcurrentWrite` | Two transactions on the same document, both built against the same base. Commit the first, then commit the second under `STRICT`. | `TransactionResult.Conflict` on second commit with `CONCURRENT_WRITE` type. |
| 5 | `lastWritePolicy_noConflict` | Same scenario as #4 but policy is `LAST_WRITE`. | Both succeed. Second commit's document wins. |
| 6 | `appendOnlyPolicy_alwaysSucceeds` | Write and then delete the same document under `APPEND_ONLY`. | Both succeed regardless of concurrent state. |
| 7 | `customResolver_returnsMergedDoc` | Conflict under `CUSTOM` policy; resolver merges both documents via `kdbJsonMerge`. | `TransactionResult.Success` with the merged document. |
| 8 | `customResolver_returnsNull_abortsTransaction` | Conflict under `CUSTOM` policy; resolver returns null. | `TransactionResult.Conflict` — full transaction aborted. |
| 9 | `replay_addsCommitAtNewBase` | Remote transaction built against hash `H1`; local HEAD is `H2`. Replay onto `H2`. | Commit appended with `parentHash = H2`; operations applied against `H2` tree. |
| 10 | `merge_twoDivergedBranches_noConflict` | Branch A adds doc X; Branch B adds doc Y; both from same root. Merge A←B. | `TransactionResult.Success`; merge commit with both docs in tree. |
| 11 | `idempotentCommit_sameTransactionId` | Submit identical transaction twice. | Second call returns `Success` with the existing commit; no duplicate commit in DAG. |
| 12 | `deleteMissingDocument_appendOnly` | Delete a document that does not exist, under `APPEND_ONLY`. | `TransactionResult.Success` (no-op write). |

---

## 8. Non-Goals

- **Persistence** — the Transaction Engine does not own any storage. It delegates entirely to `StorageAdapter` and `CommitDag`.
- **Network** — transaction serialisation for wire transport is the responsibility of the Wire Protocol (Component 21).
- **Schema evolution** — `SchemaMigration` operations are passed through to the DAG; the Transaction Engine validates that the resulting schema is structurally valid but does not perform data migration.
- **Rights enforcement** — the authorship envelope (`principal`, `rights_token`) is stored in every `KdbCommit` but the Transaction Engine does not validate it. That is the responsibility of an Auth Interceptor above this layer.
- **Index updates** — index maintenance is the responsibility of the Index Layer (Component 8).
- **Compaction** — out of scope.

---

## 9. Implementation Notes

### Conflict detection algorithm

For each `KdbOp.Write` or `KdbOp.Delete`:

1. Load the base document (at `transaction.baseVersion`) from the storage adapter.
2. Load the existing document (at `targetHead`) from the storage adapter.
3. Compare content hashes:
   - If `base.contentHash == existing.contentHash` → no concurrent modification → apply.
   - If hashes differ → conflict detected → apply policy.
4. For `APPEND_ONLY`: always apply, skip step 3.
5. For `LAST_WRITE`: always apply, skip step 3.
6. For `STRICT`: any hash mismatch → add to conflict list → do not apply.
7. For `CUSTOM`: invoke resolver → if resolver returns a document, use it; if null, abort.

Conflict detection and application are two passes. Pass 1 collects all conflicts. If `STRICT` and any conflicts exist, return `Conflict` without applying any operations (all-or-nothing). If `CUSTOM`, the resolver is called during pass 1; if any resolver call returns null, abort after pass 1.

### Document tree mutation

The Transaction Engine builds a new `DocumentTree` in memory by starting from the existing tree at `targetHead` and applying each non-conflicting operation's result. This is an immutable functional update — `DocumentTree.with` and `DocumentTree.without` are called in sequence, producing a new tree. The new tree is written to storage via `StorageAdapter` and its hash is recorded in the new commit.

### Schema validation pass

Schema validation always runs before conflict detection. If any `KdbOp.Write` produces schema violations, return `TransactionResult.SchemaError` immediately without running conflict detection. This keeps the error surface clean: callers get either a schema error OR a conflict, never both.

### Replay ordering for merge

When merging branch B into branch A, retrieve the commits on branch B since the common ancestor using `CommitDag.commitsSince`. Walk them in topological order (parent before child). Replay each commit's transaction in sequence onto a rolling HEAD that starts at `primaryHead`. This means later commits on branch B see the result of earlier replays, which is the correct behaviour for multi-commit merges.

### Kotlin Multiplatform

No `expect/actual` required. The engine is pure `commonMain` logic. The `StorageAdapter` (Component 9) handles platform I/O. Coroutines are used throughout; all public methods are `suspend`.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `ConflictPolicy` + `ConflictResolver` + `DocumentConflict` | 80 |
| `TransactionBuilder` | 150 |
| `TransactionEngine` interface + factory | 50 |
| `TransactionEngineImpl` — commit path | 400 |
| `TransactionEngineImpl` — replay path | 200 |
| `TransactionEngineImpl` — merge path | 300 |
| `TransactionEngineImpl` — validate path | 100 |
| `TransactionResult` + detail types | 80 |
| Exception classes | 60 |
| Unit tests | 500 |
| **Total** | **~1,920** |
