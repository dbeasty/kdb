package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.json.JsonValue
import dev.kdb.storage.StorageAdapter

/**
 * An index fed whole documents rather than extracted keys (FULLTEXT and VECTOR, Layer 16
 * Components 64/65). The registry and writer recognise stores by this interface: on every commit
 * a `WriteOp` becomes [putDocument] and a `DeleteOp` becomes a tombstone via [IndexStore.delete].
 */
public interface DocumentIndexStore : IndexStore {
    /**
     * Throws (a schema-violation class exception such as `VectorDimensionMismatchException`) when
     * [json] cannot be indexed because of the document's own fault. Called for every write of a
     * commit *before* any store is mutated (§10: nothing is half-applied).
     */
    public fun validateDocument(
        docId: KdbUuid,
        json: String,
    )

    /** Indexes the document text/vector found at the descriptor's field paths, tagged with [commitHash]. */
    public suspend fun putDocument(
        docId: KdbUuid,
        commitHash: KdbHash,
        json: String,
    )

    /** Writes the snapshot now (also happens every `flushEvery` commits). */
    public suspend fun flush()

    /** Loads the persisted snapshot; the result says whether the caller must rebuild. */
    public suspend fun restoreFromStorage(): SnapshotRestoreResult
}

public enum class SnapshotRestoreStatus {
    /** Snapshot found and its head matches the DAG head. */
    RESTORED,

    /** No snapshot under the index's blob key. */
    MISSING,

    /** Snapshot found but written at another head; contents were discarded. */
    STALE,

    /** Snapshot bytes could not be parsed; contents were discarded. */
    CORRUPT,
}

public data class SnapshotRestoreResult(
    val status: SnapshotRestoreStatus,
    /** `headCommitHex` in the snapshot manifest, when one was readable. */
    val snapshotHeadHex: String?,
    val message: String = "",
) {
    val restored: Boolean get() = status == SnapshotRestoreStatus.RESTORED
}

public data class IndexRestoreReport(
    val descriptor: IndexDescriptor,
    val restore: SnapshotRestoreResult,
    val rebuilt: Boolean,
    val documentsScanned: Int,
)

/**
 * Rebuilds a [DocumentIndexStore] from every document in the tree at [treeHash], tagging entries
 * with [commitHash] (§6.5 "rebuild-from-scan"). Clears the store first. Returns the number of
 * documents scanned.
 */
public suspend fun rebuildFromScan(
    store: DocumentIndexStore,
    storage: StorageAdapter,
    namespaceId: String,
    treeHash: KdbHash,
    commitHash: KdbHash,
): Int {
    store.clear()
    var scanned = 0
    storage.scanDocuments(namespaceId, treeHash, batchSize = 256) { batch ->
        for (doc in batch) {
            store.putDocument(doc.id, commitHash, doc.json)
            scanned++
        }
    }
    return scanned
}

/** [rebuildFromScan] at the DAG head. */
public suspend fun rebuildFromScan(
    store: DocumentIndexStore,
    storage: StorageAdapter,
    dag: CommitDag,
): Int {
    val head = dag.head()
    val commit = dag.getCommitOrThrow(head)
    return rebuildFromScan(store, storage, dag.namespaceId, commit.documentTreeHash, head)
}

/**
 * Restores [store] from its snapshot, rebuilding from scan when the snapshot is missing, stale
 * or corrupt; a rebuilt store is flushed so the next open finds a current snapshot.
 */
public suspend fun restoreOrRebuild(
    store: DocumentIndexStore,
    storage: StorageAdapter,
    dag: CommitDag,
): IndexRestoreReport {
    val restore = store.restoreFromStorage()
    if (restore.restored) return IndexRestoreReport(store.descriptor, restore, rebuilt = false, documentsScanned = 0)
    val scanned = rebuildFromScan(store, storage, dag)
    store.flush()
    return IndexRestoreReport(store.descriptor, restore, rebuilt = true, documentsScanned = scanned)
}

/**
 * Path evaluation with implicit array traversal (§2, Mongo semantics): walking `a.b`, an array
 * at any segment applies the rest of the path to every element in document order; an array at
 * the end contributes its elements. Returns the candidate list (empty when the path is absent).
 * The path is written without the leading `$.`; a leading `$.` is tolerated. With
 * [flattenFinalArray] false the values at the path are returned as they are (a vector field is
 * one array value, not its elements).
 */
public fun documentPathCandidates(
    root: JsonValue,
    path: String,
    flattenFinalArray: Boolean = true,
): List<JsonValue> {
    val trimmed = path.removePrefix("$.").removePrefix("$")
    if (trimmed.isEmpty()) return listOf(root)
    val segments = trimmed.split('.')
    var current: List<JsonValue> = listOf(root)
    for (seg in segments) {
        val next = mutableListOf<JsonValue>()
        for (value in current) descend(value, seg, next)
        current = next
        if (current.isEmpty()) return current
    }
    if (!flattenFinalArray) return current
    val out = mutableListOf<JsonValue>()
    for (value in current) {
        if (value is JsonValue.JArray) out.addAll(value.elements) else out.add(value)
    }
    return out
}

private fun descend(
    value: JsonValue,
    segment: String,
    out: MutableList<JsonValue>,
) {
    when (value) {
        is JsonValue.JObject -> value.fields[segment]?.let { out.add(it) }
        is JsonValue.JArray -> for (element in value.elements) descend(element, segment, out)
        else -> Unit
    }
}

/** Parses [json] leniently for indexing: a document that is not valid JSON contributes nothing. */
public fun parseDocumentForIndex(json: String): JsonValue? =
    try {
        JsonValue.fromJsonString(json)
    } catch (_: Throwable) {
        null
    }
