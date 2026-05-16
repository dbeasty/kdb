package dev.kdb.stream

import dev.kdb.index.IndexHint
import dev.kdb.index.IndexManager

public interface IndexHintApplier {
    public suspend fun apply(namespaceId: String, hints: List<IndexHint>)
}

public fun defaultIndexHintApplier(indexManager: IndexManager): IndexHintApplier =
    RecordingIndexHintApplier()

/** v1 test applier: records hints; full store mutation deferred to index rebuild path. */
public class RecordingIndexHintApplier : IndexHintApplier {
    private val applied = mutableListOf<Pair<String, IndexHint>>()

    override suspend fun apply(namespaceId: String, hints: List<IndexHint>) {
        for (h in hints) {
            applied += namespaceId to h
        }
    }

    public fun recorded(namespaceId: String): List<IndexHint> =
        applied.filter { it.first == namespaceId }.map { it.second }
}
