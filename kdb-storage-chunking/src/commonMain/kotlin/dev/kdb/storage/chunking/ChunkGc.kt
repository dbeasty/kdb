package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash

public data class ChunkGcResult(
    val manifestsRemoved: Int,
    val chunksRemoved: Int,
    val bytesReclaimed: Long,
)

/**
 * Chunk-level reachability sweep for [ChunkedBlobStore], replacing blob-level GC
 * (`dev.kdb.compaction.OrphanBlobGc`) for chunked blobs. A chunk is reachable only if some
 * *reachable* manifest still references it — a chunk shared by two versions of a file stays
 * live as long as either version is retained by the commit DAG, and is only reclaimed once
 * every manifest that referenced it has been pruned.
 */
public class ChunkGc(private val store: ChunkedBlobStore) {

    /**
     * [reachableContentHashes] is the set of blob content hashes still reachable from a
     * retained commit (as `dev.kdb.compaction.OrphanBlobGc.sweep`'s `reachableHashes` would be).
     * Any manifest not in that set is removed, then any chunk no remaining manifest
     * references is removed.
     */
    public fun sweep(reachableContentHashes: Set<KdbHash>): ChunkGcResult {
        val manifestStore = store.manifests()
        val chunkStore = store.chunks()

        val liveChunkRefs = reachableContentHashes.flatMap { store.chunkRefs(it) }.toSet()

        var bytesReclaimed = 0L

        val orphanManifests = manifestStore.allHashes() - reachableContentHashes
        for (hash in orphanManifests) {
            manifestStore.get(hash)?.let { bytesReclaimed += it.size }
            manifestStore.remove(hash)
        }

        val orphanChunks = chunkStore.allHashes() - liveChunkRefs
        for (hash in orphanChunks) {
            chunkStore.get(hash)?.let { bytesReclaimed += it.size }
            chunkStore.remove(hash)
        }

        return ChunkGcResult(
            manifestsRemoved = orphanManifests.size,
            chunksRemoved = orphanChunks.size,
            bytesReclaimed = bytesReclaimed,
        )
    }
}
