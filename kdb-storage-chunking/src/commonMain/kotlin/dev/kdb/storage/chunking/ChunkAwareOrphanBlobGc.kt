package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash
import dev.kdb.compaction.OrphanBlobGc

/**
 * [OrphanBlobGc] implementation for a [ChunkedStorageAdapter]. `dev.kdb.compaction.DefaultOrphanBlobGc`
 * is a no-op in v1 because plain storage adapters don't expose blob enumeration; a
 * [ChunkedBlobStore] does (it has to, to reconstruct chunked blobs), so this can actually sweep.
 */
public class ChunkAwareOrphanBlobGc(
    private val adapter: ChunkedStorageAdapter,
) : OrphanBlobGc {

    /** Returns the bytes-reclaimed estimate required by [OrphanBlobGc.sweep]'s contract. */
    override suspend fun sweep(
        @Suppress("UNUSED_PARAMETER") namespaceId: String,
        reachableHashes: Set<KdbHash>,
    ): Long = ChunkGc(adapter.blobStore).sweep(reachableHashes).bytesReclaimed
}
