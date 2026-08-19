package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash
import dev.kdb.storage.StorageAdapter

/**
 * [StorageAdapter] decorator that routes [writeBlob]/[readBlob] through a [ChunkedBlobStore] for
 * chunk-level dedup of near-duplicate blobs, and delegates everything else (documents, trees,
 * delta ingest) unchanged to [delegate].
 *
 * Wrapping any existing adapter with this is enough to make chunk-level dedup apply wherever
 * that adapter is used — e.g. `kdb-file`'s ingest/extract path takes a [StorageAdapter]
 * polymorphically and needs no changes to benefit from it.
 */
public class ChunkedStorageAdapter(
    private val delegate: StorageAdapter,
    public val blobStore: ChunkedBlobStore = ChunkedBlobStore(),
) : StorageAdapter by delegate {

    override suspend fun readBlob(contentHash: KdbHash): ByteArray? = blobStore.readBlob(contentHash)

    override suspend fun writeBlob(bytes: ByteArray): KdbHash = blobStore.writeBlob(bytes)
}
