package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash
import dev.kdb.document.kdbSha256

/**
 * Blob store with automatic chunk-level dedup for near-duplicate blobs.
 *
 * `writeBlob`/`readBlob` keep the same caller-facing contract as [dev.kdb.storage.StorageAdapter]:
 * the returned [KdbHash] is always the hash of the full logical content, never a manifest or
 * chunk hash — chunking is an internal storage detail.
 *
 * Blobs at or above [chunkThresholdBytes] are split by [ContentDefinedChunker] and each chunk is
 * stored once in [chunkStore], content-addressed. Two blobs sharing byte runs (a file re-ingested
 * with a small edit) automatically reuse the unchanged chunks; two unrelated blobs simply produce
 * no shared chunks, which costs no more than storing them separately — there is no explicit
 * similarity check or "worth it" threshold anywhere in this path, chunk-level dedup handles both
 * cases uniformly. Blobs below the threshold are stored as a single literal chunk (skips chunking
 * overhead for small payloads).
 */
public class ChunkedBlobStore(
    private val chunkStore: ChunkStore = InMemoryChunkStore(),
    private val manifestStore: ManifestStore = InMemoryManifestStore(),
    private val chunkThresholdBytes: Int = 128 * 1024,
    private val chunkerConfig: ChunkerConfig = ChunkerConfig(),
) {
    public fun writeBlob(bytes: ByteArray): KdbHash {
        val contentHash = KdbHash.fromBytes(kdbSha256(bytes))
        if (manifestStore.get(contentHash) != null) return contentHash

        val manifest =
            if (bytes.size < chunkThresholdBytes) {
                BlobManifest.Raw(bytes)
            } else {
                val chunkHashes =
                    ContentDefinedChunker.chunk(bytes, chunkerConfig).map { slice ->
                        val chunkBytes = bytes.copyOfRange(slice.offset, slice.offset + slice.length)
                        val chunkHash = KdbHash.fromBytes(kdbSha256(chunkBytes))
                        chunkStore.put(chunkHash, chunkBytes)
                        chunkHash
                    }
                BlobManifest.Chunked(chunkHashes)
            }
        manifestStore.put(contentHash, manifest.encode())
        return contentHash
    }

    public fun readBlob(contentHash: KdbHash): ByteArray? {
        val manifestBytes = manifestStore.get(contentHash) ?: return null
        return when (val manifest = BlobManifest.decode(manifestBytes)) {
            is BlobManifest.Raw -> manifest.bytes
            is BlobManifest.Chunked -> {
                val chunks = manifest.chunkHashes.map { hash -> chunkStore.get(hash) ?: error("missing chunk $hash referenced by manifest $contentHash") }
                val total = chunks.sumOf { it.size }
                val out = ByteArray(total)
                var offset = 0
                for (chunkBytes in chunks) {
                    chunkBytes.copyInto(out, destinationOffset = offset)
                    offset += chunkBytes.size
                }
                out
            }
        }
    }

    /** Chunk hashes [contentHash]'s manifest references — empty for raw blobs or an absent manifest. */
    public fun chunkRefs(contentHash: KdbHash): Set<KdbHash> {
        val manifestBytes = manifestStore.get(contentHash) ?: return emptySet()
        return when (val manifest = BlobManifest.decode(manifestBytes)) {
            is BlobManifest.Raw -> emptySet()
            is BlobManifest.Chunked -> manifest.chunkHashes.toSet()
        }
    }

    public fun isChunked(contentHash: KdbHash): Boolean {
        val manifestBytes = manifestStore.get(contentHash) ?: return false
        return BlobManifest.decode(manifestBytes) is BlobManifest.Chunked
    }

    public fun manifests(): ManifestStore = manifestStore

    public fun chunks(): ChunkStore = chunkStore
}
