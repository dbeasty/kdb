package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash

/**
 * Maps a blob's logical content hash to its manifest bytes (either the literal
 * payload for small blobs, or an ordered list of chunk hashes for chunked ones).
 */
public interface ManifestStore {
    public fun put(
        contentHash: KdbHash,
        manifestBytes: ByteArray,
    )

    public fun get(contentHash: KdbHash): ByteArray?

    public fun allHashes(): Set<KdbHash>

    public fun remove(contentHash: KdbHash)
}

public class InMemoryManifestStore : ManifestStore {
    private val manifests = LinkedHashMap<KdbHash, ByteArray>()

    override fun put(
        contentHash: KdbHash,
        manifestBytes: ByteArray,
    ) {
        manifests.putIfAbsent(contentHash, manifestBytes.copyOf())
    }

    override fun get(contentHash: KdbHash): ByteArray? = manifests[contentHash]?.copyOf()

    override fun allHashes(): Set<KdbHash> = manifests.keys.toSet()

    override fun remove(contentHash: KdbHash) {
        manifests.remove(contentHash)
    }
}
