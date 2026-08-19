package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash

/** Content-addressed store for chunk bytes, shared across all blobs. */
public interface ChunkStore {
    public fun has(hash: KdbHash): Boolean

    public fun put(
        hash: KdbHash,
        bytes: ByteArray,
    )

    public fun get(hash: KdbHash): ByteArray?

    /** v1: in-memory only — enumeration is what a durable backend would need to expose for GC. */
    public fun allHashes(): Set<KdbHash>

    public fun remove(hash: KdbHash)
}

public class InMemoryChunkStore : ChunkStore {
    private val chunks = LinkedHashMap<KdbHash, ByteArray>()

    override fun has(hash: KdbHash): Boolean = chunks.containsKey(hash)

    override fun put(
        hash: KdbHash,
        bytes: ByteArray,
    ) {
        chunks.putIfAbsent(hash, bytes.copyOf())
    }

    override fun get(hash: KdbHash): ByteArray? = chunks[hash]?.copyOf()

    override fun allHashes(): Set<KdbHash> = chunks.keys.toSet()

    override fun remove(hash: KdbHash) {
        chunks.remove(hash)
    }

    public val chunkCount: Int get() = chunks.size

    public val totalBytes: Long get() = chunks.values.sumOf { it.size.toLong() }
}
