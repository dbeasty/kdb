package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim

public data class BlockHandle(val offset: Long, val compressedSize: Int, val uncompressedSize: Int)

public data class SsTableHandle(val fileHash: KdbHash, val level: Int, val segmentName: String)

public interface SsTableWriter {
    public suspend fun put(key: KdbHash, value: ByteArray)
    public suspend fun finish(): SsTableHandle
}

public interface SsTableReader {
    public suspend fun get(key: KdbHash): ByteArray?
}

public class BlockCache(private val capacityBytes: Long) {
    private val map = LinkedHashMap<Pair<KdbHash, Long>, ByteArray>(16, 0.75f, true)
    private var used = 0L

    public suspend fun get(key: KdbHash, offset: Long, loader: suspend () -> ByteArray): ByteArray {
        val k = key to offset
        map[k]?.let { return it }
        val v = loader()
        put(k, v)
        return v
    }

    private fun put(k: Pair<KdbHash, Long>, v: ByteArray) {
        used += v.size
        map[k] = v
        while (used > capacityBytes && map.isNotEmpty()) {
            val eldest = map.entries.first()
            map.remove(eldest.key)
            used -= eldest.value.size
        }
    }
}

public class LsmBlobStore(
    private val ioShim: PlatformIoShim,
    private val namespaceId: String,
    private val cache: BlockCache,
    private val tables: MutableList<SsTableHandle> = mutableListOf(),
) {
    public suspend fun put(key: KdbHash, value: ByteArray) {
        // in-memory overlay handled by memtable; tables updated on flush
    }

    public suspend fun get(key: KdbHash): ByteArray? {
        for (t in tables.asReversed()) {
            val reader = DefaultSsTableReader(ioShim, t)
            reader.get(key)?.let { return it }
        }
        return null
    }

    public fun addTable(handle: SsTableHandle) {
        tables.add(handle)
    }
}
