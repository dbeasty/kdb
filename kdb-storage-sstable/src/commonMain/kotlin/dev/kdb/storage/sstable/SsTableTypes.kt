package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim

/**
 * Points at a compressed block in a segment, or - when [deleted] is set - records that the key was
 * deleted in this table and no block exists for it at all.
 *
 * [deleted] is a tombstone: this table says the key is gone, and older tables must not be
 * consulted for it. Before the format carried this, a delete of a key that had already been
 * flushed held only for as long as its tombstone lived in the memtable - the next flush dropped
 * it, and the value came back from the SSTable underneath.
 */
public data class BlockHandle(
    val offset: Long,
    val compressedSize: Int,
    val uncompressedSize: Int,
    val deleted: Boolean = false,
)

/** What one table knows about a key - see [SsTableReader.lookup]. */
public data class SsTableEntry(val value: ByteArray?, val deleted: Boolean) {
    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is SsTableEntry && deleted == other.deleted && value.contentEquals(other.value))

    override fun hashCode(): Int = 31 * (value?.contentHashCode() ?: 0) + deleted.hashCode()
}

public data class SsTableHandle(val fileHash: KdbHash, val level: Int, val segmentName: String)

public interface SsTableWriter {
    public suspend fun put(key: KdbHash, value: ByteArray)

    /**
     * Records a tombstone for [key]: this table asserts the key is gone, shadowing whatever older
     * tables hold for it.
     */
    public suspend fun delete(key: KdbHash)

    public suspend fun finish(): SsTableHandle
}

public interface SsTableReader {
    public suspend fun get(key: KdbHash): ByteArray?

    /**
     * Reports what this table knows about [key]. `null` means the table has never seen it and the
     * caller should keep searching older tables; a returned [SsTableEntry] with `deleted = true`
     * is a tombstone, and the search must stop there. [get] cannot express the difference - it
     * returns `null` for both - which is exactly how a delete used to fall through to an older
     * generation.
     */
    public suspend fun lookup(key: KdbHash): SsTableEntry?
}

/**
 * LRU-bounded block cache. Java's access-order `LinkedHashMap(capacity, loadFactor, true)`
 * constructor is JVM-only, so recency is tracked manually here (remove + re-insert on hit)
 * over a plain insertion-order [LinkedHashMap] — portable to JVM/JS/Native alike.
 */
public class BlockCache(private val capacityBytes: Long) {
    private val map = LinkedHashMap<Pair<KdbHash, Long>, ByteArray>()
    private var used = 0L

    public suspend fun get(key: KdbHash, offset: Long, loader: suspend () -> ByteArray): ByteArray {
        val k = key to offset
        map.remove(k)?.let { hit ->
            map[k] = hit
            return hit
        }
        val v = loader()
        put(k, v)
        return v
    }

    private fun put(k: Pair<KdbHash, Long>, v: ByteArray) {
        used += v.size
        map[k] = v
        while (used > capacityBytes && map.isNotEmpty()) {
            val eldestKey = map.keys.first()
            val eldestVal = map.remove(eldestKey)
            if (eldestVal != null) used -= eldestVal.size
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

    /**
     * Searches tables newest-first, stopping at the first table with an opinion about [key] -
     * including a tombstone, which means the key is gone and the older tables underneath must not
     * be consulted. Reading through a tombstone (which is what searching for the first non-null
     * value did) is how a flushed delete used to resurrect the value it deleted.
     */
    public suspend fun get(key: KdbHash): ByteArray? = lookup(key)?.takeUnless { it.deleted }?.value

    /**
     * [get] with the distinction it cannot express: whether the key is absent from every table, or
     * present as a tombstone.
     */
    public suspend fun lookup(key: KdbHash): SsTableEntry? {
        for (t in tables.asReversed()) {
            val reader = DefaultSsTableReader(ioShim, t)
            reader.lookup(key)?.let { return it }
        }
        return null
    }

    public fun addTable(handle: SsTableHandle) {
        tables.add(handle)
    }
}
