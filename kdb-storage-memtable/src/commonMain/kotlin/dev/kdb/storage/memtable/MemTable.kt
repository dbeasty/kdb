package dev.kdb.storage.memtable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.sstable.DefaultSsTableWriter
import dev.kdb.storage.sstable.LsmBlobStore
import dev.kdb.storage.sstable.SsTableHandle
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * What one memtable generation knows about a key. A null [value] is a tombstone - deliberately
 * distinguishable from "no entry at all", which [MemTable.lookup] signals by returning null.
 */
public data class MemTableEntry(val value: ByteArray?) {
    public val deleted: Boolean get() = value == null

    override fun equals(other: Any?): Boolean =
        this === other || (other is MemTableEntry && value.contentEquals(other.value))

    override fun hashCode(): Int = value?.contentHashCode() ?: 0
}

public interface MemTable {
    public suspend fun put(key: KdbHash, value: ByteArray)

    /**
     * Returns the value for [key], or null if the key is absent *or* tombstoned. Callers that need
     * to tell those two apart - anything that would otherwise fall through to an older generation -
     * must use [lookup] instead.
     */
    public suspend fun get(key: KdbHash): ByteArray?

    /**
     * Reports what this generation knows about [key]. A null return means it has never seen the
     * key and the caller should keep searching older generations; a [MemTableEntry] with
     * `deleted = true` is a tombstone, and older generations must NOT be consulted - reading
     * through a tombstone is exactly how a delete failed to shadow an entry already flushed to an
     * SSTable.
     */
    public suspend fun lookup(key: KdbHash): MemTableEntry?

    public suspend fun delete(key: KdbHash)
    public val sizeBytes: Long
}

public class SortedMemTable : MemTable {
    private val map = linkedMapOf<KdbHash, ByteArray?>()
    private var bytes = 0L

    // put/delete now net out the *previous* value's size against the new one - previously bytes
    // only ever grew (every put added value.size, delete added nothing but never subtracted
    // anything either), so sizeBytes drifted arbitrarily far above the table's real footprint
    // under any workload with overwrites or deletes. Currently dormant (nothing reads sizeBytes
    // to trigger a flush yet - see MemTableManager's doc comment on the still-missing automatic
    // flush trigger), but wrong regardless, and would misfire the moment such a trigger exists.
    override suspend fun put(key: KdbHash, value: ByteArray) {
        val old = map.put(key, value)
        bytes += value.size - (old?.size ?: 0)
    }

    override suspend fun get(key: KdbHash): ByteArray? = map[key]

    override suspend fun lookup(key: KdbHash): MemTableEntry? =
        if (map.containsKey(key)) MemTableEntry(map[key]) else null

    // A key deleted before it was ever written still joins the insertion order (LinkedHashMap
    // keeps it), so a later put of it is not silently dropped from the flush snapshot.
    override suspend fun delete(key: KdbHash) {
        val old = map.put(key, null)
        bytes -= old?.size ?: 0
    }

    override val sizeBytes: Long get() = bytes

    internal fun snapshotEntries(): List<Pair<KdbHash, ByteArray?>> = map.entries.map { it.key to it.value }
}

public class MemTableManager(
    private val namespaceId: String,
    private val ioShim: PlatformIoShim,
    private val blobStore: LsmBlobStore,
) {
    private val mutex = Mutex()
    private var active = SortedMemTable()
    private var pendingFlush: SortedMemTable? = null

    public suspend fun put(key: KdbHash, value: ByteArray) = mutex.withLock { active.put(key, value) }

    /**
     * Tombstones [key] in the active memtable. The tombstone hides any value in the generation
     * being flushed and in the blob store, and survives the flush: [flush] writes it into the
     * SSTable as a real delete marker ([dev.kdb.storage.sstable.BlockHandle.deleted]), so a delete
     * of an already-flushed key stays deleted. Mirrors Go's memtable.Manager.Delete, which this
     * type previously had no counterpart for at all.
     */
    public suspend fun delete(key: KdbHash): Unit = mutex.withLock { active.delete(key) }

    /**
     * Reads through the generations newest-first, stopping at the first one with an opinion about
     * [key] - a tombstone included. The old chain of `?:` fallbacks could not stop at a tombstone
     * (they are indistinguishable from "absent" once flattened to a null value), so a delete never
     * shadowed the SSTable underneath it.
     */
    public suspend fun get(key: KdbHash): ByteArray? =
        mutex.withLock {
            active.lookup(key)?.let { return@withLock it.value }
            pendingFlush?.lookup(key)?.let { return@withLock it.value }
            blobStore.lookup(key)?.let { return@withLock if (it.deleted) null else it.value }
            null
        }

    // Clears pendingFlush only *after* the SSTable write actually succeeds - previously cleared
    // it immediately after staging the writer, before writer.finish() (the call that can
    // actually fail: I/O errors, a full disk, ...) ever ran. A throw there used to lose the
    // whole generation silently: active had already been swapped to a fresh empty table, so by
    // the time the exception propagated, the flushed-but-not-yet-durable writes were reachable
    // from neither active, pendingFlush (already nulled), nor blobStore (finish() never
    // completed) - get() would report every one of them as simply absent. They're still not
    // durably in an SSTable if finish() throws (this doesn't add a retry mechanism), but they
    // remain visible via pendingFlush until whatever called flush() decides how to recover.
    public suspend fun flush(level: Int = 0): SsTableHandle? =
        mutex.withLock {
            val snap = active
            pendingFlush = snap
            active = SortedMemTable()
            val writer = DefaultSsTableWriter(ioShim, namespaceId, level)
            var count = 0
            for ((k, v) in snap.snapshotEntries()) {
                // Tombstones are written, not skipped. Skipping them is what made a delete of an
                // already-flushed key temporary: the tombstone lived only in the memtable, so the
                // next flush erased the only record that the key had been deleted, and the value
                // reappeared from the SSTable it had been flushed into earlier. The format now
                // carries a delete marker (BlockHandle.deleted) on both sides of the port.
                if (v != null) writer.put(k, v) else writer.delete(k)
                count++
            }
            if (count == 0) {
                pendingFlush = null
                return@withLock null
            }
            val handle = writer.finish()
            blobStore.addTable(handle)
            pendingFlush = null
            handle
        }
}
