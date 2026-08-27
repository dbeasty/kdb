package dev.kdb.storage.memtable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.sstable.DefaultSsTableWriter
import dev.kdb.storage.sstable.LsmBlobStore
import dev.kdb.storage.sstable.SsTableHandle
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface MemTable {
    public suspend fun put(key: KdbHash, value: ByteArray)
    public suspend fun get(key: KdbHash): ByteArray?
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

    public suspend fun get(key: KdbHash): ByteArray? =
        mutex.withLock {
            active.get(key) ?: pendingFlush?.get(key) ?: blobStore.get(key)
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
                if (v != null) {
                    writer.put(k, v)
                    count++
                }
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
