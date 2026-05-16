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

    override suspend fun put(key: KdbHash, value: ByteArray) {
        map[key] = value
        bytes += value.size
    }

    override suspend fun get(key: KdbHash): ByteArray? = map[key]

    override suspend fun delete(key: KdbHash) {
        map[key] = null
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
            pendingFlush = null
            if (count == 0) return@withLock null
            val handle = writer.finish()
            blobStore.addTable(handle)
            handle
        }
}
