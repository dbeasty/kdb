package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public actual object FileBackedPlatformIoShimFactory {
    public actual fun open(config: PlatformIoConfig): PlatformIoShim = BrowserFileBackedPlatformIoShim(config)
}

public actual class JvmFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(config, BrowserSegmentByteStore()),
    PlatformIoShim

public actual class NativeFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(config, BrowserSegmentByteStore()),
    PlatformIoShim

public actual class BrowserFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(config, BrowserSegmentByteStore()),
    PlatformIoShim

internal class BrowserSegmentByteStore : SegmentByteStore {
    private val segments = mutableMapOf<String, ByteArray>()
    private val mutex = Mutex()

    override suspend fun append(segmentName: String, bytes: ByteArray): Long =
        mutex.withLock {
            val cur = segments[segmentName] ?: byteArrayOf()
            val next = ByteArray(cur.size + bytes.size)
            cur.copyInto(next)
            bytes.copyInto(next, destinationOffset = cur.size)
            segments[segmentName] = next
            next.size.toLong()
        }

    override suspend fun read(segmentName: String, offset: Long, length: Int): ByteArray =
        mutex.withLock {
            val full = segments[segmentName] ?: throw PlatformIoException("unknown segment", segmentName)
            val off = offset.toInt()
            val safeLen = length.coerceAtMost(full.size - off).coerceAtLeast(0)
            if (safeLen <= 0) return@withLock byteArrayOf()
            full.copyOfRange(off, off + safeLen)
        }

    override suspend fun flush(segmentName: String, fsync: Boolean) {}

    override suspend fun markSealed(segmentName: String) {}

    override suspend fun list(prefix: String): List<String> =
        mutex.withLock { segments.keys.filter { it.startsWith(prefix) }.toList() }

    override suspend fun delete(segmentName: String) {
        mutex.withLock { segments.remove(segmentName) }
    }

    override suspend fun availableBytes(): Long = Long.MAX_VALUE

    override suspend fun readSnapshot(key: String): ByteArray? = BrowserSnapshotStore.read(key)

    override suspend fun writeSnapshot(key: String, data: ByteArray) {
        BrowserSnapshotStore.write(key, data)
    }

    override suspend fun deleteSnapshot(key: String) {
        BrowserSnapshotStore.delete(key)
    }
}
