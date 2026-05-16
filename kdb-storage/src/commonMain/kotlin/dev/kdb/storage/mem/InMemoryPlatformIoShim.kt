package dev.kdb.storage.mem

import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Pure in-memory [PlatformIoShim] for JVM/JS/native tests ([Component 9] shim contract).
 *
 * Physical engines substitute real platform I/O in Layer 4a.
 */
public class InMemoryPlatformIoShim : PlatformIoShim {
    private val mutex = Mutex()
    private val segments = mutableMapOf<String, ByteArray>()
    private val snapshots = mutableMapOf<String, ByteArray>()

    override suspend fun appendToSegment(
        segmentName: String,
        bytes: ByteArray,
    ): Long =
        mutex.withLock {
            val cur = segments[segmentName] ?: byteArrayOf()
            val next = ByteArray(cur.size + bytes.size)
            cur.copyInto(destination = next)
            bytes.copyInto(destination = next, destinationOffset = cur.size)
            segments[segmentName] = next
            next.size.toLong()
        }

    override suspend fun readFromSegment(
        segmentName: String,
        offset: Long,
        length: Int,
    ): ByteArray =
        mutex.withLock {
            val full =
                segments[segmentName]
                    ?: throw IllegalStateException("unknown segment $segmentName")
            require(length >= 0)
            require(offset >= 0 && offset <= full.size.toLong())
            val off = offset.toInt()
            val safeLen = length.coerceAtMost(full.size - off)
            if (safeLen <= 0) return@withLock byteArrayOf()
            full.copyInto(ByteArray(safeLen), startIndex = off, endIndex = off + safeLen)
        }

    override suspend fun flushSegment(segmentName: String) {
        mutex.withLock { }
    }

    override suspend fun sealSegment(segmentName: String) {
        mutex.withLock { }
    }

    override suspend fun listSegments(namespaceId: String): List<String> =
        mutex.withLock {
            val prefix = "ns/$namespaceId/"
            segments.keys.filter { it.startsWith(prefix) }.toList()
        }

    override suspend fun deleteSegment(segmentName: String) {
        mutex.withLock {
            segments.remove(segmentName)
        }
    }

    override suspend fun availableBytes(): Long = Long.MAX_VALUE

    override suspend fun readSnapshot(key: String): ByteArray? = mutex.withLock { snapshots[key] }

    override suspend fun writeSnapshot(
        key: String,
        data: ByteArray,
    ) {
        mutex.withLock {
            snapshots[key] = data.copyOf()
        }
    }

    override suspend fun deleteSnapshot(key: String) {
        mutex.withLock {
            snapshots.remove(key)
        }
    }
}
