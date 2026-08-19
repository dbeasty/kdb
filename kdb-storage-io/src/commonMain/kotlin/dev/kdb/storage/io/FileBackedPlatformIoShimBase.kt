package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Shared [PlatformIoShim] logic; platform code supplies [SegmentByteStore].
 */
public abstract class FileBackedPlatformIoShimBase(
    protected val config: PlatformIoConfig,
    private val store: SegmentByteStore,
) : PlatformIoShim {
    private val segmentMutexes = mutableMapOf<String, Mutex>()
    // Deliberately separate from segmentMutexes: appendToSegment needs mutual exclusion against
    // other appends (position bookkeeping isn't atomic in the underlying store), but a running
    // fsync must not block new appends from being written and queuing up for the *next* sync --
    // that pipelining is what makes WAL-level group commit (DefaultWriteAheadLog.sync) actually
    // batch multiple writers onto one fsync instead of degenerating back to one-at-a-time.
    private val flushMutexes = mutableMapOf<String, Mutex>()
    private val sealedSegments = mutableSetOf<String>()
    private val globalMutex = Mutex()

    private suspend fun mutexFor(segmentName: String): Mutex =
        globalMutex.withLock {
            segmentMutexes.getOrPut(segmentName) { Mutex() }
        }

    private suspend fun flushMutexFor(segmentName: String): Mutex =
        globalMutex.withLock {
            flushMutexes.getOrPut(segmentName) { Mutex() }
        }

    override suspend fun appendToSegment(segmentName: String, bytes: ByteArray): Long {
        validateSegmentName(segmentName)
        if (bytes.size > config.maxAppendBytes) {
            throw PlatformIoException(
                "append size ${bytes.size} exceeds max ${config.maxAppendBytes}",
                segmentName,
            )
        }
        return mutexFor(segmentName).withLock {
            if (segmentName in sealedSegments) {
                throw PlatformIoException("segment sealed", segmentName)
            }
            try {
                store.append(segmentName, bytes)
            } catch (e: Exception) {
                throw PlatformIoException("append failed: ${e.message}", segmentName, e)
            }
        }
    }

    override suspend fun readFromSegment(segmentName: String, offset: Long, length: Int): ByteArray {
        validateSegmentName(segmentName)
        require(offset >= 0 && length >= 0)
        return mutexFor(segmentName).withLock {
            try {
                store.read(segmentName, offset, length)
            } catch (e: PlatformIoException) {
                throw e
            } catch (e: Exception) {
                throw PlatformIoException("read failed: ${e.message}", segmentName, e)
            }
        }
    }

    // Deliberately does NOT take mutexFor(segmentName): that mutex also
    // guards appendToSegment, and group commit relies on new appends
    // being able to proceed (and register with the GroupCommitter)
    // *while* a physical fsync is in flight. Sharing the lock here would
    // serialize every writer behind each fsync's full duration, silently
    // defeating batching - this was found by benchmarking (see Phase 1
    // notes in docs/benchmarks/phase0-baseline.md): with a slow physical
    // fsync (JVM's FileChannel.force on macOS), throughput collapsed to
    // one write per fsync round-trip until this lock was removed here.
    // Safe because SegmentByteStore implementations must tolerate
    // force()/flush() running concurrently with writes to the same
    // segment (true of java.nio.FileChannel and Go's os.File).
    override suspend fun flushSegment(segmentName: String) {
        validateSegmentName(segmentName)
        flushMutexFor(segmentName).withLock {
            try {
                store.flush(segmentName, config.fsyncOnFlush)
            } catch (e: Exception) {
                throw PlatformIoException("flush failed: ${e.message}", segmentName, e)
            }
        }
    }

    override suspend fun sealSegment(segmentName: String) {
        validateSegmentName(segmentName)
        mutexFor(segmentName).withLock {
            if (segmentName in sealedSegments) return@withLock
            try {
                store.flush(segmentName, config.fsyncOnFlush)
                sealedSegments.add(segmentName)
                store.markSealed(segmentName)
            } catch (e: Exception) {
                throw PlatformIoException("seal failed: ${e.message}", segmentName, e)
            }
        }
    }

    override suspend fun listSegments(namespaceId: String): List<String> {
        val prefix = SegmentNameBuilder.namespacePrefix(namespaceId)
        return store.list(prefix).sorted()
    }

    override suspend fun deleteSegment(segmentName: String) {
        validateSegmentName(segmentName)
        mutexFor(segmentName).withLock {
            sealedSegments.remove(segmentName)
            segmentMutexes.remove(segmentName)
            flushMutexes.remove(segmentName)
            store.delete(segmentName)
        }
    }

    override suspend fun availableBytes(): Long = store.availableBytes()

    override suspend fun readSnapshot(key: String): ByteArray? = store.readSnapshot(key)

    override suspend fun writeSnapshot(key: String, data: ByteArray) {
        store.writeSnapshot(key, data)
    }

    override suspend fun deleteSnapshot(key: String) {
        store.deleteSnapshot(key)
    }
}

public interface SegmentByteStore {
    suspend fun append(segmentName: String, bytes: ByteArray): Long
    suspend fun read(segmentName: String, offset: Long, length: Int): ByteArray
    suspend fun flush(segmentName: String, fsync: Boolean)
    suspend fun markSealed(segmentName: String)
    suspend fun list(prefix: String): List<String>
    suspend fun delete(segmentName: String)
    suspend fun availableBytes(): Long
    suspend fun readSnapshot(key: String): ByteArray?
    suspend fun writeSnapshot(key: String, data: ByteArray)
    suspend fun deleteSnapshot(key: String)
}
