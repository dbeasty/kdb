package dev.kdb.storage.io

import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class JvmSegmentByteStore(private val root: File) : SegmentByteStore {
    private val channels = mutableMapOf<String, FileChannel>()
    // Tracked explicitly rather than re-derived from ch.size() per append:
    // size() then write(buf, pos) is two separate calls, so two concurrent
    // appends could both read the same size() before either writes,
    // landing at the same offset and corrupting the segment. getAndAdd is
    // the atomic reservation that sequence of calls was missing.
    private val sizes = mutableMapOf<String, AtomicLong>()
    private val sealed = mutableSetOf<String>()
    private val mutex = Mutex()

    init {
        root.mkdirs()
        File(root, "snap").mkdirs()
    }

    private fun segmentFile(segmentName: String): File {
        validateSegmentName(segmentName)
        val path = segmentName.replace('/', File.separatorChar)
        return File(root, path).also { it.parentFile?.mkdirs() }
    }

    private suspend fun channel(segmentName: String): FileChannel =
        mutex.withLock {
            channels.getOrPut(segmentName) {
                RandomAccessFile(segmentFile(segmentName), "rw").channel
            }
        }

    private suspend fun sizeCounter(segmentName: String, channel: FileChannel): AtomicLong =
        mutex.withLock {
            sizes.getOrPut(segmentName) { AtomicLong(channel.size()) }
        }

    override suspend fun append(segmentName: String, bytes: ByteArray): Long {
        val ch = channel(segmentName)
        val counter = sizeCounter(segmentName, ch)
        val pos = counter.getAndAdd(bytes.size.toLong())
        ch.write(java.nio.ByteBuffer.wrap(bytes), pos)
        return pos + bytes.size
    }

    override suspend fun read(segmentName: String, offset: Long, length: Int): ByteArray {
        if (!segmentFile(segmentName).exists()) {
            throw PlatformIoException("unknown segment", segmentName)
        }
        val ch = channel(segmentName)
        val size = ch.size()
        if (offset > size) throw PlatformIoException("offset past end", segmentName)
        val off = offset.toInt()
        val safeLen = length.coerceAtMost((size - offset).toInt()).coerceAtLeast(0)
        if (safeLen == 0) return byteArrayOf()
        val buf = java.nio.ByteBuffer.allocate(safeLen)
        ch.read(buf, offset)
        return buf.array()
    }

    override suspend fun flush(segmentName: String, fsync: Boolean) {
        val ch = channels[segmentName] ?: return
        ch.force(fsync)
    }

    override suspend fun markSealed(segmentName: String) {
        mutex.withLock {
            sealed.add(segmentName)
            channels.remove(segmentName)?.close()
            sizes.remove(segmentName)
        }
    }

    override suspend fun list(prefix: String): List<String> {
        val results = mutableListOf<String>()
        fun walk(dir: File, rel: String) {
            if (!dir.exists()) return
            dir.listFiles()?.forEach { f ->
                val name = if (rel.isEmpty()) f.name else "$rel/${f.name}"
                val normalized = name.replace(File.separatorChar, '/')
                if (f.isDirectory) {
                    walk(f, normalized)
                } else if (normalized.startsWith(prefix)) {
                    results.add(normalized)
                }
            }
        }
        walk(root, "")
        return results.sorted()
    }

    override suspend fun delete(segmentName: String) {
        mutex.withLock {
            channels.remove(segmentName)?.close()
            sizes.remove(segmentName)
            segmentFile(segmentName).delete()
            sealed.remove(segmentName)
        }
    }

    override suspend fun availableBytes(): Long =
        try {
            root.usableSpace
        } catch (_: Exception) {
            Long.MAX_VALUE
        }

    /**
     * The single flat file a snapshot key is stored under. Must stay identical to Go's
     * storio.SnapFileName and to NativeSegmentByteStore.snapPath: a snapshot written by one
     * runtime has to be found by the other.
     *
     * The colon goes because the raw key ("kdb:snap:<id>", "kdb:checkpoint:<id>") is not a
     * portable path name. The slash goes because a namespace id may contain one and this has to
     * stay a single path component: nesting it made "repro/account" write the FILE
     * snap/kdb_checkpoint_repro/account while "repro/account/ro" needed that same "account" to be
     * a directory, so whichever came second could never be written. "%2F" cannot collide, since a
     * namespace id segment allows only [A-Za-z0-9._-].
     */
    private fun snapFile(key: String): File =
        File(File(root, "snap"), key.replace(':', '_').replace("/", "%2F"))

    /** Where a snapshot written before the slash was flattened still lives. */
    private fun legacySnapFile(key: String): File =
        File(File(root, "snap"), key.replace(':', '_').replace('/', File.separatorChar))

    override suspend fun readSnapshot(key: String): ByteArray? {
        val f = snapFile(key)
        if (f.exists()) return f.readBytes()
        // For a namespace bootstrapped from a peer's snapshot the checkpoint is the only record
        // of its state and open refuses to proceed without it, so a pre-flattening file has to
        // still be found or upgrading would strand the namespace. writeSnapshot retires it.
        val legacy = legacySnapFile(key)
        return if (legacy != f && legacy.exists()) legacy.readBytes() else null
    }

    override suspend fun writeSnapshot(key: String, data: ByteArray) {
        val f = snapFile(key)
        f.parentFile?.mkdirs()
        f.writeBytes(data)
        val legacy = legacySnapFile(key)
        if (legacy != f) legacy.delete()
    }

    override suspend fun deleteSnapshot(key: String) {
        snapFile(key).delete()
        val legacy = legacySnapFile(key)
        if (legacy != snapFile(key)) legacy.delete()
    }
}
