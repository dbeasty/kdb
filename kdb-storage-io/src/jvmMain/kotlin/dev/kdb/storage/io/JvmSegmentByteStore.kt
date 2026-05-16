package dev.kdb.storage.io

import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class JvmSegmentByteStore(private val root: File) : SegmentByteStore {
    private val channels = mutableMapOf<String, FileChannel>()
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

    override suspend fun append(segmentName: String, bytes: ByteArray): Long {
        val ch = channel(segmentName)
        val pos = ch.size()
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

    private fun snapFile(key: String): File = File(File(root, "snap"), key.replace(':', '_'))

    override suspend fun readSnapshot(key: String): ByteArray? {
        val f = snapFile(key)
        return if (f.exists()) f.readBytes() else null
    }

    override suspend fun writeSnapshot(key: String, data: ByteArray) {
        snapFile(key).writeBytes(data)
    }

    override suspend fun deleteSnapshot(key: String) {
        snapFile(key).delete()
    }
}
