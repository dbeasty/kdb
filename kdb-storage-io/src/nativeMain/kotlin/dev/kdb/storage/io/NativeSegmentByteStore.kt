package dev.kdb.storage.io

import kotlinx.io.Buffer
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class NativeSegmentByteStore(private val root: Path) : SegmentByteStore {
    private val fs = SystemFileSystem
    private val buffers = mutableMapOf<String, Buffer>()
    private val mutex = Mutex()

    init {
        if (!fs.exists(root)) fs.createDirectories(root)
    }

    private fun segmentPath(segmentName: String): Path {
        validateSegmentName(segmentName)
        return root / segmentName
    }

    private suspend fun load(segmentName: String): Buffer =
        mutex.withLock {
            buffers.getOrPut(segmentName) {
                val path = segmentPath(segmentName)
                val buf = Buffer()
                if (fs.exists(path)) {
                    fs.read(path) { buf.transferFrom(it) }
                }
                buf
            }
        }

    private suspend fun persist(segmentName: String) {
        val buf = buffers[segmentName] ?: return
        val path = segmentPath(segmentName)
        path.parent?.let { if (!fs.exists(it)) fs.createDirectories(it) }
        fs.sink(path, append = false).use { sink ->
            val bytes = ByteArray(buf.size.toInt())
            buf.copyTo(bytes, 0, bytes.size)
            sink.write(bytes)
        }
    }

    override suspend fun append(segmentName: String, bytes: ByteArray): Long {
        val buf = load(segmentName)
        val pos = buf.size
        buf.write(bytes)
        return pos + bytes.size
    }

    override suspend fun read(segmentName: String, offset: Long, length: Int): ByteArray {
        val path = segmentPath(segmentName)
        if (!fs.exists(path) && segmentName !in buffers) {
            throw PlatformIoException("unknown segment", segmentName)
        }
        val buf = load(segmentName)
        val size = buf.size
        if (offset > size) throw PlatformIoException("offset past end", segmentName)
        val safeLen = length.coerceAtMost((size - offset).toInt()).coerceAtLeast(0)
        if (safeLen == 0) return byteArrayOf()
        val out = ByteArray(safeLen)
        buf.copyTo(out, offset, offset + safeLen)
        return out
    }

    override suspend fun flush(segmentName: String, fsync: Boolean) {
        persist(segmentName)
    }

    override suspend fun markSealed(segmentName: String) {
        flush(segmentName, true)
        mutex.withLock { buffers.remove(segmentName) }
    }

    override suspend fun list(prefix: String): List<String> {
        if (!fs.exists(root)) return emptyList()
        val out = mutableListOf<String>()
        collect(root, "", prefix, out)
        return out
    }

    private fun collect(dir: Path, rel: String, prefix: String, out: MutableList<String>) {
        fs.list(dir).forEach { name ->
            val child = dir / name
            val relPath = if (rel.isEmpty()) name else "$rel/$name"
            if (fs.metadata(child).isDirectory) {
                collect(child, relPath, prefix, out)
            } else if (relPath.startsWith(prefix)) {
                out.add(relPath)
            }
        }
    }

    override suspend fun delete(segmentName: String) {
        mutex.withLock {
            buffers.remove(segmentName)
            val path = segmentPath(segmentName)
            if (fs.exists(path)) fs.delete(path)
        }
    }

    override suspend fun availableBytes(): Long = Long.MAX_VALUE

    private fun snapPath(key: String): Path = root / "snap" / key.replace(':', '_')

    override suspend fun readSnapshot(key: String): ByteArray? {
        val p = snapPath(key)
        return if (fs.exists(p)) fs.read(p) { it.readByteArray() } else null
    }

    override suspend fun writeSnapshot(key: String, data: ByteArray) {
        val p = snapPath(key)
        p.parent?.let { if (!fs.exists(it)) fs.createDirectories(it) }
        fs.sink(p).use { it.write(data) }
    }

    override suspend fun deleteSnapshot(key: String) {
        val p = snapPath(key)
        if (fs.exists(p)) fs.delete(p)
    }
}
