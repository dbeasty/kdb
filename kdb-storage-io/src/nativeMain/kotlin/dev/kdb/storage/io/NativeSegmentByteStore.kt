package dev.kdb.storage.io

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray

/**
 * In-memory byte buffer per segment, persisted to a real file on flush/seal — the same model
 * [JvmSegmentByteStore]/[InMemoryPlatformIoShim] use, built on kotlinx-io's `Path`/`FileSystem`
 * (no `/` operator on [Path]; join via `Path(base, child)`; a [kotlinx.io.files.RawSource]/
 * [kotlinx.io.files.RawSink] must be `.buffered()` before the `ByteArray`-friendly read/write
 * helpers are available).
 */
internal class NativeSegmentByteStore(private val root: Path) : SegmentByteStore {
    private val fs = SystemFileSystem
    private val buffers = mutableMapOf<String, ByteArray>()
    private val mutex = Mutex()

    init {
        if (!fs.exists(root)) fs.createDirectories(root)
    }

    private fun segmentPath(segmentName: String): Path {
        validateSegmentName(segmentName)
        return Path(root, segmentName)
    }

    private fun readFileBytes(path: Path): ByteArray =
        if (fs.exists(path)) {
            fs.source(path).buffered().use { it.readByteArray() }
        } else {
            ByteArray(0)
        }

    /** Caller must hold [mutex]. */
    private fun loadLocked(segmentName: String): ByteArray = buffers.getOrPut(segmentName) { readFileBytes(segmentPath(segmentName)) }

    private fun persistLocked(segmentName: String) {
        val bytes = buffers[segmentName] ?: return
        val path = segmentPath(segmentName)
        path.parent?.let { if (!fs.exists(it)) fs.createDirectories(it) }
        fs.sink(path).buffered().use { it.write(bytes) }
    }

    override suspend fun append(
        segmentName: String,
        bytes: ByteArray,
    ): Long =
        mutex.withLock {
            val cur = loadLocked(segmentName)
            val next = ByteArray(cur.size + bytes.size)
            cur.copyInto(next)
            bytes.copyInto(next, destinationOffset = cur.size)
            buffers[segmentName] = next
            next.size.toLong()
        }

    override suspend fun read(
        segmentName: String,
        offset: Long,
        length: Int,
    ): ByteArray =
        mutex.withLock {
            val path = segmentPath(segmentName)
            if (!fs.exists(path) && segmentName !in buffers) {
                throw PlatformIoException("unknown segment", segmentName)
            }
            val buf = loadLocked(segmentName)
            val size = buf.size.toLong()
            if (offset > size) throw PlatformIoException("offset past end", segmentName)
            val off = offset.toInt()
            val safeLen = length.coerceAtMost((size - offset).toInt()).coerceAtLeast(0)
            if (safeLen == 0) return@withLock byteArrayOf()
            buf.copyOfRange(off, off + safeLen)
        }

    override suspend fun flush(
        segmentName: String,
        fsync: Boolean,
    ) {
        mutex.withLock { persistLocked(segmentName) }
    }

    override suspend fun markSealed(segmentName: String) {
        mutex.withLock {
            persistLocked(segmentName)
            buffers.remove(segmentName)
        }
    }

    override suspend fun list(prefix: String): List<String> {
        // append() only writes into buffers (see above) - a real file only exists once
        // flush()/markSealed() persists it - so a directory scan alone misses anything
        // appended-to but not yet flushed. Union with the in-memory keys so a segment is listed
        // as soon as it's been written to, matching JvmSegmentByteStore (whose append() writes
        // straight through to a real file, so its own directory-scan-based list() doesn't need
        // this - the two stores' listing had drifted out of sync with each other purely from
        // this timing difference, not from any deliberate design decision).
        val out = mutableListOf<String>()
        if (fs.exists(root)) collect(root, "", prefix, out)
        mutex.withLock {
            for (name in buffers.keys) {
                if (name.startsWith(prefix) && name !in out) out.add(name)
            }
        }
        return out
    }

    private fun collect(
        dir: Path,
        rel: String,
        prefix: String,
        out: MutableList<String>,
    ) {
        for (child in fs.list(dir)) {
            val name = child.name
            val relPath = if (rel.isEmpty()) name else "$rel/$name"
            val metadata = fs.metadataOrNull(child)
            if (metadata?.isDirectory == true) {
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
            if (fs.exists(path)) fs.delete(path, mustExist = false)
        }
    }

    override suspend fun availableBytes(): Long = Long.MAX_VALUE

    private fun snapPath(key: String): Path = Path(Path(root, "snap"), key.replace(':', '_'))

    override suspend fun readSnapshot(key: String): ByteArray? {
        val p = snapPath(key)
        return if (fs.exists(p)) fs.source(p).buffered().use { it.readByteArray() } else null
    }

    override suspend fun writeSnapshot(
        key: String,
        data: ByteArray,
    ) {
        val p = snapPath(key)
        p.parent?.let { if (!fs.exists(it)) fs.createDirectories(it) }
        fs.sink(p).buffered().use { it.write(data) }
    }

    override suspend fun deleteSnapshot(key: String) {
        val p = snapPath(key)
        if (fs.exists(p)) fs.delete(p, mustExist = false)
    }
}
