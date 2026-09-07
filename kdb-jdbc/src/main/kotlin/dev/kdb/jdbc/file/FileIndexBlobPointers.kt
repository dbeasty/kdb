package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.index.IndexBlobPointers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.exists

/**
 * Durable key → content-hash pointers for a file-backed runtime (Layer 16 §6.5/§9.2).
 *
 * [dev.kdb.index.StorageAdapterIndexBlobStore] puts the snapshot *bytes* in the namespace's
 * content-addressed blob store, which is already durable - what is not durable is the name that
 * finds them again, since `writeBlob` returns a hash and nothing maps `index/<id>/snapshot` back
 * to it. Without this, a restarted server has snapshots on disk it can no longer name, and
 * silently rebuilds every FULLTEXT/VECTOR index by scan (or, if the rebuild is skipped, serves an
 * empty one). The default pointer table is in-memory for exactly that reason; a file runtime
 * supplies this.
 *
 * The table is a tiny line-oriented file next to the namespace's other metadata, rewritten whole
 * and moved into place atomically - it holds one short line per index, so rewriting it costs less
 * than the bookkeeping an append-and-compact log would need.
 */
public class FileIndexBlobPointers(private val file: Path) : IndexBlobPointers {
    private val mutex = Mutex()
    private val pointers: MutableMap<String, KdbHash> = load()

    override suspend fun get(key: String): KdbHash? = mutex.withLock { pointers[key] }

    override suspend fun put(
        key: String,
        hash: KdbHash,
    ) {
        mutex.withLock {
            if (pointers[key] == hash) return@withLock
            pointers[key] = hash
            persist()
        }
    }

    override suspend fun remove(key: String) {
        mutex.withLock {
            if (pointers.remove(key) != null) persist()
        }
    }

    private fun load(): MutableMap<String, KdbHash> {
        val out = LinkedHashMap<String, KdbHash>()
        if (!file.exists()) return out
        for (line in Files.readAllLines(file)) {
            if (line.isBlank()) continue
            // key<TAB>hex - a key can contain anything except a tab, which no index key uses.
            val tab = line.lastIndexOf('\t')
            if (tab <= 0) continue
            val hash = runCatching { KdbHash.fromHex(line.substring(tab + 1)) }.getOrNull() ?: continue
            out[line.substring(0, tab)] = hash
        }
        return out
    }

    /** Temp file + atomic move: a crash mid-write leaves the previous table intact rather than a
     * half-written one that would strand every pointer, not just the one being updated. */
    private fun persist() {
        Files.createDirectories(file.parent)
        val tmp = file.resolveSibling(file.fileName.toString() + ".tmp")
        Files.write(tmp, pointers.entries.map { (k, v) -> "$k\t${v.toHex()}" })
        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: java.nio.file.AtomicMoveNotSupportedException) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING)
        }
    }
}
