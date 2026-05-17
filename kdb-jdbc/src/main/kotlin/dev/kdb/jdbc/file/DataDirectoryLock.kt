package dev.kdb.jdbc.file

import dev.kdb.error.DataDirectoryLockedException
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Exclusive workspace lock at `{dataRoot}/.kdb.lock`.
 *
 * Cross-process exclusivity uses [FileChannel.tryLock]; within one JVM, [DataDirectoryLockRegistry]
 * reference-counts a single OS lock per [dataRoot].
 */
public object NamespacePathsLock {
    public const val LOCK_FILE_NAME: String = ".kdb.lock"

    public fun lockPath(dataRoot: String): Path = Path.of(dataRoot).resolve(LOCK_FILE_NAME)
}

public data class DataDirectoryLockInfo(
    val pid: Long,
    val holder: String,
    val host: String,
    val acquiredAt: String,
)

public sealed class StaleLockReleaseResult {
    public data object NoLockFile : StaleLockReleaseResult()

    public data class Removed(val previous: DataDirectoryLockInfo?) : StaleLockReleaseResult()

    public data class StillHeld(val info: DataDirectoryLockInfo) : StaleLockReleaseResult()
}

public class DataDirectoryLockLease internal constructor(
    private val dataRoot: String,
    private val onRelease: () -> Unit,
) : AutoCloseable {
    public val dataRootPath: String get() = dataRoot

    override fun close() {
        onRelease()
    }
}

public object DataDirectoryLockRegistry {
    private data class Entry(
        val lock: DataDirectoryLock,
        var refCount: Int,
    )

    private val entries = mutableMapOf<String, Entry>()
    private val registryLock = ReentrantLock()

    fun acquire(dataRoot: String, holder: String): DataDirectoryLockLease =
        registryLock.withLock {
            val normalized = Path.of(dataRoot).toAbsolutePath().normalize().toString()
            val entry =
                entries.getOrPut(normalized) {
                    Entry(DataDirectoryLock.acquire(normalized, holder), refCount = 0)
                }
            entry.refCount++
            DataDirectoryLockLease(normalized) {
                registryLock.withLock {
                    releaseLocked(normalized)
                }
            }
        }

    private fun releaseLocked(dataRoot: String) {
        val entry = entries[dataRoot] ?: return
        entry.refCount--
        if (entry.refCount <= 0) {
            entry.lock.close()
            entries.remove(dataRoot)
        }
    }

    fun releaseBlocking(dataRoot: String) {
        registryLock.withLock {
            val normalized = Path.of(dataRoot).toAbsolutePath().normalize().toString()
            releaseLocked(normalized)
        }
    }

    /** Test hook: drop in-process lock state without touching the lock file. */
    fun clearAllForTests() {
        registryLock.withLock {
            entries.values.forEach { it.lock.closeQuietly() }
            entries.clear()
        }
    }
}

/** Acquire exclusive workspace lock; release via [DataDirectoryLockLease.close]. */
public fun acquireDataDirectoryLock(dataRoot: String, holder: String): DataDirectoryLockLease =
    DataDirectoryLockRegistry.acquire(dataRoot, holder)

internal class DataDirectoryLock private constructor(
    private val fileLock: java.nio.channels.FileLock,
    private val channel: FileChannel,
    private val lockFile: RandomAccessFile,
) : AutoCloseable {
    override fun close() {
        closeQuietly()
    }

    fun closeQuietly() {
        try {
            fileLock.release()
        } catch (_: Exception) {
        }
        try {
            channel.close()
        } catch (_: Exception) {
        }
        try {
            lockFile.close()
        } catch (_: Exception) {
        }
    }

    companion object {
        fun acquire(dataRoot: String, holder: String): DataDirectoryLock {
            Files.createDirectories(Path.of(dataRoot))
            val path = NamespacePathsLock.lockPath(dataRoot)
            val raf = RandomAccessFile(path.toFile(), "rw")
            val channel = raf.channel
            val fileLock =
                try {
                    channel.tryLock()
                } catch (_: OverlappingFileLockException) {
                    null
                }
            if (fileLock == null) {
                channel.close()
                raf.close()
                val info = readLockInfo(path)
                val pid = info?.pid
                val label = info?.holder
                val detail =
                    when {
                        pid != null && label != null ->
                            " (held by $label, pid $pid on ${info.host})"
                        pid != null -> " (pid $pid on ${info?.host ?: "unknown host"})"
                        else -> ""
                    }
                throw DataDirectoryLockedException(
                    "database workspace is already open$detail; close the other process or run: kdb unlock",
                    dataRoot = dataRoot,
                    holderPid = pid,
                    holderLabel = label,
                )
            }
            val payload =
                DataDirectoryLockInfo(
                    pid = ProcessHandle.current().pid(),
                    holder = holder,
                    host = hostname(),
                    acquiredAt = Instant.now().toString(),
                )
            channel.truncate(0)
            val bytes = encodeLockInfo(payload).toByteArray()
            channel.position(0)
            channel.write(java.nio.ByteBuffer.wrap(bytes))
            channel.force(true)
            return DataDirectoryLock(fileLock, channel, raf)
        }

        fun readLockInfo(dataRoot: String): DataDirectoryLockInfo? =
            readLockInfo(NamespacePathsLock.lockPath(dataRoot))

        fun readLockInfo(path: Path): DataDirectoryLockInfo? {
            if (!Files.isRegularFile(path)) return null
            return try {
                val text = Files.readString(path).trim()
                if (text.isEmpty()) return null
                decodeLockInfo(text)
            } catch (_: Exception) {
                null
            }
        }

        private fun encodeLockInfo(info: DataDirectoryLockInfo): String =
            buildString {
                append("pid=").append(info.pid).append('\n')
                append("holder=").append(info.holder).append('\n')
                append("host=").append(info.host).append('\n')
                append("acquiredAt=").append(info.acquiredAt).append('\n')
            }

        private fun decodeLockInfo(text: String): DataDirectoryLockInfo? {
            val fields = mutableMapOf<String, String>()
            for (line in text.lineSequence()) {
                val eq = line.indexOf('=')
                if (eq <= 0) continue
                fields[line.substring(0, eq)] = line.substring(eq + 1)
            }
            val pid = fields["pid"]?.toLongOrNull() ?: return null
            val holder = fields["holder"] ?: return null
            val host = fields["host"] ?: return null
            val acquiredAt = fields["acquiredAt"] ?: return null
            return DataDirectoryLockInfo(pid, holder, host, acquiredAt)
        }

        fun releaseStaleLock(dataRoot: String): StaleLockReleaseResult {
            val path = NamespacePathsLock.lockPath(dataRoot)
            if (!Files.exists(path)) {
                return StaleLockReleaseResult.NoLockFile
            }
            val info = readLockInfo(path)
            if (info != null && isProcessAlive(info.pid)) {
                return StaleLockReleaseResult.StillHeld(info)
            }
            val previous = info
            Files.deleteIfExists(path)
            return StaleLockReleaseResult.Removed(previous)
        }

        private fun isProcessAlive(pid: Long): Boolean =
            try {
                ProcessHandle.of(pid).map { it.isAlive }.orElse(false)
            } catch (_: Exception) {
                false
            }

        private fun hostname(): String =
            try {
                java.net.InetAddress.getLocalHost().hostName
            } catch (_: Exception) {
                "localhost"
            }
    }
}

/** Removes a stale `.kdb.lock` when the recorded holder process is no longer running. */
public fun releaseStaleDataDirectoryLock(dataRoot: String): StaleLockReleaseResult =
    DataDirectoryLock.releaseStaleLock(dataRoot)

public fun readDataDirectoryLockInfo(dataRoot: String): DataDirectoryLockInfo? =
    DataDirectoryLock.readLockInfo(dataRoot)
