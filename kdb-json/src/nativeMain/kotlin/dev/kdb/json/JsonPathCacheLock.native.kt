package dev.kdb.json

internal actual object JsonPathCacheLock {
    private val lock = Any()

    actual fun <T> withLock(block: () -> T): T = synchronized(lock, block)
}
