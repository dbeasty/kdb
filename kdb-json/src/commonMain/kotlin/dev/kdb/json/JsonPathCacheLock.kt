package dev.kdb.json

internal expect object JsonPathCacheLock {
    fun <T> withLock(block: () -> T): T
}
