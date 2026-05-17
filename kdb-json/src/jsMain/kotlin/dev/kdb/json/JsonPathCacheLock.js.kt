package dev.kdb.json

internal actual object JsonPathCacheLock {
    actual fun <T> withLock(block: () -> T): T = block()
}
