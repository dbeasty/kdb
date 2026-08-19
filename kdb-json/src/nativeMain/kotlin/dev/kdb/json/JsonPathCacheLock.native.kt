package dev.kdb.json

// kotlin.synchronized is JVM-only; the JS actual already runs this cache lock-free (single
// threaded), so native does the same here rather than pulling in a new concurrency primitive
// for a small memoization cache.
internal actual object JsonPathCacheLock {
    actual fun <T> withLock(block: () -> T): T = block()
}
