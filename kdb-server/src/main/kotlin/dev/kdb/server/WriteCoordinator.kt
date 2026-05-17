package dev.kdb.server

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Serializes commits/replays against a shared server runtime. */
public class WriteCoordinator {
    private val mutex = Mutex()

    public suspend fun <T> run(block: suspend () -> T): T = mutex.withLock { block() }
}
