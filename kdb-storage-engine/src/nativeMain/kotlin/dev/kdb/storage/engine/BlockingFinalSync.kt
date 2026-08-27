package dev.kdb.storage.engine

import dev.kdb.storage.wal.WriteAheadLog
import kotlinx.coroutines.runBlocking

internal actual fun blockingFinalSync(wal: WriteAheadLog?) {
    runBlocking { wal?.sync() }
}
