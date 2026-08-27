package dev.kdb.storage.engine

import dev.kdb.storage.wal.WriteAheadLog
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

/**
 * JS has no synchronous way to wait on a suspend function from non-suspend code (no
 * `runBlocking` - a single-threaded event loop can't block itself on its own event queue
 * draining). [ServerStorageEngine.stopAsyncSync] is called from [kotlin.AutoCloseable.close],
 * which is synchronous by contract, so there is no way to *guarantee* this flush has landed
 * before `close()` returns on this platform - starting it and letting it run on the event loop
 * is the honest best effort, not a full substitute for JVM/Native's actually-blocking behavior.
 * [BrowserStorageEngine]'s own persistence is already a known-incomplete in-memory stub (see
 * docs/kdb-finish-up-plan.md's browser-persistence finding), so this doesn't regress anything
 * that currently relies on a synchronous guarantee here.
 */
@OptIn(DelicateCoroutinesApi::class)
internal actual fun blockingFinalSync(wal: WriteAheadLog?) {
    if (wal == null) return
    GlobalScope.launch { wal.sync() }
}
