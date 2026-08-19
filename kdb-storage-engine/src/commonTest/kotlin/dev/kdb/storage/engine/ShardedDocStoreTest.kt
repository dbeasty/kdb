package dev.kdb.storage.engine

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ShardedDocStoreTest {
    @Test
    fun concurrentPutGetDelete() =
        runTest {
            val store = ShardedDocStore()
            val ids = (0 until 500).map { KdbUuid.random() }

            coroutineScope {
                ids.map { id ->
                    async { store.put(KdbDocument(id, """{"v":1}""")) }
                }.forEach { it.await() }
            }

            assertEquals(ids.size, store.snapshot().size)
            for (id in ids) {
                assertEquals(id, store.get(id)?.id)
            }

            coroutineScope {
                ids.map { id -> async { store.delete(id) } }.forEach { it.await() }
            }

            assertEquals(0, store.snapshot().size)
            for (id in ids) {
                assertNull(store.get(id))
            }
        }
}
