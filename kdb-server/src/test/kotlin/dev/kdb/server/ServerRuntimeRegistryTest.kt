package dev.kdb.server

import dev.kdb.embed.openMemoryRuntime
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame

/**
 * Regression tests for the finding recorded in docs/kdb-finish-up-plan.md as 1-K7 (the same bug
 * shape already fixed on the Go side as 1-G5): [ServerRuntimeRegistry.getOrOpen] retained a
 * fresh runtime twice on top of [KdbServerRuntime]'s own initial refCount of 1 (refCount started
 * at 3, not 2, for the first caller), and [ServerRuntimeRegistry.release] never removed the
 * registry's map entry even once a runtime's refCount actually reached zero - so a single caller
 * opening then releasing left a zero-refCount runtime sitting in the map, silently handed back
 * out to the next [ServerRuntimeRegistry.getOrOpen] for the same key instead of that key being
 * reopened fresh.
 */
class ServerRuntimeRegistryTest {
    private suspend fun testRuntime(): KdbServerRuntime {
        val schema = KdbSchema.build(listOf(SchemaField("id", KdbFieldType.StringType, required = true, indexed = true)))
        return KdbServerRuntime(openMemoryRuntime("demo", "ns", schema))
    }

    @Test
    fun releaseRemovesTheEntryOnceTheOnlyCallerReleasesIt() =
        runTest {
            val key = "releasesAndReopensFresh"
            var opens = 0
            val open: suspend () -> KdbServerRuntime = { opens++; testRuntime() }

            val rt1 = ServerRuntimeRegistry.getOrOpen(key, open)
            assertEquals(1, opens)
            assertEquals(1, rt1.refCount.get(), "the first caller's own reference should be the only one outstanding")

            ServerRuntimeRegistry.release(key)

            val rt2 = ServerRuntimeRegistry.getOrOpen(key, open)
            assertEquals(2, opens, "expected getOrOpen to reopen fresh after a full release")
            assertNotSame(rt1, rt2, "expected a distinct runtime instance after the first was fully released")

            ServerRuntimeRegistry.release(key)
        }

    @Test
    fun sharedAcrossConcurrentCallersUntilBothRelease() =
        runTest {
            val key = "sharedAcrossConcurrentCallers"
            var opens = 0
            val open: suspend () -> KdbServerRuntime = { opens++; testRuntime() }

            val rtA = ServerRuntimeRegistry.getOrOpen(key, open)
            val rtB = ServerRuntimeRegistry.getOrOpen(key, open)
            assertEquals(1, opens, "expected the second getOrOpen to hit the cache, not reopen")
            assertSame(rtA, rtB, "expected both callers to share the same runtime instance")
            assertEquals(2, rtA.refCount.get(), "one reference per caller")

            ServerRuntimeRegistry.release(key) // A's checkout
            val rtC = ServerRuntimeRegistry.getOrOpen(key, open)
            assertEquals(1, opens, "B still holds a reference - must not reopen while any caller is still checked out")
            assertSame(rtB, rtC)

            ServerRuntimeRegistry.release(key) // B's checkout
            ServerRuntimeRegistry.release(key) // C's checkout

            val rtD = ServerRuntimeRegistry.getOrOpen(key, open)
            assertEquals(2, opens, "expected a fresh open once every caller released")
            assertNotSame(rtA, rtD)
            ServerRuntimeRegistry.release(key)
        }
}
