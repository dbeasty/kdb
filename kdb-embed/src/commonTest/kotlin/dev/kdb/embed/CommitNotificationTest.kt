package dev.kdb.embed

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Component 44 (Layer 12): a commit listener registered on [EmbeddedKdbRuntime] fires for every
 * successful commit through [commitViaEngine] - the fix for "SQL writes never notify Mode 1
 * stream subscribers, only peer-sync-arrived writes do" (see the Layer 12 gap analysis §5.1).
 */
class CommitNotificationTest {
    @Test
    fun putJsonNotifiesRegisteredListener() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            val seen = mutableListOf<String>()
            runtime.addCommitListener { namespaceId, commit ->
                seen.add(namespaceId)
                assertTrue(commit.operations.isNotEmpty())
            }

            putJson(runtime, ns, """{"userId":"u1"}""")

            assertEquals(listOf(ns), seen)
        }

    @Test
    fun multipleCommitsNotifyOncePerCommit() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            var count = 0
            runtime.addCommitListener { _, _ -> count++ }

            putJson(runtime, ns, """{"userId":"u1"}""")
            putJson(runtime, ns, """{"userId":"u2"}""")
            putJson(runtime, ns, """{"userId":"u3"}""")

            assertEquals(3, count)
        }

    @Test
    fun multipleListenersAllFire() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            var firstFired = false
            var secondFired = false
            runtime.addCommitListener { _, _ -> firstFired = true }
            runtime.addCommitListener { _, _ -> secondFired = true }

            putJson(runtime, ns, """{"userId":"u1"}""")

            assertTrue(firstFired)
            assertTrue(secondFired)
        }

    // A broken listener must not break the write path it's observing - the whole point of
    // notification being a side effect, not a dependency of the commit itself.
    @Test
    fun throwingListenerDoesNotFailTheCommit() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            var secondListenerFired = false
            runtime.addCommitListener { _, _ -> throw IllegalStateException("boom") }
            runtime.addCommitListener { _, _ -> secondListenerFired = true }

            // Must not throw.
            val docId = putJson(runtime, ns, """{"userId":"u1"}""")

            assertTrue(secondListenerFired, "a later listener must still run after an earlier one throws")
            val json = getJson(runtime, ns, docId)
            assertTrue(json.contains("u1"), json)
        }

    // Nobody registered a listener - must be a true no-op, not an error.
    @Test
    fun noListenersRegisteredIsFine() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"u1"}""")
        }
}
