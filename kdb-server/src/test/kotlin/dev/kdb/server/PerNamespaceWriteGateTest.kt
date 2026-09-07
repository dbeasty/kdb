package dev.kdb.server

import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Component 73 (§12): the write gate is per namespace inside a runtime, not one gate per runtime, so
 * a commit stuck behind namespace A's gate does not hold up namespace B.
 *
 * Real threads (runBlocking + Dispatchers.Default), not runTest's virtual clock: the property under
 * test is that one coroutine genuinely proceeds while another is blocked.
 */
class PerNamespaceWriteGateTest {
    private val nsA = "demo/gate-a"
    private val nsB = "demo/gate-b"

    /** Guards: each namespace gets its own coordinator instance, and the legacy runtime-wide
     * accessor resolves to the default namespace's. */
    @Test
    fun eachNamespaceHasItsOwnCoordinator() =
        runBlocking<Unit> {
            val server = KdbServerRuntime(openMemoryRuntime("demo", nsA, KdbSchema.NONE))
            assertNotEquals(server.writeCoordinatorFor(nsA), server.writeCoordinatorFor(nsB))
            assertEquals(server.writeCoordinatorFor(nsA), server.writeCoordinator)
            assertEquals(server.writeCoordinatorFor(nsB), server.writeCoordinatorFor(nsB))
        }

    /** Guards: a commit blocked in namespace A's coordinator does not delay a commit in namespace B -
     * while a second commit in A itself does wait, proving the gate is still doing its job. */
    @Test
    fun aCommitBlockedInOneNamespaceDoesNotDelayAnother() =
        runBlocking<Unit> {
            val server = KdbServerRuntime(openMemoryRuntime("demo", nsB, KdbSchema.NONE))
            val holding = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val blocker =
                launch(Dispatchers.Default) {
                    server.writeCoordinatorFor(nsA).run {
                        holding.complete(Unit)
                        release.await()
                    }
                }
            holding.await()

            // Namespace B commits while A's gate is held.
            val commit =
                withTimeout(5_000) {
                    server.upsert(nsB, KdbUuid.random(), """{"v":1}""")
                }
            assertNotNull(commit)

            // The same runtime's namespace A is genuinely blocked meanwhile.
            val blocked =
                withTimeoutOrNull(300) {
                    server.writeCoordinatorFor(nsA).run { "entered" }
                }
            assertNull(blocked, "namespace A's gate must still serialize its own writers")

            release.complete(Unit)
            blocker.join()
            assertNotNull(withTimeout(5_000) { server.writeCoordinatorFor(nsA).run { "entered" } })
        }

    /** Guards: queueDepth/meanServiceTime (and therefore the conflict retry-after) are measured per
     * namespace - pressure on A must not inflate B's hint. */
    @Test
    fun queueDepthAndBackoffAreMeasuredPerNamespace() =
        runBlocking<Unit> {
            val server = KdbServerRuntime(openMemoryRuntime("demo", nsB, KdbSchema.NONE))
            val holding = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val blocker =
                launch(Dispatchers.Default) {
                    server.writeCoordinatorFor(nsA).run {
                        holding.complete(Unit)
                        release.await()
                    }
                }
            holding.await()
            val waiters =
                (1..3).map {
                    launch(Dispatchers.Default) { server.writeCoordinatorFor(nsA).run { } }
                }
            withTimeout(5_000) {
                while (server.writeCoordinatorFor(nsA).queueDepth() < 4) {
                    kotlinx.coroutines.yield()
                }
            }
            assertEquals(0, server.writeCoordinatorFor(nsB).queueDepth(), "B's gate must be idle while A is busy")
            assertTrue(server.conflictRetryAfterMs(nsB) in 2..250)

            release.complete(Unit)
            blocker.join()
            waiters.forEach { it.join() }
        }
}
