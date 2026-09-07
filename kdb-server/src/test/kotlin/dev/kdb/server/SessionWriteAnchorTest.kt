package dev.kdb.server

import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/**
 * Mirrors go/kdb/server/session_write_anchor_test.go.
 *
 * A READ_COMMITTED or READ_YOUR_WRITES session takes no read pin, so its statements read at the
 * live head - but its write base stayed frozen at the last transaction boundary. Any commit
 * landing in between left the session writing against a version older than the one its own
 * statement had just read, and conflict detection reported a conflict against a change that
 * statement had already seen.
 *
 * Kotlin has no wire Upsert (those messages are Go-only), so the Go tree carries the end-to-end
 * reproduction over the wire. What is shared, and what these pin, is the anchoring rule in
 * [SessionManager.pendingBuilder].
 */
class SessionWriteAnchorTest {
    private val ns = "app/data"

    private suspend fun newServer(): KdbServerRuntime =
        KdbServerRuntime(openMemoryRuntime("demo", ns, KdbSchema.NONE))

    @Test
    fun readCommittedSessionAnchorsItsTransactionAtTheLiveHead() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)
            val session = sessions.begin(ns, ReadConsistency.READ_COMMITTED)
            val headAtBegin = session.baseVersion

            // A commit lands after the session opened - another connection's, or in Go's case
            // the client's own sessionless Upsert.
            server.upsert(ns, KdbUuid.random(), """{"title":"someone else"}""")
            val headNow = server.runtime.dag.head()
            assertNotEquals(headAtBegin, headNow, "the upsert should have advanced the head")

            // Opening the session's transaction is what re-anchors it.
            sessions.pendingBuilder(session)
            assertEquals(
                headNow,
                session.baseVersion,
                "a READ_COMMITTED session must anchor its writes where its reads resolve",
            )
        }

    @Test
    fun snapshotSessionKeepsItsAnchorAtThePin() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)
            val session = sessions.begin(ns, ReadConsistency.SNAPSHOT)
            val pinned = session.baseVersion

            server.upsert(ns, KdbUuid.random(), """{"title":"moved on"}""")
            assertNotEquals(pinned, server.runtime.dag.head(), "the upsert should have advanced the head")

            sessions.pendingBuilder(session)
            assertEquals(
                pinned,
                session.baseVersion,
                "a SNAPSHOT session must keep writing at the snapshot it reads, or it commits " +
                    "over changes it cannot see",
            )
        }

    @Test
    fun onlyTheFirstBufferedWriteReAnchors() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)
            val session = sessions.begin(ns, ReadConsistency.READ_COMMITTED)

            sessions.pendingBuilder(session).write(KdbUuid.random(), """{"n":1}""")
            val anchored = session.baseVersion

            // A writer arriving mid-transaction must not move the anchor from underneath it -
            // that anchor is what makes the concurrent write detectable at commit.
            server.upsert(ns, KdbUuid.random(), """{"title":"mid-transaction"}""")
            sessions.pendingBuilder(session).write(KdbUuid.random(), """{"n":2}""")

            assertEquals(
                anchored,
                session.baseVersion,
                "a second statement in the same transaction must not re-anchor it",
            )
        }
}
