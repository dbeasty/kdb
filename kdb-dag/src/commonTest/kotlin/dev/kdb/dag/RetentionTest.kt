package dev.kdb.dag

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbTransaction
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Mirrors go/kdb/dag/retention_test.go - see [CommitDag.pin]'s doc comment for what this
 * closes: a SNAPSHOT read pin (and an in-flight commit's base version) is not a branch head,
 * which was the only retention root [InMemoryCommitDag.squash]/[InMemoryCommitDag.stubCommit]
 * consulted. */
class RetentionTest {
    /** Appends two commits and returns (middle, head) - middle is neither genesis nor a branch
     * head, i.e. exactly the kind [CommitDag.squash] is allowed to reclaim and a reader may
     * still be holding. */
    private suspend fun middleAndHead(dag: CommitDag): Pair<dev.kdb.codec.KdbHash, dev.kdb.codec.KdbHash> {
        val genesis = dag.head()
        val c1 = dag.appendCommit(newTx(genesis), genesis, DocumentTree.EMPTY, null)
        val c2 = dag.appendCommit(newTx(c1.hash), c1.hash, DocumentTree.EMPTY, null)
        return c1.hash to c2.hash
    }

    private fun newTx(base: dev.kdb.codec.KdbHash): KdbTransaction =
        KdbTransaction(
            id = KdbUuid.random(),
            baseVersion = base,
            operations = emptyList(),
            timestamp = KdbTimestamp.now(),
            authorNodeId = KdbUuid.random(),
        )

    @Test
    fun squashRefusesPinnedCommit() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val (middle, _) = middleAndHead(dag)

            // Without a pin the middle commit is reclaimable - the case that used to silently
            // strand a SNAPSHOT reader.
            val release = dag.pin(middle)
            assertFailsWith<CompactionSafetyException> {
                dag.squash(listOf(middle), middle, DocumentTree.EMPTY, null)
            }
            assertTrue(dag.hasCommit(middle), "pinned commit was removed despite the refusal")

            // Releasing makes it reclaimable again - a pin is a hold, not a permanent veto.
            release()
            assertEquals(0, dag.pinnedCount())
            dag.squash(listOf(middle), middle, DocumentTree.EMPTY, null)
        }

    @Test
    fun stubCommitRefusesPinnedCommit() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val (middle, _) = middleAndHead(dag)
            val release = dag.pin(middle)
            try {
                assertFailsWith<CompactionSafetyException> {
                    dag.stubCommit(middle, "ice://bucket/obj")
                }
                assertTrue(dag.hasCommit(middle), "pinned commit was archived despite the refusal")
            } finally {
                release()
            }
        }

    /** Two readers pinning the same commit is the normal case, not the exotic one: every
     * SNAPSHOT session that opened in the same transaction window pins the same head. One
     * finishing must not expose the commit to reclamation while the other still holds it. */
    @Test
    fun pinsAreCounted() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val (middle, _) = middleAndHead(dag)
            val first = dag.pin(middle)
            val second = dag.pin(middle)

            first()
            assertTrue(dag.isPinned(middle), "commit unpinned while a second reader still holds it")
            assertFailsWith<CompactionSafetyException> {
                dag.squash(listOf(middle), middle, DocumentTree.EMPTY, null)
            }

            second()
            assertFalse(dag.isPinned(middle), "commit still pinned after every reader released")
        }

    /** Release is idempotent so callers can both defer it (via try/finally) and call it
     * explicitly on an early path - double release must not decrement another reader's count. */
    @Test
    fun pinReleaseIsIdempotent() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val (middle, _) = middleAndHead(dag)
            val mine = dag.pin(middle)
            val other = dag.pin(middle)
            try {
                mine()
                mine()
                mine()
                assertTrue(dag.isPinned(middle), "repeated release dropped another reader's pin")
            } finally {
                other()
            }
        }
}
