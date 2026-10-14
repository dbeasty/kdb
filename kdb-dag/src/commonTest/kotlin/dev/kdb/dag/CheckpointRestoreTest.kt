package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A DAG restored from a checkpoint holds commits without their operations, and must never hand
 * one out that way: reads load them, check them against the commit's hash, or fail loudly.
 */
class CheckpointRestoreTest {
    private suspend fun history(n: Int): Pair<CommitDag, List<KdbCommit>> {
        val dag = inMemoryCommitDag("app/t")
        val out = mutableListOf<KdbCommit>()
        var head = dag.head()
        for (i in 0 until n) {
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = head,
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"i":$i}""")),
                    timestamp = KdbTimestamp(1_700_000_000_000L + i, i),
                    authorNodeId = KdbUuid.random(),
                )
            val c = dag.appendCommit(tx, head, DocumentTree.EMPTY, schemaHash = null, message = "c$i")
            out += c
            head = c.hash
        }
        return dag to out
    }

    private fun stripped(c: KdbCommit) = CheckpointCommit(c.copy(operations = emptyList()), c.operations.size)

    @Test
    fun restoredCommitsLoadTheirOperationsWhenRead() =
        runTest {
            val (source, commits) = history(4)
            val dag = inMemoryCommitDag("app/t")
            val r = dag as CheckpointRestorableDag
            val loads = mutableListOf<KdbHash>()
            r.setOperationsLoader { h -> loads += h; commits.firstOrNull { it.hash == h } }
            // Out of order, as a checkpoint lists them: parents are not required on the way in.
            r.restoreCheckpoint(commits.reversed().asSequence().map { stripped(it) }, source.listBranches(), emptyList())
            r.restoreCommitOperations(listOf(commits.last()))
            assertEquals(commits.last().hash, dag.head())
            assertEquals(3, r.commitsWithoutOperations())

            assertEquals(commits[1].operations, dag.getCommitOrThrow(commits[1].hash).operations)
            assertEquals(listOf(commits[1].hash), loads)
            // Every walked commit comes back whole.
            val walked = dag.walk(dag.head()).filterIsInstance<TraversalEntry.Full>().map { it.commit }
            assertTrue(walked.filter { it.message.startsWith("c") }.all { it.operations.size == 1 })
            assertEquals(0, r.commitsWithoutOperations())
            assertNotNull(dag.getCommitByTransactionId(commits[0].transactionId))
        }

    @Test
    fun operationsThatDoNotReproduceTheHashAreRefused() =
        runTest {
            val (source, commits) = history(2)
            val dag = inMemoryCommitDag("app/t")
            val r = dag as CheckpointRestorableDag
            r.setOperationsLoader { h ->
                commits.first { it.hash == h }.let { it.copy(operations = listOf(KdbOp.Delete(KdbUuid.random()))) }
            }
            r.restoreCheckpoint(commits.asSequence().map { stripped(it) }, source.listBranches(), emptyList())
            assertFailsWith<DagConsistencyException> { dag.getCommit(commits[0].hash) }
            assertFailsWith<DagConsistencyException> {
                r.restoreCommitOperations(listOf(commits[1].copy(message = "tampered")))
            }
        }

    @Test
    fun withoutALoaderAStrippedCommitIsAnErrorNeverAnEmptyOne() =
        runTest {
            val (source, commits) = history(2)
            val dag = inMemoryCommitDag("app/t")
            val r = dag as CheckpointRestorableDag
            r.restoreCheckpoint(commits.asSequence().map { stripped(it) }, source.listBranches(), emptyList())
            assertFailsWith<DagConsistencyException> { dag.getCommitOrThrow(commits[0].hash) }
        }

    @Test
    fun aShallowRootIsAdmittedWithoutItsParents() =
        runTest {
            val (_, commits) = history(3)
            val dag = inMemoryCommitDag("app/t")
            assertFailsWith<DagConsistencyException> { dag.putCommit(commits[2]) }
            (dag as CheckpointRestorableDag).putShallowCommit(commits[2])
            assertTrue(dag.hasCommit(commits[2].hash))
            assertFailsWith<DagConsistencyException> {
                dag.putShallowCommit(commits[1].copy(message = "not its hash"))
            }
        }
}
