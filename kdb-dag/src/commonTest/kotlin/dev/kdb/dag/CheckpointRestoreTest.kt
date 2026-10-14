package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.attachmentContentHash
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

    @Test
    fun anOlderCommitsAttachmentTableIsRebuiltFromLoadedOperations() =
        runTest {
            // Attachment changes, then plain writes: the changes' commits are no head, so the
            // checkpoint seeds only the head's table and the older ones must be replayed.
            val source = inMemoryCommitDag("app/t")
            val doc = KdbUuid.random()
            val att = KdbUuid.random()
            val v1 = attachmentContentHash("one".encodeToByteArray())
            val v2 = attachmentContentHash("two!".encodeToByteArray())
            val made = mutableListOf<KdbCommit>()
            for ((i, ops) in listOf(
                listOf(KdbOp.Write(doc, """{"d":1}"""), KdbOp.AttachmentPut(doc, att, "a.txt", 1, v1, 3, "")),
                listOf(KdbOp.AttachmentPut(doc, att, "a.txt", 1, v2, 4, "")),
                listOf(KdbOp.Write(doc, """{"d":2}""")),
            ).withIndex()) {
                val head = source.head()
                made += source.appendCommit(
                    KdbTransaction(KdbUuid.random(), head, ops, KdbTimestamp(1_700_000_000_000L + i), KdbUuid.random()),
                    head,
                    DocumentTree.EMPTY,
                    null,
                )
            }
            val want = made.map { source.attachmentTableAt(it.hash) }

            val dag = inMemoryCommitDag("app/t")
            val r = dag as CheckpointRestorableDag
            r.setOperationsLoader { h -> made.firstOrNull { it.hash == h } }
            r.restoreCheckpoint(made.asSequence().map { stripped(it) }, source.listBranches(), emptyList())
            r.restoreCommitOperations(listOf(made.last()))
            dag.seedAttachmentTable(want.last())
            assertEquals(2, r.commitsWithoutOperations())
            for ((i, c) in made.withIndex()) {
                val got = dag.attachmentTableAt(c.hash)
                assertEquals(want[i].root, got.root, "commit $i")
                assertEquals(want[i].list(doc), got.list(doc), "commit $i")
            }
            assertEquals(v1, dag.attachmentTableAt(made[0].hash).list(doc).single().contentHash)

            // With no way to load them, the older table is an error - never one built from no operations.
            val blind = inMemoryCommitDag("app/t") as CheckpointRestorableDag
            blind.restoreCheckpoint(made.asSequence().map { stripped(it) }, source.listBranches(), emptyList())
            assertFailsWith<DagConsistencyException> { (blind as CommitDag).attachmentTableAt(made[0].hash) }
        }
}
