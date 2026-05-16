package dev.kdb.compaction

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CompactionSafetyException
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbTransaction
import dev.kdb.policy.CompactionPolicy
import dev.kdb.policy.SquashMode
import dev.kdb.policy.defaultMutable
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CompactionEngineTest {
    @Test
    fun neverSquashPolicy() =
        runTest {
            val ns = "app/data"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(
                defaultMutable(ns).copy(
                    compaction = CompactionPolicy(squashAfter = SquashMode.NEVER),
                ),
            )
            val engine = compactionEngine(dag, storage, policies)
            val plan = engine.plan(CompactionRequest(ns))
            assertTrue(plan.boundaries.isEmpty())
        }

    @Test
    fun tagSurvivesSquash() =
        runTest {
            val dag = inMemoryCommitDag("app/data")
            val root = dag.head()
            val c1 = appendEmpty(dag, root)
            val c2 = appendEmpty(dag, c1.hash)
            dag.createTag("v1", c1.hash)
            val tree = dag.getDocumentTreeOrThrow(c2.documentTreeHash)
            val synthetic =
                dag.squash(
                    squashHashes = listOf(c1.hash),
                    boundary = c2.hash,
                    syntheticTree = tree,
                    syntheticSchemaHash = null,
                )
            assertEquals(synthetic.hash, dag.getTag("v1")?.commitHash)
        }

    @Test
    fun squashBlockedWhenBranchHeadInSquash() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val head = dag.head()
            assertFailsWith<CompactionSafetyException> {
                dag.squash(
                    squashHashes = listOf(head),
                    boundary = head,
                    syntheticTree = DocumentTree.EMPTY,
                    syntheticSchemaHash = null,
                )
            }
        }

    @Test
    fun planWithForceRunCycle() =
        runTest {
            val ns = "app/log"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(defaultMutable(ns))
            var parent = dag.head()
            repeat(4) {
                parent = appendEmpty(dag, parent).hash
            }
            val engine = compactionEngine(dag, storage, policies)
            val plan = engine.plan(CompactionRequest(ns, force = true))
            if (plan.boundaries.isNotEmpty()) {
                val result = engine.runCycle(CompactionRequest(ns, force = true))
                assertTrue(result.squashedCount >= 0)
            }
        }

    private suspend fun appendEmpty(
        dag: dev.kdb.dag.CommitDag,
        parent: dev.kdb.codec.KdbHash,
    ): dev.kdb.document.KdbCommit {
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                emptyList(),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        return dag.appendCommit(tx, parent, DocumentTree.EMPTY, null)
    }
}
