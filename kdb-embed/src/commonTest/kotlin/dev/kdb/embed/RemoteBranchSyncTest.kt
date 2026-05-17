package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.peersync.computeSyncPlan
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RemoteBranchSyncTest {
    @Test
    fun putUsesWriteBaseVersion() =
        runTest {
            val ns = "app/write-base"
            val runtime = openMemoryRuntime("app", ns, KdbSchema.NONE)
            val root = runtime.dag.head()
            appendCommit(runtime, ns, root, """{"v":"sibling"}""")
            runtime.dag.setHead("main", root)
            runtime.writeBaseVersion = root
            val docId = putJson(runtime, ns, """{"v":"fork"}""")
            val commit = runtime.dag.getCommitOrThrow(runtime.dag.head())
            assertEquals(root, commit.parentHashes.single())
            assertTrue(getJson(runtime, ns, docId).contains("fork"))
        }

    @Test
    fun computePlanMatchesForkScenario() =
        runTest {
            val ns = "app/fork-plan"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val root = dag.head()
            val a = appendCommit(dag, storage, ns, root, "a")
            val b = appendCommit(dag, storage, ns, a.hash, "b")
            dag.setHead("main", a.hash)
            val c = appendCommit(dag, storage, ns, a.hash, "c")
            val plan = computeSyncPlan(dag, b.hash, c.hash)
            assertEquals(a.hash, plan.commonAncestor)
            assertTrue(plan.localOnly.contains(b.hash))
            assertTrue(plan.remoteOnly.contains(c.hash))
        }

    private suspend fun appendCommit(
        runtime: EmbeddedKdbRuntime,
        ns: String,
        parent: KdbHash,
        json: String,
    ): KdbCommit {
        val dag = runtime.dag
        val storage = runtime.storage
        return appendCommit(dag, storage, ns, parent, json)
    }

    private suspend fun appendCommit(
        dag: dev.kdb.dag.CommitDag,
        storage: StorageAdapter,
        ns: String,
        parent: KdbHash,
        json: String,
    ): KdbCommit {
        val doc = KdbDocument(KdbUuid.random(), json)
        storage.putDocument(ns, doc)
        val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                listOf(KdbOp.Write(doc.id, doc.json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        return dag.appendCommit(tx, parent, tree, null)
    }
}
