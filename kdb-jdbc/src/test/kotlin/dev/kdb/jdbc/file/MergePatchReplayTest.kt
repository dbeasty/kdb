package dev.kdb.jdbc.file

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import dev.kdb.jdbc.JdbcTestSupport
import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before

/**
 * A merge-patch write (KdbOp.Write with replace = false) must be logged as the document it stored,
 * not as the patch: replay - Kotlin's DeltaNamespaceReplayer and Go's delta_replay - reads a logged
 * write as the whole document. Logging the patch brought the namespace back holding the patch, under
 * a commit whose tree hash recorded the merged body (Go's operationsAsStored is the fix this ports).
 */
class MergePatchReplayTest {
    private val ns = "demo/merge"

    @Before
    fun setUp() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @After
    fun tearDown() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @Test
    fun mergePatchWrite_survivesFullReplay() =
        runBlocking {
            val root = createTempDirectory("kdb-merge-replay").toString()
            val id = KdbUuid.random()

            val first = openFileRuntime(root, "demo", ns)
            write(first, KdbOp.Write(id, """{"i":2,"pad":"pp"}"""))
            val committed = write(first, KdbOp.Write(id, """{"i":2,"rewrite":2}"""))
            val merged = KdbDocument.fromJson(id, """{"i":2,"pad":"pp","rewrite":2}""")
            assertEquals(
                merged.json,
                first.storage.getDocument(ns, id, committed.documentTreeHash)?.json,
                "the live engine merges the patch",
            )
            val logged = committed.operations.single() as KdbOp.Write
            assertEquals(merged.json, logged.patch, "the commit records the stored document, not the patch")

            // Full replay: nothing but the log to rebuild from.
            File(root, "snap").listFiles { f -> f.name.startsWith("kdb_checkpoint_") }?.forEach { it.deleteRecursively() }

            val again = openFileRuntime(root, "demo", ns)
            val head = again.dag.getCommitOrThrow(again.dag.head())
            assertEquals(committed.hash, head.hash)
            val liveTree = again.storage.commitTree(ns, head.documentTreeHash)
            assertEquals(head.documentTreeHash, liveTree.treeHash, "main's commit tree is the replayed live tree")
            assertEquals(merged.json, again.storage.getDocument(ns, id, head.documentTreeHash)?.json)
        }

    private suspend fun write(
        runtime: EmbeddedKdbRuntime,
        op: KdbOp,
    ) = commitViaEngine(
        runtime,
        ns,
        KdbTransaction(KdbUuid.random(), runtime.dag.head(), listOf(op), KdbTimestamp.now(), KdbUuid.random()),
    )
}
