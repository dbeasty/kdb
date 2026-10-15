package dev.kdb.replica

import dev.kdb.embed.shallowRootsOf
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.boolean
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Full-mode namespaces in a Kotlin file runtime ([fileNamespaces]): a replica that survives a
 * restart in a node's on-disk format. A squash bootstrap leaves a root only the namespace's
 * checkpoint records (Go's format, written by the Kotlin runtime), so reopening one proves the
 * replica persists through that checkpoint; and Go opens the replica's directory as its own.
 * Against a real Go node (kdb-replicahost); skipped without Go.
 */
class FileReplicaTest {
    companion object {
        private var host: GoHost? = null

        @BeforeClass
        @JvmStatic
        fun startHost() {
            if (goAvailable()) host = GoHost.start()
        }

        @AfterClass
        @JvmStatic
        fun stopHost() {
            host?.stop()
        }

        /** go/cmd/kdb-cpinterop: opens a data directory read-only and prints its state. */
        private val goDump: File by lazy {
            val out = File(Files.createTempDirectory("kdb-cpinterop-").toFile(), "kdb-cpinterop")
            val p =
                ProcessBuilder("go", "build", "-o", out.absolutePath, "./cmd/kdb-cpinterop")
                    .directory(File(repoRoot, "go")).redirectErrorStream(true).start()
            val log = p.inputStream.readAllBytes().decodeToString()
            check(p.waitFor(600, TimeUnit.SECONDS) && p.exitValue() == 0) { "go build kdb-cpinterop failed:\n$log" }
            out.deleteOnExit()
            out
        }
    }

    private val opened = ArrayList<Replica>()

    @AfterTest
    fun closeReplicas() =
        runBlocking {
            for (r in opened) r.close()
        }

    private fun host(): GoHost {
        assumeTrue("no Go toolchain", host != null)
        return host!!
    }

    private suspend fun replica(ns: String, dir: String): Replica =
        Replica.open(
            ReplicaOptions(
                store = MemoryReplicaStore(),
                url = host().url,
                namespaces = listOf(NamespaceSpec.Full(ns)),
                token = "rw-token",
                autoSync = false,
                fullNamespaces = fileNamespaces(dir),
            ),
        ).also { opened += it }

    private suspend fun close(r: Replica) {
        opened.remove(r)
        r.close()
    }

    @Test
    fun aSnapshotBootstrapReopensFromDiskAndSyncsOn() =
        runBlocking {
            val h = host()
            val ns = "app/u/file-boot"
            for (i in 0 until 20) h.put(ns, randomUuid(), """{"i":$i}""")
            val dir = Files.createTempDirectory("kdb-replica-file").toString()
            val r1 = replica(ns, dir)
            assertEquals("snapshot", r1.syncNow().single().route)
            val head = r1.namespace(ns).head
            close(r1)

            val r2 = replica(ns, dir)
            assertEquals(head, r2.namespace(ns).head)
            assertEquals(h.docs(ns), docsOf(r2, ns))
            val id = randomUuid()
            h.put(ns, id, """{"late":true}""")
            assertEquals("replay", r2.syncNow().single().route)
            assertEquals(h.docs(ns), docsOf(r2, ns))
            close(r2)
            // The replayed commits are in the log: a third open has them.
            val r3 = replica(ns, dir)
            assertEquals("""{"late":true}""", r3.get(ns, id))
            assertEquals(h.head(ns).first, r3.namespace(ns).head)
        }

    @Test
    fun aSquashedReplicaReopensFromItsCheckpointWithItsUnpushedWrite() =
        runBlocking {
            val h = host()
            val ns = "app/u/file-squash"
            h.put(ns, randomUuid(), """{"start":1}""")
            val dir = Files.createTempDirectory("kdb-replica-file").toString()
            val r1 = replica(ns, dir)
            r1.syncNow()
            h.cmd("seed", "ns" to ns, "commits" to 1500, "docs" to 10) // a rewrite-heavy backlog
            assertEquals("squash", r1.syncNow().single().route)
            val (squashed, tree) = h.head(ns)
            assertEquals(squashed, r1.namespace(ns).head)
            assertEquals(tree, r1.namespace(ns).rootHash())
            val offline = randomUuid()
            r1.put(ns, offline, """{"offline":true}""")
            val local = r1.namespace(ns).head
            close(r1)

            // The squash root is in no log segment: only the checkpoint written at close holds it.
            val r2 = replica(ns, dir)
            val n2 = r2.namespace(ns)
            assertEquals(local, n2.head)
            assertEquals(1, r2.status().namespaces.getValue(ns).pending)
            // The first sync bootstrapped by snapshot: its root is a shallow root too.
            val roots = (n2 as FileReplicaNamespace).runtime.shallowRootsOf(ns).map { it.toHex() }.sorted()
            assertEquals(2, roots.size, "$roots")
            assertTrue(squashed in roots, "$roots")
            assertEquals(h.docs(ns) + (offline to """{"offline":true}"""), docsOf(r2, ns))

            // Go opens the replica's directory as its own, from the same checkpoint.
            close(r2)
            val dump = goDumpOf(dir, ns)
            assertFalse(dump["replayedInFull"]!!.jsonPrimitive.boolean, "Go replayed the replica's log in full")
            assertEquals(local, dump["head"]!!.jsonPrimitive.content)
            assertEquals(roots, dump["shallowRoots"]!!.jsonArray.map { it.jsonPrimitive.content }.sorted())

            val r3 = replica(ns, dir)
            r3.syncNow()
            assertEquals("""{"offline":true}""", h.docs(ns)[offline], "the write made before the restart was pushed")
            assertEquals(0, r3.status().namespaces.getValue(ns).pending)
            assertTrue(r3.namespace(ns).head == h.head(ns).first)
        }

    private fun goDumpOf(dir: String, ns: String) =
        ProcessBuilder(goDump.absolutePath, "dump", "-dir", dir, "-ns", ns).redirectError(ProcessBuilder.Redirect.INHERIT).start().let { p ->
            val out = p.inputStream.readAllBytes().decodeToString()
            check(p.waitFor(120, TimeUnit.SECONDS) && p.exitValue() == 0) { "kdb-cpinterop dump failed" }
            wireJson.parseToJsonElement(out.trim()).jsonObject
        }
}
