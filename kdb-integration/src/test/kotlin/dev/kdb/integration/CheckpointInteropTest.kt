package dev.kdb.integration

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.Attachment
import dev.kdb.document.AttachmentRef
import dev.kdb.document.AttachmentTable
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.kdbSha256
import dev.kdb.embed.AttachmentSource
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.PutAttachmentOptions
import dev.kdb.embed.SnapshotState
import dev.kdb.embed.attachmentTableAt
import dev.kdb.embed.commitViaEngine
import dev.kdb.embed.installSnapshot
import dev.kdb.embed.putAttachment
import dev.kdb.embed.shallowRootsOf
import dev.kdb.jdbc.file.openFileRuntime
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.boolean
import org.junit.Assume
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * One checkpoint format for both runtimes (owner decision 2026-10-14, docs/kdb-client-replica-plan.md):
 * the Kotlin file runtime writes Go's namespace checkpoint (NamespaceCheckpointer, GoCheckpointWriter)
 * and Go opens it, and the reverse - with the same heads, trees, documents, refs, attachment tables
 * and shallow roots on both sides.
 *
 * The Go side is go/cmd/kdb-cpinterop, built here with `go build`: `dump` opens a directory
 * read-only and prints its state, including whether Go replayed the log in full - which it must not
 * have done for a Kotlin checkpoint it can use. `squash` writes a Go namespace caught up by squash,
 * a state only a checkpoint records, so Kotlin can only open it from Go's checkpoint and Go can only
 * reopen it from Kotlin's.
 *
 * Skipped without a Go toolchain, except where KDB_REQUIRE_GO_SERVICE=1 (the CI integration job).
 */
class CheckpointInteropTest {
    private val ns = "app/data"

    data class State(
        val replayedInFull: Boolean = false,
        val head: String,
        val tree: String,
        val branches: Map<String, String>,
        val tags: Map<String, String>,
        val shallowRoots: List<String>,
        val attachmentRoots: Map<String, String>,
        val documents: Map<String, String>,
    )

    // --- the Go side ----------------------------------------------------------------------------

    private val repo: File by lazy {
        generateSequence(File("").absoluteFile) { it.parentFile }.firstOrNull { File(it, "go/go.mod").isFile }
            ?: error("cannot find the repository root")
    }

    private val helper: File? by lazy {
        val out = File(Files.createTempDirectory("kdb-cpinterop-").toFile(), "kdb-cpinterop")
        val p =
            try {
                ProcessBuilder("go", "build", "-o", out.absolutePath, "./cmd/kdb-cpinterop")
                    .directory(File(repo, "go")).redirectErrorStream(true).start()
            } catch (_: Exception) {
                return@lazy null
            }
        val log = p.inputStream.readAllBytes().decodeToString()
        check(p.waitFor(600, TimeUnit.SECONDS) && p.exitValue() == 0) { "go build ./cmd/kdb-cpinterop failed:\n$log" }
        out.deleteOnExit()
        out
    }

    private fun requireGo(): File {
        val h = helper
        if (h == null) {
            if (System.getenv("KDB_REQUIRE_GO_SERVICE") == "1") fail("no Go toolchain to build go/cmd/kdb-cpinterop")
            Assume.assumeTrue("no Go toolchain; skipping the Go side", false)
        }
        return h!!
    }

    private fun go(vararg args: String): String {
        val p = ProcessBuilder(requireGo().absolutePath, *args).start()
        val err = StringBuilder()
        val drain = Thread { err.append(p.errorStream.readAllBytes().decodeToString()) }.also { it.start() }
        val out = p.inputStream.readAllBytes().decodeToString()
        check(p.waitFor(300, TimeUnit.SECONDS)) { "kdb-cpinterop ${args.toList()} did not finish" }
        drain.join()
        if (p.exitValue() != 0) fail("kdb-cpinterop ${args.toList()} failed:\n$err")
        return out
    }

    private fun goDump(dir: String): State {
        val o = Json.parseToJsonElement(go("dump", "-dir", dir, "-ns", ns).trim()).jsonObject
        fun map(k: String) = (o[k] as JsonObject).mapValues { it.value.jsonPrimitive.content }
        return State(
            replayedInFull = o["replayedInFull"]!!.jsonPrimitive.boolean,
            head = o["head"]!!.jsonPrimitive.content,
            tree = o["tree"]!!.jsonPrimitive.content,
            branches = map("branches"),
            tags = map("tags"),
            shallowRoots = o["shallowRoots"]!!.jsonArray.map { it.jsonPrimitive.content },
            attachmentRoots = map("attachmentRoots"),
            documents = map("documents"),
        )
    }

    // --- the Kotlin side ------------------------------------------------------------------------

    private fun open(dir: String): EmbeddedKdbRuntime = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)

    private suspend fun kotlinState(rt: EmbeddedKdbRuntime): State {
        val head = rt.dag.head()
        val tree = rt.dag.getCommitOrThrow(head).documentTreeHash
        val docs = HashMap<String, String>()
        rt.storage.scanDocuments(ns, tree, 1000) { batch -> for (d in batch) docs[d.id.toString()] = d.json }
        val branches = rt.dag.listBranches()
        val tags = rt.dag.listTags()
        val refs = branches.map { it.headHash } + tags.map { it.commitHash }
        return State(
            head = head.toHex(),
            tree = tree.toHex(),
            branches = branches.associate { it.name to it.headHash.toHex() },
            tags = tags.associate { it.name to it.commitHash.toHex() },
            shallowRoots = rt.shallowRootsOf(ns).map { it.toHex() }.sorted(),
            attachmentRoots = refs.associate { it.toHex() to (rt.attachmentTableAt(it).root?.toHex() ?: "") },
            documents = docs,
        )
    }

    /** One commit writing (or, for a null body, deleting) each document. */
    private suspend fun commit(rt: EmbeddedKdbRuntime, bodies: Map<KdbUuid, String?>) {
        val head = rt.dag.head()
        val ops = bodies.map { (id, body) -> if (body == null) KdbOp.Delete(id) else KdbOp.Write(id, body, replace = true) }
        commitViaEngine(rt, ns, KdbTransaction(KdbUuid.random(), head, ops, KdbTimestamp.now(), KdbUuid.random()), targetHead = head)
    }

    /** Documents in batches, three rewrites and a delete: every kind of change. */
    private suspend fun seed(rt: EmbeddedKdbRuntime, n: Int, batch: Int): List<KdbUuid> {
        val ids = List(n) { KdbUuid.random() }
        ids.chunked(batch).forEachIndexed { b, chunk ->
            commit(rt, chunk.withIndex().associate { (i, id) -> id to """{"i":${b * batch + i},"pad":"${"p".repeat(i % 7)}"}""" })
        }
        for (r in 0 until 3) commit(rt, mapOf(ids[r] to """{"i":$r,"rewrite":$r}"""))
        commit(rt, mapOf(ids[n - 1] to null))
        return ids
    }

    private fun sections(dir: String): List<String> =
        File(dir, "snap").listFiles().orEmpty().map { it.name }.filter { it.startsWith("kdb_checkpoint-section_") }

    private fun checkpointFile(dir: String) = File(dir, "snap/kdb_checkpoint_app%2Fdata")

    private fun assertSame(
        kotlin: State,
        go: State,
        what: String,
    ) {
        assertFalse(go.replayedInFull, "$what: Go replayed the log in full instead of opening from the checkpoint")
        val k = kotlin.copy(replayedInFull = false)
        val g = go.copy(replayedInFull = false)
        if (k == g) return
        val diff = StringBuilder()
        if (k.head != g.head || k.tree != g.tree) diff.append("head/tree: Kotlin ${k.head}/${k.tree}, Go ${g.head}/${g.tree}\n")
        if (k.branches != g.branches) diff.append("branches: Kotlin ${k.branches}, Go ${g.branches}\n")
        if (k.tags != g.tags) diff.append("tags: Kotlin ${k.tags}, Go ${g.tags}\n")
        if (k.shallowRoots != g.shallowRoots) diff.append("shallow roots: Kotlin ${k.shallowRoots}, Go ${g.shallowRoots}\n")
        if (k.attachmentRoots != g.attachmentRoots) diff.append("attachment roots: Kotlin ${k.attachmentRoots}, Go ${g.attachmentRoots}\n")
        for (id in (k.documents.keys + g.documents.keys).sorted()) {
            if (k.documents[id] != g.documents[id]) diff.append("document $id: Kotlin ${k.documents[id]}, Go ${g.documents[id]}\n")
        }
        fail("$what: the runtimes disagree\n$diff")
    }

    // --- Kotlin writes, Go reads ----------------------------------------------------------------

    private fun kotlinWritesGoReads(n: Int, batch: Int, wantSection: Boolean) =
        runBlocking {
            requireGo()
            val dir = Files.createTempDirectory("kdb-cp-kt").toString()
            val rt = open(dir)
            val ids = seed(rt, n, batch)
            val first = rt.dag.head()
            rt.dag.createBranch("first", first)
            rt.dag.createTag("v1", first, "the first state")
            rt.putAttachment(ns, ids[1], AttachmentSource.of(ByteArray(3000) { (it * 7).toByte() }), PutAttachmentOptions(name = "k.bin", mime = "application/octet-stream"))
            commit(rt, mapOf(ids[2] to """{"after":"the attachment"}"""))
            val want = kotlinState(rt)
            rt.close()
            assertTrue(checkpointFile(dir).isFile, "Kotlin wrote no checkpoint at close")
            assertEquals(if (wantSection) 1 else 0, sections(dir).size, "section files: ${sections(dir)}")
            assertSame(want, goDump(dir), "Go opening Kotlin's checkpoint")

            // A second session on top: the checkpoint is rewritten at close, with a new section
            // that replaces the old one, and the log after the first one replays onto nothing.
            val again = open(dir)
            assertEquals(want.copy(replayedInFull = false), kotlinState(again), "Kotlin reopening its own checkpoint")
            commit(again, mapOf(ids[3] to """{"second":"session"}""", ids[4] to null))
            val want2 = kotlinState(again)
            again.close()
            assertEquals(if (wantSection) 1 else 0, sections(dir).size, "section files after a rewrite: ${sections(dir)}")
            assertSame(want2, goDump(dir), "Go opening Kotlin's second checkpoint")
        }

    @Test
    fun goOpensASmallKotlinCheckpoint() = kotlinWritesGoReads(n = 60, batch = 10, wantSection = true)

    @Test
    fun goOpensALargeKotlinCheckpoint() = kotlinWritesGoReads(n = 21_000, batch = 1_000, wantSection = true)

    @Test
    fun goOpensAVersion2KotlinCheckpoint() =
        runBlocking {
            requireGo()
            // No attachments and few entries: version 2, a record and no section, as Go writes it.
            val dir = Files.createTempDirectory("kdb-cp-kt2").toString()
            val rt = open(dir)
            val ids = seed(rt, 40, 10)
            rt.dag.createBranch("side", rt.dag.head())
            commit(rt, mapOf(ids[5] to """{"main":"moved on"}"""))
            val want = kotlinState(rt)
            rt.close()
            assertTrue(checkpointFile(dir).isFile)
            assertEquals(emptyList(), sections(dir))
            assertSame(want, goDump(dir), "Go opening Kotlin's version 2 checkpoint")
        }

    @Test
    fun goOpensAKotlinSnapshotBootstrapWithWritesOnTop() =
        runBlocking {
            requireGo()
            val dir = Files.createTempDirectory("kdb-cp-snap").toString()
            val rt = open(dir)
            val docs = (1..30).map { KdbDocument(KdbUuid(0x3000L + it, it.toLong()), """{"n":$it}""") }
            val tree = DocumentTree.build(docs.associate { it.id to it.contentHash })
            val table =
                AttachmentTable.EMPTY.put(
                    docs[0].id,
                    Attachment(KdbUuid(9, 9), "p.png", AttachmentRef(ATTACHMENT_HASH_SCHEME_CHUNKED_1, KdbHash.fromBytes(kdbSha256(byteArrayOf(7))), 1234, "image/png")),
                )
            val root =
                KdbCommit.build(
                    listOf(KdbHash.fromBytes(kdbSha256(byteArrayOf(1)))), ns, KdbUuid(1, 2), KdbTimestamp.fromEpochMicros(1_760_000_000_000_000),
                    KdbUuid(3, 4), listOf(KdbOp.Write(docs[0].id, docs[0].json)), tree.treeHash, null, "a peer's head", table.root,
                )
            rt.installSnapshot(ns, SnapshotState(root, docs, AttachmentTable.pack(listOf(table))))
            commit(rt, mapOf(docs[1].id to """{"n":2,"local":true}""", docs[2].id to null, KdbUuid(0x9999, 1) to """{"new":1}"""))
            val want = kotlinState(rt)
            assertEquals(listOf(root.hash.toHex()), want.shallowRoots)
            rt.close()
            assertSame(want, goDump(dir), "Go opening a Kotlin snapshot bootstrap with writes on top")
        }

    @Test
    fun aKilledKotlinRuntimeLeavesACheckpointFromItsNextOpen() =
        runBlocking {
            requireGo()
            // No close (a killed process): the next open replays the whole log and writes a
            // checkpoint right away (Go's checkpointAfterFullReplay), before anything else.
            val dir = Files.createTempDirectory("kdb-cp-kill").toString()
            val rt = open(dir)
            seed(rt, 30, 10)
            assertFalse(checkpointFile(dir).exists())
            val reopened = open(dir)
            val want = kotlinState(reopened)
            assertTrue(checkpointFile(dir).isFile, "no checkpoint after the full replay")
            assertSame(want, goDump(dir), "Go opening the checkpoint Kotlin wrote after a full replay")
        }

    // --- Go writes, Kotlin reads, Kotlin writes, Go reads ----------------------------------------

    @Test
    fun aGoSquashRoundTripsThroughAKotlinCheckpoint() =
        runBlocking {
            requireGo()
            val dir = Files.createTempDirectory("kdb-cp-squash").toString()
            go("squash", "-dir", dir, "-ns", ns)
            val goState = goDump(dir)
            assertEquals(1, goState.shallowRoots.size, "Go's squash left no shallow root: $goState")

            // Kotlin can only open this from Go's checkpoint: the log alone ends before the squash.
            val rt = open(dir)
            assertSame(kotlinState(rt), goState, "Kotlin opening Go's squash checkpoint")

            // Kotlin writes on top and closes: its checkpoint now records the squash root.
            val ids = goState.documents.keys.sorted().map { KdbUuid.fromString(it) }
            commit(rt, mapOf(ids[0] to """{"kotlin":"on top"}""", ids[1] to null))
            rt.dag.createTag("kotlin", rt.dag.head(), "")
            rt.putAttachment(ns, ids[2], AttachmentSource.of("from kotlin".encodeToByteArray()), PutAttachmentOptions(name = "kt.txt", mime = "text/plain"))
            val want = kotlinState(rt)
            rt.close()
            val back = goDump(dir)
            assertSame(want, back, "Go opening Kotlin's checkpoint of a squashed namespace")
            assertEquals(goState.shallowRoots, back.shallowRoots)
            assertEquals(want.copy(replayedInFull = false), kotlinState(open(dir)), "Kotlin reopening its own checkpoint")
        }

    @Test
    fun anUnchangedNamespaceIsNotRewrittenAtClose() =
        runBlocking {
            requireGo()
            val dir = Files.createTempDirectory("kdb-cp-same").toString()
            go("squash", "-dir", dir, "-ns", ns)
            val before = Files.readAllBytes(checkpointFile(dir).toPath())
            val rt = open(dir)
            kotlinState(rt)
            rt.close()
            assertTrue(before.contentEquals(Files.readAllBytes(checkpointFile(dir).toPath())), "a read-only session rewrote Go's checkpoint")
        }
}
