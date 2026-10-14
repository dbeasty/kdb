package dev.kdb.jdbc.file

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
import dev.kdb.document.kdbSha256
import dev.kdb.embed.SnapshotInstallException
import dev.kdb.embed.SnapshotState
import dev.kdb.embed.canInstallSnapshot
import dev.kdb.embed.installSnapshot
import dev.kdb.embed.putJson
import dev.kdb.embed.shallowRootsOf
import java.nio.file.Files
import kotlin.io.path.createTempDirectory
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A namespace installed from a peer's snapshot, in a file runtime: durable as Go makes it (bodies in
 * SSTables, a Go checkpoint, the marker naming the root), so it reopens at the root - documents,
 * attachment table and all - and a write on top of it survives a reopen too.
 */
class FileSnapshotInstallTest {
    private val ns = "app/data"

    private fun h(seed: Int) = KdbHash.fromBytes(kdbSha256(byteArrayOf(seed.toByte())))

    private fun state(attach: Boolean): SnapshotState {
        val docs = (1..40).map { KdbDocument(KdbUuid(0x2000L + it, it.toLong()), """{"n":$it,"name":"doc $it"}""") }
        val tree = DocumentTree.build(docs.associate { it.id to it.contentHash })
        var table = AttachmentTable.EMPTY
        if (attach) {
            table = table.put(docs[0].id, Attachment(KdbUuid(9, 9), "photo.png", AttachmentRef(ATTACHMENT_HASH_SCHEME_CHUNKED_1, h(7), 1234, "image/png")))
        }
        // The root names parents this node never sees: it is held as a shallow root.
        val root =
            KdbCommit.build(
                listOf(h(1)), ns, KdbUuid(1, 2), KdbTimestamp.fromEpochMicros(1_760_000_000_000_000), KdbUuid(3, 4),
                listOf(KdbOp.Write(docs[0].id, docs[0].json)), tree.treeHash, null, "a peer's head", table.root,
            )
        return SnapshotState(root, docs, if (attach) AttachmentTable.pack(listOf(table)) else null)
    }

    private fun check(attach: Boolean) =
        runBlocking {
            val dir = createTempDirectory("kdb-snap").toString()
            val st = state(attach)
            val rt = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)
            assertTrue(rt.canInstallSnapshot(ns))
            rt.installSnapshot(ns, st)
            assertEquals(st.root.hash, rt.dag.head())
            assertFalse(rt.canInstallSnapshot(ns))
            assertFailsWith<SnapshotInstallException> { rt.installSnapshot(ns, st) }

            val again = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)
            assertEquals(st.root.hash, again.dag.head(), "reopened at the root")
            assertEquals(listOf(st.root.hash), again.shallowRootsOf(ns))
            for (d in st.documents) assertEquals(d.json, again.storage.getDocument(ns, d.id, again.dag.head())?.json)
            if (attach) assertEquals(st.root.attachmentTreeHash, again.dag.attachmentTableAt(st.root.hash).root)

            // A write on top of the root is logged, and replays onto it at the next open.
            putJson(again, ns, """{"id":"${KdbUuid(0x9999, 1)}","after":true}""")
            val head = again.dag.head()
            assertEquals(listOf(st.root.hash), again.dag.getCommitOrThrow(head).parentHashes)
            val third = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)
            assertEquals(head, third.dag.head())
            assertEquals(st.documents.size + 1, third.dag.getDocumentTreeOrThrow(third.dag.getCommitOrThrow(head).documentTreeHash).size)
            for (d in st.documents) assertEquals(d.json, third.storage.getDocument(ns, d.id, head)?.json)
            val marker = Files.readString(NamespacePaths.metaFile(dir, ns))
            assertTrue("\"bodiesExternal\":true" in marker && st.root.hash.toHex() in marker, marker)
        }

    @Test
    fun aSnapshotInstallSurvivesReopen() = check(attach = false)

    @Test
    fun aSnapshotWithAttachmentsSurvivesReopen() = check(attach = true)

    @Test
    fun documentsThatDoNotHashToTheRootAreRefused() =
        runBlocking {
            val dir = createTempDirectory("kdb-snap").toString()
            val st = state(false)
            val rt = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)
            assertFailsWith<SnapshotInstallException> { rt.installSnapshot(ns, SnapshotState(st.root, st.documents.drop(1))) }
            assertTrue(rt.canInstallSnapshot(ns), "a refused install wrote something")
            assertFalse(Files.exists(NamespacePaths.metaFile(dir, ns)) && "shallowRoots" in Files.readString(NamespacePaths.metaFile(dir, ns)))
        }
}
