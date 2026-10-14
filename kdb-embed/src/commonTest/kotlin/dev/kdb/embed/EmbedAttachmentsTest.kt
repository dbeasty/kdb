package dev.kdb.embed

import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.AttachmentException
import dev.kdb.document.attachmentContentHash
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** The embed attachment API - go/kdb/embed/attachments.go's behaviour, in Kotlin. */
class EmbedAttachmentsTest {
    private val ns = "app/att"

    private fun bytes(
        n: Int,
        seed: Int,
    ): ByteArray = ByteArray(n) { (it * 31 + seed).toByte() }

    /** Waits in real time (runTest's own clock is virtual) - for an upload grace to run out. */
    private suspend fun sleep(d: Duration) = withContext(Dispatchers.Default) { delay(d) }

    private suspend fun EmbeddedKdbRuntime.read(
        doc: KdbUuid,
        sel: AttachmentSelector,
        asOf: dev.kdb.codec.KdbHash? = null,
    ): ByteArray {
        val r = getAttachment(ns, doc, sel, asOf)
        try {
            return r.reader.readAll()
        } finally {
            r.reader.close()
        }
    }

    @Test
    fun putGetListRenameDelete() =
        runTest {
            val rt = openMemoryRuntime("c", ns)
            val doc = KdbUuid.fromString(putJson(rt, ns, """{"title":"x"}"""))
            val big = bytes(2 * ATTACHMENT_CHUNK_BYTES + 123, 1)
            val (c1, a) = rt.putAttachment(ns, doc, AttachmentSource.of(big), PutAttachmentOptions(name = "big.bin", mime = "application/octet-stream"))
            assertEquals(attachmentContentHash(big), a.contentHash)
            assertEquals(big.size.toLong(), a.size)
            assertEquals(a.contentHash, c1.operations.single().let { (it as dev.kdb.document.KdbOp.AttachmentPut).contentHash })
            assertTrue(c1.attachmentTreeHash != null)
            // No id and no name: a fresh id, named after itself.
            val (_, b) = rt.putAttachment(ns, doc, AttachmentSource.of("hello".encodeToByteArray()))
            assertEquals(b.id.toString(), b.name)
            assertContentEquals(big, rt.read(doc, AttachmentSelector.ByName("big.bin")))
            assertContentEquals("hello".encodeToByteArray(), rt.read(doc, AttachmentSelector.ById(b.id)))
            assertEquals(2, rt.countAttachments(ns, doc))
            assertEquals(listOf("big.bin", b.name).sorted(), rt.listAttachments(ns, doc).map { it.name })

            // A put by name replaces that attachment, keeping its id.
            val (c2, a2) = rt.putAttachment(ns, doc, AttachmentSource.of("v2".encodeToByteArray()), PutAttachmentOptions(name = "big.bin"))
            assertEquals(a.id, a2.id)
            assertContentEquals("v2".encodeToByteArray(), rt.read(doc, AttachmentSelector.ById(a.id)))
            // As of the earlier commit, the earlier bytes.
            assertContentEquals(big, rt.read(doc, AttachmentSelector.ById(a.id), c1.hash))
            // Rename keeps id and bytes; the old name no longer resolves.
            rt.renameAttachment(ns, doc, AttachmentSelector.ById(a.id), "renamed.bin")
            assertContentEquals("v2".encodeToByteArray(), rt.read(doc, AttachmentSelector.ByName("renamed.bin")))
            assertFailsWith<NoSuchAttachmentException> { rt.read(doc, AttachmentSelector.ByName("big.bin")) }
            // A name another attachment has is refused.
            assertFailsWith<AttachmentException> { rt.renameAttachment(ns, doc, AttachmentSelector.ById(b.id), "renamed.bin") }
            // Delete one; the other stays.
            rt.deleteAttachment(ns, doc, AttachmentSelector.ByName("renamed.bin"))
            assertEquals(1, rt.countAttachments(ns, doc))
            assertFailsWith<NoSuchAttachmentException> { rt.deleteAttachment(ns, doc, AttachmentSelector.ByName("renamed.bin")) }
            // The document's JSON is untouched throughout.
            assertEquals("""{"title":"x"}""", getJson(rt, ns, doc.toString()))
            assertNotEquals(c1.hash, c2.hash)
            // An attachment needs its document.
            assertFailsWith<NoSuchDocumentException> { rt.putAttachment(ns, KdbUuid.random(), AttachmentSource.of(ByteArray(1))) }
            assertFailsWith<AttachmentException> { rt.putAttachment(ns, doc, AttachmentSource.of(ByteArray(1)), PutAttachmentOptions(name = "")) }
        }

    @Test
    fun anEmptyAttachmentRoundTrips() =
        runTest {
            val rt = openMemoryRuntime("c", ns)
            val doc = KdbUuid.fromString(putJson(rt, ns, "{}"))
            val (_, a) = rt.putAttachment(ns, doc, AttachmentSource.of(ByteArray(0)), PutAttachmentOptions(name = "empty"))
            assertEquals(attachmentContentHash(ByteArray(0)), a.contentHash)
            assertContentEquals(ByteArray(0), rt.read(doc, AttachmentSelector.ByName("empty")))
        }

    @Test
    fun historySettingsAndCompaction() =
        runTest {
            val rt = openMemoryRuntime("c", ns)
            rt.setAttachmentUploadGrace(50.milliseconds)
            val doc = KdbUuid.fromString(putJson(rt, ns, "{}"))
            val (c1, _) = rt.putAttachment(ns, doc, AttachmentSource.of(bytes(1000, 1)), PutAttachmentOptions(name = "f"))
            rt.putAttachment(ns, doc, AttachmentSource.of(bytes(1000, 2)), PutAttachmentOptions(name = "f"))
            // An upload never committed: held for its grace (its commit may yet come), then collected.
            val orphan = rt.attachmentStore(ns).put(AttachmentSource.of(bytes(10, 3)))
            assertEquals(0, rt.compactAttachments(ns).removed)
            assertTrue(rt.attachmentStore(ns).has(orphan.hash))
            sleep(100.milliseconds)
            assertEquals(1, rt.compactAttachments(ns).removed)
            assertFalse(rt.attachmentStore(ns).has(orphan.hash))
            assertContentEquals(bytes(1000, 1), rt.read(doc, AttachmentSelector.ByName("f"), c1.hash))

            rt.setAttachmentHistory(AttachmentHistory.LATEST)
            // A reader open across compaction finishes on its version.
            val open = rt.getAttachment(ns, doc, AttachmentSelector.ByName("f"), c1.hash)
            assertEquals(1, rt.compactAttachments(ns).removed)
            assertContentEquals(bytes(1000, 1), open.reader.readAll())
            open.reader.close()
            assertFailsWith<AttachmentNotRetainedException> { rt.read(doc, AttachmentSelector.ByName("f"), c1.hash) }
            assertContentEquals(bytes(1000, 2), rt.read(doc, AttachmentSelector.ByName("f")))
        }

    @Test
    fun deletingTheDocumentRemovesItsAttachments() =
        runTest {
            val rt = openMemoryRuntime("c", ns)
            val doc = KdbUuid.fromString(putJson(rt, ns, "{}"))
            rt.putAttachment(ns, doc, AttachmentSource.of(bytes(5, 1)), PutAttachmentOptions(name = "a"))
            val head = rt.dag.head()
            val tx = dev.kdb.document.KdbTransaction(KdbUuid.random(), head, listOf(dev.kdb.document.KdbOp.Delete(doc)), dev.kdb.codec.KdbTimestamp.now(), KdbUuid.random())
            val c = commitViaEngine(rt, ns, tx)
            assertEquals(null, c.attachmentTreeHash)
            assertEquals(0, rt.countAttachments(ns, doc))
            assertEquals(1, rt.countAttachments(ns, doc, head))
        }

    @Test
    fun aDurableCommitEndsTheHold() =
        runTest {
            // In the store: once committed, references alone decide.
            val store = AttachmentBlobStore(InMemoryPlatformIoShim(), ns)
            val blob = store.put(AttachmentSource.of(bytes(100, 8)))
            store.committed(listOf(blob.hash))
            assertEquals(1, store.collect { false })

            // Through the runtime, under the default (hour-long) grace: a version committed and then
            // replaced goes at the next compaction, not an hour later.
            val rt = openMemoryRuntime("c", ns)
            rt.setAttachmentHistory(AttachmentHistory.LATEST)
            val doc = KdbUuid.fromString(putJson(rt, ns, "{}"))
            val (c1, _) = rt.putAttachment(ns, doc, AttachmentSource.of(bytes(1000, 1)), PutAttachmentOptions(name = "f"))
            rt.putAttachment(ns, doc, AttachmentSource.of(bytes(1000, 2)), PutAttachmentOptions(name = "f"))
            assertEquals(1, rt.compactAttachments(ns).removed)
            assertFailsWith<AttachmentNotRetainedException> { rt.read(doc, AttachmentSelector.ByName("f"), c1.hash) }
            assertContentEquals(bytes(1000, 2), rt.read(doc, AttachmentSelector.ByName("f")))
        }

    @Test
    fun compactionStoresReplacedVersionsAsDeltas() =
        runTest {
            val rt = openMemoryRuntime("c", ns)
            val doc = KdbUuid.fromString(putJson(rt, ns, "{}"))
            val v1 = bytes(ATTACHMENT_CHUNK_BYTES + 100, 1)
            val v2 = v1.copyOf().also { it[5] = 0; it[ATTACHMENT_CHUNK_BYTES + 7] = 0 }
            val v3 = v2.copyOf().also { it[9] = 1 }
            val (c1, _) = rt.putAttachment(ns, doc, AttachmentSource.of(v1), PutAttachmentOptions(name = "f"))
            val (c2, _) = rt.putAttachment(ns, doc, AttachmentSource.of(v2), PutAttachmentOptions(name = "f"))
            assertEquals(AttachmentMaintenance(removed = 0, deltas = 1), rt.compactAttachments(ns))
            // The mark: the next pass looks only at commits since.
            assertEquals(AttachmentMaintenance(removed = 0, deltas = 0), rt.compactAttachments(ns))
            rt.putAttachment(ns, doc, AttachmentSource.of(v3), PutAttachmentOptions(name = "f"))
            assertEquals(AttachmentMaintenance(removed = 0, deltas = 1), rt.compactAttachments(ns))
            assertContentEquals(v1, rt.read(doc, AttachmentSelector.ByName("f"), c1.hash))
            assertContentEquals(v2, rt.read(doc, AttachmentSelector.ByName("f"), c2.hash))
            assertContentEquals(v3, rt.read(doc, AttachmentSelector.ByName("f")))
            assertTrue(rt.attachmentStore(ns).mark().isNotEmpty())
        }
}
