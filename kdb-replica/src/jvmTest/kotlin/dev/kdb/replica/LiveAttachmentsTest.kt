package dev.kdb.replica

import dev.kdb.document.attachmentContentHash
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Live interop for attachments (docs/kdb-attachments-plan.md; plan §9): a Kotlin replica and a real
 * Go node (go/cmd/kdb-replicahost) exchanging attachment refs, tables and bytes - the Kotlin twin of
 * the attachment tests in packages/kdb-replica/test/sync.test.ts and filtered.test.ts. Skipped when
 * there is no Go toolchain.
 */
class LiveAttachmentsTest {
    companion object {
        private var host: GoHost? = null
        private const val EU = "region = 'EU'"

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

    private suspend fun full(ns: String, catchUp: CatchUp = CatchUp.AUTO): Replica =
        Replica.open(
            ReplicaOptions(
                store = MemoryReplicaStore(),
                url = host().url,
                namespaces = listOf(NamespaceSpec.Full(ns)),
                token = "rw-token",
                autoSync = false,
                catchUp = catchUp,
            ),
        ).also { opened += it }

    private suspend fun filtered(ns: String, writeBack: Boolean = true): Replica =
        Replica.open(
            ReplicaOptions(
                store = MemoryReplicaStore(),
                url = host().url,
                namespaces = listOf(NamespaceSpec.Filtered(ns, EU, writeBack)),
                token = "rw-token",
                autoSync = false,
            ),
        ).also { opened += it }

    private fun hashOf(b: ByteArray) = attachmentContentHash(b).toHex()

    private fun pattern(size: Int, seed: Int) = ByteArray(size) { ((it * 7 + seed + (it shr 10)) and 0xff).toByte() }

    // --- full mode -----------------------------------------------------------------------------

    @Test
    fun fetchedBytesAreABoundedCacheAndPinnedBytesStay() =
        runBlocking {
            val h = host()
            val ns = "app/u/kt-evict"
            val mib = 1L shl 20
            val docs = List(3) { randomUuid() }
            val texts = docs.indices.map { i -> ('a' + i).toString().repeat((3 * mib).toInt()) }
            for ((i, id) in docs.withIndex()) {
                h.put(ns, id, """{"i":$i}""")
                h.attach(ns, id, "f.txt", texts[i])
            }
            suspend fun open(autoSync: Boolean = false) =
                Replica.open(
                    ReplicaOptions(
                        store = MemoryReplicaStore(), url = h.url, namespaces = listOf(NamespaceSpec.Full(ns)),
                        token = "rw-token", autoSync = autoSync, blobCacheBytes = 7 * mib,
                    ),
                ).also { opened += it }
            val r = open()
            r.syncNow()
            suspend fun read(i: Int) = r.readAttachment(ns, docs[i], name = "f.txt").decodeToString()
            assertEquals(texts[0], read(0))
            assertEquals(texts[1], read(1))
            assertEquals(6 * mib, r.blobCache().bytes)
            assertEquals(texts[2], read(2)) // 9 MiB fetched: the least recently used goes
            val stats = r.blobCache()
            assertTrue(stats.bytes <= 7 * mib, "cache holds ${stats.bytes} bytes, over its bound")
            assertEquals(2, stats.blobs)
            assertEquals(texts[0], read(0), "an evicted blob is fetched again, verified, on read")

            // Bytes written here and not yet uploaded are never evicted; once the host holds them they are.
            val mine = ByteArray((2 * mib).toInt()) { 7 }
            val local = open()
            local.syncNow()
            local.putAttachment(ns, docs[0], "mine.bin", mine)
            assertEquals(0L, local.evictBlobs(0))
            assertEquals(2 * mib, local.blobCache().pinnedBytes)
            local.syncNow() // uploads, then unpins
            assertEquals(0L, local.blobCache().pinnedBytes)
            assertEquals(2 * mib, local.evictBlobs(0))
            assertEquals(0L, local.blobCache().bytes)
            assertContentEquals(mine, local.readAttachment(ns, docs[0], name = "mine.bin"), "fetched back from the host")
        }

    @Test
    fun attachmentRefsReplayWithTheirCommitsAndEveryRootIsVerified() =
        runBlocking {
            val h = host()
            val ns = "app/u/attach"
            val doc = randomUuid()
            val other = randomUuid()
            h.put(ns, doc, """{"title":"with files"}""")
            h.put(ns, other, """{"title":"other"}""")
            val r = full(ns)
            r.syncNow() // a snapshot of a namespace with no attachments yet
            h.attach(ns, doc, "a.txt", "hello")
            h.attach(ns, doc, "b.txt", "world")
            h.attach(ns, other, "c.txt", "!")
            h.detach(ns, doc, "b.txt")
            val res = r.syncNow().single()
            assertEquals("replay", res.route)
            val atts = r.attachments(ns, doc)
            assertEquals(listOf(Triple("a.txt", "text/plain", 5L)), atts.map { Triple(it.name, it.mime, it.size) })
            assertEquals(hashOf("hello".encodeToByteArray()), atts[0].contentHash)
            // Bytes are lazy: the first read fetches them by BLOB_FETCH, verified chunk by chunk.
            assertFalse(r.blobs.complete(atts[0].contentHash))
            assertEquals("hello", r.readAttachment(ns, doc, name = "a.txt").decodeToString())
            assertTrue(r.blobs.complete(atts[0].contentHash), "a read keeps the bytes")
            assertEquals(1, r.attachments(ns, other).size)
            // A document delete takes its attachments with it, in Go and here.
            h.cmd("commit", "ns" to ns, "ops" to JsonArray(listOf(JsonObject(mapOf("id" to JsonPrimitive(other), "delete" to JsonPrimitive(true))))))
            r.syncNow()
            assertEquals(emptyList(), r.attachments(ns, other))
            assertEquals(h.head(ns).first, r.namespace(ns).head)
            assertEquals(r.namespace(ns).headCommit()?.attachmentRoot, r.namespace(ns).attachmentRoot())
        }

    @Test
    fun aSnapshotBootstrapTakesTheTableAndReadsAMultiChunkBlobInVerifiedChunks() =
        runBlocking {
            val h = host()
            val ns = "app/u/attach-boot"
            val doc = randomUuid()
            h.put(ns, doc, """{"title":"big"}""")
            val big = "0123456789abcdef".repeat(200_000) // 3.2 MB: four chunks, the last short
            h.attach(ns, doc, "big.bin", big, "application/octet-stream")
            h.attach(ns, doc, "small.txt", "tiny")
            val r = full(ns)
            assertEquals("snapshot", r.syncNow().single().route)
            val head = r.namespace(ns).headCommit()
            assertNotNull(head?.attachmentRoot, "the snapshot commit has attachments")
            assertEquals(head!!.attachmentRoot, r.namespace(ns).attachmentRoot())
            assertEquals(listOf("big.bin", "small.txt"), r.attachments(ns, doc).map { it.name }.sorted())
            val bytes = r.readAttachment(ns, doc, name = "big.bin")
            assertEquals(big.length, bytes.size)
            assertEquals(big, bytes.decodeToString())
            assertEquals(hashOf(bytes), r.attachments(ns, doc).single { it.name == "big.bin" }.contentHash)
        }

    @Test
    fun anAttachmentWrittenOnTheReplicaUploadsBeforeItsCommitAndTheHostHoldsIt() =
        runBlocking {
            val h = host()
            val ns = "app/u/attach-up"
            val doc = randomUuid()
            h.put(ns, doc, """{"title":"upload"}""")
            val r = full(ns)
            r.syncNow()
            val data = "from the replica ".repeat(100_000).encodeToByteArray() // 1.7 MB: two chunks
            val local = r.putAttachment(ns, doc, "note.txt", data, "text/plain")
            assertTrue(r.blobs.pinned(hashOf(data)), "pinned until the host has it")
            assertContentEquals(data, r.readAttachment(ns, doc, name = "note.txt"), "readable locally at once")
            val res = r.syncNow().single()
            assertEquals("fast-forwarded", res.pushOutcome)
            assertEquals(local, h.head(ns).first)
            assertFalse(r.blobs.pinned(hashOf(data)), "unpinned once the push is accepted")
            assertEquals(listOf(Triple("note.txt", hashOf(data), data.size.toLong())), h.attachments(ns, doc))
            // A second replica reads it from the host.
            val other = full(ns)
            other.syncNow()
            assertContentEquals(data, other.readAttachment(ns, doc, name = "note.txt"))
            // Replacing keeps the attachment's id; deletes travel the same way.
            val id = r.attachments(ns, doc).single().id
            r.putAttachment(ns, doc, "note.txt", "short".encodeToByteArray(), "text/plain")
            r.syncNow()
            assertEquals(id, r.attachments(ns, doc).single().id)
            assertEquals(listOf(Triple("note.txt", hashOf("short".encodeToByteArray()), 5L)), h.attachments(ns, doc))
            r.deleteAttachment(ns, doc, name = "note.txt")
            r.syncNow()
            assertEquals(emptyList(), h.attachments(ns, doc))
            other.syncNow()
            assertEquals(emptyList(), other.attachments(ns, doc))
        }

    @Test
    fun concurrentAttachmentWritesOnBothSidesMergeAndConvergeWithTheHost() =
        runBlocking {
            val h = host()
            val ns = "app/u/attach-merge"
            val doc = randomUuid()
            h.put(ns, doc, """{"title":"base"}""")
            val r = full(ns)
            r.syncNow()
            h.attach(ns, doc, "a.txt", "hello")
            r.syncNow()
            val names = sortedSetOf("a.txt")
            for (round in 0 until 4) { // both parent orders, whichever hashes first
                // The replica edits the body and attaches; the host attaches another meanwhile.
                r.put(ns, doc, """{"title":"edited $round"}""")
                r.putAttachment(ns, doc, "r$round.bin", pattern(ATTACH_BYTES + round, round))
                h.attach(ns, doc, "h$round.txt", "merged $round")
                names += listOf("r$round.bin", "h$round.txt")
                val merged = r.syncNow().single()
                assertEquals("merged", merged.pushOutcome, "round $round")
                assertEquals(h.head(ns).first, r.namespace(ns).head, "round $round")
                assertEquals(r.namespace(ns).headCommit()?.attachmentRoot, r.namespace(ns).attachmentRoot())
                assertEquals(names.toList(), r.attachments(ns, doc).map { it.name }.sorted(), "round $round")
                assertEquals(
                    h.attachments(ns, doc).map { it.first to it.second }.sortedBy { it.first },
                    r.attachments(ns, doc).map { it.name to it.contentHash }.sortedBy { it.first },
                    "round $round: the replica's set is the host's",
                )
            }
            assertEquals("""{"title":"edited 3"}""", r.get(ns, doc))
            // A fresh replica reads the replica's bytes back through the host.
            val other = full(ns)
            other.syncNow()
            assertContentEquals(pattern(ATTACH_BYTES + 2, 2), other.readAttachment(ns, doc, name = "r2.bin"))
        }

    @Test
    fun aDiffConvergenceTakesTheAttachmentTableByTreeNodes() =
        runBlocking {
            val h = host()
            val ns = "app/u/attach-squash"
            val docs = List(6) { randomUuid() }
            for (d in docs) h.put(ns, d, """{"d":"$d"}""")
            h.attach(ns, docs[0], "keep.txt", "kept")
            h.attach(ns, docs[1], "gone.txt", "removed later")
            val r = full(ns, CatchUp.SQUASH)
            r.syncNow()
            h.detach(ns, docs[1], "gone.txt")
            h.attach(ns, docs[0], "keep.txt", "replaced")
            for ((i, d) in docs.drop(2).withIndex()) h.attach(ns, d, "n$i.txt", "new $i")
            h.put(ns, docs[5], """{"d":"changed"}""")
            // The host refuses to squash a namespace with attachments (go/kdb/peersync/squash.go): a
            // replica catches up by replay, which carries the attachment ops.
            val conn = PeerConnection.open(h.url, randomUuid(), "rw-token", listOf(ns), defaultPeerConnector)
            val sh = conn.squashHead(ns, r.namespace(ns).head!!)
            val target = conn.snapshot(ns, h.head(ns).first, "", 1024).commit
            conn.close()
            assertTrue("attachments" in sh.refused, sh.refused)
            // A diff convergence still reaches such a namespace (the repair after a snapshot laid
            // over an interrupted one): started by hand here, it diffs the document tree, then the
            // attachment table by TREE_NODES tree "attachments", each set checked against its hash.
            r.namespace(ns).beginConverge(target, "diff")
            val res = r.syncNow().single()
            assertEquals("squash", res.route)
            assertEquals(h.head(ns).first, r.namespace(ns).head)
            assertEquals(r.namespace(ns).headCommit()?.attachmentRoot, r.namespace(ns).attachmentRoot())
            for (d in docs) {
                assertEquals(
                    h.attachments(ns, d).map { it.first to it.second },
                    r.attachments(ns, d).map { it.name to it.contentHash }.sortedBy { it.first },
                    "document $d",
                )
            }
            assertEquals("replaced", r.readAttachment(ns, docs[0], name = "keep.txt").decodeToString())
        }

    // --- filtered mode -------------------------------------------------------------------------

    @Test
    fun aFilteredNamespaceHoldsItsDocumentsAttachmentRefsAndReadsTheirBytes() =
        runBlocking {
            val h = host()
            val ns = "shop/attached"
            val eu = randomUuid()
            val us = randomUuid()
            h.put(ns, eu, """{"region":"EU"}""")
            h.put(ns, us, """{"region":"US"}""")
            h.attach(ns, eu, "invoice.pdf", "%PDF eu", "application/pdf")
            h.attach(ns, us, "invoice.pdf", "%PDF us", "application/pdf")
            val r = filtered(ns, writeBack = false)
            r.syncNow()
            assertEquals(listOf("invoice.pdf"), r.attachments(ns, eu).map { it.name })
            assertEquals(emptyList(), r.attachments(ns, us), "a document outside the filter brings no refs")
            assertEquals("%PDF eu", r.readAttachment(ns, eu, name = "invoice.pdf").decodeToString())
            // An attachment-only change at the source comes down.
            h.attach(ns, eu, "receipt.txt", "paid")
            r.syncNow()
            assertEquals(listOf("invoice.pdf", "receipt.txt"), r.attachments(ns, eu).map { it.name }.sorted())
            // A document leaving the filter takes its refs with it.
            h.put(ns, eu, """{"region":"US"}""")
            r.syncNow()
            assertEquals(emptyList(), r.attachments(ns, eu))
        }

    @Test
    fun attachmentsWrittenOnAWriteBackReplicaReachTheSourceBytesAndAll() =
        runBlocking {
            val h = host()
            val ns = "shop/attach-wb"
            val id = randomUuid()
            h.put(ns, id, """{"region":"EU","n":1}""")
            val r = filtered(ns)
            r.syncNow()
            val big = pattern(3 * 1024 * 1024 + 11, 9) // several chunks
            r.putAttachment(ns, id, "scan.bin", big)
            r.putAttachment(ns, id, "note.txt", "hello".encodeToByteArray(), "text/plain")
            assertEquals(2, r.status().namespaces.getValue(ns).pending)
            assertEquals(big.size, r.readAttachment(ns, id, name = "scan.bin").size, "readable locally at once")
            r.syncNow()
            assertEquals(2, r.lastProjectionResults.single().applied)
            val atHost = h.attachments(ns, id)
            assertEquals(listOf("note.txt", "scan.bin"), atHost.map { it.first }.sorted())
            assertEquals(big.size.toLong(), atHost.single { it.first == "scan.bin" }.third)
            assertFalse(r.blobs.pinned(hashOf(big)), "unpinned once the source holds it")

            // A fresh replica reads the bytes back from the source.
            val other = filtered(ns, writeBack = false)
            other.syncNow()
            assertContentEquals(big, other.readAttachment(ns, id, name = "scan.bin"))

            // A JSON-only write-back leaves the document's attachments alone at the source.
            r.put(ns, id, """{"region":"EU","n":2}""")
            r.syncNow()
            assertEquals("""{"region":"EU","n":2}""", h.docs(ns)[id])
            assertEquals(2, h.attachments(ns, id).size, "a body write-back must not drop attachments")

            // Deletes go back too; an attachment-only change at the source comes down.
            r.deleteAttachment(ns, id, name = "note.txt")
            r.syncNow()
            assertEquals(listOf("scan.bin"), h.attachments(ns, id).map { it.first })
            h.attach(ns, id, "from-host.txt", "from the host")
            r.syncNow()
            assertEquals(listOf("from-host.txt", "scan.bin"), r.attachments(ns, id).map { it.name }.sorted())
            assertEquals("from the host", r.readAttachment(ns, id, name = "from-host.txt").decodeToString())
        }

    @Test
    fun anAttachmentWriteTheSourceChangedMeanwhileIsAConflictAndTheSourcesAttachmentsComeBack() =
        runBlocking {
            val h = host()
            val ns = "shop/attach-conflict"
            val id = randomUuid()
            h.put(ns, id, """{"region":"EU"}""")
            val r = filtered(ns)
            r.syncNow()
            val events = ArrayList<ConflictEvent>()
            r.onConflict { events += it }
            r.putAttachment(ns, id, "mine.txt", "replica".encodeToByteArray())
            h.attach(ns, id, "theirs.txt", "host")
            r.syncNow()
            assertEquals("conflict", events.firstOrNull()?.outcome)
            assertEquals(listOf("theirs.txt"), r.attachments(ns, id).map { it.name })
            assertEquals("host", r.readAttachment(ns, id, name = "theirs.txt").decodeToString())
            assertEquals(listOf("theirs.txt"), h.attachments(ns, id).map { it.first })
            assertEquals(0, r.status().namespaces.getValue(ns).pending)
        }
}

/** A replica-written blob just over one chunk, so every upload in the merge test is multi-chunk. */
private const val ATTACH_BYTES = (1 shl 20) + 100
