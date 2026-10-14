package dev.kdb.jdbc

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.attachmentContentHash
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.verifyMerkleInclusion
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.attachmentStore
import dev.kdb.embed.compactAttachments
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.jdbc.remote.KdbRemoteConnection
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.SUPPORTED_WIRE_FEATURES
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENTS
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENT_PROOFS
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import java.io.File
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The Kotlin server's attachment messages (go/kdb/server/attachments_wire.go's behaviour) - the
 * JDBC client's attachment API against a real SqlWireHost over WebSocket on a file runtime, as
 * kdb-integration's GoServerJdbcAttachmentInteropTest drives the Go server, plus raw frames for what a
 * client only sees on the wire: resumable uploads, Expect, offsets out of range, damaged bytes and
 * a read's close.
 *
 * Attachment SQL (`SET ATTACHMENT`, `ATTACHMENT()`) is not covered: the Kotlin SQL engine has none.
 */
class KotlinServerAttachmentTest {
    init {
        KdbDriver
    }

    private lateinit var dir: File
    private lateinit var runtime: EmbeddedKdbRuntime
    private lateinit var server: KdbServerRuntime
    private lateinit var ws: JvmNetworkWebSocketServer
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val wire = defaultWireCodec()

    private fun pattern(n: Int): ByteArray = ByteArray(n) { ((it * 31 + 7) and 0xFF).toByte() }

    @Before
    fun start() {
        dir = Files.createTempDirectory("kdb-kt-att").toFile()
        runtime = openFileRuntime(dir.absolutePath, "app", NS, lockHolder = "kotlin-server-attachment-test")
        server = KdbServerRuntime(runtime)
        ws = JvmNetworkWebSocketServer()
        scope.launch {
            ws.start("127.0.0.1", 0, "/kdb") { conn ->
                val host = SqlWireHost(wire, server, NS) // one per connection, as SqlWireListen makes them
                try {
                    conn.incoming().collect { frame -> host.handleFrame(frame)?.let { conn.send(it) } }
                } finally {
                    host.endSession()
                }
            }
        }
        runBlocking { while (ws.port == 0) delay(10) }
    }

    @After
    fun stop() {
        runCatching { runBlocking { ws.stop() } }
        scope.cancel()
        runCatching { runBlocking { server.close() } }
        dev.kdb.jdbc.file.DataDirectoryLockRegistry.releaseBlocking(dir.absolutePath)
        dir.deleteRecursively()
    }

    private fun connect(block: (Connection) -> Unit) = DriverManager.getConnection("jdbc:kdb://127.0.0.1:${ws.port}/$NS").use(block)

    private fun newDoc(): String {
        val id = KdbUuid.random()
        runBlocking { server.upsert(NS, id, """{"title":"t"}""") }
        return id.toString()
    }

    @Test
    fun attachmentFeaturesAreAgreed() =
        connect { conn ->
            assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentsSupported)
            assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentProofsSupported)
        }

    @Test
    fun putListGetRenameDeleteThroughKdbAttachments() =
        connect { conn ->
            val doc = newDoc()
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val bytes = pattern(3 * MIB + 5)
            val put = att.put(doc, bytes, KdbPutAttachmentOptions(name = "photo.png", mime = "image/png"))
            assertEquals("794771af5a7c3582979427373b32cf5f5335b6e9b1a4263092022ad768a8b465", put.info.hashHex)
            assertEquals(bytes.size.toLong(), put.info.size)
            assertEquals("image/png", put.info.mime)
            assertEquals(listOf(put.info), att.list(doc))
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byId(put.info.id)))

            val again = att.put(doc, "v2".encodeToByteArray(), KdbPutAttachmentOptions(name = "photo.png"))
            assertEquals(put.info.id, again.info.id)
            assertEquals(1, att.count(doc))
            val anon = att.put(doc, ByteArray(0))
            assertEquals(anon.info.id.toString(), anon.info.name)
            assertContentEquals(ByteArray(0), att.get(doc, KdbAttachmentSelector.byId(anon.info.id)))
            val renamed = att.rename(doc, KdbAttachmentSelector.byName("photo.png"), "p.png")
            assertEquals(put.info.id, renamed.info.id)
            assertNull(att.info(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals("v2".encodeToByteArray(), att.get(doc, KdbAttachmentSelector.byName("p.png")))
            assertTrue(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertFalse(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertEquals(1, att.count(doc))
            // The original version is still there as of its commit.
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byId(put.info.id), put.commitHex))

            val e = assertFailsWith<KdbAttachmentException> { att.get(doc, KdbAttachmentSelector.byName("nope")) }
            assertEquals("NOT_FOUND", e.errorCode)
        }

    @Test
    fun pinnedReadSurvivesConcurrentReplaceAndCompaction() =
        connect { conn ->
            val doc = newDoc()
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val v1 = pattern(2 * MIB + 1)
            att.put(doc, v1, KdbPutAttachmentOptions(name = "a"))
            att.open(doc, KdbAttachmentSelector.byName("a")).use { s ->
                val first = ByteArray(10)
                assertEquals(10, s.read(first))
                connect { other ->
                    other.unwrap(KdbAttachments::class.java).put(doc, v1.copyOf().also { it[7] = 0 }, KdbPutAttachmentOptions(name = "a"))
                }
                // Maintenance re-stores the version being read as a delta: the read keeps its copy.
                runBlocking { runtime.compactAttachments(NS) }.let { assertEquals(1, it.deltas) }
                val rest = s.readAllBytes()
                assertContentEquals(v1, first + rest)
                assertTrue(s.superseded)
            }
        }

    @Test
    fun verifiedRangeReads() =
        connect { conn ->
            val doc = newDoc()
            val att = conn.unwrap(KdbAttachments::class.java)
            // document's range-proof golden attachment: 4 MiB + 5 bytes, byte i = i mod 251.
            val data = ByteArray(4 * MIB + 5) { (it % 251).toByte() }
            val size = data.size.toLong()
            val put = att.put(doc, data, KdbPutAttachmentOptions(name = "r.bin"))
            assertEquals("1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27", put.info.hashHex)
            val ref = KdbAttachmentSelector.byName("r.bin")
            for ((off, length) in listOf(0L to 10L, MIB + 100L to 50L, MIB - 10L to 20L, 100L to 4L * MIB + 7 - 100, 0L to -1L, size - 1 to 1L, size to -1L, 2L * MIB + 1 to 0L)) {
                val s = att.openRange(doc, ref, null, off, length)
                val got = s.use { it.readAllBytes() }
                val end = if (length >= 0) minOf(size, off + length) else size
                assertContentEquals(data.copyOfRange(off.toInt(), end.toInt()), got, "range $off+$length")
                assertEquals(put.commitHex, s.commitHex)
                assertFalse(s.superseded)
            }
            val past = assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, ref, null, size + 1, 1) }
            assertEquals(size, past.size)
            // A failed open leaves no read pinned: more of them than a connection's read limit.
            repeat(70) { assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, ref, null, size + 1, 1) } }
            att.openRange(doc, ref, null, MIB - 1L, 2L * MIB).use { s ->
                val head = ByteArray(2)
                assertEquals(2, s.readNBytes(head, 0, 2))
                connect { other ->
                    other.unwrap(KdbAttachments::class.java).put(doc, "replaced".encodeToByteArray(), KdbPutAttachmentOptions(name = "r.bin"))
                }
                val rest = s.readAllBytes()
                assertContentEquals(data.copyOfRange(MIB - 1, 3 * MIB - 1), head + rest)
                assertTrue(s.superseded, "the range read did not report the newer version")
            }
            att.put(doc, ByteArray(0), KdbPutAttachmentOptions(name = "empty"))
            for (length in listOf(-1L, 0L, 10L)) {
                assertEquals(0, att.openRange(doc, KdbAttachmentSelector.byName("empty"), null, 0, length).use { it.readAllBytes() }.size)
            }
        }

    // ---- raw frames ----

    private class Raw(
        val host: SqlWireHost,
    ) {
        private val wire = defaultWireCodec()
        private var corr = 100

        fun send(m: (Int) -> WireMessage): WireMessage = runBlocking { wire.decode(host.handleFrame(wire.encode(m(++corr)))!!) }
    }

    private fun raw(): Raw {
        val r = Raw(SqlWireHost(wire, server, NS))
        val ack =
            r.send { c ->
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, c, 0),
                    HandshakePayload(nodeId = "t", namespaces = listOf(NS), localHeads = emptyMap(), clientMode = WireClientMode.SQL_CLIENT, wireFeatures = SUPPORTED_WIRE_FEATURES),
                )
            } as WireMessage.HandshakeAck
        assertTrue(WIRE_FEATURE_ATTACHMENTS in ack.response.wireFeatures && WIRE_FEATURE_ATTACHMENT_PROOFS in ack.response.wireFeatures)
        return r
    }

    private fun h(
        c: Int,
        t: WireMessageType,
    ) = WireHeader(t, KDB_WIRE_PROTOCOL_VERSION, c, 0)

    private fun Raw.upload(
        id: String,
        offset: Long,
        data: ByteArray,
        final: Boolean,
        expect: KdbHash? = null,
    ) = send { c -> WireMessage.AttachmentUpload(h(c, WireMessageType.ATTACHMENT_UPLOAD), NS, "", id, offset, data, final, expect) } as WireMessage.AttachmentUploadResult

    @Test
    fun uploadsResumeOnANewConnectionAndExpectSkipsHeldBlobs() {
        val data = pattern(MIB + 300)
        val a = raw()
        val first = a.upload("", 0, data.copyOf(MIB), false)
        assertNull(first.error)
        assertEquals(MIB.toLong(), first.received)
        // A chunk that does not follow is refused, with where to resume.
        val gap = a.upload(first.uploadId, MIB + 5L, ByteArray(3), false)
        assertNotNull(gap.error)
        assertEquals(MIB.toLong(), gap.received)
        // Resumed on another connection.
        val b = raw()
        val done = b.upload(first.uploadId, MIB.toLong(), data.copyOfRange(MIB, data.size), true)
        assertNull(done.error)
        assertEquals(attachmentContentHash(data), done.hash)
        assertEquals(data.size.toLong(), done.size)
        // A retried final chunk answers the same.
        assertEquals(done.hash, b.upload(first.uploadId, MIB.toLong(), data.copyOfRange(MIB, data.size), true).hash)
        // Expect: the server holds it, so it answers at once.
        val skip = b.upload("", 0, ByteArray(0), false, expect = done.hash)
        assertEquals("", skip.uploadId)
        assertEquals(done.hash, skip.hash)
        assertEquals(data.size.toLong(), skip.received)
        // An unknown upload id.
        assertEquals("NOT_FOUND", b.upload("nope", 0, ByteArray(1), false).errorCode)
        // An unknown Expect falls through to a real upload.
        val fresh = b.upload("", 0, byteArrayOf(1, 2, 3), true, expect = KdbHash(ByteArray(32) { 5 }))
        assertEquals(attachmentContentHash(byteArrayOf(1, 2, 3)), fresh.hash)
    }

    @Test
    fun readsReportOffsetsOutOfRangeCorruptBytesAndClose() {
        val doc = KdbUuid.fromString(newDoc())
        val data = pattern(3 * MIB)
        connect { conn -> conn.unwrap(KdbAttachments::class.java).put(doc.toString(), data, KdbPutAttachmentOptions(name = "f")) }
        val r = raw()

        fun get(
            offset: Long,
            maxBytes: Long = 0,
            readId: String = "",
            close: Boolean = false,
            proofs: Boolean = false,
        ) = r.send { c ->
            WireMessage.AttachmentGet(
                h(c, WireMessageType.ATTACHMENT_GET), NS, docId = doc, name = "f", offset = offset, maxBytes = maxBytes,
                readId = readId, close = close, proofs = proofs,
            )
        } as WireMessage.AttachmentData

        // Out of range: SCHEMA_VIOLATION, with the info so the client learns the size.
        val past = get(data.size + 1L)
        assertEquals("SCHEMA_VIOLATION", past.errorCode)
        assertEquals(data.size.toLong(), past.info.size)
        assertEquals("", past.readId)

        // Proofs: chunk-aligned pages, every chunk proved against the tree.
        val p = get(MIB + 7L, maxBytes = 100, proofs = true)
        assertNull(p.error)
        assertEquals(MIB.toLong(), p.offset)
        assertEquals(MIB, p.data.size)
        assertEquals(1, p.chunks.size)
        assertEquals(attachmentContentHash(data), attachmentIdentity(p.tree!!, data.size.toLong()))
        assertTrue(verifyMerkleInclusion(chunkLeafHash(p.data), 1, 3, p.chunks.single().path, p.tree!!))
        // Close: ends the read, says whether it was superseded.
        val closed = get(0, readId = p.readId, close = true)
        assertTrue(closed.eof)
        assertFalse(closed.superseded)
        assertEquals("NOT_FOUND", get(0, readId = p.readId).errorCode)

        // A damaged stored copy: INTEGRITY at the chunk, not before.
        runBlocking { runtime.attachmentStore(NS).corruptForTest(attachmentContentHash(data), 2L * ATTACHMENT_CHUNK_BYTES + 9) }
        val ok = get(0, maxBytes = MIB.toLong())
        assertNull(ok.error)
        val bad = get(2L * MIB, readId = ok.readId)
        assertEquals("INTEGRITY", bad.errorCode)
    }

    private companion object {
        const val NS = "app/data"
        const val MIB = 1 shl 20
    }
}
