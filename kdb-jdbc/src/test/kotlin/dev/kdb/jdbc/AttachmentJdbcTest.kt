package dev.kdb.jdbc

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.KdbOp
import dev.kdb.document.MerkleTree
import dev.kdb.document.attachmentChunkCount
import dev.kdb.document.chunkLeafHash
import dev.kdb.stream.WireConnection
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.wire.HandshakeAckPayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.MAX_ATTACHMENT_CHUNK_BYTES
import dev.kdb.wire.SUPPORTED_WIRE_FEATURES
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENT_PROOFS
import dev.kdb.wire.WireAttachmentInfo
import dev.kdb.wire.WireBlobChunkProof
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import dev.kdb.wire.withWireFeatures
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The JDBC attachment client against a stand-in for the Go server's attachment handling (the real
 * Go server is exercised by kdb-integration's GoServerJdbcAttachmentInteropTest; this one covers
 * failure paths a real server will not produce on demand): it speaks the same frames - the codec is
 * pinned to Go's golden frames in kdb-wire - and keeps blobs, uploads, read pins and an attachment
 * table in memory, applying committed AttachmentPut / AttachmentDelete ops the way the Go engine does.
 */
class AttachmentJdbcTest {
    init {
        KdbDriver
    }

    private val doc = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"

    private fun pattern(n: Int): ByteArray = ByteArray(n) { ((it * 31 + 7) and 0xFF).toByte() }

    @Test
    fun putListGetRenameDelete() =
        withFakeServer { _, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            val bytes = pattern(3 * (1 shl 20) + 5)
            val put = att.put(doc, bytes, KdbPutAttachmentOptions(name = "photo.png", mime = "image/png"))
            assertEquals("794771af5a7c3582979427373b32cf5f5335b6e9b1a4263092022ad768a8b465", put.info.hashHex)
            assertEquals(bytes.size.toLong(), put.info.size)
            assertTrue(put.commitHex.isNotEmpty())

            val listed = att.list(doc)
            assertEquals(listOf(put.info), listed)
            assertEquals(1, att.count(doc))

            att.pageBytes = 1 shl 20
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byId(put.info.id)))

            // Same name again: replaces that attachment, keeping its ID.
            val again = att.put(doc, "v2".encodeToByteArray(), KdbPutAttachmentOptions(name = "photo.png"))
            assertEquals(put.info.id, again.info.id)
            assertEquals(1, att.count(doc))

            // Neither ID nor name: a fresh ID, named after it.
            val anon = att.put(doc, ByteArray(0))
            assertEquals(anon.info.id.toString(), anon.info.name)
            assertEquals(2, att.count(doc))
            assertContentEquals(ByteArray(0), att.get(doc, KdbAttachmentSelector.byId(anon.info.id)))

            val renamed = att.rename(doc, KdbAttachmentSelector.byName("photo.png"), "p.png")
            assertEquals(put.info.id, renamed.info.id)
            assertNull(att.info(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals("v2".encodeToByteArray(), att.get(doc, KdbAttachmentSelector.byName("p.png")))

            assertTrue(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertFalse(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertEquals(1, att.count(doc))

            val missing = assertFailsWith<KdbAttachmentException> { att.get(doc, KdbAttachmentSelector.byName("nope")) }
            assertEquals("NOT_FOUND", missing.errorCode)
        }

    @Test
    fun readPinsItsVersionAndReportsSuperseded() =
        withFakeServer { _, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val v1 = pattern(2 * (1 shl 20) + 1)
            att.put(doc, v1, KdbPutAttachmentOptions(name = "a"))
            att.open(doc, KdbAttachmentSelector.byName("a")).use { s ->
                val first = ByteArray(10)
                assertEquals(10, s.read(first))
                // A newer version commits mid-read: the read keeps the version it pinned.
                att.put(doc, "new".encodeToByteArray(), KdbPutAttachmentOptions(name = "a"))
                val rest = s.readAllBytes()
                assertContentEquals(v1, first + rest)
                assertTrue(s.superseded)
            }
        }

    @Test
    fun corruptDownloadFailsVerification() =
        withFakeServer { server, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            att.put(doc, pattern(5000), KdbPutAttachmentOptions(name = "a"))
            server.corruptReads = true
            val e = assertFailsWith<IOException> { att.get(doc, KdbAttachmentSelector.byName("a")) }
            assertTrue(e.message!!.contains("verification"), e.message)
            assertTrue(e.cause is KdbAttachmentCorruptException, "verification failure is typed: ${e.cause}")
        }

    @Test
    fun serverRefusingItsDamagedCopyIsTyped() =
        withFakeServer { server, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val bytes = pattern(3 * (1 shl 20))
            att.put(doc, bytes, KdbPutAttachmentOptions(name = "a"))

            // The first page: thrown as is.
            server.damagedAt = 5
            val first = assertFailsWith<KdbAttachmentCorruptException> { att.get(doc, KdbAttachmentSelector.byName("a")) }
            assertEquals("INTEGRITY", first.errorCode)
            assertEquals("XX001", first.sqlState)

            // A later page, after an intact first one: the stream's IOException carries it.
            server.damagedAt = (2L shl 20) + 5
            att.open(doc, KdbAttachmentSelector.byName("a")).use { s ->
                val head = ByteArray(1 shl 20)
                assertEquals(head.size, s.readNBytes(head, 0, head.size))
                val later = assertFailsWith<IOException> { s.readAllBytes() }
                val cause = later.cause
                assertTrue(cause is KdbAttachmentCorruptException, "later page: $cause")
            }

            // SQL ATTACHMENT().
            val sql =
                assertFailsWith<KdbAttachmentCorruptException> {
                    conn.createStatement().use { it.executeQuery("SELECT ATTACHMENT('a') FROM docs") }
                }
            assertEquals("XX001", sql.sqlState)

            // Other refusals stay what they were.
            server.damagedAt = -1
            val missing = assertFailsWith<KdbAttachmentException> { att.get(doc, KdbAttachmentSelector.byName("nope")) }
            assertFalse(missing is KdbAttachmentCorruptException)
        }

    @Test
    fun interruptedUploadResumes() =
        withFakeServer { server, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            val bytes = pattern(3 * (1 shl 20) + 17)
            // The second chunk is stored but its reply is an error, as if the connection had dropped.
            server.failChunkAt = 1L shl 20
            val e = assertFailsWith<KdbAttachmentException> { att.upload(ByteArrayInputStream(bytes)) }
            val state = assertNotNull(e.uploadState)
            assertEquals(1L shl 20, state.received)
            // Resuming resends from where the client last heard; the server already has more, says so,
            // and the client sends only the rest.
            val blob = att.upload(ByteArrayInputStream(bytes), state)
            assertEquals(AttachmentContentHash.hex(bytes), blob.hashHex)
            assertEquals(bytes.size.toLong(), blob.size)
            assertEquals(1, server.uploadsStarted)
            assertContentEquals(bytes, server.blobs[blob.hashHex])
        }

    @Test
    fun preparedStatementBindsBytesAndStreams() =
        withFakeServer { server, conn ->
            conn.prepareStatement("UPDATE t SET ATTACHMENT 'a' = ?, ATTACHMENT 'b' = STREAM ?, n = ?, s = ? WHERE x = ?").use { ps ->
                ps.setBytes(1, byteArrayOf(1, 2, 3))
                val big = pattern((1 shl 20) + 3)
                ps.setBinaryStream(2, ByteArrayInputStream(big), big.size.toLong())
                ps.setLong(3, 7)
                ps.setString(4, "x")
                ps.setNull(5, Types.VARCHAR)
                ps.executeUpdate()
                // Autocommit: the driver commits what the (Go-style) server buffered.
                assertEquals(1, server.sqlCommits)
                val params = Json.parseToJsonElement(server.lastParams!!) as JsonArray
                assertEquals(Base64.getEncoder().encodeToString(byteArrayOf(1, 2, 3)), params[0].jsonObject["\$bytes"]!!.jsonPrimitive.content)
                val ref = params[1].jsonObject["\$attachment"] as JsonObject
                assertEquals(AttachmentContentHash.hex(big), ref["hash"]!!.jsonPrimitive.content)
                assertEquals(big.size.toLong(), ref["size"]!!.jsonPrimitive.content.toLong())
                assertContentEquals(big, server.blobs[AttachmentContentHash.hex(big)])
                // The plain forms Go decodes, not the Kotlin server's tagged ones.
                assertEquals("7", params[2].jsonPrimitive.content)
                assertEquals("x", params[3].jsonPrimitive.content)
                assertEquals(JsonNull, params[4])
            }
            conn.prepareStatement("UPDATE t SET ATTACHMENT 'c' = ? WHERE x = 1").use { ps ->
                ps.setBlob(1, javax.sql.rowset.serial.SerialBlob(byteArrayOf(9)))
                ps.executeUpdate()
                val params = Json.parseToJsonElement(server.lastParams!!) as JsonArray
                assertEquals(AttachmentContentHash.hex(byteArrayOf(9)), params[0].jsonObject["\$attachment"]!!.jsonObject["hash"]!!.jsonPrimitive.content)
            }
        }

    @Test
    fun resultSetReadsBinaryCells() =
        withFakeServer { _, conn ->
            conn.createStatement().executeQuery("SELECT ATTACHMENT('a'), n FROM t").use { rs ->
                assertEquals(Types.VARBINARY, rs.metaData.getColumnType(1))
                assertEquals("[B", rs.metaData.getColumnClassName(1))
                assertEquals(Types.VARCHAR, rs.metaData.getColumnType(2))
                assertTrue(rs.next())
                assertContentEquals("hi there".encodeToByteArray(), rs.getBytes(1))
                assertContentEquals("hi there".encodeToByteArray(), rs.getBinaryStream(1).readAllBytes())
                val blob = rs.getBlob(1)
                assertEquals(8, blob.length())
                assertContentEquals("hi there".encodeToByteArray(), rs.getObject(1) as ByteArray)
                assertEquals("aGkgdGhlcmU=", rs.getString(1))
                assertTrue(rs.next())
                assertNull(rs.getBytes(1))
                assertTrue(rs.wasNull())
                assertNull(rs.getBinaryStream(1))
            }
        }

    // --- verified ranged reads (attachment-proofs) ---

    private val mib = 1L shl 20

    /** Go's range-proof golden attachment: 4 MiB + 5 bytes, byte i = i mod 251. */
    private fun rangeBytes(): ByteArray = ByteArray(4 * (1 shl 20) + 5) { (it % 251).toByte() }

    private fun KdbAttachments.readRange(
        selector: KdbAttachmentSelector,
        asOf: String?,
        off: Long,
        length: Long,
    ): Pair<ByteArray, KdbAttachmentRangeInputStream> {
        val s = openRange(doc, selector, asOf, off, length)
        return s.use { it.readAllBytes() } to s
    }

    @Test
    fun rangeReadsAreVerifiedAndTrimmed() =
        withFakeServer { server, conn ->
            assertTrue(conn.unwrap(dev.kdb.jdbc.remote.KdbRemoteConnection::class.java).attachmentProofsSupported)
            val att = conn.unwrap(KdbAttachments::class.java)
            val data = rangeBytes()
            val size = data.size.toLong()
            val put = att.put(doc, data, KdbPutAttachmentOptions(name = "r.bin"))
            val ref = KdbAttachmentSelector.byName("r.bin")
            val cases =
                listOf(
                    "start" to (0L to 10L),
                    "mid-chunk" to (mib + 100 to 50L),
                    "across a chunk boundary" to (mib - 10 to 20L),
                    "across pages" to (100L to 4 * mib + 7 - 100),
                    "whole" to (0L to -1L),
                    "from a chunk boundary to the end" to (3 * mib to -1L),
                    "into the short last chunk" to (4 * mib - 3 to -1L),
                    "last byte" to (size - 1 to 1L),
                    "past the end is cut" to (size - 2 to 100L),
                    "empty mid-way" to (2 * mib + 1 to 0L),
                    "at the end" to (size to -1L),
                    "at the end, with a length" to (size to 5L),
                )
            for ((name, range) in cases) {
                val (off, length) = range
                server.gets.clear()
                val (got, s) = att.readRange(ref, null, off, length)
                val end = if (length >= 0) minOf(size, off + length) else size
                assertContentEquals(data.copyOfRange(off.toInt(), end.toInt()), got, name)
                assertFalse(s.superseded, name)
                assertEquals(put.commitHex, s.commitHex, name)
                assertEquals(put.info, s.info, name)
                // Every page asks for proofs, at most 4 MiB, later ones from a chunk boundary.
                val pages = server.gets.filter { !it.close }
                assertTrue(pages.all { it.proofs && it.maxBytes in 1..MAX_ATTACHMENT_CHUNK_BYTES.toLong() }, name)
                assertTrue(pages.drop(1).all { it.offset % mib == 0L }, name)
                // A range ending short of the attachment's end releases its read once, at the end.
                assertEquals(if (end < size) 1 else 0, server.gets.count { it.close }, name)
                assertTrue(server.pins.isEmpty(), "$name: read left pinned")
            }
            server.gets.clear()
            att.readRange(ref, null, 100, 4 * mib + 7 - 100)
            assertEquals(listOf(100L, 4 * mib), server.gets.filter { !it.close }.map { it.offset }, "two pages")
            assertEquals(listOf(4 * mib - 93, 5L), server.gets.filter { !it.close }.map { it.maxBytes })

            val past = assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, ref, null, size + 1, 1) }
            assertEquals(size + 1, past.offset)
            assertEquals(size, past.size)
            assertEquals("22011", past.sqlState)
            server.gets.clear()
            val negative = assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, ref, null, -1, 1) }
            assertEquals(-1, negative.size)
            assertTrue(server.gets.isEmpty(), "a negative offset is refused without asking")
            assertFailsWith<java.sql.SQLException> { att.openRange(doc, ref, null, 0, -2) }
            repeat(70) { assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, ref, null, size + 1, 1) } }
            assertTrue(server.pins.isEmpty())

            // readAt, positional-read style.
            val p = ByteArray(10)
            assertEquals(10, att.readAt(doc, ref, null, p, 2 * mib - 5))
            assertContentEquals(data.copyOfRange((2 * mib - 5).toInt(), (2 * mib + 5).toInt()), p)
            assertEquals(3, att.readAt(doc, ref, null, p, size - 3))
            assertContentEquals(data.copyOfRange((size - 3).toInt(), size.toInt()), p.copyOf(3))
            assertEquals(0, att.readAt(doc, ref, null, p, size))
            assertFailsWith<KdbAttachmentRangeException> { att.readAt(doc, ref, null, p, size + 10) }
            assertContentEquals(data.copyOfRange(7, 19), att.getRange(doc, ref, null, 7, 12))

            // A zero-byte attachment: its one empty chunk is proved.
            att.put(doc, ByteArray(0), KdbPutAttachmentOptions(name = "empty"))
            for (length in listOf(-1L, 0L, 10L)) {
                val (got, s) = att.readRange(KdbAttachmentSelector.byName("empty"), null, 0, length)
                assertEquals(0, got.size)
                assertEquals(0, s.info.size)
            }
            assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, KdbAttachmentSelector.byName("empty"), null, 1, 1) }
            assertTrue(server.pins.isEmpty())
        }

    @Test
    fun rangeReadReportsSupersededAtTheEndOfItsRange() =
        withFakeServer { server, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            val data = rangeBytes()
            att.put(doc, data, KdbPutAttachmentOptions(name = "r.bin"))
            server.gets.clear()
            att.openRange(doc, KdbAttachmentSelector.byName("r.bin"), null, mib - 1, 2 * mib).use { s ->
                val head = ByteArray(2)
                assertEquals(2, s.readNBytes(head, 0, 2))
                att.put(doc, "replaced".encodeToByteArray(), KdbPutAttachmentOptions(name = "r.bin"))
                val rest = s.readAllBytes()
                // The pinned version, though a newer one committed.
                assertContentEquals(data.copyOfRange((mib - 1).toInt(), (3 * mib - 1).toInt()), head + rest)
                assertTrue(s.superseded, "the close at the range's end reports the newer version")
                assertEquals(1, server.gets.count { it.close })
            }
            assertEquals(1, server.gets.count { it.close }, "closing again sends nothing")
        }

    @Test
    fun rangeReadRejectsATamperedChunkBeforeHandingOutBytes() =
        withFakeServer { server, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            val data = rangeBytes().copyOf(2 * (1 shl 20) + 10) // three chunks
            att.put(doc, data, KdbPutAttachmentOptions(name = "a"))
            val ref = KdbAttachmentSelector.byName("a")
            server.tamperChunk = 1
            // Chunk 0 is sound, so a range inside it reads.
            assertContentEquals(data.copyOfRange(5, 105), att.getRange(doc, ref, null, 5, 100))
            // A range touching chunk 1 fails before handing out anything, even bytes of chunk 0.
            for ((off, length) in listOf(mib + 100 to 10L, mib - 10 to 20L, 0L to -1L)) {
                val e = assertFailsWith<KdbAttachmentCorruptException>("range $off+$length") { att.openRange(doc, ref, null, off, length) }
                assertEquals("INTEGRITY", e.errorCode)
            }
            assertTrue(server.pins.isEmpty(), "a failed open releases its read")

            // Five chunks, the last tampered: the first page (chunks 0-3) reads; the next fails the read
            // with nothing of chunk 4 handed out.
            server.tamperChunk = -1
            val big = rangeBytes()
            att.put(doc, big, KdbPutAttachmentOptions(name = "big"))
            server.tamperChunk = 4
            val out = java.io.ByteArrayOutputStream()
            val s = att.openRange(doc, KdbAttachmentSelector.byName("big"), null, 0, -1)
            val e =
                assertFailsWith<IOException> {
                    s.use {
                        val buf = ByteArray(64 * 1024)
                        while (true) {
                            val n = it.read(buf)
                            if (n < 0) break
                            out.write(buf, 0, n)
                        }
                    }
                }
            assertTrue(e.cause is KdbAttachmentCorruptException, "$e")
            assertEquals(4 * mib, out.size().toLong())
            assertContentEquals(big.copyOf(4 * (1 shl 20)), out.toByteArray())
            assertFailsWith<KdbAttachmentCorruptException> { att.readAt(doc, KdbAttachmentSelector.byName("big"), null, ByteArray(10), size(big) - 2) }
            server.tamperChunk = -1

            // A tree that does not hash to the content hash, and bytes no proof covers.
            server.badTree = true
            assertFailsWith<KdbAttachmentCorruptException> { att.getRange(doc, ref, null, 0, 1) }
            server.badTree = false
            server.dropLastProof = true
            assertFailsWith<KdbAttachmentCorruptException> { att.getRange(doc, ref, null, 2 * mib, 1) }
            server.dropLastProof = false
            assertContentEquals(data, att.getRange(doc, ref, null, 0, data.size))
        }

    private fun size(b: ByteArray): Long = b.size.toLong()

    @Test
    fun rangeReadNeedsTheProofsFeature() =
        withFakeServer(offered = SUPPORTED_WIRE_FEATURES - WIRE_FEATURE_ATTACHMENT_PROOFS) { _, conn ->
            val remote = conn.unwrap(dev.kdb.jdbc.remote.KdbRemoteConnection::class.java)
            assertTrue(remote.attachmentsSupported)
            assertFalse(remote.attachmentProofsSupported)
            val att = conn.unwrap(KdbAttachments::class.java)
            att.put(doc, "hello".encodeToByteArray(), KdbPutAttachmentOptions(name = "a"))
            val ref = KdbAttachmentSelector.byName("a")
            val e = assertFailsWith<SQLFeatureNotSupportedException> { att.openRange(doc, ref, null, 0, -1) }
            assertTrue(e.message!!.contains("attachment-proofs"), e.message)
            assertFailsWith<SQLFeatureNotSupportedException> { att.readAt(doc, ref, null, ByteArray(1), 0) }
            assertFailsWith<SQLFeatureNotSupportedException> { att.blob(doc, ref)!!.getBytes(1, 1) }
            // The whole-attachment read does not need it.
            assertContentEquals("hello".encodeToByteArray(), att.get(doc, ref))
            assertContentEquals("hello".encodeToByteArray(), att.blob(doc, ref)!!.binaryStream.readAllBytes())
        }

    @Test
    fun attachmentBlobReadsVerifiedRanges() =
        withFakeServer { _, conn ->
            val att = conn.unwrap(KdbAttachments::class.java)
            val data = rangeBytes()
            val size = data.size.toLong()
            val put = att.put(doc, data, KdbPutAttachmentOptions(name = "r.bin"))
            assertNull(att.blob(doc, KdbAttachmentSelector.byName("nope")))
            val blob = assertNotNull(att.blob(doc, KdbAttachmentSelector.byName("r.bin")))
            assertEquals(put.info, blob.info)
            assertEquals(size, blob.length())
            // JDBC positions are 1-based.
            assertContentEquals(data.copyOf(10), blob.getBytes(1, 10))
            assertContentEquals(data.copyOfRange((mib - 10).toInt(), (mib + 10).toInt()), blob.getBytes(mib - 9, 20))
            assertContentEquals(data.copyOfRange((size - 1).toInt(), size.toInt()), blob.getBytes(size, 10))
            assertContentEquals(ByteArray(0), blob.getBytes(size + 1, 5))
            assertContentEquals(ByteArray(0), blob.getBytes(7, 0))
            for ((pos, len) in listOf(0L to 1, -3L to 1, 1L to -1, size + 2 to 1)) {
                assertFailsWith<java.sql.SQLException>("getBytes($pos, $len)") { blob.getBytes(pos, len) }
            }
            assertContentEquals(data.copyOfRange(3 * (1 shl 20) - 2, 4 * (1 shl 20) + 5), blob.getBinaryStream(3 * mib - 1, mib + 7).readAllBytes())
            assertContentEquals(data.copyOfRange((size - 1).toInt(), size.toInt()), blob.getBinaryStream(size, 1).readAllBytes())
            assertContentEquals(ByteArray(0), blob.getBinaryStream(5, 0).readAllBytes())
            for ((pos, len) in listOf(0L to 1L, 1L to -1L, size + 1 to 0L, size to 2L, 1L to size + 1)) {
                assertFailsWith<java.sql.SQLException>("getBinaryStream($pos, $len)") { blob.getBinaryStream(pos, len) }
            }
            assertContentEquals(data, blob.binaryStream.readAllBytes())
            assertFailsWith<SQLFeatureNotSupportedException> { blob.setBytes(1, byteArrayOf(1)) }
            assertFailsWith<SQLFeatureNotSupportedException> { blob.truncate(1) }
            assertFailsWith<SQLFeatureNotSupportedException> { blob.position(byteArrayOf(1), 1) }
            // The blob keeps the version it was made from.
            att.put(doc, "v2".encodeToByteArray(), KdbPutAttachmentOptions(name = "r.bin"))
            assertEquals(size, blob.length())
            blob.free()
            assertFailsWith<java.sql.SQLException> { blob.length() }
            assertFailsWith<java.sql.SQLException> { blob.getBytes(1, 1) }
        }

    // --- the stand-in server ---

    private data class Pin(
        val info: WireAttachmentInfo,
        val commit: String,
        val doc: KdbUuid,
    )

    private class FakeAttachmentServer {
        val codec = defaultWireCodec()
        var replyCodec = codec
        val blobs = ConcurrentHashMap<String, ByteArray>()
        val uploads = HashMap<String, ByteArrayOutputStream>()
        var uploadsStarted = 0
        val table = HashMap<KdbUuid, MutableList<WireAttachmentInfo>>()
        val pins = HashMap<String, Pin>()
        val gets = ArrayList<WireMessage.AttachmentGet>()
        var readsOpened = 0
        var offered = SUPPORTED_WIRE_FEATURES
        var tamperChunk = -1
        var badTree = false
        var dropLastProof = false
        var head = 0
        var lastParams: String? = null
        var sqlCommits = 0
        var failChunkAt = -1L
        var corruptReads = false

        /** Refuses the page holding this offset (and SQL) as a server whose stored copy is damaged does. */
        var damagedAt = -1L

        fun headHex(): String = head.toString(16).padStart(64, '0')

        fun hdr(
            corr: Int,
            t: WireMessageType,
        ) = WireHeader(t, KDB_WIRE_PROTOCOL_VERSION, corr, 0)

        @Synchronized
        fun handle(frame: ByteArray): ByteArray? {
            val m = codec.decode(frame)
            val corr = m.header.correlationId
            val reply: WireMessage =
                when (m) {
                    is WireMessage.Handshake -> {
                        val features = offered.filter { it in m.request.wireFeatures }
                        val ack =
                            WireMessage.HandshakeAck(
                                hdr(corr, WireMessageType.HANDSHAKE),
                                HandshakeAckPayload(true, codec.encoding, KDB_WIRE_PROTOCOL_VERSION, wireFeatures = features),
                            )
                        val out = codec.encode(ack)
                        replyCodec = codec.withWireFeatures(features)
                        return out
                    }
                    is WireMessage.SessionBegin ->
                        WireMessage.SessionBeginAck(hdr(corr, WireMessageType.SESSION_BEGIN_ACK), m.namespace, "sess-1", headHex(), m.readConsistency)
                    is WireMessage.AttachmentUpload -> upload(corr, m)
                    is WireMessage.AttachmentList ->
                        WireMessage.AttachmentListResult(
                            hdr(corr, WireMessageType.ATTACHMENT_LIST_RESULT), m.namespace, headHex(),
                            table[m.docId].orEmpty().sortedBy { it.name },
                        )
                    is WireMessage.AttachmentGet -> get(corr, m)
                    is WireMessage.TxCommit -> {
                        if (m.transactionBytes.isEmpty()) {
                            // Like Go: commits the DML the session buffered.
                            sqlCommits++
                            head++
                            return replyCodec.encode(
                                WireMessage.SqlResult(hdr(corr, WireMessageType.SQL_RESULT), m.namespace, m.sessionId, emptyList(), emptyList(), 0, headHex(), false),
                            )
                        }
                        val tx = TransactionWireCodec.decode(m.transactionBytes)
                        for (op in tx.operations) {
                            when (op) {
                                is KdbOp.AttachmentPut -> {
                                    require(blobs.containsKey(op.contentHash.toHex())) { "blob not stored" }
                                    require(op.scheme == 1)
                                    val set = table.getOrPut(op.docId) { mutableListOf() }
                                    set.removeAll { it.id == op.attachmentId }
                                    set.add(WireAttachmentInfo(op.attachmentId, op.name, op.contentHash, op.size, op.mime))
                                }
                                is KdbOp.AttachmentDelete -> table[op.docId]?.removeAll { it.id == op.attachmentId }
                                else -> error("unexpected op $op")
                            }
                        }
                        head++
                        WireMessage.SqlResult(hdr(corr, WireMessageType.SQL_RESULT), m.namespace, m.sessionId, emptyList(), emptyList(), 0, headHex(), false)
                    }
                    is WireMessage.SqlExec -> {
                        lastParams = m.parametersJson
                        if (damagedAt >= 0) {
                            return replyCodec.encode(
                                WireMessage.SqlResult(
                                    hdr(corr, WireMessageType.SQL_RESULT), m.namespace, m.sessionId, emptyList(), emptyList(), 0, "", true,
                                    error = "ATTACHMENT(\"a\"): attachstore: stored bytes do not match their content hash", errorCode = "INTEGRITY",
                                ),
                            )
                        }
                        val b64 = Base64.getEncoder().encodeToString("hi there".encodeToByteArray())
                        WireMessage.SqlResult(
                            hdr(corr, WireMessageType.SQL_RESULT), m.namespace, m.sessionId, listOf("bin", "n"),
                            listOf(listOf(b64, "1"), listOf("", "")), 1, headHex(),
                            // Like Go: a read is readOnly; DML is buffered and not.
                            readOnly = m.sql.trimStart().startsWith("SELECT", ignoreCase = true),
                            cellKinds = "6211",
                        )
                    }
                    else -> return null
                }
            return replyCodec.encode(reply)
        }

        private fun upload(
            corr: Int,
            m: WireMessage.AttachmentUpload,
        ): WireMessage.AttachmentUploadResult {
            val h = hdr(corr, WireMessageType.ATTACHMENT_UPLOAD_RESULT)
            val id =
                m.uploadId.ifEmpty {
                    uploadsStarted++
                    "up-$uploadsStarted"
                }
            val buf = uploads.getOrPut(id) { ByteArrayOutputStream() }
            val have = buf.size().toLong()
            if (m.offset != have) {
                return WireMessage.AttachmentUploadResult(h, m.namespace, id, have, error = "offset ${m.offset} does not follow $have", errorCode = "CONFLICT")
            }
            buf.write(m.data)
            if (m.offset == failChunkAt) {
                failChunkAt = -1
                return WireMessage.AttachmentUploadResult(h, m.namespace, id, have, error = "connection reset", errorCode = "UNAVAILABLE")
            }
            if (!m.final) return WireMessage.AttachmentUploadResult(h, m.namespace, id, buf.size().toLong())
            val bytes = buf.toByteArray()
            uploads.remove(id)
            val hex = AttachmentContentHash.hex(bytes)
            blobs[hex] = bytes
            return WireMessage.AttachmentUploadResult(h, m.namespace, id, bytes.size.toLong(), KdbHash.fromHex(hex), bytes.size.toLong())
        }

        private fun get(
            corr: Int,
            m: WireMessage.AttachmentGet,
        ): WireMessage.AttachmentData {
            gets.add(m)
            val h = hdr(corr, WireMessageType.ATTACHMENT_DATA)
            if (m.close) {
                // Like Go since attachment-proofs: the close reply carries info, commit and superseded.
                val pin = pins.remove(m.readId) ?: return WireMessage.AttachmentData(h, m.namespace, m.readId, m.docId, eof = true)
                val current = table[pin.doc].orEmpty().firstOrNull { it.id == pin.info.id }
                return WireMessage.AttachmentData(
                    h, m.namespace, m.readId, pin.doc, pin.info, pin.commit, eof = true, superseded = current?.hash != pin.info.hash,
                )
            }
            val pin =
                if (m.readId.isEmpty()) {
                    val set = table[m.docId].orEmpty()
                    val found =
                        (if (m.attachmentId != null) set.firstOrNull { it.id == m.attachmentId } else set.firstOrNull { it.name == m.name })
                            ?: return WireMessage.AttachmentData(h, m.namespace, error = "no such attachment", errorCode = "NOT_FOUND")
                    Pin(found, headHex(), m.docId!!)
                } else {
                    pins[m.readId] ?: return WireMessage.AttachmentData(h, m.namespace, error = "unknown read", errorCode = "NOT_FOUND")
                }
            val info = pin.info
            val commit = pin.commit
            val readId = m.readId.ifEmpty { "r${++readsOpened}-${UUID.randomUUID()}" }
            pins[readId] = pin
            val bytes = blobs[info.hash!!.toHex()]!!
            val size = bytes.size.toLong()
            var off = m.offset
            var page = minOf(if (m.maxBytes > 0) m.maxBytes else 1L shl 20, MAX_ATTACHMENT_CHUNK_BYTES.toLong())
            if (off < 0 || off > size) {
                // Like Go: info stays on the error, and the read is not left pinned.
                pins.remove(readId)
                return WireMessage.AttachmentData(
                    h, m.namespace, "", m.docId, info, commit, error = "offset $off outside the attachment's $size bytes",
                    errorCode = "SCHEMA_VIOLATION",
                )
            }
            var tree: KdbHash? = null
            val proofs = ArrayList<WireBlobChunkProof>()
            if (m.proofs) {
                val chunk = ATTACHMENT_CHUNK_BYTES.toLong()
                var end = (off + page + chunk - 1) / chunk * chunk
                off = off / chunk * chunk
                end = minOf(size, maxOf(end, off + chunk), off + MAX_ATTACHMENT_CHUNK_BYTES)
                page = end - off
                val t = merkle(bytes)
                tree = if (badTree) KdbHash(ByteArray(32) { 5 }) else t.root()
                var i = off / chunk
                while (i < attachmentChunkCount(size) && (i * chunk < end || size == 0L)) {
                    proofs.add(WireBlobChunkProof(i, t.proof(i.toInt())))
                    i++
                }
                if (dropLastProof && proofs.isNotEmpty()) proofs.removeAt(proofs.size - 1)
            }
            val end = (off + minOf(page, size - off)).toInt()
            if (damagedAt in off until end.toLong()) {
                pins.remove(readId)
                return WireMessage.AttachmentData(
                    h, m.namespace, readId, m.docId,
                    error = "attachstore: stored bytes do not match their content hash: chunk 0 of ${info.hash!!.toHex()}", errorCode = "INTEGRITY",
                )
            }
            val data = bytes.copyOfRange(off.toInt(), end)
            if (corruptReads && data.isNotEmpty()) data[0] = (data[0] + 1).toByte()
            if (tamperChunk >= 0) {
                val at = tamperChunk.toLong() * ATTACHMENT_CHUNK_BYTES + 3 - off
                if (at in 0 until data.size) data[at.toInt()] = (data[at.toInt()].toInt() xor 0xff).toByte()
            }
            val eof = end.toLong() == size
            val current = table[pin.doc].orEmpty().firstOrNull { it.id == info.id }
            if (eof) pins.remove(readId)
            return WireMessage.AttachmentData(
                h, m.namespace, readId, m.docId, info, commit, off, data, eof,
                superseded = eof && current?.hash != info.hash,
                tree = tree, chunks = proofs, scheme = if (m.proofs) 1 else 0,
            )
        }

        private val trees = HashMap<String, MerkleTree>()

        private fun merkle(bytes: ByteArray): MerkleTree =
            trees.getOrPut(AttachmentContentHash.hex(bytes)) {
                val leaves = ArrayList<KdbHash>()
                var off = 0
                while (true) {
                    val end = minOf(off + ATTACHMENT_CHUNK_BYTES, bytes.size)
                    leaves.add(chunkLeafHash(bytes, off, end - off))
                    if (end == bytes.size) break
                    off = end
                }
                MerkleTree(leaves)
            }
    }

    private fun withFakeServer(
        offered: List<String> = SUPPORTED_WIRE_FEATURES,
        block: (FakeAttachmentServer, Connection) -> Unit,
    ) = runBlocking {
            val fake = FakeAttachmentServer()
            fake.offered = offered
            val server = JvmNetworkWebSocketServer()
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val job =
                scope.launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn: WireConnection ->
                        conn.incoming().collect { frame ->
                            fake.handle(frame)?.let { conn.send(it) }
                        }
                    }
                }
            while (server.port == 0) delay(10)
            try {
                DriverManager.getConnection("jdbc:kdb://127.0.0.1:${server.port}/app/data").use { conn ->
                    assertTrue(conn.unwrap(dev.kdb.jdbc.remote.KdbRemoteConnection::class.java).attachmentsSupported)
                    block(fake, conn)
                }
            } finally {
                runCatching { server.stop() }
                job.cancel()
                scope.cancel()
            }
        }
}
