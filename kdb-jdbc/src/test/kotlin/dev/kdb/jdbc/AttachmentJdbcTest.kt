package dev.kdb.jdbc

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.stream.WireConnection
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.wire.HandshakeAckPayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.SUPPORTED_WIRE_FEATURES
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireAttachmentInfo
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

    // --- the stand-in server ---

    private class FakeAttachmentServer {
        val codec = defaultWireCodec()
        var replyCodec = codec
        val blobs = ConcurrentHashMap<String, ByteArray>()
        val uploads = HashMap<String, ByteArrayOutputStream>()
        var uploadsStarted = 0
        val table = HashMap<KdbUuid, MutableList<WireAttachmentInfo>>()
        val pins = HashMap<String, Pair<WireAttachmentInfo, String>>()
        var head = 0
        var lastParams: String? = null
        var sqlCommits = 0
        var failChunkAt = -1L
        var corruptReads = false

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
                        val features = SUPPORTED_WIRE_FEATURES.filter { it in m.request.wireFeatures }
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
            val h = hdr(corr, WireMessageType.ATTACHMENT_DATA)
            if (m.close) {
                pins.remove(m.readId)
                return WireMessage.AttachmentData(h, m.namespace, m.readId, m.docId, eof = true)
            }
            val (info, commit) =
                if (m.readId.isEmpty()) {
                    val set = table[m.docId].orEmpty()
                    val found =
                        (if (m.attachmentId != null) set.firstOrNull { it.id == m.attachmentId } else set.firstOrNull { it.name == m.name })
                            ?: return WireMessage.AttachmentData(h, m.namespace, error = "no such attachment", errorCode = "NOT_FOUND")
                    found to headHex()
                } else {
                    pins[m.readId] ?: return WireMessage.AttachmentData(h, m.namespace, error = "unknown read", errorCode = "NOT_FOUND")
                }
            val readId = m.readId.ifEmpty { "r${pins.size + 1}-${UUID.randomUUID()}" }
            pins[readId] = info to commit
            val bytes = blobs[info.hash!!.toHex()]!!
            val end = minOf(bytes.size.toLong(), m.offset + (if (m.maxBytes > 0) m.maxBytes else 4L shl 20)).toInt()
            val data = bytes.copyOfRange(m.offset.toInt(), end)
            if (corruptReads && data.isNotEmpty()) data[0] = (data[0] + 1).toByte()
            val eof = end == bytes.size
            val current = table[m.docId].orEmpty().firstOrNull { it.id == info.id }
            if (eof) pins.remove(readId)
            return WireMessage.AttachmentData(
                h, m.namespace, readId, m.docId, info, commit, m.offset, data, eof,
                superseded = eof && current?.hash != info.hash,
            )
        }
    }

    private fun withFakeServer(block: (FakeAttachmentServer, Connection) -> Unit) =
        runBlocking {
            val fake = FakeAttachmentServer()
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
