package dev.kdb.jdbc

import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.jdbc.remote.KdbRemoteConnection
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.SUPPORTED_WIRE_FEATURES
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENTS
import dev.kdb.wire.WIRE_FEATURE_ATTACHMENT_PROOFS
import dev.kdb.wire.WIRE_FEATURE_BINARY
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
import java.io.ByteArrayInputStream
import java.sql.DriverManager
import java.sql.SQLFeatureNotSupportedException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Where attachments are not available, they fail clearly instead of being dropped. */
class AttachmentUnsupportedTest {
    init {
        KdbDriver
    }

    @Before
    fun setUp() = JdbcTestSupport.clearMemoryRegistries()

    @After
    fun tearDown() = JdbcTestSupport.clearMemoryRegistries()

    @Test
    fun contentHashMatchesSchemeOneGoldens() {
        // "hello" is Go's pinned golden (document/attachment_golden_test.go); the others come from an
        // independent implementation of the definition, over bytes (i*31+7) & 0xff.
        assertEquals("cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b", AttachmentContentHash.hex("hello".encodeToByteArray()))
        fun pattern(n: Int) = ByteArray(n) { ((it * 31 + 7) and 0xFF).toByte() }
        val mib = 1 shl 20
        val want =
            mapOf(
                0 to "c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea05425",
                mib to "e8382e4ca27905252faaf7c1a7598f3457729a69ce9d0d64e805ee895091e58e",
                mib + 1 to "f5166bf299f5d2dd3199f2c041f4139f59efbcf6c24469542dbd17e46249e6db",
                3 * mib + 5 to "794771af5a7c3582979427373b32cf5f5335b6e9b1a4263092022ad768a8b465",
                5 * mib to "e7528a3a6e31eb032a2afb51c52ba3c4dcce6d8fa31fc05e2aa54660f8bf7873",
            )
        for ((n, hex) in want) {
            assertEquals(hex, AttachmentContentHash.hex(pattern(n)), "size $n")
            // Fed in odd pieces, the incremental hash is the same.
            val h = AttachmentContentHash()
            val bytes = pattern(n)
            var off = 0
            while (off < n) {
                val len = minOf(777_777, n - off)
                h.update(bytes, off, len)
                off += len
            }
            assertEquals(hex, h.finishHex(), "size $n, incremental")
        }
    }

    @Test
    fun embeddedRefusesBinaryParameters() {
        DriverManager.getConnection("jdbc:kdb:memory:///demo/users").use { conn ->
            conn.prepareStatement("SELECT _doc FROM users WHERE userId = ?").use { ps ->
                ps.setBytes(1, byteArrayOf(1))
                val e = assertFailsWith<SQLFeatureNotSupportedException> { ps.executeQuery() }
                assertTrue(e.message!!.contains("attachments"), e.message)
                ps.setBinaryStream(1, ByteArrayInputStream(byteArrayOf(1)))
                assertFailsWith<SQLFeatureNotSupportedException> { ps.executeQuery() }
            }
        }
    }

    @Test
    fun embeddedRejectsAttachmentSql() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users") as KdbConnection
        conn.use {
            JdbcTestSupport.seedUsers(conn)
            val statements =
                listOf(
                    "INSERT INTO users (userId, status) VALUES ('u9', 'x') WITH ATTACHMENT 'a' = X'0102'",
                    "UPDATE users SET status = 'y', ATTACHMENT 'a' = X'0102' WHERE userId = 'u1'",
                    "UPDATE users SET DROP ATTACHMENT 'a' WHERE userId = 'u1'",
                    "SELECT ATTACHMENT_COUNT() FROM users",
                    "SELECT _attachments FROM users",
                )
            for (sql in statements) {
                val outcome = runCatching { conn.createStatement().use { st -> st.execute(sql) } }
                val e = outcome.exceptionOrNull()
                assertTrue(e is SQLFeatureNotSupportedException, "embedded mode: $sql -> $e")
                assertTrue(e.message!!.contains("attachments"), e.message)
            }
        }
    }

    @Test
    fun kotlinServerServesAttachmentsButNotBinarySqlOrBlobFetch() =
        runBlocking {
            val ns = "demo/users"
            val schema = KdbSchema.build(listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)))
            val runtime = openMemoryRuntimeBlocking("demo", ns, schema)
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)

            // Peer sync's BLOB_FETCH (sync v2, not hosted here yet) gets a typed UNSUPPORTED answer.
            val fetch = WireMessage.BlobFetch(WireHeader(WireMessageType.BLOB_FETCH, KDB_WIRE_PROTOCOL_VERSION, 42, 0), ns)
            val reply = wire.decode(host.handleFrame(wire.encode(fetch))!!) as WireMessage.BlobFetchResult
            assertEquals(42, reply.header.correlationId)
            assertEquals("UNSUPPORTED", reply.errorCode)

            // Offered every feature, it acks attachments and attachment-proofs (KotlinServerAttachmentTest
            // covers what it serves), and sends no serverVersion: the driver keeps its own SQL dialect.
            val hs =
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 43, 0),
                    HandshakePayload(
                        nodeId = "t", namespaces = listOf(ns), localHeads = emptyMap(), clientMode = WireClientMode.SQL_CLIENT,
                        wireFeatures = SUPPORTED_WIRE_FEATURES,
                    ),
                )
            val ack = wire.decode(host.handleFrame(wire.encode(hs))!!) as WireMessage.HandshakeAck
            assertTrue(ack.response.accepted)
            assertTrue(WIRE_FEATURE_BINARY in ack.response.wireFeatures, "${ack.response.wireFeatures}")
            assertTrue(WIRE_FEATURE_ATTACHMENTS in ack.response.wireFeatures)
            assertTrue(WIRE_FEATURE_ATTACHMENT_PROOFS in ack.response.wireFeatures)
            assertEquals("", ack.response.serverVersion)

            val server = JvmNetworkWebSocketServer()
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val job =
                scope.launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame -> host.handleFrame(frame)?.let { conn.send(it) } }
                    }
                }
            while (server.port == 0) delay(10)
            try {
                DriverManager.getConnection("jdbc:kdb://127.0.0.1:${server.port}/$ns").use { conn ->
                    assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentsSupported)
                    assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentProofsSupported)
                    conn.prepareStatement("SELECT _doc FROM users WHERE userId = ?").use { ps ->
                        ps.setBytes(1, byteArrayOf(1))
                        val e = assertFailsWith<SQLFeatureNotSupportedException> { ps.executeQuery() }
                        assertTrue(e.message!!.contains("KdbAttachments"), e.message)
                        // Ordinary parameters still go in the Kotlin server's own form.
                        ps.setString(1, "u1")
                        ps.executeQuery().close()
                    }
                }
            } finally {
                runCatching { server.stop() }
                job.cancel()
                scope.cancel()
            }
        }
}
