package dev.kdb.integration

import dev.kdb.jdbc.AttachmentContentHash
import dev.kdb.jdbc.KdbAttachmentException
import dev.kdb.jdbc.KdbAttachmentRangeException
import dev.kdb.jdbc.KdbAttachmentSelector
import dev.kdb.jdbc.KdbAttachments
import dev.kdb.jdbc.KdbDriver
import dev.kdb.jdbc.KdbPutAttachmentOptions
import dev.kdb.jdbc.remote.KdbRemoteConnection
import org.junit.After
import org.junit.Assume
import org.junit.Before
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Types
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The Kotlin JDBC driver's attachment support against the real Go server: a `kdb-service` process
 * (KDB_SERVICE_BIN, default go/bin/kdb-service - `make build-go`) serving the SQL wire over
 * WebSocket, driven through `jdbc:kdb://` in remote mode. kdb-jdbc's AttachmentJdbcTest pins the
 * client to a Kotlin stand-in; this proves the two trees agree on the real frames, parameter forms
 * and SQL.
 *
 * Skipped when the binary is not built, except where KDB_REQUIRE_GO_SERVICE=1 - the CI integration
 * job, which builds it first, so a skip there would hide a broken setup. (CI's kotlin-test job runs
 * `./gradlew build` without Go binaries, and skips.)
 */
class GoServerJdbcAttachmentInteropTest {
    init {
        KdbDriver
    }

    private lateinit var dataDir: File
    private lateinit var logFile: File
    private var proc: Process? = null
    private var wsPort = 0

    private fun pattern(n: Int): ByteArray = ByteArray(n) { ((it * 31 + 7) and 0xFF).toByte() }

    @Before
    fun startGoServer() {
        val bin = serviceBinary()
        if (bin == null) {
            if (System.getenv("KDB_REQUIRE_GO_SERVICE") == "1") fail("kdb-service not built (KDB_SERVICE_BIN / go/bin/kdb-service) - run make build-go")
            Assume.assumeTrue("kdb-service not built; run make build-go or set KDB_SERVICE_BIN", false)
            return
        }
        dataDir = Files.createTempDirectory("kdb-go-jdbc-att").toFile()
        logFile = File(dataDir, "service.log")
        proc =
            ProcessBuilder(
                bin.absolutePath,
                "--data-dir", File(dataDir, "data").absolutePath,
                "--namespace", NS,
                "--sql-addr", "",
                "--peer-addr", "",
                "--stream-addr", "",
                "--ws-addr", "ws://127.0.0.1:0/kdb",
                "--admin-addr", "127.0.0.1:0",
                "--log-format", "json",
            ).redirectErrorStream(true).redirectOutput(logFile).start()
        // The started line reports the bound address: "ws":"enabled (127.0.0.1:NNNN)".
        val portRe = Regex(""""ws":"enabled \(127\.0\.0\.1:(\d+)\)"""")
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
        while (System.nanoTime() < deadline) {
            val p = proc!!
            if (!p.isAlive) fail("kdb-service exited early (${p.exitValue()}):\n${logFile.readText()}")
            portRe.find(logFile.readText())?.let {
                wsPort = it.groupValues[1].toInt()
                return
            }
            Thread.sleep(50)
        }
        fail("kdb-service did not report its ws listener within 30s:\n${logFile.readText()}")
    }

    @After
    fun stopGoServer() {
        proc?.let { p ->
            p.destroy()
            if (!p.waitFor(10, TimeUnit.SECONDS)) p.destroyForcibly().waitFor()
        }
        if (::dataDir.isInitialized) dataDir.deleteRecursively()
    }

    private fun serviceBinary(): File? {
        System.getenv("KDB_SERVICE_BIN")?.takeIf { it.isNotEmpty() }?.let { return File(it).takeIf(File::canExecute) }
        // Gradle runs tests in the module directory; walk up to the repo root.
        var dir: File? = File("").absoluteFile
        while (dir != null) {
            val candidate = File(dir, "go/bin/kdb-service")
            if (candidate.canExecute()) return candidate
            dir = dir.parentFile
        }
        return null
    }

    private fun connect(block: (Connection) -> Unit) =
        try {
            DriverManager.getConnection("jdbc:kdb://127.0.0.1:$wsPort/$NS").use(block)
        } catch (e: Throwable) {
            throw AssertionError("${e.message}\n--- kdb-service log ---\n${logFile.readText()}", e)
        }

    /** A fresh row; its kdb_id is the document attachments hang off. */
    private fun newDoc(
        conn: Connection,
        title: String,
    ): String {
        conn.prepareStatement("INSERT INTO docs (title) VALUES (?)").use { ps ->
            ps.setString(1, title)
            assertEquals(1, ps.executeUpdate())
        }
        return conn.prepareStatement("SELECT kdb_id FROM docs WHERE title = ?").use { ps ->
            ps.setString(1, title)
            ps.executeQuery().use { rs ->
                assertTrue(rs.next(), "row $title not found (autocommit INSERT not committed?)")
                rs.getString(1)
            }
        }
    }

    @Test
    fun autocommitDmlIsCommitted() =
        connect { conn ->
            // The Go server buffers a session's DML until TX_COMMIT; the driver must commit
            // autocommit statements itself, and one that matched nothing must not fail.
            conn.createStatement().use { st ->
                assertEquals(1, st.executeUpdate("INSERT INTO docs (title) VALUES ('lit')"))
                assertEquals(0, st.executeUpdate("UPDATE docs SET n = 1 WHERE title = 'zzz'"))
                assertEquals(0, st.executeUpdate("DELETE FROM docs WHERE title = 'zzz'"))
            }
            // Visible to a different connection (a different server session).
            connect { other ->
                other.createStatement().executeQuery("SELECT COUNT(*) FROM docs WHERE title = 'lit'").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt(1))
                }
            }
            fun count(title: String): Int =
                conn.createStatement().executeQuery("SELECT COUNT(*) FROM docs WHERE title = '$title'").use { rs ->
                    assertTrue(rs.next())
                    rs.getInt(1)
                }
            // A manual transaction still buffers until commit; rolled back, nothing lands.
            conn.autoCommit = false
            conn.createStatement().use { st -> assertEquals(1, st.executeUpdate("INSERT INTO docs (title) VALUES ('rolled')")) }
            conn.rollback()
            conn.autoCommit = true
            assertEquals(0, count("rolled"))
            // Committed, it does.
            conn.autoCommit = false
            conn.createStatement().use { st -> assertEquals(1, st.executeUpdate("INSERT INTO docs (title) VALUES ('kept')")) }
            conn.commit()
            assertEquals(1, count("kept"))
            // A transaction that only read commits cleanly too.
            assertEquals(1, count("lit"))
            conn.commit()
            conn.autoCommit = true
        }

    @Test
    fun attachmentsFeatureIsAgreed() =
        connect { conn ->
            assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentsSupported, "Go server did not agree wire feature attachments")
        }

    @Test
    fun putListGetRenameDeleteThroughKdbAttachments() =
        connect { conn ->
            val doc = newDoc(conn, "api")
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val bytes = pattern(3 * (1 shl 20) + 5)
            val put = att.put(doc, bytes, KdbPutAttachmentOptions(name = "photo.png", mime = "image/png"))
            // Go's content hash, the client's own (it verifies every download against it) and the
            // constant kdb-jdbc's AttachmentJdbcTest pins for the same bytes all agree.
            assertEquals("794771af5a7c3582979427373b32cf5f5335b6e9b1a4263092022ad768a8b465", put.info.hashHex)
            assertEquals(AttachmentContentHash.hex(bytes), put.info.hashHex)
            assertEquals(bytes.size.toLong(), put.info.size)
            assertEquals("image/png", put.info.mime)
            assertTrue(put.commitHex.isNotEmpty())

            assertEquals(listOf(put.info), att.list(doc))
            assertEquals(1, att.count(doc))
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals(bytes, att.get(doc, KdbAttachmentSelector.byId(put.info.id)))

            // Same name again replaces it, keeping its ID.
            val again = att.put(doc, "v2".encodeToByteArray(), KdbPutAttachmentOptions(name = "photo.png"))
            assertEquals(put.info.id, again.info.id)
            assertEquals(1, att.count(doc))

            // Neither ID nor name: a fresh ID, named after it; empty content round-trips.
            val anon = att.put(doc, ByteArray(0))
            assertEquals(anon.info.id.toString(), anon.info.name)
            assertEquals(2, att.count(doc))
            assertContentEquals(ByteArray(0), att.get(doc, KdbAttachmentSelector.byId(anon.info.id)))

            val renamed = att.rename(doc, KdbAttachmentSelector.byName("photo.png"), "p.png")
            assertEquals(put.info.id, renamed.info.id)
            assertNull(att.info(doc, KdbAttachmentSelector.byName("photo.png")))
            assertContentEquals("v2".encodeToByteArray(), att.get(doc, KdbAttachmentSelector.byName("p.png")))

            // The SQL view of the same document agrees with the API's.
            conn.prepareStatement("SELECT ATTACHMENT_COUNT(), ATTACHMENT('p.png') FROM docs WHERE kdb_id = ?").use { ps ->
                ps.setString(1, doc)
                ps.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    assertEquals(2, rs.getInt(1))
                    assertContentEquals("v2".encodeToByteArray(), rs.getBytes(2))
                }
            }

            assertTrue(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertFalse(att.delete(doc, KdbAttachmentSelector.byName("p.png")))
            assertEquals(1, att.count(doc))
        }

    @Test
    fun notFoundIsMapped() =
        connect { conn ->
            val doc = newDoc(conn, "missing")
            val att = conn.unwrap(KdbAttachments::class.java)
            val e = assertFailsWith<KdbAttachmentException> { att.get(doc, KdbAttachmentSelector.byName("nope")) }
            assertEquals("NOT_FOUND", e.errorCode)
            assertFailsWith<KdbAttachmentException> { att.rename(doc, KdbAttachmentSelector.byName("nope"), "x") }
        }

    @Test
    fun streamParameterUploadsAndSelectReadsBack() =
        connect { conn ->
            val doc = newDoc(conn, "stream")
            val big = pattern(5 * (1 shl 20) + 123)
            conn.prepareStatement("UPDATE docs SET ATTACHMENT 'x' = STREAM ? WHERE kdb_id = ?").use { ps ->
                ps.setBinaryStream(1, ByteArrayInputStream(big), big.size.toLong())
                ps.setString(2, doc)
                assertEquals(1, ps.executeUpdate())
            }
            conn.prepareStatement("SELECT ATTACHMENT('x'), ATTACHMENT_SIZE('x') FROM docs WHERE kdb_id = ?").use { ps ->
                ps.setString(1, doc)
                ps.executeQuery().use { rs ->
                    assertEquals(Types.VARBINARY, rs.metaData.getColumnType(1))
                    assertTrue(rs.next())
                    assertContentEquals(big, rs.getBytes(1))
                    assertContentEquals(big, rs.getBinaryStream(1).readAllBytes())
                    assertEquals(big.size.toLong(), rs.getLong(2))
                }
            }
            // The API reads the same attachment.
            val att = conn.unwrap(KdbAttachments::class.java)
            assertContentEquals(big, att.get(doc, KdbAttachmentSelector.byName("x")))
        }

    @Test
    fun inlineBytesAndBlobParameters() =
        connect { conn ->
            val doc = newDoc(conn, "inline")
            conn.prepareStatement("UPDATE docs SET ATTACHMENT 'a' = ?, ATTACHMENT 'b' = ?, n = ? WHERE kdb_id = ?").use { ps ->
                ps.setBytes(1, byteArrayOf(1, 2, 3, -1))
                ps.setBlob(2, javax.sql.rowset.serial.SerialBlob(byteArrayOf(9, 8)))
                ps.setLong(3, 7)
                ps.setString(4, doc)
                assertEquals(1, ps.executeUpdate())
            }
            conn.prepareStatement("SELECT ATTACHMENT('a'), ATTACHMENT('b'), ATTACHMENT('none'), n FROM docs WHERE kdb_id = ?").use { ps ->
                ps.setString(1, doc)
                ps.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    assertContentEquals(byteArrayOf(1, 2, 3, -1), rs.getBytes(1))
                    assertContentEquals(byteArrayOf(9, 8), rs.getBytes(2))
                    assertNull(rs.getBytes(3))
                    assertTrue(rs.wasNull())
                    assertEquals(7L, rs.getLong(4))
                }
            }
        }

    @Test
    fun pinnedReadSurvivesConcurrentReplace() =
        connect { conn ->
            val doc = newDoc(conn, "pinned")
            val att = conn.unwrap(KdbAttachments::class.java)
            att.pageBytes = 1 shl 20
            val v1 = pattern(2 * (1 shl 20) + 1)
            att.put(doc, v1, KdbPutAttachmentOptions(name = "a"))
            att.open(doc, KdbAttachmentSelector.byName("a")).use { s ->
                val first = ByteArray(10)
                assertEquals(10, s.read(first))
                // A newer version commits mid-read - through a second connection, as another client would.
                connect { other ->
                    other.unwrap(KdbAttachments::class.java).put(doc, "new".encodeToByteArray(), KdbPutAttachmentOptions(name = "a"))
                }
                val rest = s.readAllBytes()
                assertContentEquals(v1, first + rest)
                assertTrue(s.superseded, "read pinned v1 but did not report superseded")
            }
            assertContentEquals("new".encodeToByteArray(), att.get(doc, KdbAttachmentSelector.byName("a")))
            assertNotNull(att.info(doc, KdbAttachmentSelector.byName("a")))
        }

    @Test
    fun verifiedRangeReadsAgainstGo() =
        connect { conn ->
            assertTrue(conn.unwrap(KdbRemoteConnection::class.java).attachmentProofsSupported, "Go server did not agree attachment-proofs")
            val doc = newDoc(conn, "ranges")
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
            val p = ByteArray(10)
            assertEquals(3, att.readAt(doc, ref, null, p, size - 3))
            assertContentEquals(data.copyOfRange((size - 3).toInt(), size.toInt()), p.copyOf(3))

            // A java.sql.Blob over it, 1-based.
            val blob = att.blob(doc, ref)!!
            assertEquals(size, blob.length())
            assertContentEquals(data.copyOfRange(MIB - 1, MIB + 9), blob.getBytes(MIB.toLong(), 10))
            assertContentEquals(data.copyOfRange(3 * MIB, 4 * MIB + 5), blob.getBinaryStream(3L * MIB + 1, MIB + 5L).readAllBytes())

            // A range pins its version and reports a newer one at the end of its range (Go's close reply).
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
            // The old version stays readable by commit, and the blob kept it.
            assertContentEquals(data.copyOfRange((size - 5).toInt(), size.toInt()), att.getRange(doc, ref, put.commitHex, size - 5, 100))
            assertContentEquals(data.copyOf(4), blob.getBytes(1, 4))

            // A zero-byte attachment: its one empty chunk is proved.
            att.put(doc, ByteArray(0), KdbPutAttachmentOptions(name = "empty"))
            for (length in listOf(-1L, 0L, 10L)) {
                assertEquals(0, att.openRange(doc, KdbAttachmentSelector.byName("empty"), null, 0, length).use { it.readAllBytes() }.size)
            }
            assertFailsWith<KdbAttachmentRangeException> { att.openRange(doc, KdbAttachmentSelector.byName("empty"), null, 1, 1) }
        }

    private companion object {
        const val NS = "app/data"
        const val MIB = 1 shl 20
    }
}
