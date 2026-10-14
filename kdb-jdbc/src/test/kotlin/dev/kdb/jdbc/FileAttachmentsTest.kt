package dev.kdb.jdbc

import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.embed.AttachmentSelector
import dev.kdb.embed.AttachmentSource
import dev.kdb.embed.PutAttachmentOptions
import dev.kdb.embed.getAttachment
import dev.kdb.embed.listAttachments
import dev.kdb.embed.putAttachment
import dev.kdb.embed.putJson
import dev.kdb.jdbc.file.openFileRuntime
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before

/** Attachments in a file runtime survive a reopen: the bytes in the data directory's segments, the
 * table re-derived (and checked) from the replayed commits. */
class FileAttachmentsTest {
    @Before
    fun setUp() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @After
    fun tearDown() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @Test
    fun attachmentsSurviveAReopen() =
        runBlocking {
            val root = createTempDirectory("kdb-file-att").toString()
            val ns = "demo/files"
            val data = ByteArray(ATTACHMENT_CHUNK_BYTES + 77) { (it * 13).toByte() }
            val rt = openFileRuntime(root, "demo", ns)
            val doc = KdbUuid.fromString(putJson(rt, ns, """{"n":1}"""))
            val (commit, info) = rt.putAttachment(ns, doc, AttachmentSource.of(data), PutAttachmentOptions(name = "f.bin"))

            val again = openFileRuntime(root, "demo", ns)
            assertEquals(commit.hash, again.dag.head())
            assertEquals(listOf(info), again.listAttachments(ns, doc))
            val read = again.getAttachment(ns, doc, AttachmentSelector.ByName("f.bin"))
            try {
                assertContentEquals(data, read.reader.readAll())
            } finally {
                read.reader.close()
            }
        }
}
