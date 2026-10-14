package dev.kdb.document

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * A commit carrying attachment operations and an attachment root, encoded by Go
 * (go/kdb/document/attachment_golden_test.go): Kotlin reads it, re-encodes it to the same bytes,
 * and derives the same hash.
 */
class AttachmentCommitGoldenTest {
    private val payload = "dc02010a0101010101010101010101010101010101010101010101010101010101010101010209086170702f64617461030f6f9619ff8b86d011b42d00cf4fc964ff040505605ba09d370600050f6f9619ff8b86d011b42d00cf4fc964ff060a03001c010f6f9619ff8b86d011b42d00cf4fc964ff0209077b2261223a317d046e010f6f9619ff8b86d011b42d00cf4fc964ff020f0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b03090970686f746f2e706e67040401000000050fabababababababababababababababababababababababababababababababab060587d6120000000000070909696d6167652f706e670524010f6f9619ff8b86d011b42d00cf4fc964ff020f0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b070f02020202020202020202020202020202020202020202020202020202020202020909016d0a0e01cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    private val hash = "ca9ab1ab70b327ae4cbc9092c4a87a55e10a1d47c6fcac17b72e0ded7beee9c3"

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    @Test
    fun decodesAndReencodesAGoCommitWithAttachments() {
        val c = KdbCommit.fromPayloadBytes(hex(payload))
        assertEquals(hash, c.hash.toHex())
        assertEquals(3, c.operations.size)
        val put = c.operations[1] as KdbOp.AttachmentPut
        assertEquals("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b", put.attachmentId.toString())
        assertEquals("photo.png", put.name)
        assertEquals(1, put.scheme)
        assertEquals(1234567L, put.size)
        assertEquals("image/png", put.mime)
        assertEquals("ab".repeat(32), put.contentHash.toHex())
        assertEquals(KdbOp.AttachmentDelete(put.docId, put.attachmentId), c.operations[2])
        assertEquals("cd".repeat(32), c.attachmentTreeHash?.toHex())
        assertEquals(payload, toHex(c.toPayloadBytes()))
        assertEquals(hash, computeCommitHash(c).toHex())
    }

    @Test
    fun aCommitWithoutAttachmentsHasNoRoot() {
        val c = KdbCommit.fromPayloadBytes(hex(payload))
        val plain = c.copy(attachmentTreeHash = null, operations = c.operations.take(1))
        assertNull(KdbCommit.fromPayloadBytes(plain.toPayloadBytes()).attachmentTreeHash)
    }
}
