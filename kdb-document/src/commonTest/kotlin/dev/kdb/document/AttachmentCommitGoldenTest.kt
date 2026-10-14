package dev.kdb.document

import dev.kdb.codec.KdbHash
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * A commit carrying attachment operations and an attachment root, encoded by Go
 * (go/kdb/document/attachment_golden_test.go): Kotlin reads it, re-encodes it to the same bytes,
 * and derives the same hash. An older Kotlin that knew neither the ops nor the field could not.
 */
class AttachmentCommitGoldenTest {
    private val payload = "a602010a0101010101010101010101010101010101010101010101010101010101010101010209086170702f64617461030f6f9619ff8b86d011b42d00cf4fc964ff040505605ba09d370600050f6f9619ff8b86d011b42d00cf4fc964ff060a03001c010f6f9619ff8b86d011b42d00cf4fc964ff0209077b2261223a317d044a010f6f9619ff8b86d011b42d00cf4fc964ff020fabababababababababababababababababababababababababababababababab030587d6120000000000040909696d6167652f706e670512010f6f9619ff8b86d011b42d00cf4fc964ff070f02020202020202020202020202020202020202020202020202020202020202020909016d0a0e01cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    private val hash = "6a3444f1692884a2d93beffc5107f6fa96c9d87cce3b43438d1e532e7a06ee76"

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    @Test
    fun decodesAndReencodesAGoCommitWithAttachments() {
        val c = KdbCommit.fromPayloadBytes(hex(payload))
        assertEquals(hash, c.hash.toHex())
        assertEquals(3, c.operations.size)
        val put = c.operations[1] as KdbOp.AttachmentPut
        assertEquals(1234567L, put.size)
        assertEquals("image/png", put.mime)
        assertEquals("ab".repeat(32), put.contentHash.toHex())
        assertEquals(KdbOp.AttachmentDelete(put.docId), c.operations[2])
        assertEquals("cd".repeat(32), c.attachmentTreeHash?.toHex())
        assertEquals(payload, toHex(c.toPayloadBytes()))
        assertEquals(hash, computeCommitHash(c).toHex())
    }

    @Test
    fun aCommitWithoutAttachmentsHasNoRoot() {
        val c = KdbCommit.fromPayloadBytes(hex(payload))
        val plain = c.copy(attachmentTreeHash = null, operations = c.operations.take(1))
        assertNull(KdbCommit.fromPayloadBytes(plain.toPayloadBytes()).attachmentTreeHash)
        assertEquals(computeCommitHash(plain), computeCommitHash(plain.copy(attachmentTreeHash = null)))
    }
}
