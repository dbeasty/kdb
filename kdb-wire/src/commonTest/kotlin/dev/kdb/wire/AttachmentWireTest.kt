package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Attachment messages (0x42-0x49). The samples and golden frames are ported from
 * go/kdb/wire/attachment_ops_test.go (attachmentSamples / attachmentGoldenFrames): Kotlin must decode
 * each frame to the same message and encode it to the same bytes.
 */
class AttachmentWireTest {
    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    private fun repeatHash(b: Int): KdbHash = KdbHash(ByteArray(32) { b.toByte() })

    private fun h(corr: Int, type: WireMessageType): WireHeader = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, corr, 0)

    private val doc = KdbUuid.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
    private val att = KdbUuid.fromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
    private val info = WireAttachmentInfo(att, "photo.png", repeatHash(7), 1L shl 33, "image/png")
    private val commit3 = repeatHash(3).toHex()

    private fun samples(): List<WireMessage> =
        listOf(
            WireMessage.AttachmentUpload(
                h(1, WireMessageType.ATTACHMENT_UPLOAD), "app/data", "s1", "u1", 1L shl 20,
                byteArrayOf(0, 1, 2, 255.toByte()), true,
            ),
            WireMessage.AttachmentUploadResult(
                h(1, WireMessageType.ATTACHMENT_UPLOAD_RESULT), "app/data", "u1", (1L shl 20) + 4, repeatHash(7),
                (1L shl 20) + 4,
            ),
            WireMessage.AttachmentUploadResult(
                h(1, WireMessageType.ATTACHMENT_UPLOAD_RESULT), "app/data",
                error = "offset 5 does not follow 4", errorCode = "CONFLICT",
            ),
            WireMessage.AttachmentGet(
                h(2, WireMessageType.ATTACHMENT_GET), "app/data", "s1", doc, att, "photo.png", commit3, 1L shl 20,
                4L shl 20, "r9", true,
            ),
            WireMessage.AttachmentData(
                h(2, WireMessageType.ATTACHMENT_DATA), "app/data", "r9", doc, info, commit3, 1L shl 20,
                "bytes".encodeToByteArray(), eof = true, superseded = true,
            ),
            WireMessage.AttachmentList(h(3, WireMessageType.ATTACHMENT_LIST), "app/data", "s1", doc, ""),
            WireMessage.AttachmentListResult(
                h(3, WireMessageType.ATTACHMENT_LIST_RESULT), "app/data", commit3,
                listOf(info, WireAttachmentInfo(id = doc, name = "b", hash = repeatHash(8))),
            ),
            WireMessage.BlobFetch(
                h(4, WireMessageType.BLOB_FETCH), "app/data",
                listOf(WireBlobFetchEntry(doc, att, commit3, repeatHash(7), 2L shl 20, 1L shl 20)),
                8L shl 20,
            ),
            WireMessage.BlobFetchResult(
                h(4, WireMessageType.BLOB_FETCH_RESULT), "app/data",
                listOf(
                    WireBlobFetchPart(
                        index = 0, hash = repeatHash(7), size = 3L shl 20, tree = repeatHash(6), offset = 2L shl 20,
                        data = byteArrayOf(9, 9),
                        chunks = listOf(WireBlobChunkProof(2, listOf(repeatHash(1), repeatHash(2)))), eof = true,
                    ),
                    WireBlobFetchPart(index = 1, error = "not retained", errorCode = "NOT_FOUND"),
                ),
                more = true,
            ),
            // A verified ranged read (WIRE_FEATURE_ATTACHMENT_PROOFS): the get asks for proofs, the page
            // covers whole chunks and proves each - leaf 0 with an empty-field leaf, leaf 1 short.
            WireMessage.AttachmentGet(
                h(5, WireMessageType.ATTACHMENT_GET), "app/data", docId = doc, name = "photo.png",
                offset = (1L shl 20) + 7, maxBytes = 100, proofs = true,
            ),
            WireMessage.AttachmentData(
                h(5, WireMessageType.ATTACHMENT_DATA), "app/data", "r10", doc,
                WireAttachmentInfo(att, "photo.png", repeatHash(7), (1L shl 20) + 2), commit3,
                data = byteArrayOf(4, 5, 6), eof = true, tree = repeatHash(6), scheme = 1,
                chunks = listOf(WireBlobChunkProof(0, listOf(repeatHash(1))), WireBlobChunkProof(1, listOf(repeatHash(2)))),
            ),
        )

    private val golden =
        listOf(
            "2b0000004400010001000000020a086170702f64617461120273311a027531208080402a04000102ff3001",
            "450000004500010001000000020a086170702f6461746112027531188480402220070707070707070707070707070707070707070707070707070707070707070728848040",
            "3d0000004500010001000000020a086170702f64617461321a6f6666736574203520646f6573206e6f7420666f6c6c6f7720343a08434f4e464c494354",
            "9b0000004600010002000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff22100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b2a0970686f746f2e706e673240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333880804040808080024a0272395001",
            "d00000004700010002000000020a086170702f64617461120272391a106f9619ff8b86d011b42d00cf4fc964ff22500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e672a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033308080403a05627974657340014801",
            "2d0000004800010003000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff",
            "e40000004900010003000000020a086170702f646174611240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330331a500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e671a370a106f9619ff8b86d011b42d00cf4fc964ff1201621a200808080808080808080808080808080808080808080808080808080808080808",
            "b00000004200010004000000020a086170702f646174611291010a106f9619ff8b86d011b42d00cf4fc964ff12100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b1a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033222007070707070707070707070707070707070707070707070707070707070707072880808001308080401880808004",
            "d50000004300010004000000020a086170702f64617461129c0112200707070707070707070707070707070707070707070707070707070707070707188080c001222006060606060606060606060606060606060606060606060606060606060606062880808001320209093a46080212200101010101010101010101010101010101010101010101010101010101010101122002020202020202020202020202020202020202020202020202020202020202024001121b0801520c6e6f742072657461696e65645a094e4f545f464f554e441801",
            "3c0000004600010005000000020a086170702f646174611a106f9619ff8b86d011b42d00cf4fc964ff2a0970686f746f2e706e673887804040645801",
            "2a0100004700010005000000020a086170702f6461746112037231301a106f9619ff8b86d011b42d00cf4fc964ff22430a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a200707070707070707070707070707070707070707070707070707070707070707208280402a40303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333a030405064001622006060606060606060606060606060606060606060606060606060606060606066a22122001010101010101010101010101010101010101010101010101010101010101016a240801122002020202020202020202020202020202020202020202020202020202020202027001",
        )

    /** The decoded header carries the payload length; the samples' headers leave it zero. */
    private fun withLength(m: WireMessage, frame: ByteArray): WireMessage {
        val hd = m.header.copy(payloadLength = frame.size - 12)
        return when (m) {
            is WireMessage.AttachmentUpload -> m.copy(header = hd)
            is WireMessage.AttachmentUploadResult -> m.copy(header = hd)
            is WireMessage.AttachmentGet -> m.copy(header = hd)
            is WireMessage.AttachmentData ->
                WireMessage.AttachmentData(
                    hd, m.namespace, m.readId, m.docId, m.info, m.commitHex, m.offset, m.data, m.eof, m.superseded,
                    m.error, m.errorCode, m.tree, m.chunks, m.scheme,
                )
            is WireMessage.AttachmentList -> m.copy(header = hd)
            is WireMessage.AttachmentListResult -> m.copy(header = hd)
            is WireMessage.BlobFetch -> m.copy(header = hd)
            is WireMessage.BlobFetchResult -> m.copy(header = hd)
            else -> error("not an attachment message")
        }
    }

    @Test
    fun codesMatchGo() {
        val want =
            mapOf(
                0x42 to WireMessageType.BLOB_FETCH, 0x43 to WireMessageType.BLOB_FETCH_RESULT,
                0x44 to WireMessageType.ATTACHMENT_UPLOAD, 0x45 to WireMessageType.ATTACHMENT_UPLOAD_RESULT,
                0x46 to WireMessageType.ATTACHMENT_GET, 0x47 to WireMessageType.ATTACHMENT_DATA,
                0x48 to WireMessageType.ATTACHMENT_LIST, 0x49 to WireMessageType.ATTACHMENT_LIST_RESULT,
            )
        for ((code, type) in want) assertEquals(type, WireMessageType.fromCode(code.toShort()))
        assertEquals(6, SqlCellKind.BYTES)
        assertTrue(WIRE_FEATURE_ATTACHMENTS in SUPPORTED_WIRE_FEATURES)
        assertEquals("attachments", WIRE_FEATURE_ATTACHMENTS)
        assertEquals("attachment-proofs", WIRE_FEATURE_ATTACHMENT_PROOFS)
        // Go's order: proofs right after attachments.
        assertEquals(SUPPORTED_WIRE_FEATURES.indexOf(WIRE_FEATURE_ATTACHMENTS) + 1, SUPPORTED_WIRE_FEATURES.indexOf(WIRE_FEATURE_ATTACHMENT_PROOFS))
    }

    @Test
    fun encodesGoGoldenFrames() {
        val samples = samples()
        assertEquals(golden.size, samples.size)
        // Always binary: a JSON-only codec, and one with every feature, write the same frame.
        for (codec in listOf(defaultWireCodec(PayloadEncoding.JSON), defaultWireCodec().withWireFeatures(SUPPORTED_WIRE_FEATURES))) {
            for ((i, msg) in samples.withIndex()) {
                assertEquals(golden[i], toHex(codec.encode(msg)), "${msg.header.messageType}")
            }
        }
    }

    @Test
    fun decodesGoGoldenFrames() {
        val codec = defaultWireCodec()
        for ((i, msg) in samples().withIndex()) {
            val frame = hex(golden[i])
            val decoded = codec.decode(frame)
            assertEquals(withLength(msg, frame), decoded, "${msg.header.messageType}")
            assertEquals(golden[i], toHex(codec.encode(decoded)))
        }
    }

    @Test
    fun uploadChunkOfMaxSizeRoundTripsWithZstd() {
        val codec = defaultWireCodec().withWireFeatures(SUPPORTED_WIRE_FEATURES)
        val data = ByteArray(1 shl 20) { (it % 7).toByte() }
        val msg = WireMessage.AttachmentUpload(h(9, WireMessageType.ATTACHMENT_UPLOAD), "ns", "s", "u", 0, data, false)
        val frame = codec.encode(msg)
        assertTrue(frame.size < data.size, "a compressible chunk goes as a zstd frame")
        val back = codec.decode(frame) as WireMessage.AttachmentUpload
        assertTrue(back.data.contentEquals(data))
    }

    @Test
    fun uploadExpectIsField7AndRoundTrips() {
        // Go's AttachmentUploadMessage.Expect: appendHash(b, 7, m.Expect), a 32-byte bytes field.
        val codec = defaultWireCodec()
        val expect = KdbHash(ByteArray(32) { 0xab.toByte() })
        val msg = WireMessage.AttachmentUpload(h(3, WireMessageType.ATTACHMENT_UPLOAD), "ns", data = byteArrayOf(1), expect = expect)
        val frame = codec.encode(msg)
        assertTrue(toHex(frame).endsWith("2a0101" + "3a20" + "ab".repeat(32)), toHex(frame))
        val back = codec.decode(frame) as WireMessage.AttachmentUpload
        assertEquals(expect, back.expect)
        assertEquals(null, (codec.decode(codec.encode(msg.copyWithoutExpect())) as WireMessage.AttachmentUpload).expect)
    }

    private fun WireMessage.AttachmentUpload.copyWithoutExpect() =
        WireMessage.AttachmentUpload(header, namespace, sessionId, uploadId, offset, data, final)

    @Test
    fun integrityRefusalsRoundTrip() {
        // A server refusing its damaged stored copy (Go: attachstore.ErrCorrupt -> INTEGRITY), on a
        // read page and on a BLOB_FETCH entry: the code survives the binary frame, as a string.
        val codec = defaultWireCodec().withWireFeatures(SUPPORTED_WIRE_FEATURES)
        val msg = "attachstore: stored bytes do not match their content hash: chunk 2 of ab"
        val data = WireMessage.AttachmentData(h(3, WireMessageType.ATTACHMENT_DATA), "ns", "r1", error = msg, errorCode = "INTEGRITY")
        val back = codec.decode(codec.encode(data)) as WireMessage.AttachmentData
        assertEquals("INTEGRITY", back.errorCode)
        assertEquals(msg, back.error)
        val fetch =
            WireMessage.BlobFetchResult(
                h(4, WireMessageType.BLOB_FETCH_RESULT), "ns",
                listOf(WireBlobFetchPart(index = 0, hash = repeatHash(1), error = msg, errorCode = "INTEGRITY")),
            )
        val part = (codec.decode(codec.encode(fetch)) as WireMessage.BlobFetchResult).parts.single()
        assertEquals("INTEGRITY", part.errorCode)
        assertEquals(msg, part.error)
    }

    @Test
    fun malformedUuidIsADecodeError() {
        // ATTACHMENT_LIST with a 2-byte docId (field 3).
        val body = hex("02" + "1a020102")
        val frame = hex("11000000" + "4800" + "0100" + "01000000") + body
        frame[0] = frame.size.toByte()
        assertFailsWith<WireDecodeException> { defaultWireCodec().decode(frame) }
    }
}
