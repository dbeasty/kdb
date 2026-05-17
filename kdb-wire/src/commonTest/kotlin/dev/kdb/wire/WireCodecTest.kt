package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compaction.CompactionIntent
import dev.kdb.document.KdbOp
import dev.kdb.error.EncodingNegotiationFailureException
import dev.kdb.error.UnsupportedProtocolVersionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class WireCodecTest {
    private val codec = defaultWireCodec()

    @Test
    fun frameRoundtripHandshake() {
        val msg =
            WireMessage.Handshake(
                header(1, WireMessageType.HANDSHAKE),
                HandshakePayload(
                    nodeId = "node-a",
                    namespaces = listOf("app/data"),
                    localHeads = mapOf("app/data" to KdbHash.fromHex("00".repeat(32)).toHex()),
                    clientMode = WireClientMode.STREAM_READ_ONLY,
                ),
            )
        val decoded = codec.decode(codec.encode(msg))
        val back = decoded as WireMessage.Handshake
        assertEquals("node-a", back.request.nodeId)
        assertEquals(WireClientMode.STREAM_READ_ONLY, back.request.clientMode)
    }

    @Test
    fun frameRoundtripDeltaCommit() {
        val hash = KdbHash.fromHex("aa".repeat(32))
        val parent = KdbHash.fromHex("bb".repeat(32))
        val msg =
            WireMessage.DeltaCommit(
                header(2, WireMessageType.DELTA_COMMIT),
                DeltaCommitPayload(
                    namespace = "app/events",
                    commitHash = hash,
                    parentHash = parent,
                    timestampMicros = 1_700_000_000_000_000,
                    operations = emptyList(),
                    indexHints = emptyList(),
                ),
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.DeltaCommit
        assertEquals(hash, back.payload.commitHash)
        assertEquals(parent, back.payload.parentHash)
    }

    @Test
    fun frameRoundtripDeltaCommit_withOps() {
        val hash = KdbHash.fromHex("11".repeat(32))
        val parent = KdbHash.fromHex("22".repeat(32))
        val docId = KdbUuid.random()
        val msg =
            WireMessage.DeltaCommit(
                header(20, WireMessageType.DELTA_COMMIT),
                DeltaCommitPayload(
                    namespace = "app/data",
                    commitHash = hash,
                    parentHash = parent,
                    timestampMicros = 1_700_000_000_000_001,
                    operations = listOf(KdbOp.Write(docId, """{"k":"v"}""")),
                    indexHints = emptyList(),
                ),
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.DeltaCommit
        assertEquals(1, back.payload.operations.size)
        val op = back.payload.operations.single() as KdbOp.Write
        assertEquals(docId, op.docId)
        assertEquals("{\"k\":\"v\"}", op.patch)
    }

    @Test
    fun rejectOversizedFrame() {
        assertFailsWith<FrameTooLargeException> {
            validateFrameLength(DEFAULT_MAX_FRAME_BYTES + 1)
        }
    }

    @Test
    fun rejectTruncatedFrame() {
        assertFailsWith<WireDecodeException> {
            codec.decode(byteArrayOf(0, 0, 0, 8))
        }
    }

    @Test
    fun negotiatePrefersBinary() {
        val negotiator = defaultHandshakeNegotiator()
        val local =
            HandshakePayload(
                nodeId = "a",
                namespaces = listOf("ns"),
                localHeads = emptyMap(),
                preferredEncodings = listOf(PayloadEncoding.KDB_BINARY, PayloadEncoding.JSON),
                clientMode = WireClientMode.STREAM_READ_ONLY,
            )
        val remote = local.copy(nodeId = "b")
        val ack = negotiator.negotiate(local, remote)
        assertEquals(PayloadEncoding.KDB_BINARY, ack.negotiatedEncoding)
        assertTrue(ack.accepted)
    }

    @Test
    fun negotiateFailsNoCommon() {
        val negotiator = defaultHandshakeNegotiator()
        val local =
            HandshakePayload(
                nodeId = "a",
                namespaces = listOf("ns"),
                localHeads = emptyMap(),
                preferredEncodings = listOf(PayloadEncoding.JSON),
                clientMode = WireClientMode.STREAM_READ_ONLY,
            )
        val remote =
            local.copy(
                preferredEncodings = listOf(PayloadEncoding.KDB_BINARY),
            )
        assertFailsWith<EncodingNegotiationFailureException> {
            negotiator.negotiate(local, remote)
        }
    }

    @Test
    fun versionRejectFuture() {
        val negotiator = defaultHandshakeNegotiator()
        val local =
            HandshakePayload(
                nodeId = "a",
                namespaces = listOf("ns"),
                localHeads = emptyMap(),
                clientMode = WireClientMode.STREAM_READ_ONLY,
            )
        val remote = local.copy(protocolVersion = 99)
        assertFailsWith<UnsupportedProtocolVersionException> {
            negotiator.negotiate(local, remote)
        }
    }

    @Test
    fun compactionNoticeRoundtrip() {
        val boundary = KdbHash.fromHex("cc".repeat(32))
        val msg =
            WireMessage.CompactionNotice(
                header(3, WireMessageType.COMPACTION_NOTICE),
                CompactionIntent("app/data", boundary, 1_000L),
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.CompactionNotice
        assertEquals(boundary, back.intent.boundary)
    }

    @Test
    fun iceNoticeRoundtrip() {
        val orig = KdbHash.fromHex("dd".repeat(32))
        val bundle = KdbHash.fromHex("ee".repeat(32))
        val msg =
            WireMessage.IceArchiveNotice(
                header(4, WireMessageType.ICE_ARCHIVE_NOTICE),
                "app/data",
                orig,
                "ice://bucket/obj",
                bundle,
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.IceArchiveNotice
        assertEquals("ice://bucket/obj", back.archiveLocation)
    }

    @Test
    fun positionAckRoundtrip() {
        val hash = KdbHash.fromHex("ff".repeat(32))
        val msg =
            WireMessage.PositionAck(
                header(5, WireMessageType.POSITION_ACK),
                "app/data",
                hash,
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.PositionAck
        assertEquals(hash, back.commitHash)
    }

    @Test
    fun jsonEncodingRoundtrip() {
        val jsonCodec = defaultWireCodec(PayloadEncoding.JSON)
        val msg =
            WireMessage.Handshake(
                header(6, WireMessageType.HANDSHAKE),
                HandshakePayload(
                    nodeId = "n",
                    namespaces = listOf("x"),
                    localHeads = emptyMap(),
                    clientMode = WireClientMode.STREAM_WRITE_BACK,
                ),
            )
        val back = jsonCodec.decode(jsonCodec.encode(msg)) as WireMessage.Handshake
        assertEquals(WireClientMode.STREAM_WRITE_BACK, back.request.clientMode)
    }

    @Test
    fun commitPushRoundtrip() {
        val commit =
            dev.kdb.document.KdbCommit.build(
                parentHashes = listOf(KdbHash.fromHex("00".repeat(32))),
                namespaceId = "app/data",
                transactionId = KdbUuid.random(),
                timestamp = dev.kdb.codec.KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
                operations = listOf(KdbOp.Write(KdbUuid.random(), """{"a":1}""")),
                documentTreeHash = KdbHash.fromHex("11".repeat(32)),
                schemaHash = null,
            )
        val msg =
            WireMessage.CommitPush(
                header(9, WireMessageType.COMMIT_PUSH),
                "app/data",
                listOf(commit),
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.CommitPush
        assertEquals(1, back.commits.size)
        assertEquals(commit.hash, back.commits.single().hash)
    }

    @Test
    fun unknownMessageTypeOnDecode() {
        val frame = ByteArray(20)
        writeInt32Le(frame, 0, 20)
        writeInt16Le(frame, 4, 0xFF)
        assertFailsWith<WireDecodeException> {
            codec.decode(frame)
        }
    }

    private fun header(correlationId: Int, type: WireMessageType): WireHeader =
        WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

    private fun writeInt32Le(buf: ByteArray, offset: Int, value: Int) {
        buf[offset] = (value and 0xFF).toByte()
        buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
        buf[offset + 2] = ((value shr 16) and 0xFF).toByte()
        buf[offset + 3] = ((value shr 24) and 0xFF).toByte()
    }

    private fun writeInt16Le(buf: ByteArray, offset: Int, value: Int) {
        buf[offset] = (value and 0xFF).toByte()
        buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
    }
}
