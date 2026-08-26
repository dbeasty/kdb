package dev.kdb.inspect

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DocumentPatch
import dev.kdb.storage.delta.DefaultDeltaSegmentWriter
import dev.kdb.storage.delta.DeltaSegmentScanner
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class InspectJsonTest {
    @Test
    fun commitJson_containsHashAndOps() {
        val id = KdbUuid.random()
        val commit =
            KdbCommit.build(
                parentHashes = emptyList(),
                namespaceId = "app/test",
                transactionId = KdbUuid.random(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
                operations = listOf(KdbOp.Write(id, """{"a":1}""")),
                documentTreeHash = KdbHash.fromHex("aa".repeat(32)),
                schemaHash = null,
            )
        val line = InspectJson.commitToJsonLine(commit)
        assertTrue(line.contains(commit.hash.toHex()))
        assertTrue(line.contains("write"))
    }

    @Test
    fun scan_threeCommits() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config =
                StorageEngineConfig(
                    globalMemoryBudgetBytes = 4_000_000,
                    ioShim = shim,
                    compressionCodec = CompressionCodec.NONE,
                )
            val writer = DefaultDeltaSegmentWriter("app/test", KdbUuid.random(), 0L, shim, config)
            val commits = mutableListOf<KdbCommit>()
            repeat(3) { i ->
                val commit =
                    KdbCommit.build(
                        parentHashes = commits.lastOrNull()?.let { listOf(it.hash) } ?: emptyList(),
                        namespaceId = "app/test",
                        transactionId = KdbUuid.random(),
                        timestamp = KdbTimestamp.now(),
                        authorNodeId = KdbUuid.random(),
                        operations = listOf(KdbOp.Write(KdbUuid.random(), """{"n":$i}""")),
                        documentTreeHash = KdbHash.fromHex("bb".repeat(32)),
                        schemaHash = null,
                    )
                commits.add(commit)
                writer.append(
                    DeltaRecord(
                        commitHash = commit.hash,
                        namespaceId = "app/test",
                        authorship =
                            DeltaAuthorshipEnvelope(
                                principal = "test",
                                timestamp = commit.timestamp,
                                rightsToken = "",
                                clientContext = "",
                            ),
                        commitPayload = commit.toPayloadBytes(),
                        documentPatches = emptyList(),
                    ),
                )
            }
            val ref = writer.seal()
            val segmentName = SegmentNameBuilder.delta("app/test", ref.segmentId.toString())
            val bytes = shim.readFromSegment(segmentName, 0, ref.sizeBytes.toInt())
            val scanned = DeltaSegmentScanner.scanSegmentBytes(bytes, CompressionCodec.NONE)
            assertEquals(3, scanned.size)
            assertEquals(commits[0].hash, scanned[0].commitHash)
        }

    @Test
    fun wireDump_handshake() {
        val codec = defaultWireCodec()
        val msg =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, correlationId = 1, payloadLength = 0),
                dev.kdb.wire.HandshakePayload(
                    nodeId = "n1",
                    namespaces = listOf("app/x"),
                    localHeads = emptyMap(),
                    clientMode = dev.kdb.wire.WireClientMode.STREAM_READ_ONLY,
                ),
            )
        val inspector = WireFrameInspector(codec)
        val dump = inspector.dumpFrame(codec.encode(msg))
        assertTrue(dump.contains("handshake") || dump.contains("HANDSHAKE"))
    }
}
