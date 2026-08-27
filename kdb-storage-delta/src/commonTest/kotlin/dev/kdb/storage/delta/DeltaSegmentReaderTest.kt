package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class DeltaSegmentReaderTest {
    @Test
    fun readAll_afterAppend() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config =
                StorageEngineConfig(
                    globalMemoryBudgetBytes = 2_000_000,
                    ioShim = shim,
                    compressionCodec = CompressionCodec.NONE,
                )
            val factory = DeltaSegmentFactory(config)
            val writer = factory.openWriter("ns1")
            val commit =
                KdbCommit.build(
                    parentHashes = emptyList(),
                    namespaceId = "ns1",
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":1}""")),
                    documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                    schemaHash = null,
                )
            writer.append(
                DeltaRecord(
                    commitHash = commit.hash,
                    namespaceId = "ns1",
                    authorship =
                        DeltaAuthorshipEnvelope(
                            principal = "p",
                            timestamp = commit.timestamp,
                            rightsToken = "",
                            clientContext = "",
                        ),
                    commitPayload = commit.toPayloadBytes(),
                    documentPatches = emptyList(),
                ),
            )
            val ref = writer.seal()
            val reader = factory.openReader("ns1")
            val records = reader.readAll(ref)
            assertEquals(1, records.size)
            assertEquals(commit.hash, records[0].commitHash)
        }

    /**
     * Regression test for docs/kdb-finish-up-plan.md's 1-K4: scanSegmentRef used to mint a fresh
     * KdbUuid.random() as segmentId on every single listSegments() call. Since consumers like
     * DeltaLogTierRegistry key their per-segment state off segmentId, that state was silently
     * discarded on every rescan (e.g. every process restart). segmentId must now be stable across
     * independent scans of the same on-disk segment.
     */
    @Test
    fun listSegments_segmentIdIsStableAcrossIndependentScans() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 2_000_000, ioShim = shim, compressionCodec = CompressionCodec.NONE)
            val factory = DeltaSegmentFactory(config)
            val writer = factory.openWriter("ns2")
            writer.append(sampleRecord("ns2"))
            writer.seal()

            val firstScan = factory.openReader("ns2").listSegments()
            val secondScan = factory.openReader("ns2").listSegments()

            assertEquals(1, firstScan.size)
            assertEquals(1, secondScan.size)
            assertEquals(firstScan[0].segmentId, secondScan[0].segmentId, "segmentId must not change between scans of the same segment")
        }

    /** Companion to the stability test: different namespaces reusing sequence number 0 must not collide on segmentId. */
    @Test
    fun listSegments_segmentIdDiffersAcrossNamespacesAtTheSameSequenceNumber() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 2_000_000, ioShim = shim, compressionCodec = CompressionCodec.NONE)
            val factory = DeltaSegmentFactory(config)
            factory.openWriter("nsA").apply { append(sampleRecord("nsA")) }.seal()
            factory.openWriter("nsB").apply { append(sampleRecord("nsB")) }.seal()

            val refA = factory.openReader("nsA").listSegments().single()
            val refB = factory.openReader("nsB").listSegments().single()

            assertEquals(0L, refA.sequenceNumber)
            assertEquals(0L, refB.sequenceNumber)
            assertTrue(refA.segmentId != refB.segmentId, "same sequence number in different namespaces must not collide")
        }

    private fun sampleRecord(namespaceId: String): DeltaRecord {
        val commit =
            KdbCommit.build(
                parentHashes = emptyList(),
                namespaceId = namespaceId,
                transactionId = KdbUuid.random(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
                operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":1}""")),
                documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                schemaHash = null,
            )
        return DeltaRecord(
            commitHash = commit.hash,
            namespaceId = namespaceId,
            authorship = DeltaAuthorshipEnvelope(principal = "p", timestamp = commit.timestamp, rightsToken = "", clientContext = ""),
            commitPayload = commit.toPayloadBytes(),
            documentPatches = emptyList(),
        )
    }
}
