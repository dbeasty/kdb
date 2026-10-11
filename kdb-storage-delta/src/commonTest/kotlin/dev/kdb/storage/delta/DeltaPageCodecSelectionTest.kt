package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.ZstdCompression
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Which codec a delta page is written with, and that the segment reads back on the same target.
 * docs/kdb-open-issues-plan.md U5: on JS and Native, ZstdCompression was a length-prefixed copy
 * of the payload, but the writer tagged every page ZSTD (the default codec) - so a JS- or
 * Native-written segment claimed to be zstd and Go and the JVM could not read it.
 */
class DeltaPageCodecSelectionTest {
    @Test
    fun defaultCodecRoundTripsAndRecordsTheCodecActuallyUsed() =
        runTest {
            val (frameCodec, refCodec) = writeOneFrame(CompressionCodec.ZSTD)
            val want = if (ZstdCompression.isAvailable) DeltaPageCodec.CODEC_ZSTD else DeltaPageCodec.CODEC_NONE
            assertEquals(want, frameCodec, "a page must record the codec its body was actually written with")
            val wantRef = if (ZstdCompression.isAvailable) CompressionCodec.ZSTD else CompressionCodec.NONE
            assertEquals(wantRef, refCodec, "the sealed segment ref must report the codec actually used")
        }

    @Test
    fun noneCodecRoundTripsOnEveryTarget() =
        runTest {
            assertEquals(DeltaPageCodec.CODEC_NONE, writeOneFrame(CompressionCodec.NONE).first)
        }
}

/**
 * Appends one commit through a [DefaultDeltaSegmentWriter] configured with [codec], seals it,
 * reads it back through [DefaultDeltaSegmentReader], and returns the codec byte of the first
 * page frame together with the codec the sealed ref reports.
 */
internal suspend fun writeOneFrame(codec: CompressionCodec): Pair<Byte, CompressionCodec> {
    val shim = InMemoryPlatformIoShim()
    val factory =
        DeltaSegmentFactory(StorageEngineConfig(globalMemoryBudgetBytes = 2_000_000, ioShim = shim, compressionCodec = codec))
    val writer = factory.openWriter("ns1")
    val commit =
        KdbCommit.build(
            parentHashes = emptyList(),
            namespaceId = "ns1",
            transactionId = KdbUuid.random(),
            timestamp = KdbTimestamp.now(),
            authorNodeId = KdbUuid.random(),
            operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":"compressible compressible compressible"}""")),
            documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
            schemaHash = null,
        )
    writer.append(
        DeltaRecord(
            commitHash = commit.hash,
            namespaceId = "ns1",
            authorship = DeltaAuthorshipEnvelope(principal = "p", timestamp = commit.timestamp, rightsToken = "", clientContext = ""),
            commitPayload = commit.toPayloadBytes(),
            documentPatches = emptyList(),
        ),
    )
    val ref = writer.seal()
    val records = factory.openReader("ns1").readAll(ref)
    assertEquals(listOf(commit.hash), records.map { it.commitHash }, "the segment must read back on the target that wrote it")
    val header = shim.readFromSegment(SegmentNameBuilder.deltaSequenced("ns1", ref.sequenceNumber), 0, DeltaPageCodec.FRAME_HEADER_SIZE)
    return header[5] to ref.compressionCodec
}

/**
 * Shared by jsTest and nativeTest: on a target with no real zstd, the default codec (ZSTD) must
 * degrade to CODEC_NONE pages, which Go and the JVM read.
 */
internal suspend fun assertTargetWithoutZstdWritesNonePages() {
    assertEquals(false, ZstdCompression.isAvailable, "this target has no zstd codec linked (roadmap R1)")
    assertEquals(
        DeltaPageCodec.CODEC_NONE,
        // The configured default, not a literal: this is the codec every embedded store gets.
        writeOneFrame(StorageEngineConfig(ioShim = InMemoryPlatformIoShim()).compressionCodec).first,
        "a target without zstd must not tag pages ZSTD, even with the default ZSTD codec configured",
    )
}
