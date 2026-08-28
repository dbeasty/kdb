package dev.kdb.integrity

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim

internal fun newTestShim(): PlatformIoShim = InMemoryPlatformIoShim()

/** Builds one valid, hash-consistent commit. parent is null for a genesis commit. */
internal fun buildCommit(ns: String, parent: KdbHash?): KdbCommit =
    KdbCommit.build(
        parentHashes = if (parent != null) listOf(parent) else emptyList(),
        namespaceId = ns,
        transactionId = KdbUuid.random(),
        timestamp = KdbTimestamp.now(),
        authorNodeId = KdbUuid.random(),
        operations = listOf(KdbOp.Write(KdbUuid.random(), "{}")),
        documentTreeHash = DocumentTree.EMPTY.treeHash,
        schemaHash = null,
        message = "test",
    )

/** Builds n commits, each the sole parent of the next. */
internal fun buildChain(n: Int, ns: String): List<KdbCommit> {
    val commits = mutableListOf<KdbCommit>()
    var parent: KdbHash? = null
    repeat(n) {
        val c = buildCommit(ns, parent)
        commits += c
        parent = c.hash
    }
    return commits
}

/**
 * Encodes one commit as an uncompressed KDBP frame, for tests that need
 * exact control over segment bytes. Reimplements the (internal, so not
 * reachable from this module) DeltaPageCodec.frame layout directly rather
 * than reaching into kdb-storage-delta's internals - same magic/length/
 * CRC framing DeltaSegmentScanner parses.
 */
internal fun rawFrame(c: KdbCommit): ByteArray {
    val payload = c.toPayloadBytes()
    val out = ByteArray(RAW_FRAME_HEADER_SIZE + payload.size)
    out[0] = 0x4B; out[1] = 0x44; out[2] = 0x42; out[3] = 0x50
    out[4] = 2 // PAGE_FORMAT_VERSION
    out[5] = 0 // CODEC_NONE
    out[6] = 0; out[7] = 0
    writeIntBe(out, 8, payload.size)
    writeIntBe(out, 12, payload.size)
    writeIntBe(out, 16, Crc32.of(payload))
    payload.copyInto(out, RAW_FRAME_HEADER_SIZE)
    return out
}

/** Mirrors DeltaPageCodec.FRAME_HEADER_SIZE, which is internal to kdb-storage-delta. */
internal const val RAW_FRAME_HEADER_SIZE: Int = 20

private fun writeIntBe(a: ByteArray, o: Int, v: Int) {
    a[o] = (v ushr 24).toByte(); a[o + 1] = (v ushr 16).toByte()
    a[o + 2] = (v ushr 8).toByte(); a[o + 3] = v.toByte()
}

/**
 * Returns a copy of a raw frame with one body byte flipped, producing a
 * CRC mismatch without changing the frame's declared length - i.e. a
 * frame that "fits" but fails the CRC check, distinct from a frame simply
 * cut short.
 */
internal fun flippedFrame(c: KdbCommit): ByteArray {
    val f = rawFrame(c).copyOf()
    // Must land past the header: flipping a header byte would change the frame's
    // declared length or codec instead of corrupting its body, which is a
    // different failure than the one this fixture exists to produce.
    val at = RAW_FRAME_HEADER_SIZE + 3
    check(f.size > at) { "frame too short to flip a body byte: ${f.size} bytes" }
    f[at] = (f[at].toInt() xor 0xFF).toByte()
    return f
}

internal suspend fun appendSegment(shim: PlatformIoShim, ns: String, seq: Long, vararg frames: ByteArray) {
    val name = SegmentNameBuilder.deltaSequenced(ns, seq)
    for (f in frames) shim.appendToSegment(name, f)
}
