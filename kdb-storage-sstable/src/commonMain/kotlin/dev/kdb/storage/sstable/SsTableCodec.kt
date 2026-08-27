package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.compression.ZstdCompression
import dev.kdb.document.kdbSha256
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder

internal object SsTableCodec {
    const val FOOTER_MAGIC: Int = 0x4B444253

    // Fixed-size trailer buildFooter appends after fileHash: a duplicate copy of indexLen at a
    // fixed offset from the true end of the file. See buildFooter's doc comment for why this
    // exists at all.
    const val FOOTER_TRAILER_SIZE: Int = 4

    fun encodeBlock(payload: ByteArray, compress: Boolean): ByteArray {
        val body = if (compress) ZstdCompression.compress(payload) else payload
        val out = ByteArray(12 + body.size)
        writeInt(out, 0, body.size)
        writeInt(out, 4, payload.size)
        writeInt(out, 8, Crc32.of(body))
        body.copyInto(out, 12)
        return out
    }

    // decodeBlock verifies body against the CRC32 encodeBlock always wrote at offset 8 before
    // decoding it - previously ignored entirely (kdb-finish-up-plan.md's 1-K1), so a corrupted or
    // truncated block was silently decompressed (or returned as-is) instead of failing loudly.
    fun decodeBlock(block: ByteArray): ByteArray {
        require(block.size >= 12) { "sstable block shorter than its 12-byte header" }
        val compSize = readInt(block, 0)
        val uncompSize = readInt(block, 4)
        val wantCrc = readInt(block, 8)
        val body = block.copyOfRange(12, block.size)
        val gotCrc = Crc32.of(body)
        check(gotCrc == wantCrc) {
            "sstable block CRC mismatch: block is corrupt (want ${wantCrc.toUInt().toString(16)}, " +
                "got ${gotCrc.toUInt().toString(16)})"
        }
        return if (compSize == uncompSize) body else ZstdCompression.decompress(body, uncompSize + 1024)
    }

    // Layout: magic(4) indexLen(4) indexBytes(indexLen) fileHash(32), then a fixed
    // FOOTER_TRAILER_SIZE-byte trailer duplicating indexLen at the very end of the footer (and,
    // since the footer is always the last thing written to a segment, at the very end of the
    // file). That trailer is what makes the footer locatable at all: parseFooter/
    // DefaultSsTableReader.get need indexLen to know where the footer *starts*, relative to the
    // end of the file, but indexLen itself was previously only ever written *inside* the footer
    // at a variable offset that depends on knowing where the footer starts - an unsolvable
    // bootstrap the old format never actually provided a way out of. get() could never locate a
    // real footer (mirrors go/kdb/storage/sstable/codec.go's identical bug, verified there by a
    // failing round trip on ordinary write-then-read) - this package had no round-trip test
    // before this fix either. The two languages' formats must stay byte-for-byte identical; see
    // go/testdata/golden/codec's regenerated fixtures.
    fun buildFooter(index: Map<KdbHash, BlockHandle>, fileHash: KdbHash): ByteArray {
        val entries = index.entries.joinToString("\n") { "${it.key.toHex()}:${it.value.offset}:${it.value.compressedSize}" }
        val indexBytes = entries.encodeToByteArray()
        val footer = ByteArray(8 + indexBytes.size + 32 + FOOTER_TRAILER_SIZE)
        writeInt(footer, 0, FOOTER_MAGIC)
        writeInt(footer, 4, indexBytes.size)
        indexBytes.copyInto(footer, 8)
        fileHash.bytes.copyInto(footer, 8 + indexBytes.size)
        writeInt(footer, footer.size - FOOTER_TRAILER_SIZE, indexBytes.size)
        return footer
    }

    fun parseFooter(footer: ByteArray): Map<KdbHash, BlockHandle> {
        if (footer.size < 40) return emptyMap()
        val indexLen = readInt(footer, 4)
        val indexStr = footer.copyOfRange(8, 8 + indexLen).decodeToString()
        if (indexStr.isEmpty()) return emptyMap()
        return indexStr.lineSequence().associate { line ->
            val parts = line.split(':')
            val hash = KdbHash.fromHex(parts[0])
            val off = parts[1].toLong()
            val cs = parts[2].toInt()
            hash to BlockHandle(off, cs, 0)
        }
    }

    private fun writeInt(arr: ByteArray, off: Int, v: Int) {
        arr[off] = (v ushr 24).toByte()
        arr[off + 1] = (v ushr 16).toByte()
        arr[off + 2] = (v ushr 8).toByte()
        arr[off + 3] = v.toByte()
    }

    private fun readInt(b: ByteArray, off: Int): Int =
        ((b[off].toInt() and 0xFF) shl 24) or
            ((b[off + 1].toInt() and 0xFF) shl 16) or
            ((b[off + 2].toInt() and 0xFF) shl 8) or
            (b[off + 3].toInt() and 0xFF)
}

public class DefaultSsTableWriter(
    private val ioShim: PlatformIoShim,
    private val namespaceId: String,
    private val level: Int,
) : SsTableWriter {
    private val entries = linkedMapOf<KdbHash, ByteArray>()

    override suspend fun put(key: KdbHash, value: ByteArray) {
        entries[key] = value
    }

    override suspend fun finish(): SsTableHandle {
        val blocks = linkedMapOf<KdbHash, BlockHandle>()
        val fileId = KdbUuid.random().toString()
        val segmentName = SegmentNameBuilder.sstable(namespaceId, level, fileId)
        var offset = 0L
        for ((k, v) in entries) {
            val block = SsTableCodec.encodeBlock(v, compress = true)
            ioShim.appendToSegment(segmentName, block)
            // compressedSize is the compressed body's own length - excluding encodeBlock's
            // 12-byte header (compSize/uncompSize/crc) - matching what get() expects when it
            // later reads bh.compressedSize+12 bytes starting at offset. This used to store
            // block.size (the full 12+body length) instead, over-reading 12 bytes into whatever
            // followed - the next block, or the footer for the last one - on every get().
            blocks[k] = BlockHandle(offset, block.size - 12, v.size)
            offset += block.size
        }
        val concat =
            buildList {
                for ((k, v) in entries) {
                    addAll(k.bytes.toList())
                    addAll(v.toList())
                }
            }.toByteArray()
        val fileHash = KdbHash.fromBytes(kdbSha256(concat))
        val footer = SsTableCodec.buildFooter(blocks, fileHash)
        ioShim.appendToSegment(segmentName, footer)
        ioShim.sealSegment(segmentName)
        return SsTableHandle(fileHash, level, segmentName)
    }
}

public class DefaultSsTableReader(
    private val ioShim: PlatformIoShim,
    private val handle: SsTableHandle,
) : SsTableReader {
    override suspend fun get(key: KdbHash): ByteArray? {
        val size = segmentSize()
        if (size < 40 + SsTableCodec.FOOTER_TRAILER_SIZE) return null
        val indexLen = readFooterIndexLen(size)
        // The footer body (everything buildFooter writes except its own trailing indexLen copy)
        // is magic(4) + indexLen(4) + indexBytes(indexLen) + fileHash(32) = 40+indexLen bytes,
        // sitting immediately before the FOOTER_TRAILER_SIZE-byte trailer at the true end of the
        // file.
        val bodyLen = 40 + indexLen
        val footerStart = size - bodyLen - SsTableCodec.FOOTER_TRAILER_SIZE
        val footer = ioShim.readFromSegment(handle.segmentName, footerStart, bodyLen)
        val index = SsTableCodec.parseFooter(footer)
        val bh = index[key] ?: return null
        val block = ioShim.readFromSegment(handle.segmentName, bh.offset, bh.compressedSize + 12)
        return SsTableCodec.decodeBlock(block)
    }

    private suspend fun segmentSize(): Long {
        var total = 0L
        val chunk = 8192
        while (true) {
            val p = ioShim.readFromSegment(handle.segmentName, total, chunk)
            if (p.isEmpty()) return total
            total += p.size
            if (p.size < chunk) return total
        }
    }

    // Reads buildFooter's trailing indexLen copy - the last FOOTER_TRAILER_SIZE bytes of the
    // file. Previously read the last 8 bytes and took bytes [4:8] of that as indexLen, which
    // (with no trailer in the old format) was actually the tail 4 bytes of the 32-byte fileHash -
    // never a real length, and the reason get() could never locate a real footer at all.
    private suspend fun readFooterIndexLen(size: Long): Int {
        val tail = ioShim.readFromSegment(handle.segmentName, size - SsTableCodec.FOOTER_TRAILER_SIZE, SsTableCodec.FOOTER_TRAILER_SIZE)
        return ((tail[0].toInt() and 0xFF) shl 24) or ((tail[1].toInt() and 0xFF) shl 16) or
            ((tail[2].toInt() and 0xFF) shl 8) or (tail[3].toInt() and 0xFF)
    }
}
