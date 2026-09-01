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

    // SSTable block header, v2 - must stay byte-identical to Go's
    // go/kdb/storage/sstable/codec.go:
    //
    //   0      version   u8  (= BLOCK_FORMAT_VERSION)
    //   1      codec     u8  (BLOCK_CODEC_NONE | BLOCK_CODEC_ZSTD)
    //   2..3   reserved  u16 (zero)
    //   4..7   compressed length   u32 (big-endian, body only)
    //   8..11  uncompressed length u32 (big-endian)
    //  12..15  crc32 of body       u32 (big-endian)
    //
    // v1 was 12 bytes with no codec, and decodeBlock inferred "was this compressed?" from
    // compSize == uncompSize - wrong for any payload whose compressed form happens to be exactly
    // its original size, and no way to change the configured codec without orphaning old files.
    const val BLOCK_HEADER_SIZE: Int = 16
    const val BLOCK_FORMAT_VERSION: Byte = 2
    const val BLOCK_CODEC_NONE: Byte = 0
    const val BLOCK_CODEC_ZSTD: Byte = 1

    fun encodeBlock(payload: ByteArray, compress: Boolean): ByteArray {
        val id = if (compress) BLOCK_CODEC_ZSTD else BLOCK_CODEC_NONE
        val body = if (compress) ZstdCompression.compress(payload) else payload
        val out = ByteArray(BLOCK_HEADER_SIZE + body.size)
        out[0] = BLOCK_FORMAT_VERSION
        out[1] = id
        out[2] = 0; out[3] = 0
        writeInt(out, 4, body.size)
        writeInt(out, 8, payload.size)
        writeInt(out, 12, Crc32.of(body))
        body.copyInto(out, BLOCK_HEADER_SIZE)
        return out
    }

    // decodeBlock verifies body against the CRC32 encodeBlock always wrote before decoding it -
    // previously ignored entirely (kdb-finish-up-plan.md's 1-K1), so a corrupted or truncated
    // block was silently decompressed (or returned as-is) instead of failing loudly.
    fun decodeBlock(block: ByteArray): ByteArray {
        require(block.size >= BLOCK_HEADER_SIZE) {
            "sstable block shorter than its $BLOCK_HEADER_SIZE-byte header"
        }
        val version = block[0]
        require(version == BLOCK_FORMAT_VERSION) {
            "unsupported sstable block version $version (this build writes and reads v$BLOCK_FORMAT_VERSION)"
        }
        val uncompSize = readInt(block, 8)
        val wantCrc = readInt(block, 12)
        val body = block.copyOfRange(BLOCK_HEADER_SIZE, block.size)
        val gotCrc = Crc32.of(body)
        check(gotCrc == wantCrc) {
            "sstable block CRC mismatch: block is corrupt (want ${wantCrc.toUInt().toString(16)}, " +
                "got ${gotCrc.toUInt().toString(16)})"
        }
        return when (block[1]) {
            BLOCK_CODEC_NONE -> body
            BLOCK_CODEC_ZSTD -> ZstdCompression.decompress(body, uncompSize)
            else -> throw IllegalArgumentException("unknown sstable block codec id ${block[1]}")
        }
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
    // The optional fourth field of a footer index line: "<hex>:<offset>:<size>:1" means the key
    // was deleted in this table and no block was written for it. Written only for tombstones, so a
    // table containing none is byte-for-byte what the three-field format produced - existing
    // segments and go/testdata/golden/codec's fixtures are unaffected, and Go's identical
    // tombstoneFlag keeps the two languages' formats the same.
    //
    // A reader predating this field takes the first three parts and ignores the rest, so it sees a
    // tombstone as a zero-length block, fails its CRC check, and skips the table - falling through
    // to older tables exactly as it does today. Wrong, but no more wrong than the behavior this
    // replaces, and never silently wrong in a new way.
    const val TOMBSTONE_FLAG: String = "1"

    fun buildFooter(index: Map<KdbHash, BlockHandle>, fileHash: KdbHash): ByteArray {
        val entries =
            index.entries.joinToString("\n") {
                val line = "${it.key.toHex()}:${it.value.offset}:${it.value.compressedSize}"
                if (it.value.deleted) "$line:$TOMBSTONE_FLAG" else line
            }
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
            val deleted = parts.size > 3 && parts[3] == TOMBSTONE_FLAG
            hash to BlockHandle(off, cs, 0, deleted)
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
    // A null value is a tombstone - deliberately distinguishable from "no entry at all", which is
    // what a missing key in this map means.
    private val entries = linkedMapOf<KdbHash, ByteArray?>()

    override suspend fun put(key: KdbHash, value: ByteArray) {
        entries[key] = value
    }

    /**
     * Records a tombstone for [key]. No block is written - the footer index entry alone carries
     * the fact - so a table of nothing but deletes costs one index line per key.
     */
    override suspend fun delete(key: KdbHash) {
        entries[key] = null
    }

    override suspend fun finish(): SsTableHandle {
        val blocks = linkedMapOf<KdbHash, BlockHandle>()
        val fileId = KdbUuid.random().toString()
        val segmentName = SegmentNameBuilder.sstable(namespaceId, level, fileId)
        var offset = 0L
        for ((k, v) in entries) {
            if (v == null) {
                blocks[k] = BlockHandle(0L, 0, 0, deleted = true)
                continue
            }
            val block = SsTableCodec.encodeBlock(v, compress = true)
            ioShim.appendToSegment(segmentName, block)
            // compressedSize is the compressed body's own length - excluding encodeBlock's
            // header - matching what get() expects when it later reads
            // bh.compressedSize+BLOCK_HEADER_SIZE bytes starting at offset. This used to store
            // block.size (the full header+body length) instead, over-reading a header's worth of
            // bytes into whatever followed - the next block, or the footer for the last one.
            blocks[k] = BlockHandle(offset, block.size - SsTableCodec.BLOCK_HEADER_SIZE, v.size)
            offset += block.size
        }
        val concat =
            buildList {
                for ((k, v) in entries) {
                    addAll(k.bytes.toList())
                    if (v == null) {
                        // A marker byte, so "deleted K" and "wrote K with an empty value" hash
                        // differently. Safe to introduce: no segment written before tombstones
                        // existed contains one. Matches Go's DefaultWriter.Finish.
                        add(0xFF.toByte())
                    } else {
                        addAll(v.toList())
                    }
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
    /**
     * Returns the value stored for [key], or `null` if this table has never seen it *or* holds a
     * tombstone for it. Callers that would otherwise fall through to an older table must use
     * [lookup].
     */
    override suspend fun get(key: KdbHash): ByteArray? = lookup(key)?.takeUnless { it.deleted }?.value

    override suspend fun lookup(key: KdbHash): SsTableEntry? {
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
        if (bh.deleted) return SsTableEntry(value = null, deleted = true)
        val block = ioShim.readFromSegment(handle.segmentName, bh.offset, bh.compressedSize + SsTableCodec.BLOCK_HEADER_SIZE)
        return SsTableEntry(SsTableCodec.decodeBlock(block), deleted = false)
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
