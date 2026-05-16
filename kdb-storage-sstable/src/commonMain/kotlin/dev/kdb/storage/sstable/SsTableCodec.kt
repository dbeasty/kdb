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

    fun encodeBlock(payload: ByteArray, compress: Boolean): ByteArray {
        val body = if (compress) ZstdCompression.compress(payload) else payload
        val out = ByteArray(12 + body.size)
        writeInt(out, 0, body.size)
        writeInt(out, 4, payload.size)
        writeInt(out, 8, Crc32.of(body))
        body.copyInto(out, 12)
        return out
    }

    fun decodeBlock(block: ByteArray): ByteArray {
        val compSize = readInt(block, 0)
        val uncompSize = readInt(block, 4)
        val body = block.copyOfRange(12, block.size)
        return if (compSize == uncompSize) body else ZstdCompression.decompress(body, uncompSize + 1024)
    }

    fun buildFooter(index: Map<KdbHash, BlockHandle>, fileHash: KdbHash): ByteArray {
        val entries = index.entries.joinToString("\n") { "${it.key.toHex()}:${it.value.offset}:${it.value.compressedSize}" }
        val indexBytes = entries.encodeToByteArray()
        val footer = ByteArray(8 + indexBytes.size + 32)
        writeInt(footer, 0, FOOTER_MAGIC)
        writeInt(footer, 4, indexBytes.size)
        indexBytes.copyInto(footer, 8)
        fileHash.bytes.copyInto(footer, footer.size - 32)
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
            blocks[k] = BlockHandle(offset, block.size, v.size)
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
        if (size < 40) return null
        val footerLen = readFooterIndexLen(size)
        val footer = ioShim.readFromSegment(handle.segmentName, size - footerLen - 32, footerLen + 40)
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

    private suspend fun readFooterIndexLen(size: Long): Int {
        val tail = ioShim.readFromSegment(handle.segmentName, size - 8, 8)
        return ((tail[4].toInt() and 0xFF) shl 24) or ((tail[5].toInt() and 0xFF) shl 16) or
            ((tail[6].toInt() and 0xFF) shl 8) or (tail[7].toInt() and 0xFF)
    }
}
