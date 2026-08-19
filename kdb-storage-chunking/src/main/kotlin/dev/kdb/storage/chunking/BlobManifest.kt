package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

private const val TAG_RAW: Byte = 0
private const val TAG_CHUNKED: Byte = 1
private const val HASH_LEN = 32

/** Binary manifest format: `[tag:1][...]`. RAW carries the literal bytes; CHUNKED carries an ordered chunk-hash list. */
internal sealed class BlobManifest {
    data class Raw(val bytes: ByteArray) : BlobManifest()

    data class Chunked(val chunkHashes: List<KdbHash>) : BlobManifest()

    fun encode(): ByteArray =
        when (this) {
            is Raw -> {
                val out = ByteArray(bytes.size + 1)
                out[0] = TAG_RAW
                System.arraycopy(bytes, 0, out, 1, bytes.size)
                out
            }
            is Chunked -> {
                val out = ByteArrayOutputStream(5 + chunkHashes.size * HASH_LEN)
                out.write(byteArrayOf(TAG_CHUNKED))
                out.write(ByteBuffer.allocate(4).putInt(chunkHashes.size).array())
                for (h in chunkHashes) out.write(h.bytes)
                out.toByteArray()
            }
        }

    companion object {
        fun decode(manifestBytes: ByteArray): BlobManifest =
            when (manifestBytes[0]) {
                TAG_RAW -> Raw(manifestBytes.copyOfRange(1, manifestBytes.size))
                TAG_CHUNKED -> {
                    val count = ByteBuffer.wrap(manifestBytes, 1, 4).int
                    val hashes = mutableListOf<KdbHash>()
                    var offset = 5
                    repeat(count) {
                        hashes += KdbHash.fromBytes(manifestBytes.copyOfRange(offset, offset + HASH_LEN))
                        offset += HASH_LEN
                    }
                    Chunked(hashes)
                }
                else -> error("unknown blob manifest tag ${manifestBytes[0]}")
            }
    }
}
