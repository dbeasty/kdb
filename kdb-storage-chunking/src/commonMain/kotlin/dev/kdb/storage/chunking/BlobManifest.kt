package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash

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
                bytes.copyInto(out, destinationOffset = 1)
                out
            }
            is Chunked -> {
                val out = ByteArray(5 + chunkHashes.size * HASH_LEN)
                out[0] = TAG_CHUNKED
                writeInt32BE(out, 1, chunkHashes.size)
                var offset = 5
                for (h in chunkHashes) {
                    h.bytes.copyInto(out, destinationOffset = offset)
                    offset += HASH_LEN
                }
                out
            }
        }

    companion object {
        fun decode(manifestBytes: ByteArray): BlobManifest =
            when (manifestBytes[0]) {
                TAG_RAW -> Raw(manifestBytes.copyOfRange(1, manifestBytes.size))
                TAG_CHUNKED -> {
                    val count = readInt32BE(manifestBytes, 1)
                    val hashes = ArrayList<KdbHash>(count)
                    var offset = 5
                    repeat(count) {
                        hashes += KdbHash.fromBytes(manifestBytes.copyOfRange(offset, offset + HASH_LEN))
                        offset += HASH_LEN
                    }
                    Chunked(hashes)
                }
                else -> error("unknown blob manifest tag ${manifestBytes[0]}")
            }

        private fun writeInt32BE(
            out: ByteArray,
            at: Int,
            value: Int,
        ) {
            out[at] = (value ushr 24).toByte()
            out[at + 1] = (value ushr 16).toByte()
            out[at + 2] = (value ushr 8).toByte()
            out[at + 3] = value.toByte()
        }

        private fun readInt32BE(
            data: ByteArray,
            at: Int,
        ): Int =
            ((data[at].toInt() and 0xFF) shl 24) or
                ((data[at + 1].toInt() and 0xFF) shl 16) or
                ((data[at + 2].toInt() and 0xFF) shl 8) or
                (data[at + 3].toInt() and 0xFF)
    }
}
