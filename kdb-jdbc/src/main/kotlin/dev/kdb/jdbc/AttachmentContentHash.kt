package dev.kdb.jdbc

import java.security.MessageDigest

/**
 * An attachment's content identity, hash scheme 1 (docs/kdb-attachments-plan.md; Go's
 * document/attachment_hash.go), computed incrementally in O(log n) memory:
 *
 * ```
 * chunk i  = bytes [i*1MiB, min((i+1)*1MiB, size))
 * leaf i   = SHA-256(0x00 || chunk i)
 * tree     = RFC 6962 Merkle tree hash over the leaves; a zero-byte blob has one leaf, of the empty chunk
 * identity = SHA-256(0x02 || be64(size) || tree)
 * ```
 *
 * A small self-contained copy for the client, which verifies what it uploads and downloads with it;
 * the engine's own implementation belongs to kdb-document.
 */
public class AttachmentContentHash {
    private val digest = MessageDigest.getInstance("SHA-256")
    private val chunk = ByteArray(CHUNK_BYTES)
    private var fill = 0
    private var leaves = 0L
    private var finished: ByteArray? = null

    /** Subtree roots, left to right, each over 2^height leaves; heights strictly decrease. */
    private val stack = ArrayList<Pair<ByteArray, Int>>()

    /** The bytes hashed so far. */
    public var size: Long = 0
        private set

    public fun update(bytes: ByteArray, offset: Int = 0, length: Int = bytes.size - offset) {
        check(finished == null) { "hash already finished" }
        var off = offset
        var left = length
        while (left > 0) {
            val n = minOf(left, CHUNK_BYTES - fill)
            System.arraycopy(bytes, off, chunk, fill, n)
            fill += n
            off += n
            left -= n
            size += n
            if (fill == CHUNK_BYTES) emitLeaf()
        }
    }

    /** The content identity (32 bytes). Idempotent. */
    public fun finish(): ByteArray {
        finished?.let { return it.copyOf() }
        if (fill > 0 || leaves == 0L) emitLeaf()
        var acc = stack.removeAt(stack.lastIndex).first
        while (stack.isNotEmpty()) acc = node(stack.removeAt(stack.lastIndex).first, acc)
        digest.reset()
        digest.update(0x02.toByte())
        for (i in 7 downTo 0) digest.update((size ushr (8 * i)).toByte())
        digest.update(acc)
        val out = digest.digest()
        finished = out
        return out.copyOf()
    }

    /** [finish] as lowercase hex. */
    public fun finishHex(): String = finish().joinToString("") { "%02x".format(it.toInt() and 0xFF) }

    private fun emitLeaf() {
        digest.reset()
        digest.update(0x00.toByte())
        digest.update(chunk, 0, fill)
        var h = digest.digest()
        var height = 0
        while (stack.isNotEmpty() && stack.last().second == height) {
            h = node(stack.removeAt(stack.lastIndex).first, h)
            height++
        }
        stack.add(h to height)
        fill = 0
        leaves++
    }

    private fun node(left: ByteArray, right: ByteArray): ByteArray {
        digest.reset()
        digest.update(0x01.toByte())
        digest.update(left)
        digest.update(right)
        return digest.digest()
    }

    public companion object {
        /** The scheme byte an AttachmentPut carries for this hash. */
        public const val SCHEME: Int = 1

        /** Chunk size of scheme 1 (and of uploads). */
        public const val CHUNK_BYTES: Int = 1 shl 20

        /** The content identity of [bytes], as lowercase hex. */
        @JvmStatic
        public fun hex(bytes: ByteArray): String = AttachmentContentHash().apply { update(bytes) }.finishHex()
    }
}
