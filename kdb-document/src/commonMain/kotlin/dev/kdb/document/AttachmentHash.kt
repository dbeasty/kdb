package dev.kdb.document

import dev.kdb.codec.KdbHash

/**
 * Attachment content hashes (docs/kdb-attachments-plan.md, "Content identity"); a byte-for-byte
 * mirror of go/kdb/document/attachment_hash.go, which every implementation must reproduce exactly.
 *
 * Scheme 1 ([ATTACHMENT_HASH_SCHEME_CHUNKED_1]):
 * ```
 * chunk i   = bytes [i*ATTACHMENT_CHUNK_BYTES, min((i+1)*ATTACHMENT_CHUNK_BYTES, size))
 * leaf i    = SHA-256(0x00 || chunk i)
 * tree      = the RFC 6962 Merkle tree hash over the leaves (n > 1: SHA-256(0x01 || MTH(first k) ||
 *             MTH(the rest)), k the largest power of two below n); a zero-byte blob has one leaf, of
 *             the empty chunk
 * identity  = SHA-256(0x02 || be64(size) || tree)
 * ```
 */
public const val ATTACHMENT_HASH_SCHEME_CHUNKED_1: Int = 1

/** Scheme 1's chunk size. Part of the scheme: changing it is a new scheme. */
public const val ATTACHMENT_CHUNK_BYTES: Int = 1 shl 20

/** The leaf hash of one chunk: SHA-256(0x00 || chunk[offset, offset+length)). */
public fun chunkLeafHash(
    chunk: ByteArray,
    offset: Int = 0,
    length: Int = chunk.size - offset,
): KdbHash {
    val buf = ByteArray(1 + length)
    chunk.copyInto(buf, 1, offset, offset + length)
    return KdbHash(kdbSha256(buf))
}

internal fun merkleNode(left: KdbHash, right: KdbHash): KdbHash {
    val b = ByteArray(65)
    b[0] = 0x01
    left.bytes.copyInto(b, 1)
    right.bytes.copyInto(b, 33)
    return KdbHash(kdbSha256(b))
}

/** Folds a tree hash and the attachment's size into its content hash. */
public fun attachmentIdentity(
    tree: KdbHash,
    size: Long,
): KdbHash {
    val b = ByteArray(41)
    b[0] = 0x02
    for (i in 0 until 8) b[1 + i] = (size ushr (56 - 8 * i)).toByte()
    tree.bytes.copyInto(b, 9)
    return KdbHash(kdbSha256(b))
}

/**
 * Computes the RFC 6962 tree hash of leaves added in order, holding O(log n) hashes: the roots of
 * the perfect subtrees the leaves so far decompose into. Streams of any size hash in constant memory.
 */
public class MerkleBuilder {
    private val stack = ArrayList<KdbHash>() // perfect subtree roots, largest (leftmost) first
    private val sizes = ArrayList<Long>() // their leaf counts, powers of two, strictly decreasing

    /** How many leaves were added. */
    public var leaves: Long = 0
        private set

    /** Appends the next leaf. */
    public fun add(leaf: KdbHash) {
        stack.add(leaf)
        sizes.add(1)
        leaves++
        // Two equal perfect subtrees side by side are one twice as large.
        while (sizes.size >= 2 && sizes[sizes.size - 1] == sizes[sizes.size - 2]) {
            val r = stack.removeAt(stack.size - 1)
            val l = stack.removeAt(stack.size - 1)
            stack.add(merkleNode(l, r))
            val s = sizes.removeAt(sizes.size - 1) * 2
            sizes.removeAt(sizes.size - 1)
            sizes.add(s)
        }
    }

    /** The tree hash of the leaves added (the empty chunk's leaf when none were). */
    public fun root(): KdbHash {
        if (stack.isEmpty()) return chunkLeafHash(ByteArray(0))
        var h = stack[stack.size - 1]
        for (i in stack.size - 2 downTo 0) h = merkleNode(stack[i], h)
        return h
    }
}

/**
 * Streaming scheme-1 hasher: feed bytes in any pieces with [update], then [finish]. Holds at most one
 * chunk plus O(log n) hashes, and exposes each chunk's leaf as it completes ([leafHashes]) so a
 * store can keep them for per-chunk verification.
 */
public class AttachmentHasher {
    private val buf = ByteArray(ATTACHMENT_CHUNK_BYTES)
    private var fill = 0
    private val merkle = MerkleBuilder()
    private val leafList = ArrayList<KdbHash>()

    /** Bytes fed so far. */
    public var size: Long = 0
        private set

    /** The leaves of the chunks completed so far, in order. */
    public val leafHashes: List<KdbHash> get() = leafList

    public fun update(
        data: ByteArray,
        offset: Int = 0,
        length: Int = data.size - offset,
    ) {
        var off = offset
        var left = length
        while (left > 0) {
            val n = minOf(left, ATTACHMENT_CHUNK_BYTES - fill)
            data.copyInto(buf, fill, off, off + n)
            fill += n
            off += n
            left -= n
            size += n
            if (fill == ATTACHMENT_CHUNK_BYTES) flushChunk()
        }
    }

    private fun flushChunk() {
        val leaf = chunkLeafHash(buf, 0, fill)
        merkle.add(leaf)
        leafList.add(leaf)
        fill = 0
    }

    /** The content hash of everything fed. A partial last chunk (or the empty blob's one leaf) is
     * hashed here. */
    public fun finish(): KdbHash {
        if (fill > 0 || merkle.leaves == 0L) flushChunk()
        return attachmentIdentity(merkle.root(), size)
    }
}

/** The scheme-1 content hash of data held in memory - for tests and small blobs. */
public fun attachmentContentHash(data: ByteArray): KdbHash {
    val m = MerkleBuilder()
    var off = 0
    while (true) {
        val end = minOf(off + ATTACHMENT_CHUNK_BYTES, data.size)
        m.add(chunkLeafHash(data, off, end - off))
        if (end == data.size) break
        off = end
    }
    return attachmentIdentity(m.root(), data.size.toLong())
}

private fun largestPowerOfTwoBelow(n: Int): Int {
    var k = 1
    while (k * 2 < n) k *= 2
    return k
}

/** The RFC 6962 tree hash of [leaves], for a caller holding them all. */
public fun merkleTreeHash(leaves: List<KdbHash>): KdbHash =
    when (leaves.size) {
        0 -> chunkLeafHash(ByteArray(0))
        1 -> leaves[0]
        else -> {
            val k = largestPowerOfTwoBelow(leaves.size)
            merkleNode(merkleTreeHash(leaves.subList(0, k)), merkleTreeHash(leaves.subList(k, leaves.size)))
        }
    }

/** The RFC 6962 audit path for leaf [index] among [leaves]: sibling hashes, bottom up. */
public fun merkleInclusionProof(
    leaves: List<KdbHash>,
    index: Int,
): List<KdbHash> {
    if (leaves.size <= 1) return emptyList()
    val k = largestPowerOfTwoBelow(leaves.size)
    return if (index < k) {
        merkleInclusionProof(leaves.subList(0, k), index) + merkleTreeHash(leaves.subList(k, leaves.size))
    } else {
        merkleInclusionProof(leaves.subList(k, leaves.size), index - k) + merkleTreeHash(leaves.subList(0, k))
    }
}

/** Checks that [leaf] is leaf [index] of a tree of [n] leaves whose hash is [tree], given its audit
 * path (RFC 6962 §2.1.1). */
public fun verifyMerkleInclusion(
    leaf: KdbHash,
    index: Long,
    n: Long,
    proof: List<KdbHash>,
    tree: KdbHash,
): Boolean {
    if (index < 0 || index >= n) return false
    var fn = index
    var sn = n - 1
    var r = leaf
    for (p in proof) {
        if (sn == 0L) return false
        if ((fn and 1L) == 1L || fn == sn) {
            r = merkleNode(p, r)
            while ((fn and 1L) == 0L && fn != 0L) {
                fn = fn shr 1
                sn = sn shr 1
            }
        } else {
            r = merkleNode(r, p)
        }
        fn = fn shr 1
        sn = sn shr 1
    }
    return sn == 0L && r == tree
}

/**
 * Every level of an RFC 6962 tree, so that each audit path costs O(log n) rather than the O(n) of
 * [merkleInclusionProof] (Go's document.MerkleTree). Built bottom up, a level's unpaired last node
 * is carried up unchanged, which gives the same tree as RFC 6962's split at the largest power of two
 * below n. No leaves: the tree of one empty chunk.
 */
public class MerkleTree(
    leaves: List<KdbHash>,
) {
    private val levels: List<List<KdbHash>> // levels[0] the leaves, the last level the root alone

    init {
        val all = ArrayList<List<KdbHash>>()
        var level: List<KdbHash> = if (leaves.isEmpty()) listOf(chunkLeafHash(ByteArray(0))) else leaves.toList()
        all.add(level)
        while (level.size > 1) {
            val next = ArrayList<KdbHash>((level.size + 1) / 2)
            for (i in 0 until (level.size + 1) / 2) {
                next.add(if (2 * i + 1 < level.size) merkleNode(level[2 * i], level[2 * i + 1]) else level[2 * i])
            }
            all.add(next)
            level = next
        }
        levels = all
    }

    /** The tree's leaf count. */
    public val leaves: Int get() = levels[0].size

    /** The tree hash. */
    public fun root(): KdbHash = levels[levels.size - 1][0]

    /** Leaf [index]'s audit path, bottom up, as [merkleInclusionProof] gives it. */
    public fun proof(index: Int): List<KdbHash> {
        val path = ArrayList<KdbHash>()
        var i = index
        for (l in 0 until levels.size - 1) {
            val level = levels[l]
            val sib = i xor 1
            if (sib < level.size) path.add(level[sib])
            i = i shr 1
        }
        return path
    }
}

/** How many scheme-1 chunks, so leaves, an attachment of [size] bytes has: a zero-byte attachment
 * has one, of the empty chunk. */
public fun attachmentChunkCount(size: Long): Long =
    if (size <= 0) 1 else (size + ATTACHMENT_CHUNK_BYTES - 1) / ATTACHMENT_CHUNK_BYTES
