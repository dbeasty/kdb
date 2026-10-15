package dev.kdb.document

import dev.kdb.document.internal.sha256Digest
import dev.kdb.document.internal.sha256OnlyChild

/** Multiplatform SHA-256 digest used by schema hashing and DAG verification ([Layer 1]). */
public fun kdbSha256(message: ByteArray): ByteArray = sha256Digest(message)

/**
 * The document trie's hash of a subtree at [depth] (0..32) holding only the entry [id] (16 bytes,
 * big-endian UUID) with content hash [content]: its leaf hash folded up through one single-child
 * internal node per level - the hash a compressed leaf stands for, identical to Go's foldLeaf.
 * Lets other modules' tries (kdb-replica) share the platform's fast spine hash.
 */
public fun kdbTrieFoldLeaf(
    id: ByteArray,
    content: ByteArray,
    depth: Int,
): ByteArray {
    val leaf = ByteArray(1 + 16 + 32)
    id.copyInto(leaf, 1)
    content.copyInto(leaf, 17)
    var h = sha256Digest(leaf)
    for (d in 31 downTo depth) {
        val b = id[d shr 1].toInt() and 0xff
        h = sha256OnlyChild(if (d and 1 == 1) b and 0x0f else b shr 4, h)
    }
    return h
}
