package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/**
 * Incremental hashing engine behind DocumentTree. Byte-for-byte mirror of
 * go/kdb/document/document_tree_trie.go - see that file's doc comment for
 * the full rationale (Phase 3's finding that hashing, not map-copying,
 * was the O(n) bottleneck; the gap-fix note in
 * docs/benchmarks/phases-1-6-summary.md on why a persistent Merkle trie
 * fixes it). Any change here must be mirrored there and vice versa, or
 * Go and Kotlin will silently compute different tree hashes for the same
 * entries - see DocumentTreeTrieParityTest for the cross-language vectors
 * that guard against that.
 *
 * 16-ary trie over the UUID's 32 hex nibbles (128 bits): a leaf hashes
 * (uuid, contentHash), an internal node hashes its 16 children (a fixed
 * all-zero sentinel for an absent child), the tree hash is the root.
 * Canonical - same (docID, contentHash) set always hashes the same,
 * independent of insertion order - and persistent: inserting/deleting one
 * entry only touches the O(32) nodes on that entry's path, sharing every
 * other subtree with the previous version.
 */

private const val TRIE_DEPTH = 32 // 128-bit UUID, 4 bits (one hex nibble) per level
private val TRIE_ZERO = ByteArray(32) // all-zero sentinel for an absent child/subtree

internal class TrieNode(
    val hash: ByteArray,
    val children: Array<TrieNode?>?,
)

private fun uuidBytes(id: KdbUuid): ByteArray {
    val out = ByteArray(16)
    writeBeLong(id.msb, out, 0)
    writeBeLong(id.lsb, out, 8)
    return out
}

private fun writeBeLong(v: Long, out: ByteArray, offset: Int) {
    for (i in 0 until 8) {
        out[offset + i] = (v shr (56 - 8 * i)).toByte()
    }
}

private fun nibbleAt(uuidBytes: ByteArray, depth: Int): Int {
    val b = uuidBytes[depth / 2].toInt() and 0xFF
    return if (depth % 2 == 0) (b shr 4) else (b and 0x0f)
}

private fun leafHash(uuidBytes: ByteArray, contentHash: KdbHash): ByteArray {
    val buf = ByteArray(1 + 16 + 32)
    buf[0] = 0x00
    uuidBytes.copyInto(buf, 1)
    contentHash.bytes.copyInto(buf, 17)
    return kdbSha256(buf)
}

private fun internalHash(children: Array<TrieNode?>): ByteArray {
    val buf = ByteArray(1 + 16 * 32)
    buf[0] = 0x01
    for (i in 0 until 16) {
        val c = children[i]
        val src = c?.hash ?: TRIE_ZERO
        src.copyInto(buf, 1 + i * 32)
    }
    return kdbSha256(buf)
}

private fun nodeHash(n: TrieNode?): ByteArray = n?.hash ?: TRIE_ZERO

internal fun trieInsert(root: TrieNode?, id: KdbUuid, contentHash: KdbHash): TrieNode {
    val ub = uuidBytes(id)
    return trieInsertAt(root, ub, contentHash, 0)
}

private fun trieInsertAt(node: TrieNode?, uuidBytes: ByteArray, contentHash: KdbHash, depth: Int): TrieNode {
    if (depth == TRIE_DEPTH) {
        return TrieNode(hash = leafHash(uuidBytes, contentHash), children = null)
    }
    val children = arrayOfNulls<TrieNode>(16)
    node?.children?.copyInto(children)
    val nib = nibbleAt(uuidBytes, depth)
    children[nib] = trieInsertAt(children[nib], uuidBytes, contentHash, depth + 1)
    return TrieNode(hash = internalHash(children), children = children)
}

internal fun trieDelete(root: TrieNode?, id: KdbUuid): TrieNode? {
    return trieDeleteAt(root, uuidBytes(id), 0)
}

private fun trieDeleteAt(node: TrieNode?, uuidBytes: ByteArray, depth: Int): TrieNode? {
    if (node == null) return null
    if (depth == TRIE_DEPTH) return null
    val nodeChildren = node.children ?: return node
    val children = nodeChildren.copyOf()
    val nib = nibbleAt(uuidBytes, depth)
    children[nib] = trieDeleteAt(children[nib], uuidBytes, depth + 1)
    if (children.all { it == null }) return null
    return TrieNode(hash = internalHash(children), children = children)
}

/** Constructs a trie from scratch (O(n)); see trieBuild's Go counterpart. */
internal fun trieBuild(entries: Map<KdbUuid, KdbHash>): TrieNode? {
    var root: TrieNode? = null
    for ((id, h) in entries) {
        root = trieInsert(root, id, h)
    }
    return root
}

internal fun trieTreeHash(root: TrieNode?): KdbHash = KdbHash(nodeHash(root))
