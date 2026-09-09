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

/**
 * A node is either a leaf or an internal node (or neither - callers use null
 * for an empty subtree rather than allocating).
 *
 * A leaf stands for the whole subtree beneath its position when that subtree
 * holds exactly one entry, at whatever depth that becomes true, rather than
 * only at TRIE_DEPTH. Without it every entry owned a private spine of 32
 * internal nodes, each carrying a 16-element array: measured on the Go side at
 * ~4,900 bytes of live heap per document regardless of the document's size.
 * See docs/benchmarks/2026-09-08-document-trie-compression.md.
 *
 * The hash is unaffected, deliberately. A subtree holding one entry has a
 * determined hash - fold leafHash up through the single-child internal nodes it
 * would have had - so a compressed leaf carries exactly the hash its 32-node
 * spine would have produced. Every tree hash is therefore unchanged, which is
 * what keeps this an internal representation change on both sides rather than a
 * format break, and what lets DocumentTreeTrieParityTest's literal vectors go
 * on passing untouched.
 */
internal class TrieNode(
    val hash: ByteArray,
    val leaf: TrieLeaf? = null,
    val children: Array<TrieNode?>? = null,
)

internal class TrieLeaf(val uuid: KdbUuid, val hash: KdbHash)

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

/**
 * internalHash for the one case compression needs: an internal node with a
 * single occupied slot. The buffer starts zeroed and TRIE_ZERO is all-zero, so
 * the absent siblings need no writing.
 */
private fun internalHashOfOnlyChild(nib: Int, childHash: ByteArray): ByteArray {
    val buf = ByteArray(1 + 16 * 32)
    buf[0] = 0x01
    childHash.copyInto(buf, 1 + nib * 32)
    return kdbSha256(buf)
}

/**
 * The hash of the subtree rooted at [depth] holding exactly the one entry -
 * the spine that used to be built out of real nodes, evaluated instead. At
 * depth 0 this is 32 rounds on top of the leaf, precisely what the
 * uncompressed trie computed for a single-entry tree.
 */
private fun foldLeaf(ub: ByteArray, contentHash: KdbHash, depth: Int): ByteArray {
    var h = leafHash(ub, contentHash)
    for (d in TRIE_DEPTH - 1 downTo depth) {
        h = internalHashOfOnlyChild(nibbleAt(ub, d), h)
    }
    return h
}

private fun newLeafNode(ub: ByteArray, id: KdbUuid, contentHash: KdbHash, depth: Int): TrieNode =
    TrieNode(hash = foldLeaf(ub, contentHash, depth), leaf = TrieLeaf(id, contentHash))

private fun nodeHash(n: TrieNode?): ByteArray = n?.hash ?: TRIE_ZERO

internal fun trieInsert(root: TrieNode?, id: KdbUuid, contentHash: KdbHash): TrieNode {
    return trieInsertAt(root, uuidBytes(id), id, contentHash, 0)
}

private fun trieInsertAt(
    node: TrieNode?,
    ub: ByteArray,
    id: KdbUuid,
    contentHash: KdbHash,
    depth: Int,
): TrieNode {
    // An empty subtree becomes one leaf covering everything below - the common
    // case, and the whole saving.
    if (node == null) return newLeafNode(ub, id, contentHash, depth)
    val existing = node.leaf
    if (existing != null) {
        if (existing.uuid == id) return newLeafNode(ub, id, contentHash, depth)
        return splitLeafAt(existing, ub, id, contentHash, depth)
    }
    if (depth == TRIE_DEPTH) return newLeafNode(ub, id, contentHash, depth)
    val children = arrayOfNulls<TrieNode>(16)
    node.children?.copyInto(children)
    val nib = nibbleAt(ub, depth)
    children[nib] = trieInsertAt(children[nib], ub, id, contentHash, depth + 1)
    return TrieNode(hash = internalHash(children), children = children)
}

/**
 * Makes room beneath a compressed leaf for a second entry. Only the levels the
 * two keys genuinely share become real nodes, so what this allocates is set by
 * where the keys diverge rather than by TRIE_DEPTH.
 */
private fun splitLeafAt(
    existing: TrieLeaf,
    ub: ByteArray,
    id: KdbUuid,
    contentHash: KdbHash,
    depth: Int,
): TrieNode {
    val existingBytes = uuidBytes(existing.uuid)
    var d = depth
    while (d < TRIE_DEPTH && nibbleAt(existingBytes, d) == nibbleAt(ub, d)) d++
    if (d == TRIE_DEPTH) {
        // Identical in all 128 bits but unequal as UUIDs is not reachable; treat
        // it as a replace rather than building a node that could never be read.
        return newLeafNode(ub, id, contentHash, depth)
    }
    // The level they part on holds both, each compressed again beneath it.
    val children = arrayOfNulls<TrieNode>(16)
    children[nibbleAt(existingBytes, d)] =
        newLeafNode(existingBytes, existing.uuid, existing.hash, d + 1)
    children[nibbleAt(ub, d)] = newLeafNode(ub, id, contentHash, d + 1)
    var node = TrieNode(hash = internalHash(children), children = children)
    // Levels depth..d-1 are shared by both keys, so they stay ordinary
    // single-child internal nodes: more than one entry now lives below them, and
    // a single entry is the only thing a compressed leaf may stand for.
    for (k in d - 1 downTo depth) {
        val c = arrayOfNulls<TrieNode>(16)
        c[nibbleAt(ub, k)] = node
        node = TrieNode(hash = internalHash(c), children = c)
    }
    return node
}

internal fun trieDelete(root: TrieNode?, id: KdbUuid): TrieNode? {
    return trieDeleteAt(root, uuidBytes(id), id, 0)
}

private fun trieDeleteAt(node: TrieNode?, ub: ByteArray, id: KdbUuid, depth: Int): TrieNode? {
    if (node == null) return null
    val leaf = node.leaf
    if (leaf != null) {
        // Comparing the key matters now that a leaf can sit above TRIE_DEPTH: the
        // path alone no longer identifies the entry, because a leaf at depth d
        // may be a different key that merely shares the prefix.
        return if (leaf.uuid == id) null else node
    }
    val nodeChildren = node.children ?: return node
    if (depth == TRIE_DEPTH) return node
    val children = nodeChildren.copyOf()
    val nib = nibbleAt(ub, depth)
    children[nib] = trieDeleteAt(children[nib], ub, id, depth + 1)
    var remaining = 0
    var only: TrieNode? = null
    for (c in children) {
        if (c != null) {
            remaining++
            only = c
        }
    }
    if (remaining == 0) return null
    // Re-compress on the way back up. One leaf left below means one entry left
    // below, which is exactly what a compressed leaf stands for. Skipping this
    // would let the spines grow back one deletion at a time.
    val onlyLeaf = only?.leaf
    if (remaining == 1 && onlyLeaf != null) {
        return newLeafNode(uuidBytes(onlyLeaf.uuid), onlyLeaf.uuid, onlyLeaf.hash, depth)
    }
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
