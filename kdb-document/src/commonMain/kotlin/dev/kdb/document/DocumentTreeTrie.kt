package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.internal.runEach
import dev.kdb.document.internal.sha256OnlyChild

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

/**
 * Also the entry DocumentTree's map view hands out, so iterating a tree allocates nothing per
 * entry. equals/hashCode follow the Map.Entry contract, so a tree's entries compare equal to a
 * plain map's.
 */
internal class TrieLeaf(val uuid: KdbUuid, val hash: KdbHash) : Map.Entry<KdbUuid, KdbHash> {
    override val key: KdbUuid get() = uuid
    override val value: KdbHash get() = hash

    override fun equals(other: Any?): Boolean = other is Map.Entry<*, *> && other.key == uuid && other.value == hash

    override fun hashCode(): Int = uuid.hashCode() xor hash.hashCode()

    override fun toString(): String = "$uuid=$hash"
}

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
 * single occupied slot. TRIE_ZERO is all-zero, so the preimage is constant but
 * for the child (see sha256OnlyChild).
 */
private fun internalHashOfOnlyChild(nib: Int, childHash: ByteArray): ByteArray = sha256OnlyChild(nib, childHash)

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

/** The nibble of [id] at [depth], read straight from its halves rather than a byte copy. */
private fun nibbleOf(id: KdbUuid, depth: Int): Int {
    val half = if (depth < 16) id.msb else id.lsb
    return ((half ushr (60 - 4 * (depth % 16))) and 0xf).toInt()
}

/**
 * The content hash [root] holds for [id], or null. Read-only, so unlike insert/delete it has no
 * Go counterpart to keep in step: it only follows the path the hashing code built.
 */
internal fun trieGet(root: TrieNode?, id: KdbUuid): KdbHash? {
    var node = root
    var depth = 0
    while (node != null) {
        val leaf = node.leaf
        if (leaf != null) return if (leaf.uuid == id) leaf.hash else null
        val children = node.children ?: return null
        if (depth == TRIE_DEPTH) return null
        node = children[nibbleOf(id, depth)]
        depth++
    }
    return null
}

/**
 * Builds the trie for a whole set of entries at once, as Go's trieBuild does: the ids sorted once,
 * then each node built and hashed exactly once. Inserting them one at a time gives the same trie,
 * node for node, but rehashes every node on an entry's path per insert and refolds a compressed
 * leaf's whole spine each time a later entry splits it - about twice the hashing.
 */
internal fun trieBuild(entries: Map<KdbUuid, KdbHash>): TrieNode? {
    if (entries is TrieEntries) return entries.root
    if (entries.isEmpty()) return null
    val ids = entries.keys.toTypedArray()
    ids.sortWith(UUID_ORDER)
    val hashes = Array(ids.size) { entries.getValue(ids[it]) }
    if (ids.size < PARALLEL_BUILD_MIN) return trieBuildSorted(ids, hashes, 0, ids.size, 0)
    // The top-level subtrees share nothing they write, so where there are threads they are built
    // at once, as Go builds them on separate goroutines.
    val runs = ArrayList<IntArray>(16)
    var start = 0
    while (start < ids.size) {
        val nib = nibbleOf(ids[start], 0)
        var end = start + 1
        while (end < ids.size && nibbleOf(ids[end], 0) == nib) end++
        runs += intArrayOf(nib, start, end)
        start = end
    }
    val subtrees = runEach(runs.size) { trieBuildSorted(ids, hashes, runs[it][1], runs[it][2], 1) }
    val children = arrayOfNulls<TrieNode>(16)
    for ((k, run) in runs.withIndex()) children[run[0]] = subtrees[k]
    return TrieNode(hash = internalHash(children), children = children)
}

// Below this, splitting the build costs more than it saves (Go's threshold too).
private const val PARALLEL_BUILD_MIN = 4096

private val UUID_ORDER =
    Comparator<KdbUuid> { a, b ->
        val c = a.msb.toULong().compareTo(b.msb.toULong())
        if (c != 0) c else a.lsb.toULong().compareTo(b.lsb.toULong())
    }

/**
 * The subtree at [depth] over ids[from, to), which are sorted and share their first [depth]
 * nibbles. One entry is a leaf standing for the whole subtree; more is an internal node over the
 * runs sharing the next nibble - a run of all of them included, the single-child chain an insert's
 * split builds.
 */
private fun trieBuildSorted(
    ids: Array<KdbUuid>,
    hashes: Array<KdbHash>,
    from: Int,
    to: Int,
    depth: Int,
): TrieNode {
    if (to - from == 1 || depth == TRIE_DEPTH) {
        val id = ids[to - 1]
        return newLeafNode(uuidBytes(id), id, hashes[to - 1], depth)
    }
    val children = arrayOfNulls<TrieNode>(16)
    var start = from
    while (start < to) {
        val nib = nibbleOf(ids[start], depth)
        var end = start + 1
        while (end < to && nibbleOf(ids[end], depth) == nib) end++
        children[nib] = trieBuildSorted(ids, hashes, start, end, depth + 1)
        start = end
    }
    return TrieNode(hash = internalHash(children), children = children)
}

internal fun trieTreeHash(root: TrieNode?): KdbHash = KdbHash(nodeHash(root))

/** Whether [a] sorts after [b] as 16 big-endian bytes, the trie's (and the canonical string's) order. */
private fun uuidAfter(a: KdbUuid, b: KdbUuid): Boolean =
    if (a.msb != b.msb) a.msb.toULong() > b.msb.toULong() else a.lsb.toULong() > b.lsb.toULong()

/**
 * Every leaf beneath [root] in nibble order, which is UUID byte (and canonical string) order,
 * starting at the first id after [after], or at the first id when it is null.
 *
 * Finding the start walks one path, so a page of k entries costs O(depth + k) rather than the
 * O(n log n) a sort of every id costs. The iterator holds a stack of at most TRIE_DEPTH frames.
 */
internal class TrieLeafIterator(root: TrieNode?, after: KdbUuid? = null) : Iterator<TrieLeaf> {
    // Frame k is an internal node whose children from next[k] on are still to be visited.
    private val nodes = arrayOfNulls<TrieNode>(TRIE_DEPTH + 1)
    private val next = IntArray(TRIE_DEPTH + 1)
    private var top = -1
    private var pending: TrieLeaf? = null

    init {
        if (root != null) {
            if (after == null) {
                descend(root)
            } else {
                seek(root, after)
            }
        }
    }

    /** Positions the stack just past [after]: every frame on its path resumes at the next sibling. */
    private fun seek(root: TrieNode, after: KdbUuid) {
        val ub = uuidBytes(after)
        var node = root
        var depth = 0
        while (true) {
            val leaf = node.leaf
            if (leaf != null) {
                // The only entry beneath this position: it may still sort after the cursor.
                if (uuidAfter(leaf.uuid, after)) pending = leaf
                return
            }
            val children = node.children ?: return
            if (depth == TRIE_DEPTH) return
            val nib = nibbleAt(ub, depth)
            push(node, nib + 1)
            node = children[nib] ?: return
            depth++
        }
    }

    /** Pushes the frames down to [node]'s first leaf and parks that leaf in pending. */
    private fun descend(start: TrieNode) {
        var node = start
        while (true) {
            val leaf = node.leaf
            if (leaf != null) {
                pending = leaf
                return
            }
            val children = node.children ?: return
            var i = 0
            while (i < 16 && children[i] == null) i++
            if (i == 16) return
            push(node, i + 1)
            node = children[i]!!
        }
    }

    private fun push(node: TrieNode, from: Int) {
        top++
        nodes[top] = node
        next[top] = from
    }

    private fun advance() {
        while (pending == null && top >= 0) {
            val children = nodes[top]!!.children!!
            var i = next[top]
            while (i < 16 && children[i] == null) i++
            if (i == 16) {
                nodes[top] = null
                top--
                continue
            }
            next[top] = i + 1
            descend(children[i]!!)
        }
    }

    override fun hasNext(): Boolean {
        advance()
        return pending != null
    }

    override fun next(): TrieLeaf {
        advance()
        val leaf = pending ?: throw NoSuchElementException()
        pending = null
        return leaf
    }
}

/**
 * One position of the trie as a peer's TREE_NODES reports it - Go's DocumentTree.Node
 * (go/kdb/document/document_tree_node.go): the subtree at [prefix] (a string of lowercase hex
 * nibbles), its children's hashes, and its entries when it holds no more than the entry limit.
 */
public class DocumentTreeNode(
    public val prefix: String,
    /** Null when the subtree is empty. */
    public val hash: KdbHash?,
    /** 16 child hashes, null for an empty child; empty when the subtree is empty. */
    public val children: List<KdbHash?>,
    /** Document id to content hash, when the subtree holds at most the entry limit; else null. */
    public val entries: Map<KdbUuid, KdbHash>?,
)

private fun hexNibble(c: Char): Int = if (c <= '9') c - '0' else c - 'a' + 10

private fun hexOf(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

/** Walks [n]'s entries until [visit] returns false; true when it saw them all. */
private fun trieWalk(n: TrieNode, visit: (KdbUuid, KdbHash) -> Boolean): Boolean {
    n.leaf?.let { return visit(it.uuid, it.hash) }
    for (c in n.children.orEmpty()) if (c != null && !trieWalk(c, visit)) return false
    return true
}

internal fun trieNodeAt(root: TrieNode?, prefix: String, entryLimit: Int): DocumentTreeNode {
    require(prefix.length <= TRIE_DEPTH && prefix.all { it in '0'..'9' || it in 'a'..'f' }) { "document tree: \"$prefix\" is not a nibble prefix" }
    val empty = DocumentTreeNode(prefix, null, emptyList(), null)
    val depth = prefix.length
    var r = root ?: return empty
    for (d in 0 until depth) {
        if (r.leaf != null) break
        r = r.children?.get(hexNibble(prefix[d])) ?: return empty
    }
    val leaf = r.leaf
    if (leaf != null) {
        // A compressed leaf stands for everything below where it sits; it belongs to this subtree
        // only if its id carries the whole prefix.
        val ub = uuidBytes(leaf.uuid)
        if (!hexOf(ub).startsWith(prefix)) return empty
        val children = MutableList<KdbHash?>(16) { null }
        if (depth < TRIE_DEPTH) children[nibbleAt(ub, depth)] = KdbHash(foldLeaf(ub, leaf.hash, depth + 1))
        return DocumentTreeNode(prefix, KdbHash(foldLeaf(ub, leaf.hash, depth)), children, if (entryLimit >= 1) mapOf(leaf.uuid to leaf.hash) else null)
    }
    val children = (0 until 16).map { i -> r.children?.get(i)?.let { KdbHash(it.hash) } }
    var entries: Map<KdbUuid, KdbHash>? = null
    if (entryLimit > 0) {
        val found = LinkedHashMap<KdbUuid, KdbHash>()
        val small = trieWalk(r) { id, h ->
            found[id] = h
            found.size <= entryLimit
        }
        if (small) entries = found
    }
    return DocumentTreeNode(prefix, KdbHash(r.hash), children, entries)
}
