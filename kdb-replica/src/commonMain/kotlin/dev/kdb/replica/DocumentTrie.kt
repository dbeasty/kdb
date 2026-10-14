package dev.kdb.replica

import dev.kdb.document.kdbSha256

/**
 * The document tree: KDB's 16-ary Merkle trie over document-id nibbles
 * (go/kdb/document/document_tree_trie.go), stored node by node so it never has to be resident.
 *
 * Why not kdb-document's DocumentTreeTrie: it hashes identically (it is Go's mirror), but its
 * nodes are a resident persistent structure, internal to that module, and the whole point here is
 * that a replica's tree lives in the store and only the nodes an edit touches are loaded. So this
 * is a port of packages/kdb-replica/src/trie.ts, checked against the same Go golden roots.
 *
 * Hashing, exactly as Go defines it:
 * - leaf:     SHA-256(0x00 ‖ uuid(16) ‖ contentHash(32))
 * - internal: SHA-256(0x01 ‖ 16 × childHash(32)), all zeros for an absent child
 * - a subtree holding exactly one entry is stored as one leaf record at the shallowest depth where
 *   that is true, and its hash is the leaf hash folded up through the single-child internal nodes
 *   it stands for ([foldLeaf]). Go's tree hashes are therefore unchanged by the compression.
 * - the empty tree's hash is 32 zero bytes.
 *
 * Records are keyed by namespace and nibble prefix. A record exists only where a walk from the root
 * reaches it: when a subtree collapses to one entry, the records beneath it are deleted, so a
 * later split never finds a stale one.
 *
 * Edits go through a [TrieEdit]: an overlay over the store that loads records as it needs them and
 * hashes only at [TrieEdit.finish], deepest first. A batch of inserts therefore hashes each changed
 * node once, however many of the batch's documents pass through it. Nothing reaches the store until
 * the caller applies the edit's batch. A fast-forward computes the root first and checks it against
 * the commit's declared tree hash, so a mismatch writes nothing.
 */
internal sealed class TrieRecord {
    abstract var hash: ByteArray?

    class Leaf(val id: ByteArray, val content: ByteArray, override var hash: ByteArray?) : TrieRecord()

    /** [children] is 16 × 32 bytes, zeros for an absent child. */
    class Internal(val children: ByteArray, override var hash: ByteArray?) : TrieRecord()
}

private const val HEXN = "0123456789abcdef"

private fun nibble(id: ByteArray, depth: Int): Int {
    val b = id[depth shr 1].toInt() and 0xff
    return if (depth and 1 == 1) b and 0x0f else b shr 4
}

internal fun leafHash(id: ByteArray, content: ByteArray): ByteArray {
    val buf = ByteArray(49)
    id.copyInto(buf, 1)
    content.copyInto(buf, 17)
    return kdbSha256(buf)
}

/** The hash of the subtree at [depth] that holds only (id, content). */
internal fun foldLeaf(id: ByteArray, content: ByteArray, depth: Int): ByteArray {
    var h = leafHash(id, content)
    val buf = ByteArray(1 + 16 * 32)
    for (d in 31 downTo depth) {
        buf.fill(0)
        buf[0] = 0x01
        h.copyInto(buf, 1 + nibble(id, d) * 32)
        h = kdbSha256(buf)
    }
    return h
}

private fun internalHash(children: ByteArray): ByteArray {
    val buf = ByteArray(1 + 16 * 32)
    buf[0] = 0x01
    children.copyInto(buf, 1)
    return kdbSha256(buf)
}

private fun childHash(children: ByteArray, nib: Int): ByteArray = children.copyOfRange(nib * 32, nib * 32 + 32)

private fun childPresent(children: ByteArray, nib: Int): Boolean {
    for (i in nib * 32 until nib * 32 + 32) if (children[i] != 0.toByte()) return true
    return false
}

// Record encoding (the TS replica's, so both lay out a store alike).
// leaf: 0x00 ‖ id(16) ‖ content(32) ‖ hash(32).
// internal: 0x01 ‖ hash(32) ‖ u16 occupancy bitmap (big-endian) ‖ 32 bytes per present child.
internal fun encodeRecord(r: TrieRecord): ByteArray {
    val hash = r.hash ?: error("kdb replica: trie record written before it was hashed")
    when (r) {
        is TrieRecord.Leaf -> {
            val out = ByteArray(81)
            r.id.copyInto(out, 1)
            r.content.copyInto(out, 17)
            hash.copyInto(out, 49)
            return out
        }
        is TrieRecord.Internal -> {
            var bitmap = 0
            var n = 0
            for (i in 0 until 16) {
                if (childPresent(r.children, i)) {
                    bitmap = bitmap or (1 shl (15 - i))
                    n++
                }
            }
            val out = ByteArray(35 + n * 32)
            out[0] = 0x01
            hash.copyInto(out, 1)
            out[33] = (bitmap shr 8).toByte()
            out[34] = bitmap.toByte()
            var off = 35
            for (i in 0 until 16) {
                if (bitmap and (1 shl (15 - i)) != 0) {
                    r.children.copyInto(out, off, i * 32, i * 32 + 32)
                    off += 32
                }
            }
            return out
        }
    }
}

internal fun decodeRecord(b: ByteArray): TrieRecord {
    if (b.isNotEmpty() && b[0] == 0x00.toByte() && b.size == 81) {
        return TrieRecord.Leaf(b.copyOfRange(1, 17), b.copyOfRange(17, 49), b.copyOfRange(49, 81))
    }
    if (b.size >= 35 && b[0] == 0x01.toByte()) {
        val bitmap = ((b[33].toInt() and 0xff) shl 8) or (b[34].toInt() and 0xff)
        val children = ByteArray(512)
        var off = 35
        for (i in 0 until 16) {
            if (bitmap and (1 shl (15 - i)) != 0) {
                if (off + 32 > b.size) throw IllegalStateException("kdb replica: corrupt trie record")
                b.copyInto(children, i * 32, off, off + 32)
                off += 32
            }
        }
        if (off != b.size) throw IllegalStateException("kdb replica: corrupt trie record")
        return TrieRecord.Internal(children, b.copyOfRange(1, 33))
    }
    throw IllegalStateException("kdb replica: corrupt trie record")
}

/** Trie records of one namespace live under this key prefix. */
internal fun trieKeyPrefix(nsKey: String): String = "t/$nsKey/"

/** Reads a namespace's trie: the root hash and records by nibble prefix. */
internal class TrieReader(private val store: ReplicaStore, nsKey: String) {
    private val prefix = trieKeyPrefix(nsKey)

    suspend fun record(p: String): TrieRecord? = store.get(prefix + p)?.let(::decodeRecord)

    suspend fun rootHash(): String = (record("")?.hash ?: ZERO32).toHex()

    /** The content hash of [id], or null when the tree does not hold it. */
    suspend fun lookup(id: String): String? {
        val key = uuidBytes(id)
        var p = ""
        for (depth in 0..32) {
            val r = record(p) ?: return null
            if (r is TrieRecord.Leaf) return if (r.id.contentEquals(key)) r.content.toHex() else null
            p += HEXN[nibble(key, depth)]
        }
        return null
    }

    /** Visits every (id, contentHash) in id order; [visit] returns false to stop. */
    suspend fun walk(p: String = "", visit: suspend (id: String, content: String) -> Boolean): Boolean {
        val r = record(p) ?: return true
        if (r is TrieRecord.Leaf) return visit(uuidString(r.id), r.content.toHex())
        r as TrieRecord.Internal
        for (i in 0 until 16) {
            if (childPresent(r.children, i) && !walk(p + HEXN[i], visit)) return false
        }
        return true
    }
}

/** A batch of tree changes over a store, hashed and written as one. */
internal class TrieEdit(private val store: ReplicaStore, nsKey: String) {
    private val prefix = trieKeyPrefix(nsKey)
    private val overlay = HashMap<String, TrieRecord?>()
    private val dirty = LinkedHashSet<String>()
    private var finished = false

    private suspend fun load(p: String): TrieRecord? {
        if (overlay.containsKey(p)) return overlay[p]
        val r = store.get(prefix + p)?.let(::decodeRecord)
        overlay[p] = r
        return r
    }

    private fun put(p: String, r: TrieRecord?) {
        r?.hash = null
        overlay[p] = r
        dirty.add(p)
    }

    private fun touch(p: String, r: TrieRecord) {
        r.hash = null
        dirty.add(p)
    }

    /** Puts (or replaces) [id] with [contentHashHex]. */
    suspend fun set(id: String, contentHashHex: String) {
        ensureOpen()
        val key = uuidBytes(id)
        val content = hex32(contentHashHex)
        var p = ""
        // depth 32 is a full id: only a leaf can be there, and only an empty position is reached.
        for (depth in 0..32) {
            val r = load(p)
            if (r == null) {
                put(p, TrieRecord.Leaf(key, content, null))
                return
            }
            if (r is TrieRecord.Leaf) {
                if (r.id.contentEquals(key)) {
                    if (!r.content.contentEquals(content)) put(p, TrieRecord.Leaf(key, content, null))
                    return
                }
                // Split: this position now holds two entries. The old one moves down one level (its
                // child position is free: nothing exists beneath a leaf), and the loop carries on down
                // for the new one, splitting again while the two share nibbles.
                put(p, TrieRecord.Internal(ByteArray(512), null))
                put(p + HEXN[nibble(r.id, depth)], TrieRecord.Leaf(r.id, r.content, null))
            } else {
                touch(p, r)
            }
            p += HEXN[nibble(key, depth)]
        }
        error("kdb replica: trie deeper than a UUID")
    }

    /** Removes [id]; a no-op when the tree does not hold it. */
    suspend fun delete(id: String) {
        ensureOpen()
        remove("", 0, uuidBytes(id))
    }

    private fun present(p: String, r: TrieRecord.Internal, nib: Int): Boolean {
        val child = p + HEXN[nib]
        if (overlay.containsKey(child)) return overlay[child] != null
        return childPresent(r.children, nib)
    }

    private suspend fun remove(p: String, depth: Int, key: ByteArray): Boolean {
        val r = load(p) ?: return false
        if (r is TrieRecord.Leaf) {
            if (!r.id.contentEquals(key)) return false
            put(p, null)
            return true
        }
        r as TrieRecord.Internal
        if (!remove(p + HEXN[nibble(key, depth)], depth + 1, key)) return false
        touch(p, r)
        var only = -1
        var count = 0
        for (i in 0 until 16) {
            if (present(p, r, i)) {
                only = i
                count++
            }
        }
        if (count == 0) {
            put(p, null)
        } else if (count == 1) {
            // A subtree down to one entry is stored as that entry's leaf, here: collapse, and delete the
            // record it came from so nothing stale is left beneath a leaf.
            val child = load(p + HEXN[only])
            if (child is TrieRecord.Leaf) {
                put(p, TrieRecord.Leaf(child.id, child.content, null))
                put(p + HEXN[only], null)
            }
        }
        return true
    }

    private fun ensureOpen() {
        check(!finished) { "kdb replica: trie edit already finished" }
    }

    /** Hashes every changed record, deepest first, and returns the new root hash. */
    suspend fun finish(): String {
        if (!finished) {
            val order = dirty.sortedByDescending { it.length }
            for (p in order) {
                val r = overlay[p] ?: continue
                when (r) {
                    is TrieRecord.Leaf -> r.hash = foldLeaf(r.id, r.content, p.length)
                    is TrieRecord.Internal -> {
                        for (i in 0 until 16) {
                            val cp = p + HEXN[i]
                            if (overlay.containsKey(cp)) (overlay[cp]?.hash ?: ZERO32).copyInto(r.children, i * 32)
                        }
                        r.hash = internalHash(r.children)
                    }
                }
            }
            finished = true
        }
        return (load("")?.hash ?: ZERO32).toHex()
    }

    /** Adds the edit's record writes and deletes to [batch]. Call after [finish]. */
    fun writeTo(batch: Batch) {
        check(finished) { "kdb replica: finish the trie edit before writing it" }
        for (p in dirty) {
            val r = overlay[p]
            batch[prefix + p] = r?.let(::encodeRecord)
        }
    }
}

/** The hex digit of nibble [nib], for building child prefixes. */
internal fun nibbleChar(nib: Int): Char = HEXN[nib]

internal fun childHashHex(children: ByteArray, nib: Int): String =
    if (childPresent(children, nib)) childHash(children, nib).toHex() else ""
