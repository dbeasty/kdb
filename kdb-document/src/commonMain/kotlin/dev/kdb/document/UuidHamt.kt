package dev.kdb.document

import dev.kdb.codec.KdbUuid

/**
 * A persistent map from [KdbUuid] to [V]: a hash array mapped trie, 5 bits of a mixed 64-bit hash
 * per level. [put] and [remove] copy one path and share every other node with the map they were
 * called on, so the versions of an [AttachmentTable] cost what changed between them, not a copy of
 * every document each (the plain [Map] they replace was copied whole on every change).
 */
internal class UuidHamt<V : Any> private constructor(
    private val root: Node<V>?,
    val size: Int,
) {
    operator fun get(key: KdbUuid): V? {
        var node = root ?: return null
        val h = mix(key)
        var shift = 0
        while (true) {
            when (node) {
                is Leaf -> return if (node.key == key) node.value else null
                is Collision -> return node.entries.firstOrNull { it.key == key }?.value
                is Branch -> {
                    val bit = 1 shl ((h ushr shift).toInt() and 31)
                    if (node.bitmap and bit == 0) return null
                    node = node.children[(node.bitmap and (bit - 1)).countOneBits()]
                    shift += 5
                }
            }
        }
    }

    fun put(
        key: KdbUuid,
        value: V,
    ): UuidHamt<V> {
        val grew = BooleanArray(1)
        val r = put(root, key, mix(key), value, 0, grew)
        return UuidHamt(r, if (grew[0]) size + 1 else size)
    }

    fun remove(key: KdbUuid): UuidHamt<V> {
        val r = root ?: return this
        val removed = BooleanArray(1)
        val next = remove(r, key, mix(key), 0, removed)
        return if (removed[0]) UuidHamt(next, size - 1) else this
    }

    /** Every entry, in no particular order. */
    fun forEach(visit: (KdbUuid, V) -> Unit) {
        fun walk(n: Node<V>) {
            when (n) {
                is Leaf -> visit(n.key, n.value)
                is Collision -> n.entries.forEach { visit(it.key, it.value) }
                is Branch -> n.children.forEach { walk(it) }
            }
        }
        root?.let { walk(it) }
    }

    fun keys(): Set<KdbUuid> {
        val out = HashSet<KdbUuid>(size)
        forEach { k, _ -> out.add(k) }
        return out
    }

    private sealed interface Node<V>

    private class Leaf<V>(
        val key: KdbUuid,
        val hash: Long,
        val value: V,
    ) : Node<V>

    /** Keys whose 64-bit hashes are equal, below the last level. */
    private class Collision<V>(
        val hash: Long,
        val entries: List<Leaf<V>>,
    ) : Node<V>

    private class Branch<V>(
        val bitmap: Int,
        val children: Array<Node<V>>,
    ) : Node<V>

    private fun put(
        node: Node<V>?,
        key: KdbUuid,
        h: Long,
        value: V,
        shift: Int,
        grew: BooleanArray,
    ): Node<V> {
        when (node) {
            null -> {
                grew[0] = true
                return Leaf(key, h, value)
            }
            is Leaf -> {
                if (node.key == key) return Leaf(key, h, value)
                grew[0] = true
                if (node.hash == h) return Collision(h, listOf(node, Leaf(key, h, value)))
                return join(node, Leaf(key, h, value), shift)
            }
            is Collision -> {
                val i = node.entries.indexOfFirst { it.key == key }
                if (i < 0) grew[0] = true
                val entries = node.entries.toMutableList()
                if (i < 0) entries.add(Leaf(key, h, value)) else entries[i] = Leaf(key, h, value)
                return Collision(h, entries)
            }
            is Branch -> {
                val bit = 1 shl ((h ushr shift).toInt() and 31)
                val idx = (node.bitmap and (bit - 1)).countOneBits()
                if (node.bitmap and bit == 0) {
                    grew[0] = true
                    val children = arrayOfNulls<Node<V>>(node.children.size + 1)
                    node.children.copyInto(children, 0, 0, idx)
                    children[idx] = Leaf(key, h, value)
                    node.children.copyInto(children, idx + 1, idx)
                    @Suppress("UNCHECKED_CAST")
                    return Branch(node.bitmap or bit, children as Array<Node<V>>)
                }
                val children = node.children.copyOf()
                children[idx] = put(children[idx], key, h, value, shift + 5, grew)
                return Branch(node.bitmap, children)
            }
        }
    }

    /** A branch (or chain of them) holding two leaves of different hashes. */
    private fun join(
        a: Leaf<V>,
        b: Leaf<V>,
        shift: Int,
    ): Node<V> {
        val ia = (a.hash ushr shift).toInt() and 31
        val ib = (b.hash ushr shift).toInt() and 31
        if (ia == ib) return Branch(1 shl ia, arrayOf(join(a, b, shift + 5)))
        val first: Node<V> = if (ia < ib) a else b
        val second: Node<V> = if (ia < ib) b else a
        return Branch((1 shl ia) or (1 shl ib), arrayOf(first, second))
    }

    private fun remove(
        node: Node<V>,
        key: KdbUuid,
        h: Long,
        shift: Int,
        removed: BooleanArray,
    ): Node<V>? {
        when (node) {
            is Leaf -> {
                if (node.key != key) return node
                removed[0] = true
                return null
            }
            is Collision -> {
                val rest = node.entries.filter { it.key != key }
                if (rest.size == node.entries.size) return node
                removed[0] = true
                return if (rest.size == 1) rest[0] else Collision(node.hash, rest)
            }
            is Branch -> {
                val bit = 1 shl ((h ushr shift).toInt() and 31)
                if (node.bitmap and bit == 0) return node
                val idx = (node.bitmap and (bit - 1)).countOneBits()
                val child = remove(node.children[idx], key, h, shift + 5, removed)
                if (!removed[0]) return node
                if (child != null) {
                    // A branch left holding one leaf (or collision) is replaced by it.
                    if (node.children.size == 1 && child !is Branch) return child
                    val children = node.children.copyOf()
                    children[idx] = child
                    return Branch(node.bitmap, children)
                }
                if (node.children.size == 1) return null
                val children = arrayOfNulls<Node<V>>(node.children.size - 1)
                node.children.copyInto(children, 0, 0, idx)
                node.children.copyInto(children, idx, idx + 1)
                val left = node.bitmap and bit.inv()
                @Suppress("UNCHECKED_CAST")
                val out = Branch(left, children as Array<Node<V>>)
                return if (out.children.size == 1 && out.children[0] !is Branch) out.children[0] else out
            }
        }
    }

    companion object {
        private val EMPTY = UuidHamt<Any>(null, 0)

        @Suppress("UNCHECKED_CAST")
        fun <V : Any> empty(): UuidHamt<V> = EMPTY as UuidHamt<V>

        /** Spreads a UUID over 64 bits (a SplitMix64 finaliser over both halves). */
        private fun mix(k: KdbUuid): Long {
            var z = k.msb * -0x61c8864680b583ebL + k.lsb
            z = (z xor (z ushr 30)) * -0x40a7b892e31b1a47L
            z = (z xor (z ushr 27)) * -0x6b2fb644ecceee15L
            return z xor (z ushr 31)
        }
    }
}
