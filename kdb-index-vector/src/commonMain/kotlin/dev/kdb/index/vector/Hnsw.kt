package dev.kdb.index.vector

import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import kotlin.math.floor
import kotlin.math.ln

private const val TWO_POW_64: Double = 18446744073709551616.0

/**
 * Node level for [docId] (§7): `floor(−ln(u) / ln(m))` with `u ∈ (0, 1]` taken from the first
 * 8 bytes (big-endian) of `sha256(docId bytes)`, so a document's level never depends on insertion
 * order and two trees build comparable graphs.
 */
public fun hnswLevelFor(
    docId: KdbUuid,
    m: Int,
): Int {
    val bytes = ByteArray(16)
    for (i in 0 until 8) bytes[i] = (docId.msb ushr (56 - 8 * i)).toByte()
    for (i in 0 until 8) bytes[8 + i] = (docId.lsb ushr (56 - 8 * i)).toByte()
    val digest = kdbSha256(bytes)
    var x = 0uL
    for (i in 0 until 8) x = (x shl 8) or (digest[i].toUByte().toULong())
    // +1 over 2^64 keeps u in (0, 1]; u == 1 (all-ones digest prefix) means level 0.
    val u = (x.toDouble() + 1.0) / TWO_POW_64
    if (u >= 1.0) return 0
    return floor(-ln(u) / ln(m.toDouble())).toInt()
}

/** Growable int list without boxing. */
internal class IntList(capacity: Int = 8) {
    private var data = IntArray(capacity)
    var size = 0
        private set

    operator fun get(i: Int): Int = data[i]

    fun add(v: Int) {
        if (size == data.size) data = data.copyOf(size * 2)
        data[size++] = v
    }

    fun clear() {
        size = 0
    }

    fun replaceWith(values: IntArray) {
        if (values.size > data.size) data = values.copyOf() else values.copyInto(data)
        size = values.size
    }

    fun toArray(): IntArray = data.copyOf(size)
}

/** Binary heap over (node, similarity) pairs; [max] true pops the highest similarity first. */
internal class NodeHeap(private val max: Boolean) {
    private var nodes = IntArray(16)
    private var sims = FloatArray(16)
    var size = 0
        private set

    fun isEmpty(): Boolean = size == 0

    fun peekNode(): Int = nodes[0]

    fun peekSim(): Float = sims[0]

    private fun before(a: Int, b: Int): Boolean =
        if (max) {
            sims[a] > sims[b] || (sims[a] == sims[b] && nodes[a] < nodes[b])
        } else {
            sims[a] < sims[b] || (sims[a] == sims[b] && nodes[a] > nodes[b])
        }

    fun push(node: Int, sim: Float) {
        if (size == nodes.size) {
            nodes = nodes.copyOf(size * 2)
            sims = sims.copyOf(size * 2)
        }
        var i = size++
        nodes[i] = node
        sims[i] = sim
        while (i > 0) {
            val parent = (i - 1) ushr 1
            if (!before(i, parent)) break
            swap(i, parent)
            i = parent
        }
    }

    fun pop() {
        size--
        if (size == 0) return
        nodes[0] = nodes[size]
        sims[0] = sims[size]
        var i = 0
        while (true) {
            val l = 2 * i + 1
            val r = l + 1
            var best = i
            if (l < size && before(l, best)) best = l
            if (r < size && before(r, best)) best = r
            if (best == i) break
            swap(i, best)
            i = best
        }
    }

    private fun swap(a: Int, b: Int) {
        val n = nodes[a]
        nodes[a] = nodes[b]
        nodes[b] = n
        val s = sims[a]
        sims[a] = sims[b]
        sims[b] = s
    }

    /** Drains into arrays sorted by the heap order (best first). */
    fun drainSorted(): Pair<IntArray, FloatArray> {
        val n = IntArray(size)
        val s = FloatArray(size)
        var i = 0
        while (!isEmpty()) {
            n[i] = peekNode()
            s[i] = peekSim()
            pop()
            i++
        }
        return n to s
    }
}

/**
 * Hierarchical Navigable Small World graph (Malkov & Yashunin) over a *similarity* (higher is
 * closer). Nodes are appended, never removed: a node that no longer represents a live document is
 * marked inactive and skipped when results are collected, but still routes searches. Single
 * writer, callers serialise access.
 */
internal class HnswGraph(
    private val m: Int,
    private val efConstruction: Int,
    private val similarity: (FloatArray, FloatArray) -> Float,
) {
    private val mMax0 = 2 * m
    private val vectors = ArrayList<FloatArray>()
    private val levels = IntList()
    private val links = ArrayList<Array<IntList>>()
    private var entryPoint = -1
    private var topLevel = -1

    val size: Int get() = vectors.size

    fun vector(node: Int): FloatArray = vectors[node]

    fun add(
        vector: FloatArray,
        level: Int,
    ): Int {
        val id = vectors.size
        vectors += vector
        levels.add(level)
        links += Array(level + 1) { IntList() }
        if (entryPoint < 0) {
            entryPoint = id
            topLevel = level
            return id
        }
        var ep = entryPoint
        var epSim = similarity(vector, vectors[ep])
        for (l in topLevel downTo level + 1) {
            var changed = true
            while (changed) {
                changed = false
                val neigh = links[ep][l]
                for (i in 0 until neigh.size) {
                    val c = neigh[i]
                    val s = similarity(vector, vectors[c])
                    if (s > epSim) {
                        epSim = s
                        ep = c
                        changed = true
                    }
                }
            }
        }
        for (l in minOf(level, topLevel) downTo 0) {
            val (candNodes, candSims) = searchLayer(vector, ep, efConstruction, l, ACCEPT_ALL)
            val maxLinks = if (l == 0) mMax0 else m
            val chosen = selectNeighbours(candNodes, candSims, m)
            links[id][l].replaceWith(chosen)
            for (c in chosen) {
                val back = links[c][l]
                back.add(id)
                if (back.size > maxLinks) prune(c, l, maxLinks)
            }
            if (candNodes.isNotEmpty()) ep = candNodes[0]
        }
        if (level > topLevel) {
            topLevel = level
            entryPoint = id
        }
        return id
    }

    /** Heuristic neighbour selection (Algorithm 4 without keepPruned), input sorted best first. */
    private fun selectNeighbours(
        candNodes: IntArray,
        candSims: FloatArray,
        count: Int,
    ): IntArray {
        val out = IntList(count)
        for (i in candNodes.indices) {
            if (out.size >= count) break
            val c = candNodes[i]
            var good = true
            for (j in 0 until out.size) {
                if (similarity(vectors[c], vectors[out[j]]) > candSims[i]) {
                    good = false
                    break
                }
            }
            if (good) out.add(c)
        }
        if (out.size < count) {
            for (i in candNodes.indices) {
                if (out.size >= count) break
                val c = candNodes[i]
                var present = false
                for (j in 0 until out.size) if (out[j] == c) { present = true; break }
                if (!present) out.add(c)
            }
        }
        return out.toArray()
    }

    private fun prune(
        node: Int,
        level: Int,
        maxLinks: Int,
    ) {
        val neigh = links[node][level]
        val v = vectors[node]
        val heap = NodeHeap(max = true)
        for (i in 0 until neigh.size) heap.push(neigh[i], similarity(v, vectors[neigh[i]]))
        val (nodes, sims) = heap.drainSorted()
        neigh.replaceWith(selectNeighbours(nodes, sims, maxLinks))
    }

    /**
     * Best-first search on one layer; returns up to [ef] nodes sorted by similarity descending.
     * Only nodes passing [accept] are collected — the rest still route the search, which is how a
     * node whose document has since been rewritten or deleted stays useful for navigation.
     */
    private fun searchLayer(
        query: FloatArray,
        entry: Int,
        ef: Int,
        level: Int,
        accept: (Int) -> Boolean,
    ): Pair<IntArray, FloatArray> {
        val visited = HashSet<Int>()
        val candidates = NodeHeap(max = true)
        val results = NodeHeap(max = false)
        val entrySim = similarity(query, vectors[entry])
        visited += entry
        candidates.push(entry, entrySim)
        if (accept(entry)) results.push(entry, entrySim)
        var worst = if (results.isEmpty()) Float.NEGATIVE_INFINITY else results.peekSim()
        while (!candidates.isEmpty()) {
            val c = candidates.peekNode()
            val cSim = candidates.peekSim()
            candidates.pop()
            if (results.size >= ef && cSim < worst) break
            val neigh = links[c][level]
            for (i in 0 until neigh.size) {
                val n = neigh[i]
                if (!visited.add(n)) continue
                val s = similarity(query, vectors[n])
                if (results.size < ef || s > worst) {
                    candidates.push(n, s)
                    if (accept(n)) {
                        results.push(n, s)
                        if (results.size > ef) results.pop()
                        worst = results.peekSim()
                    }
                }
            }
        }
        val (nodes, sims) = results.drainSorted()
        // drainSorted on a min-heap yields worst first; reverse to best first.
        nodes.reverse()
        sims.reverse()
        return nodes to sims
    }

    /** Up to [ef] accepted nodes closest to [query], best first. */
    fun search(
        query: FloatArray,
        ef: Int,
        accept: (Int) -> Boolean,
    ): Pair<IntArray, FloatArray> {
        if (entryPoint < 0) return IntArray(0) to FloatArray(0)
        var ep = entryPoint
        var epSim = similarity(query, vectors[ep])
        for (l in topLevel downTo 1) {
            var changed = true
            while (changed) {
                changed = false
                val neigh = links[ep][l]
                for (i in 0 until neigh.size) {
                    val c = neigh[i]
                    val s = similarity(query, vectors[c])
                    if (s > epSim) {
                        epSim = s
                        ep = c
                        changed = true
                    }
                }
            }
        }
        return searchLayer(query, ep, ef, 0, accept)
    }

    private companion object {
        val ACCEPT_ALL: (Int) -> Boolean = { true }
    }
}
