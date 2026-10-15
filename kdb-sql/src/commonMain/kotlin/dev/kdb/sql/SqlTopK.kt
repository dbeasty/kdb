package dev.kdb.sql

/**
 * Bounded top-k for `ORDER BY ... LIMIT` (qtask production plan U7, Go `sql/topk.go`): keeps only
 * the best [k] candidates while they stream past, where a sort materialized all of them.
 *
 * Entries are compared by [comparator] and then by arrival order, so ties break exactly as the
 * stable sort this replaces breaks them: the result is that sort's first [k] rows, in order.
 * Internally a max-heap whose root is the worst row kept, the one a better candidate evicts.
 */
internal class SqlTopK<T>(
    private val k: Int,
    private val comparator: Comparator<T>,
) {
    private class Entry<T>(val value: T, val seq: Int)

    private val heap = ArrayList<Entry<T>>(minOf(k, 1024))
    private var seq = 0

    /** Offers one candidate; the [k] best seen so far are kept. */
    fun offer(value: T) {
        if (k <= 0) return
        val e = Entry(value, seq++)
        if (heap.size < k) {
            heap += e
            siftUp(heap.size - 1)
            return
        }
        if (before(e, heap[0])) {
            heap[0] = e
            siftDown(0)
        }
    }

    /** The kept candidates, best first (ties in arrival order). */
    fun sorted(): List<T> = heap.sortedWith { a, b -> if (before(a, b)) -1 else if (before(b, a)) 1 else 0 }.map { it.value }

    /** Whether [a] sorts before [b]: the comparator, then arrival order. */
    private fun before(
        a: Entry<T>,
        b: Entry<T>,
    ): Boolean {
        val c = comparator.compare(a.value, b.value)
        return if (c != 0) c < 0 else a.seq < b.seq
    }

    private fun siftUp(start: Int) {
        var i = start
        while (i > 0) {
            val parent = (i - 1) / 2
            if (!before(heap[parent], heap[i])) return
            swap(i, parent)
            i = parent
        }
    }

    private fun siftDown(start: Int) {
        var i = start
        while (true) {
            val l = 2 * i + 1
            val r = l + 1
            var worst = i
            if (l < heap.size && before(heap[worst], heap[l])) worst = l
            if (r < heap.size && before(heap[worst], heap[r])) worst = r
            if (worst == i) return
            swap(i, worst)
            i = worst
        }
    }

    private fun swap(
        i: Int,
        j: Int,
    ) {
        val t = heap[i]
        heap[i] = heap[j]
        heap[j] = t
    }

    companion object {
        /** The largest LIMIT + OFFSET served by a top-k; past it the ordinary sort runs. */
        const val MAX_K: Int = 100_000
    }
}
