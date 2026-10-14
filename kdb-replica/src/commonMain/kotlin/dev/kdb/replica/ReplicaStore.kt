package dev.kdb.replica

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A set of writes applied atomically; a null value deletes the key. Insertion-ordered, so a key
 * written twice in one batch keeps its last value.
 */
public typealias Batch = LinkedHashMap<String, ByteArray?>

/**
 * The replica's storage: a small ordered key-value interface (packages/kdb-replica/src/store.ts),
 * so the same engine runs over any backend. Keys are compared as Kotlin strings (UTF-16 code
 * units), which is the order the TS replica's stores use too.
 *
 * Nothing in the engine loads a store whole: documents, trie nodes, commits and outbox entries
 * are separate keys (plan §5), so a replica can hold namespaces larger than its memory.
 *
 * The durability rule is the one the engine relies on: [apply] is one atomic transaction, and it
 * returns only once that transaction is durable. A local write is acknowledged only after its
 * apply returns, so an unpushed write survives a crash.
 *
 * **Persistent backend: not built yet, deliberately.** Only [MemoryReplicaStore] exists. The
 * owner chose the Kotlin embed runtime (`FileEmbeddedRuntime`, kdb-jdbc) as the replica's on-disk
 * store (plan §3.3, decision D12), so a replica's data directory is one Go tools can read. That
 * needs an on-disk format decision that is still pending: how the replica's metadata, trie nodes
 * and outbox map onto the runtime's delta log and checkpoints, and shallow-root support in
 * Kotlin's replayer (plan step 5a). Until then, a persistent backend would be a second format to
 * migrate away from, so this interface is the seam it will plug into.
 */
public interface ReplicaStore {
    public suspend fun get(key: String): ByteArray?

    public suspend fun getMany(keys: List<String>): List<ByteArray?>

    /** Entries with `start <= key < end`, in key order, at most [limit] of them. */
    public suspend fun scan(start: String, end: String, limit: Int = Int.MAX_VALUE): List<Pair<String, ByteArray>>

    /** Applies [batch] atomically; returns once it is durable. */
    public suspend fun apply(batch: Batch)

    public suspend fun close()
}

/** The smallest key greater than every key with this prefix. */
public fun prefixEnd(prefix: String): String = prefix + '￿'

/** An in-memory store, for tests and for replicas that need no persistence. */
public class MemoryReplicaStore : ReplicaStore {
    private val data = HashMap<String, ByteArray>()
    private var sorted: List<String>? = null
    private val lock = Mutex()

    /** Counts apply calls; tests use it to check atomicity. */
    public var applies: Int = 0
        private set

    /** Set to make the next apply fail before writing anything (crash and failure tests). */
    public var failNextApply: Throwable? = null

    override suspend fun get(key: String): ByteArray? = lock.withLock { data[key] }

    override suspend fun getMany(keys: List<String>): List<ByteArray?> = lock.withLock { keys.map { data[it] } }

    override suspend fun scan(start: String, end: String, limit: Int): List<Pair<String, ByteArray>> =
        lock.withLock {
            val keys = sorted ?: data.keys.sorted().also { sorted = it }
            var lo = 0
            var hi = keys.size
            while (lo < hi) {
                val mid = (lo + hi) ushr 1
                if (keys[mid] < start) lo = mid + 1 else hi = mid
            }
            val out = ArrayList<Pair<String, ByteArray>>()
            var i = lo
            while (i < keys.size && out.size < limit) {
                val k = keys[i]
                if (k >= end) break
                out.add(k to data.getValue(k))
                i++
            }
            out
        }

    override suspend fun apply(batch: Batch) {
        lock.withLock {
            failNextApply?.let {
                failNextApply = null
                throw it
            }
            applies++
            for ((k, v) in batch) {
                if (v == null) {
                    if (data.remove(k) != null) sorted = null
                } else {
                    if (data.put(k, v) == null) sorted = null
                }
            }
        }
    }

    override suspend fun close() {}

    /** The bytes held (keys as UTF-16 length, values as bytes): what a test compares with memory. */
    public val size: Long
        get() = data.entries.sumOf { (it.key.length + it.value.size).toLong() }
}

/**
 * A read cache in front of a store, bounded by bytes, least recently used out first. Every read
 * of the trie and of documents goes through one of these, so the replica's resident set is the
 * cache's budget plus whatever the operation in progress holds, whatever the store's size.
 */
public class CachedReplicaStore(
    private val inner: ReplicaStore,
    private val budgetBytes: Long,
) : ReplicaStore {
    // An access-ordered LinkedHashMap would do on the JVM; this keeps to common code with an
    // insertion-ordered map, re-inserting on a hit, so the first key is the least recently used.
    private val cache = LinkedHashMap<String, ByteArray?>()
    private var bytes = 0L
    private val lock = Mutex()
    public var hits: Long = 0
        private set
    public var misses: Long = 0
        private set

    public val residentBytes: Long get() = bytes

    private fun cost(key: String, v: ByteArray?): Long = key.length * 2L + (v?.size ?: 0) + 48

    private fun remember(key: String, v: ByteArray?) {
        if (cache.containsKey(key)) {
            bytes -= cost(key, cache.remove(key))
        }
        val c = cost(key, v)
        if (c > budgetBytes / 4) return // one entry never takes a large share of the cache
        cache[key] = v
        bytes += c
        val it = cache.entries.iterator()
        while (bytes > budgetBytes && it.hasNext()) {
            val e = it.next()
            bytes -= cost(e.key, e.value)
            it.remove()
        }
    }

    override suspend fun get(key: String): ByteArray? {
        lock.withLock {
            if (cache.containsKey(key)) {
                hits++
                val v = cache.remove(key)
                cache[key] = v
                return v
            }
            misses++
        }
        val v = inner.get(key)
        lock.withLock { remember(key, v) }
        return v
    }

    override suspend fun getMany(keys: List<String>): List<ByteArray?> = keys.map { get(it) }

    override suspend fun scan(start: String, end: String, limit: Int): List<Pair<String, ByteArray>> =
        inner.scan(start, end, limit)

    override suspend fun apply(batch: Batch) {
        inner.apply(batch)
        lock.withLock { for ((k, v) in batch) remember(k, v) }
    }

    override suspend fun close() {
        lock.withLock {
            cache.clear()
            bytes = 0
        }
        inner.close()
    }
}
