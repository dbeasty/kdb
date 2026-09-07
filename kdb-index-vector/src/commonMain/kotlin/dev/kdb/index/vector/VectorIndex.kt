package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.index.DocumentIndexStore
import dev.kdb.index.IndexBlobStore
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.IndexTypeMismatchException
import dev.kdb.index.RankedResult
import dev.kdb.index.SnapshotRestoreResult
import dev.kdb.index.SnapshotRestoreStatus
import dev.kdb.index.documentPathCandidates
import dev.kdb.index.indexSnapshotBlobKey
import dev.kdb.index.parseDocumentForIndex
import dev.kdb.index.storageAdapterIndexBlobStore
import dev.kdb.json.JsonValue
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.sqrt

public class VectorDimensionMismatchException(
    message: String,
    val expected: Int,
    val actual: Int,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public enum class VectorMetric {
    COSINE,
    L2,
    INNER_PRODUCT,
    ;

    /** The DDL / option spelling (`cosine`, `l2`, `inner_product`). */
    public val optionName: String get() = if (this == INNER_PRODUCT) "inner_product" else name.lowercase()

    public companion object {
        public fun fromOption(value: String): VectorMetric =
            when (value.trim().lowercase()) {
                "cosine" -> COSINE
                "l2", "euclidean" -> L2
                "inner_product", "ip", "dot" -> INNER_PRODUCT
                else -> throw IllegalArgumentException("unknown vector metric: $value")
            }
    }
}

public data class HnswConfig(
    val m: Int = 16,
    val efConstruction: Int = 200,
    val efSearch: Int = 64,
    val metric: VectorMetric = VectorMetric.COSINE,
) {
    public companion object {
        public val DEFAULT: HnswConfig = HnswConfig()
    }
}

/** Live vectors at or below which exact search is used; above it, HNSW (§7). */
public const val DEFAULT_EXACT_THRESHOLD: Int = 1000

/** Snapshot format written by [DefaultVectorIndexStore.snapshot]. */
public const val VECTOR_SNAPSHOT_FORMAT_VERSION: Int = 1

/**
 * Metric scores (§7); higher is always better. Every metric accumulates in double precision and
 * rounds once at the end, so both trees land on the same `float32` well inside the 1e-5 tolerance.
 */
public object VectorMetrics {
    public fun score(
        metric: VectorMetric,
        query: FloatArray,
        vector: FloatArray,
    ): Float =
        when (metric) {
            VectorMetric.COSINE -> cosine(query, vector)
            VectorMetric.L2 -> (1.0 / (1.0 + l2(query, vector))).toFloat()
            VectorMetric.INNER_PRODUCT -> dot(query, vector).toFloat()
        }

    public fun dot(
        a: FloatArray,
        b: FloatArray,
    ): Double {
        var d = 0.0
        for (i in a.indices) d += a[i].toDouble() * b[i].toDouble()
        return d
    }

    public fun cosine(
        a: FloatArray,
        b: FloatArray,
    ): Float {
        var d = 0.0
        var na = 0.0
        var nb = 0.0
        for (i in a.indices) {
            val x = a[i].toDouble()
            val y = b[i].toDouble()
            d += x * y
            na += x * x
            nb += y * y
        }
        val denom = sqrt(na) * sqrt(nb)
        return if (denom == 0.0) 0f else (d / denom).toFloat()
    }

    public fun l2(
        a: FloatArray,
        b: FloatArray,
    ): Double {
        var s = 0.0
        for (i in a.indices) {
            val diff = a[i].toDouble() - b[i].toDouble()
            s += diff * diff
        }
        return sqrt(s)
    }
}

/** One indexed vector: its graph node id and the document version it belongs to. */
internal class VecNode(
    val docId: KdbUuid,
    val vector: FloatArray,
    val node: Int,
)

/** A put ([node] non-null) or a tombstone, at one commit. */
internal class VecEvent(
    val seq: Long,
    val commitHash: KdbHash,
    val node: VecNode?,
)

/**
 * Vector index (Layer 16, Component 65): exact brute-force search — the oracle — up to
 * [exactThreshold] live vectors, HNSW above it. A negative threshold forces HNSW at every size,
 * which is how the recall test drives the graph.
 *
 * Deletes are tombstones: a read `atCommit` earlier than the tombstone still sees the vector,
 * head reads do not. HNSW nodes are never removed — a node whose document version is no longer
 * visible still routes searches but is not collected, so recall stays stable after rewrites.
 *
 * Descriptor options override the constructor defaults: `dimensions`, `metric`, `m`,
 * `ef_construction`, `ef_search`, `exact_threshold`.
 */
public class DefaultVectorIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    storage: StorageAdapter,
    dimensions: Int,
    config: HnswConfig = HnswConfig.DEFAULT,
    exactThreshold: Int = DEFAULT_EXACT_THRESHOLD,
    private val blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
    private val flushEvery: Int = 64,
) : DocumentIndexStore {

    public val dimensions: Int = descriptor.options["dimensions"]?.trim()?.toIntOrNull() ?: dimensions
    public val config: HnswConfig =
        HnswConfig(
            m = descriptor.options["m"]?.trim()?.toIntOrNull() ?: config.m,
            efConstruction = descriptor.options["ef_construction"]?.trim()?.toIntOrNull() ?: config.efConstruction,
            efSearch = descriptor.options["ef_search"]?.trim()?.toIntOrNull() ?: config.efSearch,
            metric = descriptor.options["metric"]?.let { VectorMetric.fromOption(it) } ?: config.metric,
        )
    public val exactThreshold: Int = descriptor.options["exact_threshold"]?.trim()?.toIntOrNull() ?: exactThreshold
    public val metric: VectorMetric get() = config.metric

    /** The indexed JSON path (the descriptor's first field). */
    public val fieldPath: String = descriptor.fields.firstOrNull() ?: descriptor.fieldName

    private val mutex = Mutex()
    private val docs = HashMap<KdbUuid, MutableList<VecEvent>>()
    private val nodes = ArrayList<VecNode>()
    private var graph = newGraph()
    private var seqCounter = 0L
    private var lastCommit: KdbHash? = null
    private var commitsSinceFlush = 0

    init {
        require(this.dimensions > 0) { "vector index ${descriptor.indexId}: dimensions must be a positive integer" }
        require(this.config.m >= 2) { "vector index ${descriptor.indexId}: option m must be at least 2" }
    }

    private fun newGraph() = HnswGraph(config.m, config.efConstruction) { a, b -> VectorMetrics.score(config.metric, a, b) }

    /** Vectors visible at [atCommit] (null = head). */
    public suspend fun liveVectorCount(atCommit: KdbHash? = null): Int = viewAt(atCommit).size

    // ---------------------------------------------------------------- extraction

    /**
     * The vector at the indexed path: null when the path is absent or holds no array (which
     * indexes as "no vector"). Throws [VectorDimensionMismatchException] on a wrong-length array
     * and on a non-numeric element, both of which are the document's fault (§7, §10).
     */
    public fun extractVector(json: String): FloatArray? {
        val root = parseDocumentForIndex(json) ?: return null
        return extractVector(root)
    }

    private fun extractVector(root: JsonValue): FloatArray? {
        val array =
            documentPathCandidates(root, fieldPath, flattenFinalArray = false)
                .firstOrNull { it is JsonValue.JArray } as? JsonValue.JArray
                ?: return null
        val out = FloatArray(array.elements.size)
        for ((i, el) in array.elements.withIndex()) {
            out[i] =
                when (el) {
                    is JsonValue.JNumber -> el.value.toFloat()
                    is JsonValue.JInt -> el.value.toFloat()
                    else ->
                        throw VectorDimensionMismatchException(
                            "vector at $fieldPath: element $i is not a number",
                            dimensions,
                            array.elements.size,
                        )
                }
        }
        checkDimensions(out.size)
        return out
    }

    private fun checkDimensions(actual: Int) {
        if (actual != dimensions) {
            throw VectorDimensionMismatchException("expected $dimensions dimensions, got $actual", dimensions, actual)
        }
    }

    override fun validateDocument(
        docId: KdbUuid,
        json: String,
    ) {
        extractVector(json)
    }

    // ---------------------------------------------------------------- writes

    override suspend fun putDocument(
        docId: KdbUuid,
        commitHash: KdbHash,
        json: String,
    ) {
        val vector = extractVector(json)
        mutex.withLock {
            noteCommitLocked(commitHash)
            appendEventLocked(docId, commitHash, vector)
        }
    }

    override suspend fun put(entry: IndexEntry) {
        when (val key = entry.key) {
            is IndexKey.VectorKey -> {
                val emb = key.asFloatArray()
                checkDimensions(emb.size)
                mutex.withLock {
                    noteCommitLocked(entry.commitHash)
                    appendEventLocked(entry.docId, entry.commitHash, emb)
                }
            }

            is IndexKey.StringKey -> putDocument(entry.docId, entry.commitHash, key.value)

            else ->
                throw IndexTypeMismatchException(
                    "VECTOR requires a VectorKey or a StringKey holding the document JSON",
                    descriptor.fieldName,
                    IndexType.VECTOR,
                    descriptor.type,
                )
        }
    }

    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        mutex.withLock {
            noteCommitLocked(atCommit)
            appendEventLocked(docId, atCommit, null)
        }
    }

    override suspend fun bulkLoad(entries: List<IndexEntry>) {
        clear()
        for (e in entries) put(e)
    }

    override suspend fun rebuild(entries: List<IndexEntry>) {
        bulkLoad(entries)
    }

    override suspend fun clear() {
        mutex.withLock { clearLocked() }
    }

    private fun clearLocked() {
        docs.clear()
        nodes.clear()
        graph = newGraph()
        seqCounter = 0L
        lastCommit = null
        commitsSinceFlush = 0
    }

    private suspend fun noteCommitLocked(commitHash: KdbHash) {
        val previous = lastCommit
        if (previous == commitHash) return
        if (previous != null) {
            commitsSinceFlush++
            if (flushEvery > 0 && commitsSinceFlush >= flushEvery) {
                writeSnapshotLocked(previous.toHex())
                commitsSinceFlush = 0
            }
        }
        lastCommit = commitHash
    }

    private fun appendEventLocked(
        docId: KdbUuid,
        commitHash: KdbHash,
        vector: FloatArray?,
    ) {
        seqCounter++
        val node =
            vector?.let {
                val id = graph.add(it, hnswLevelFor(docId, config.m))
                val n = VecNode(docId, it, id)
                while (nodes.size <= id) nodes.add(n)
                nodes[id] = n
                n
            }
        docs.getOrPut(docId) { mutableListOf() } += VecEvent(seqCounter, commitHash, node)
    }

    // ---------------------------------------------------------------- reads

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException("lookup not on VECTOR", descriptor.fieldName, IndexType.HASH, IndexType.VECTOR)

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException("range not on VECTOR", descriptor.fieldName, IndexType.BTREE, IndexType.VECTOR)

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<RankedResult> =
        throw IndexTypeMismatchException("search not on VECTOR", descriptor.fieldName, IndexType.FULLTEXT, IndexType.VECTOR)

    /** The vector visible at [atCommit] for each document that has one. */
    private suspend fun viewAt(atCommit: KdbHash?): Map<KdbUuid, VecNode> {
        val cutoff = atCommit ?: dag.head()
        val ancestry = HashMap<KdbHash, Boolean>()
        val events = mutex.withLock { docs.entries.map { it.key to it.value.toList() } }
        val visible = HashMap<KdbUuid, VecNode>()
        for ((docId, log) in events) {
            var last: VecEvent? = null
            for (ev in log) {
                if (ancestry.getOrPut(ev.commitHash) { dag.isAncestor(ev.commitHash, cutoff) }) last = ev
            }
            visible[docId] = last?.node ?: continue
        }
        return visible
    }

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> {
        checkDimensions(queryVector.size)
        if (k <= 0) return emptyList()
        val view = viewAt(atCommit)
        if (view.isEmpty()) return emptyList()
        if (exactThreshold >= 0 && view.size <= exactThreshold) return exact(queryVector, k, view)
        return approximate(queryVector, k, view)
    }

    /** Brute force over every vector visible at [atCommit] — the oracle, whatever the size (§7). */
    public suspend fun exactNearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash? = null,
    ): List<RankedResult> {
        checkDimensions(queryVector.size)
        if (k <= 0) return emptyList()
        return exact(queryVector, k, viewAt(atCommit))
    }

    private fun exact(
        query: FloatArray,
        k: Int,
        view: Map<KdbUuid, VecNode>,
    ): List<RankedResult> {
        val hits = ArrayList<RankedResult>(view.size)
        for ((docId, node) in view) hits += RankedResult(docId, VectorMetrics.score(config.metric, query, node.vector))
        return rank(hits, k)
    }

    private suspend fun approximate(
        query: FloatArray,
        k: Int,
        view: Map<KdbUuid, VecNode>,
    ): List<RankedResult> {
        val ef = maxOf(config.efSearch, k)
        val (found, _) =
            mutex.withLock {
                graph.search(query, ef) { id -> id < nodes.size && view[nodes[id].docId] === nodes[id] }
            }
        val hits = ArrayList<RankedResult>(found.size)
        for (id in found) {
            val node = nodes.getOrNull(id) ?: continue
            hits += RankedResult(node.docId, VectorMetrics.score(config.metric, query, node.vector))
        }
        return rank(hits, k)
    }

    private fun rank(
        hits: MutableList<RankedResult>,
        k: Int,
    ): List<RankedResult> {
        hits.sortWith(compareByDescending<RankedResult> { it.score }.thenBy { it.docId.toString() })
        return if (hits.size > k) hits.subList(0, k).toList() else hits
    }

    override suspend fun isValid(atCommit: KdbHash): Boolean = dag.hasCommit(atCommit)

    // ---------------------------------------------------------------- persistence

    override suspend fun flush() {
        val head = dag.head().toHex()
        mutex.withLock {
            writeSnapshotLocked(head)
            commitsSinceFlush = 0
        }
    }

    private suspend fun writeSnapshotLocked(headHex: String) {
        blobs.write(indexSnapshotBlobKey(descriptor.indexId), snapshotLocked(headHex))
    }

    override suspend fun snapshot(): ByteArray {
        val head = dag.head().toHex()
        return mutex.withLock { snapshotLocked(head) }
    }

    override suspend fun restoreSnapshot(data: ByteArray) {
        mutex.withLock {
            clearLocked()
            VectorSnapshotCodec.load(data.decodeToString(), this)
        }
    }

    override suspend fun restoreFromStorage(): SnapshotRestoreResult {
        val bytes =
            blobs.read(indexSnapshotBlobKey(descriptor.indexId))
                ?: return SnapshotRestoreResult(SnapshotRestoreStatus.MISSING, null, "no snapshot for ${descriptor.indexId}")
        val text = bytes.decodeToString()
        val manifest =
            try {
                VectorSnapshotCodec.manifest(text)
            } catch (e: Throwable) {
                return SnapshotRestoreResult(SnapshotRestoreStatus.CORRUPT, null, e.message ?: "corrupt snapshot")
            }
        if (manifest.indexId != descriptor.indexId.toString()) {
            return SnapshotRestoreResult(
                SnapshotRestoreStatus.CORRUPT,
                manifest.headCommitHex,
                "snapshot belongs to index ${manifest.indexId}",
            )
        }
        if (manifest.dimensions != dimensions) {
            return SnapshotRestoreResult(
                SnapshotRestoreStatus.CORRUPT,
                manifest.headCommitHex,
                "snapshot has ${manifest.dimensions} dimensions, index has $dimensions",
            )
        }
        val head = dag.head().toHex()
        if (manifest.headCommitHex != head) {
            return SnapshotRestoreResult(
                SnapshotRestoreStatus.STALE,
                manifest.headCommitHex,
                "snapshot head ${manifest.headCommitHex} != DAG head $head",
            )
        }
        return mutex.withLock {
            clearLocked()
            try {
                VectorSnapshotCodec.load(text, this)
                SnapshotRestoreResult(SnapshotRestoreStatus.RESTORED, manifest.headCommitHex)
            } catch (e: Throwable) {
                clearLocked()
                SnapshotRestoreResult(SnapshotRestoreStatus.CORRUPT, manifest.headCommitHex, e.message ?: "corrupt snapshot")
            }
        }
    }

    private fun snapshotLocked(headHex: String): ByteArray = VectorSnapshotCodec.write(this, headHex, docs, seqCounter).encodeToByteArray()

    /** Replays one snapshot event (the caller holds the lock); the graph is rebuilt as it goes. */
    internal fun ingestVersion(
        docId: KdbUuid,
        seq: Long,
        commitHash: KdbHash,
        vector: FloatArray?,
    ) {
        vector?.let { checkDimensions(it.size) }
        seqCounter = maxOf(seqCounter, seq - 1)
        appendEventLocked(docId, commitHash, vector)
        lastCommit = commitHash
    }

    internal fun ingestTombstone(
        docId: KdbUuid,
        seq: Long,
        commitHash: KdbHash,
    ) {
        seqCounter = maxOf(seqCounter, seq - 1)
        appendEventLocked(docId, commitHash, null)
        lastCommit = commitHash
    }
}

public fun vectorIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    dimensions: Int,
    config: HnswConfig = HnswConfig.DEFAULT,
    exactThreshold: Int = DEFAULT_EXACT_THRESHOLD,
    blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
    flushEvery: Int = 64,
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.VECTOR) {
            "VectorIndexStoreFactory expected VECTOR, got ${descriptor.type}"
        }
        DefaultVectorIndexStore(descriptor, dag, storage, dimensions, config, exactThreshold, blobs, flushEvery)
    }
