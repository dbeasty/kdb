package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexStore
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.IndexTypeMismatchException
import dev.kdb.index.RankedResult
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

private data class VectorNode(
    val docId: KdbUuid,
    val embedding: FloatArray,
    val commitHash: KdbHash,
)

public class DefaultVectorIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    public val dimensions: Int,
    private val config: HnswConfig = HnswConfig.DEFAULT,
) : IndexStore {

    private val mutex = Mutex()
    private val nodes = mutableMapOf<KdbUuid, VectorNode>()

    override suspend fun put(entry: IndexEntry) {
        val key =
            entry.key as? IndexKey.VectorKey
                ?: throw IndexTypeMismatchException(
                    "VECTOR requires VectorKey",
                    descriptor.fieldName,
                    IndexType.VECTOR,
                    descriptor.type,
                )
        val emb = key.asFloatArray()
        if (emb.size != dimensions) {
            throw VectorDimensionMismatchException(
                "expected $dimensions dimensions, got ${emb.size}",
                dimensions,
                emb.size,
            )
        }
        mutex.withLock {
            nodes[entry.docId] = VectorNode(entry.docId, emb.copyOf(), entry.commitHash)
        }
    }

    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        mutex.withLock {
            nodes.remove(docId)
        }
    }

    override suspend fun bulkLoad(entries: List<IndexEntry>) {
        mutex.withLock {
            nodes.clear()
            for (e in entries) {
                putLocked(e)
            }
        }
    }

    override suspend fun rebuild(entries: List<IndexEntry>) {
        bulkLoad(entries)
    }

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException(
            "lookup not on VECTOR",
            descriptor.fieldName,
            IndexType.HASH,
            IndexType.VECTOR,
        )

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException(
            "range not on VECTOR",
            descriptor.fieldName,
            IndexType.BTREE,
            IndexType.VECTOR,
        )

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<RankedResult> =
        throw IndexTypeMismatchException(
            "search not on VECTOR",
            descriptor.fieldName,
            IndexType.FULLTEXT,
            IndexType.VECTOR,
        )

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> {
        if (queryVector.size != dimensions) {
            throw VectorDimensionMismatchException(
                "query vector dimension mismatch",
                dimensions,
                queryVector.size,
            )
        }
        val cutoff = atCommit ?: dag.head()
        val ranked =
            mutex.withLock {
                nodes.values
                    .filter { dag.isAncestor(it.commitHash, cutoff) }
                    .map { node ->
                        RankedResult(node.docId, score(queryVector, node.embedding))
                    }.sortedByDescending { it.score }
            }
        return ranked.take(k.coerceAtLeast(0))
    }

    override suspend fun clear() {
        mutex.withLock { nodes.clear() }
    }

    override suspend fun isValid(atCommit: KdbHash): Boolean = dag.hasCommit(atCommit)

    override suspend fun snapshot(): ByteArray =
        mutex.withLock {
            nodes.values
                .joinToString("\n") { n ->
                    "${n.docId}|${n.commitHash}|${n.embedding.joinToString(",")}"
                }.encodeToByteArray()
        }

    override suspend fun restoreSnapshot(data: ByteArray) {
        mutex.withLock {
            nodes.clear()
            for (line in data.decodeToString().lines().filter { it.isNotBlank() }) {
                val parts = line.split('|', limit = 3)
                val docId = KdbUuid.fromString(parts[0])
                val commit = KdbHash.fromHex(parts[1])
                val emb = parts[2].split(',').map { it.toFloat() }.toFloatArray()
                nodes[docId] = VectorNode(docId, emb, commit)
            }
        }
    }

    private fun putLocked(entry: IndexEntry) {
        val key = entry.key as IndexKey.VectorKey
        val emb = key.asFloatArray()
        if (emb.size != dimensions) {
            throw VectorDimensionMismatchException(
                "expected $dimensions dimensions, got ${emb.size}",
                dimensions,
                emb.size,
            )
        }
        nodes[entry.docId] = VectorNode(entry.docId, emb.copyOf(), entry.commitHash)
    }

    private fun score(
        query: FloatArray,
        vector: FloatArray,
    ): Float =
        when (config.metric) {
            VectorMetric.COSINE -> cosineSimilarity(query, vector)
            VectorMetric.L2 -> {
                val d = l2(query, vector)
                (1.0 / (1.0 + d)).toFloat()
            }
            VectorMetric.INNER_PRODUCT -> {
                var dot = 0f
                for (i in query.indices) dot += query[i] * vector[i]
                dot
            }
        }

    private fun cosineSimilarity(
        a: FloatArray,
        b: FloatArray,
    ): Float {
        var dot = 0f
        var na = 0f
        var nb = 0f
        for (i in a.indices) {
            dot += a[i] * b[i]
            na += a[i] * a[i]
            nb += b[i] * b[i]
        }
        val denom = sqrt(na) * sqrt(nb)
        return if (denom == 0f) 0f else dot / denom
    }

    private fun l2(
        a: FloatArray,
        b: FloatArray,
    ): Float {
        var sum = 0f
        for (i in a.indices) {
            val d = a[i] - b[i]
            sum += d * d
        }
        return sqrt(sum)
    }
}

public fun vectorIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    dimensions: Int,
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.VECTOR) {
            "VectorIndexStoreFactory expected VECTOR, got ${descriptor.type}"
        }
        DefaultVectorIndexStore(descriptor, dag, storage, dimensions)
    }
