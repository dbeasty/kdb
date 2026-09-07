package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

public data class VectorSnapshotManifest(
    val formatVersion: Int,
    val indexId: String,
    val headCommitHex: String,
    val dimensions: Int,
    val metric: VectorMetric,
)

/**
 * Versioned, self-describing text snapshot (§6.5, §7). The graph is not persisted; it is rebuilt
 * from the stored vectors on load.
 *
 * ```
 * kdb-vector-snapshot 1
 * I <indexId>
 * H <headCommitHex>
 * D <dimensions>
 * M <metric>
 * S <sequence counter>
 * V <docId> <seq> <commitHex> <f1,f2,…|->   (version; `-` = no vector at the path)
 * T <docId> <seq> <commitHex>               (tombstone)
 * ```
 */
internal object VectorSnapshotCodec {
    private const val HEADER = "kdb-vector-snapshot"

    fun write(
        store: DefaultVectorIndexStore,
        headHex: String,
        docs: Map<KdbUuid, List<VecEvent>>,
        seqCounter: Long,
    ): String {
        val sb = StringBuilder()
        sb.append(HEADER).append(' ').append(VECTOR_SNAPSHOT_FORMAT_VERSION).append('\n')
        sb.append("I ").append(store.descriptor.indexId).append('\n')
        sb.append("H ").append(headHex).append('\n')
        sb.append("D ").append(store.dimensions).append('\n')
        sb.append("M ").append(store.metric.optionName).append('\n')
        sb.append("S ").append(seqCounter).append('\n')
        val events = ArrayList<Pair<KdbUuid, VecEvent>>()
        for ((docId, list) in docs) for (ev in list) events += docId to ev
        events.sortBy { it.second.seq }
        for ((docId, ev) in events) {
            val node = ev.node
            if (node == null) {
                sb.append("T ").append(docId).append(' ').append(ev.seq).append(' ').append(ev.commitHash.toHex()).append('\n')
                continue
            }
            sb.append("V ").append(docId).append(' ').append(ev.seq).append(' ').append(ev.commitHash.toHex()).append(' ')
            sb.append(node.vector.joinToString(",") { it.toString() }).append('\n')
        }
        return sb.toString()
    }

    fun manifest(text: String): VectorSnapshotManifest {
        val lines = text.lineSequence().iterator()
        require(lines.hasNext()) { "empty snapshot" }
        val header = lines.next().split(' ')
        require(header.size == 2 && header[0] == HEADER) { "not a vector snapshot" }
        val version = header[1].toIntOrNull() ?: error("snapshot version corrupt")
        require(version == VECTOR_SNAPSHOT_FORMAT_VERSION) { "unsupported vector snapshot version $version" }
        var indexId = ""
        var head = ""
        var dims = 0
        var metric = VectorMetric.COSINE
        while (lines.hasNext()) {
            val line = lines.next()
            if (line.isEmpty()) continue
            when (line[0]) {
                'I' -> indexId = line.substring(2)
                'H' -> head = line.substring(2)
                'D' -> dims = line.substring(2).toInt()
                'M' -> metric = VectorMetric.fromOption(line.substring(2))
                'S', 'V', 'T' -> break
                else -> error("snapshot line corrupt: $line")
            }
        }
        require(indexId.isNotEmpty() && head.isNotEmpty() && dims > 0) { "snapshot manifest incomplete" }
        return VectorSnapshotManifest(version, indexId, head, dims, metric)
    }

    fun load(
        text: String,
        into: DefaultVectorIndexStore,
    ) {
        for (line in text.lineSequence()) {
            if (line.isEmpty()) continue
            when (line[0]) {
                'V' -> {
                    val parts = line.split(' ')
                    val vector =
                        if (parts[4] == "-") null else parts[4].split(',').map { it.toFloat() }.toFloatArray()
                    into.ingestVersion(KdbUuid.fromString(parts[1]), parts[2].toLong(), KdbHash.fromHex(parts[3]), vector)
                }
                'T' -> {
                    val parts = line.split(' ')
                    into.ingestTombstone(KdbUuid.fromString(parts[1]), parts[2].toLong(), KdbHash.fromHex(parts[3]))
                }
                else -> Unit
            }
        }
    }
}
