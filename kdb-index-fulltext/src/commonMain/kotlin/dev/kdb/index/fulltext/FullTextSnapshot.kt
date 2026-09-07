package dev.kdb.index.fulltext

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.index.CatalogEscape

/** Manifest lines of a full-text snapshot (§6.5). */
public data class FullTextSnapshotManifest(
    val formatVersion: Int,
    val indexId: String,
    val fields: List<String>,
    val weights: List<Double>,
    val headCommitHex: String,
    val documentCount: Int,
    val averageLengths: List<Double>,
)

/**
 * Versioned, self-describing text snapshot:
 *
 * ```
 * kdb-fulltext-snapshot 1
 * I <indexId>
 * H <headCommitHex>
 * F <fieldIndex> <escaped path> <weight>      (one per field)
 * N <documents with tokens>
 * A <avglen per field, space separated>
 * S <sequence counter>
 * D <docId> <seq> <commitHex>                 (a document version …)
 * L <fieldIndex> <length>                     (… its field lengths …)
 * P <fieldIndex> <escaped term> <p1,p2,…>     (… and postings)
 * T <docId> <seq> <commitHex>                 (a tombstone)
 * ```
 * Events are written in sequence order so replaying them rebuilds identical state.
 */
internal object FullTextSnapshotCodec {
    private const val HEADER = "kdb-fulltext-snapshot"

    fun write(
        store: DefaultFullTextIndexStore,
        headHex: String,
        docs: Map<KdbUuid, List<DocEvent>>,
        documentCount: Int,
        averageLengths: DoubleArray,
        seqCounter: Long,
    ): String {
        val sb = StringBuilder()
        sb.append(HEADER).append(' ').append(FULLTEXT_SNAPSHOT_FORMAT_VERSION).append('\n')
        sb.append("I ").append(store.descriptor.indexId).append('\n')
        sb.append("H ").append(headHex).append('\n')
        for (i in store.fields.indices) {
            sb.append("F ").append(i).append(' ').append(CatalogEscape.escape(store.fields[i])).append(' ').append(store.weights[i]).append('\n')
        }
        sb.append("N ").append(documentCount).append('\n')
        sb.append("A ").append(averageLengths.joinToString(" ")).append('\n')
        sb.append("S ").append(seqCounter).append('\n')
        val events = ArrayList<Pair<KdbUuid, DocEvent>>()
        for ((docId, list) in docs) for (ev in list) events += docId to ev
        events.sortBy { it.second.seq }
        for ((docId, ev) in events) {
            val version = ev.version
            if (version == null) {
                sb.append("T ").append(docId).append(' ').append(ev.seq).append(' ').append(ev.commitHash.toHex()).append('\n')
                continue
            }
            sb.append("D ").append(docId).append(' ').append(ev.seq).append(' ').append(ev.commitHash.toHex()).append('\n')
            for (f in version.fields.indices) {
                val ft = version.fields[f] ?: continue
                sb.append("L ").append(f).append(' ').append(ft.length).append('\n')
                for (term in ft.postings.keys.sorted()) {
                    sb.append("P ").append(f).append(' ').append(CatalogEscape.escape(term)).append(' ')
                    sb.append(ft.postings.getValue(term).joinToString(",")).append('\n')
                }
            }
        }
        return sb.toString()
    }

    fun manifest(text: String): FullTextSnapshotManifest {
        val lines = text.lineSequence().iterator()
        require(lines.hasNext()) { "empty snapshot" }
        val header = lines.next().split(' ')
        require(header.size == 2 && header[0] == HEADER) { "not a full-text snapshot" }
        val version = header[1].toIntOrNull() ?: error("snapshot version corrupt")
        require(version == FULLTEXT_SNAPSHOT_FORMAT_VERSION) { "unsupported full-text snapshot version $version" }
        var indexId = ""
        var head = ""
        val fields = ArrayList<String>()
        val weights = ArrayList<Double>()
        var n = 0
        var avg = emptyList<Double>()
        while (lines.hasNext()) {
            val line = lines.next()
            if (line.isEmpty()) continue
            when (line[0]) {
                'I' -> indexId = line.substring(2)
                'H' -> head = line.substring(2)
                'F' -> {
                    val parts = line.split(' ')
                    fields += CatalogEscape.unescape(parts[2])
                    weights += parts[3].toDouble()
                }
                'N' -> n = line.substring(2).toInt()
                'A' -> avg = line.substring(2).split(' ').filter { it.isNotEmpty() }.map { it.toDouble() }
                'S', 'D', 'T', 'L', 'P' -> break
                else -> error("snapshot line corrupt: $line")
            }
        }
        require(indexId.isNotEmpty() && head.isNotEmpty()) { "snapshot manifest incomplete" }
        return FullTextSnapshotManifest(version, indexId, fields, weights, head, n, avg)
    }

    fun load(
        text: String,
        into: DefaultFullTextIndexStore,
    ) {
        val fieldCount = into.fields.size
        var docId: KdbUuid? = null
        var seq = 0L
        var commit: KdbHash? = null
        var tokens: Array<FieldTokens?>? = null
        var lengths = IntArray(fieldCount)
        var postings: Array<HashMap<String, IntArray>?> = arrayOfNulls(fieldCount)

        fun flushVersion() {
            val id = docId ?: return
            val t = tokens ?: return
            for (f in 0 until fieldCount) {
                val p = postings[f] ?: continue
                t[f] = FieldTokens(lengths[f], p)
            }
            into.ingestVersion(id, seq, commit!!, t)
            docId = null
            tokens = null
        }

        for (line in text.lineSequence()) {
            if (line.isEmpty()) continue
            when (line[0]) {
                'D' -> {
                    flushVersion()
                    val parts = line.split(' ')
                    docId = KdbUuid.fromString(parts[1])
                    seq = parts[2].toLong()
                    commit = KdbHash.fromHex(parts[3])
                    tokens = arrayOfNulls(fieldCount)
                    lengths = IntArray(fieldCount)
                    postings = arrayOfNulls(fieldCount)
                }
                'L' -> {
                    val parts = line.split(' ')
                    val f = parts[1].toInt()
                    lengths[f] = parts[2].toInt()
                    if (postings[f] == null) postings[f] = HashMap()
                }
                'P' -> {
                    val parts = line.split(' ')
                    val f = parts[1].toInt()
                    val term = CatalogEscape.unescape(parts[2])
                    val positions = parts[3].split(',').filter { it.isNotEmpty() }.map { it.toInt() }.toIntArray()
                    (postings[f] ?: HashMap<String, IntArray>().also { postings[f] = it })[term] = positions
                }
                'T' -> {
                    flushVersion()
                    val parts = line.split(' ')
                    into.ingestTombstone(KdbUuid.fromString(parts[1]), parts[2].toLong(), KdbHash.fromHex(parts[3]))
                }
                else -> Unit // manifest lines
            }
        }
        flushVersion()
    }
}
