package dev.kdb.index.fulltext

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
import kotlin.math.ln

public class FullTextQueryException(
    message: String,
    val query: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

/**
 * A parsed query (§6.4): [terms] are the distinct analyzed terms of the whole query — free terms
 * and phrase terms alike, in first-seen order — and [phrases] the analyzed phrases that must
 * occur contiguously. [fuzzy] is the explicit opt-in for Levenshtein expansion of unknown terms;
 * it is off by default and Go does not implement it.
 */
public data class FullTextQuery(
    val terms: List<String>,
    val phrases: List<List<String>> = emptyList(),
    val fuzzy: Boolean = false,
) {
    /** First phrase re-joined, for callers written against the single-phrase shape. */
    val phrase: String? get() = phrases.firstOrNull()?.joinToString(" ")
}

/**
 * Analyzes a query string. Text inside double quotes is a phrase (an unterminated quote runs to
 * the end of the string); everything else contributes free terms. Terms are deduplicated in
 * first-seen order; a phrase that analyzes to nothing (all stopwords) imposes no constraint.
 */
public fun parseFullTextQuery(raw: String): FullTextQuery {
    val terms = LinkedHashSet<String>()
    val phrases = mutableListOf<List<String>>()
    var rest = raw
    while (true) {
        val open = rest.indexOf('"')
        if (open < 0) {
            terms += FullTextAnalyzer.analyze(rest)
            break
        }
        terms += FullTextAnalyzer.analyze(rest.substring(0, open))
        rest = rest.substring(open + 1)
        val close = rest.indexOf('"')
        val phraseText: String
        if (close < 0) {
            phraseText = rest
            rest = ""
        } else {
            phraseText = rest.substring(0, close)
            rest = rest.substring(close + 1)
        }
        val phrase = FullTextAnalyzer.analyze(phraseText)
        terms += phrase
        if (phrase.isNotEmpty()) phrases += phrase
        if (close < 0) break
    }
    return FullTextQuery(terms.toList(), phrases)
}

public interface TextTokenizer {
    public fun tokenize(text: String): List<String>
}

/** The §6.1 analyzer behind the [TextTokenizer] seam (positions = list indexes). */
public class UnicodeTokenizer : TextTokenizer {
    override fun tokenize(text: String): List<String> = FullTextAnalyzer.analyze(text)
}

public data class FuzzyMatchConfig(
    val maxEditDistance: Int = 2,
    val prefixLength: Int = 3,
    val maxCandidatesPerTerm: Int = 32,
) {
    public companion object {
        public val DEFAULT: FuzzyMatchConfig = FuzzyMatchConfig()
    }
}

/** BM25 constants fixed by §6.4. */
public const val BM25_K1: Double = 1.2
public const val BM25_B: Double = 0.75

/**
 * Position gap between consecutive array elements of one field (§6.3): the tokens of element
 * `i+1` start one slot after the last token of element `i`, so a phrase never spans two elements.
 */
public const val ARRAY_POSITION_GAP: Int = 1

/** Snapshot format written by [DefaultFullTextIndexStore.snapshot] (§6.5). */
public const val FULLTEXT_SNAPSHOT_FORMAT_VERSION: Int = 1

/** Default number of commits between automatic snapshot flushes (§6.5). */
public const val DEFAULT_FLUSH_EVERY: Int = 64

/** Parses `options["weights"]` (`title=3,description=1`); fields not named keep weight 1. */
public fun parseFieldWeights(
    fields: List<String>,
    options: Map<String, String>,
): DoubleArray {
    val weights = DoubleArray(fields.size) { 1.0 }
    val raw = options["weights"]?.trim() ?: return weights
    if (raw.isEmpty()) return weights
    val byName = HashMap<String, Int>(fields.size)
    for (i in fields.indices) byName[fields[i]] = i
    for (part in raw.split(',')) {
        val entry = part.trim()
        if (entry.isEmpty()) continue
        val eq = entry.indexOf('=')
        require(eq > 0) { "index option weights: malformed entry \"$entry\"" }
        val name = entry.substring(0, eq).trim()
        val value = entry.substring(eq + 1).trim().toDoubleOrNull()
        require(value != null && value >= 0.0) { "index option weights: bad weight in \"$entry\"" }
        val i = byName[name]
        require(i != null) { "index option weights: \"$name\" is not an indexed field" }
        weights[i] = value
    }
    return weights
}

/** Tokens of one field of one document version: `term → ascending positions`. */
internal class FieldTokens(
    val length: Int,
    val postings: Map<String, IntArray>,
)

/** One put of a document: its analyzed fields. */
internal class DocVersion(
    val seq: Long,
    val fields: Array<FieldTokens?>,
) {
    val total: Int = fields.sumOf { it?.length ?: 0 }
}

/** A put ([version] non-null) or a tombstone, at one commit. */
internal class DocEvent(
    val seq: Long,
    val commitHash: KdbHash,
    val version: DocVersion?,
)

/** Points from a term to one field of one document version. */
internal class Posting(
    val docId: KdbUuid,
    val version: DocVersion,
    val field: Int,
    val positions: IntArray,
)

/** What is visible at one cutoff: each document's version, `N`, and per-field total lengths. */
internal class SearchView(
    val visible: Map<KdbUuid, DocVersion>,
    val n: Int,
    val fieldTotal: LongArray,
) {
    fun avgLen(field: Int): Double = if (n == 0) 0.0 else fieldTotal[field].toDouble() / n.toDouble()
}

/**
 * Scored, multi-field full-text index (Layer 16, Component 63).
 *
 * Every [putDocument] appends a version event and every [delete] a tombstone, both tagged with
 * their commit. A read `atCommit` resolves, per document, the last event whose commit is an
 * ancestor of the cutoff (`dag.isAncestor`), so a head read hides a deleted document while an
 * earlier `atCommit` read still sees it. Corpus statistics (`N`, `n_t`, `avglen_f`) are computed
 * over exactly that visible set, so historical reads score consistently.
 *
 * @param fuzzyConfig explicit opt-in for Levenshtein expansion of query terms absent from the
 * dictionary; null (the default) disables it entirely.
 * @param blobs where snapshots are written; defaults to the storage adapter's blob store.
 * @param flushEvery commits between automatic snapshots (0 disables automatic flushing).
 */
public class DefaultFullTextIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    storage: StorageAdapter,
    private val tokenizer: TextTokenizer = UnicodeTokenizer(),
    private val fuzzyConfig: FuzzyMatchConfig? = null,
    private val blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
    private val flushEvery: Int = DEFAULT_FLUSH_EVERY,
) : DocumentIndexStore {

    /** The indexed JSON paths, in descriptor order (`fields`, falling back to `fieldName`). */
    public val fields: List<String> = descriptor.fields.ifEmpty { listOf(descriptor.fieldName) }

    /** Per-field weights from `options["weights"]`, in [fields] order. */
    public val weights: DoubleArray = parseFieldWeights(fields, descriptor.options)

    private val mutex = Mutex()
    private val docs = HashMap<KdbUuid, MutableList<DocEvent>>()
    private val postings = HashMap<String, MutableList<Posting>>()
    private var seqCounter = 0L
    private var lastCommit: KdbHash? = null
    private var commitsSinceFlush = 0

    // ---------------------------------------------------------------- statistics

    /** `N` at [atCommit] (null = head): documents with at least one indexed token. */
    public suspend fun documentCount(atCommit: KdbHash? = null): Int = viewAt(atCommit).n

    /** `n_t` at [atCommit]: documents containing the already-analyzed [term] in any field. */
    public suspend fun documentFrequency(
        term: String,
        atCommit: KdbHash? = null,
    ): Int {
        val view = viewAt(atCommit)
        return mutex.withLock {
            val seen = HashSet<KdbUuid>()
            for (p in postings[term] ?: return@withLock 0) {
                if (view.visible[p.docId] === p.version) seen += p.docId
            }
            seen.size
        }
    }

    /** `avglen_f` for the field at index [field] at [atCommit]. */
    public suspend fun averageFieldLength(
        field: Int,
        atCommit: KdbHash? = null,
    ): Double = viewAt(atCommit).avgLen(field)

    // ---------------------------------------------------------------- writes

    override fun validateDocument(
        docId: KdbUuid,
        json: String,
    ) {
        // Text extraction never fails because of the document: a value that is not a string (or
        // an array of strings) at the indexed path simply contributes nothing.
    }

    override suspend fun putDocument(
        docId: KdbUuid,
        commitHash: KdbHash,
        json: String,
    ) {
        val root = parseDocumentForIndex(json)
        val tokens = Array(fields.size) { i -> root?.let { extractField(it, fields[i]) } }
        mutex.withLock {
            noteCommitLocked(commitHash)
            appendEventLocked(docId, commitHash, DocVersion(seqCounter + 1, tokens))
        }
    }

    /**
     * The [IndexStore] entry point: the key is a [IndexKey.StringKey] holding the document JSON
     * (what the commit path's hints carry). A key that is not JSON is indexed as the text of the
     * first field.
     */
    override suspend fun put(entry: IndexEntry) {
        val key =
            entry.key as? IndexKey.StringKey
                ?: throw IndexTypeMismatchException(
                    "FULLTEXT requires StringKey",
                    descriptor.fieldName,
                    IndexType.FULLTEXT,
                    descriptor.type,
                )
        val root = parseDocumentForIndex(key.value)
        if (root is JsonValue.JObject) {
            putDocument(entry.docId, entry.commitHash, key.value)
            return
        }
        val tokens = arrayOfNulls<FieldTokens>(fields.size)
        tokens[0] = tokensOf(listOf(key.value))
        mutex.withLock {
            noteCommitLocked(entry.commitHash)
            appendEventLocked(entry.docId, entry.commitHash, DocVersion(seqCounter + 1, tokens))
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
        postings.clear()
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
                // Snapshot the state as of the *previous* commit, whose effects are fully applied.
                writeSnapshotLocked(previous.toHex())
                commitsSinceFlush = 0
            }
        }
        lastCommit = commitHash
    }

    private fun appendEventLocked(
        docId: KdbUuid,
        commitHash: KdbHash,
        version: DocVersion?,
    ) {
        seqCounter++
        docs.getOrPut(docId) { mutableListOf() } += DocEvent(seqCounter, commitHash, version)
        if (version == null) return
        for (f in version.fields.indices) {
            val ft = version.fields[f] ?: continue
            for ((term, positions) in ft.postings) {
                postings.getOrPut(term) { mutableListOf() } += Posting(docId, version, f, positions)
            }
        }
    }

    // ---------------------------------------------------------------- extraction

    private fun extractField(
        root: JsonValue,
        path: String,
    ): FieldTokens? {
        val strings = documentPathCandidates(root, path).filterIsInstance<JsonValue.JString>().map { it.value }
        if (strings.isEmpty()) return null
        return tokensOf(strings)
    }

    /** Analyzes every element, leaving [ARRAY_POSITION_GAP] empty positions between elements. */
    private fun tokensOf(elements: List<String>): FieldTokens {
        val out = HashMap<String, MutableList<Int>>()
        var offset = 0
        var length = 0
        for (element in elements) {
            val tokens = tokenizer.tokenize(element)
            for ((i, term) in tokens.withIndex()) out.getOrPut(term) { mutableListOf() } += offset + i
            offset += tokens.size + ARRAY_POSITION_GAP
            length += tokens.size
        }
        return FieldTokens(length, out.mapValues { (_, p) -> p.toIntArray() })
    }

    // ---------------------------------------------------------------- reads

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException("lookup not on FULLTEXT", descriptor.fieldName, IndexType.HASH, IndexType.FULLTEXT)

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException("range not on FULLTEXT", descriptor.fieldName, IndexType.BTREE, IndexType.FULLTEXT)

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> =
        throw IndexTypeMismatchException("VECTOR not on FULLTEXT", descriptor.fieldName, IndexType.VECTOR, IndexType.FULLTEXT)

    /** Resolves which version of each document is visible at [atCommit] (null = head). */
    private suspend fun viewAt(atCommit: KdbHash?): SearchView {
        val cutoff = atCommit ?: dag.head()
        val ancestry = HashMap<KdbHash, Boolean>()
        val events = mutex.withLock { docs.entries.map { it.key to it.value.toList() } }
        val visible = HashMap<KdbUuid, DocVersion>()
        val fieldTotal = LongArray(fields.size)
        var n = 0
        for ((docId, log) in events) {
            var last: DocEvent? = null
            for (ev in log) {
                if (ancestry.getOrPut(ev.commitHash) { dag.isAncestor(ev.commitHash, cutoff) }) last = ev
            }
            val version = last?.version ?: continue
            visible[docId] = version
            if (version.total == 0) continue
            n++
            for (f in version.fields.indices) fieldTotal[f] += version.fields[f]?.length?.toLong() ?: 0L
        }
        return SearchView(visible, n, fieldTotal)
    }

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<RankedResult> = search(parseFullTextQuery(query).copy(fuzzy = fuzzyConfig != null), atCommit, limit)

    /**
     * Ranks the documents visible at [atCommit] that hold at least one query term (OR semantics)
     * and every quoted phrase, by BM25F-lite score descending then document id ascending.
     * A [limit] of 0 or less returns every hit.
     */
    public suspend fun search(
        query: FullTextQuery,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<RankedResult> {
        if (query.terms.isEmpty()) return emptyList()
        val view = viewAt(atCommit)
        if (view.n == 0) return emptyList()
        val fuzzy = if (query.fuzzy) fuzzyConfig else null

        val scores = HashMap<KdbUuid, Double>()
        mutex.withLock {
            for (term in query.terms) {
                for (dictTerm in resolveTermLocked(term, fuzzy)) {
                    val live = ArrayList<Posting>()
                    val docsWithTerm = HashSet<KdbUuid>()
                    for (p in postings[dictTerm] ?: continue) {
                        if (view.visible[p.docId] !== p.version) continue
                        live += p
                        docsWithTerm += p.docId
                    }
                    if (live.isEmpty()) continue
                    val nt = docsWithTerm.size.toDouble()
                    val idf = ln(1.0 + (view.n.toDouble() - nt + 0.5) / (nt + 0.5))
                    for (p in live) {
                        val tf = p.positions.size.toDouble()
                        val length = p.version.fields[p.field]?.length?.toDouble() ?: 0.0
                        val avg = view.avgLen(p.field)
                        val ratio = if (avg > 0.0) length / avg else 1.0
                        val norm = tf * (BM25_K1 + 1.0) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * ratio))
                        scores[p.docId] = (scores[p.docId] ?: 0.0) + weights[p.field] * idf * norm
                    }
                }
            }
        }

        val hits = ArrayList<RankedResult>(scores.size)
        for ((docId, score) in scores) {
            val version = view.visible[docId] ?: continue
            if (!matchesPhrases(version, query.phrases)) continue
            hits += RankedResult(docId, score.toFloat())
        }
        hits.sortWith(compareByDescending<RankedResult> { it.score }.thenBy { it.docId.toString() })
        return if (limit > 0 && hits.size > limit) hits.subList(0, limit).toList() else hits
    }

    /** Every phrase must occur contiguously in some single field of the version. */
    private fun matchesPhrases(
        version: DocVersion,
        phrases: List<List<String>>,
    ): Boolean = phrases.all { phrase -> version.fields.any { it != null && fieldHasPhrase(it, phrase) } }

    private fun fieldHasPhrase(
        ft: FieldTokens,
        phrase: List<String>,
    ): Boolean {
        val first = ft.postings[phrase[0]] ?: return false
        for (start in first) {
            var ok = true
            for (i in 1 until phrase.size) {
                val positions = ft.postings[phrase[i]]
                if (positions == null || !contains(positions, start + i)) {
                    ok = false
                    break
                }
            }
            if (ok) return true
        }
        return false
    }

    private fun contains(
        sorted: IntArray,
        value: Int,
    ): Boolean {
        var lo = 0
        var hi = sorted.size - 1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val v = sorted[mid]
            if (v == value) return true
            if (v < value) lo = mid + 1 else hi = mid - 1
        }
        return false
    }

    private fun resolveTermLocked(
        term: String,
        fuzzy: FuzzyMatchConfig?,
    ): List<String> {
        if (postings.containsKey(term)) return listOf(term)
        if (fuzzy == null) return emptyList()
        val prefix = term.take(fuzzy.prefixLength)
        return postings.keys
            .filter { it.startsWith(prefix) }
            .sorted()
            .take(fuzzy.maxCandidatesPerTerm)
            .filter { levenshtein(it, term) <= fuzzy.maxEditDistance }
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
        val head = dag.head()
        val view = viewAt(head)
        return mutex.withLock { snapshotLocked(head.toHex(), view) }
    }

    override suspend fun restoreSnapshot(data: ByteArray) {
        mutex.withLock {
            clearLocked()
            FullTextSnapshotCodec.load(data.decodeToString(), this)
        }
    }

    override suspend fun restoreFromStorage(): SnapshotRestoreResult {
        val bytes =
            blobs.read(indexSnapshotBlobKey(descriptor.indexId))
                ?: return SnapshotRestoreResult(SnapshotRestoreStatus.MISSING, null, "no snapshot for ${descriptor.indexId}")
        val text = bytes.decodeToString()
        val manifest =
            try {
                FullTextSnapshotCodec.manifest(text)
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
                FullTextSnapshotCodec.load(text, this)
                SnapshotRestoreResult(SnapshotRestoreStatus.RESTORED, manifest.headCommitHex)
            } catch (e: Throwable) {
                clearLocked()
                SnapshotRestoreResult(SnapshotRestoreStatus.CORRUPT, manifest.headCommitHex, e.message ?: "corrupt snapshot")
            }
        }
    }

    /** Snapshot without recomputing the view (manifest stats are then written as zeroes). */
    private fun snapshotLocked(headHex: String): ByteArray = snapshotLocked(headHex, null)

    private fun snapshotLocked(
        headHex: String,
        view: SearchView?,
    ): ByteArray =
        FullTextSnapshotCodec
            .write(
                store = this,
                headHex = headHex,
                docs = docs,
                documentCount = view?.n ?: 0,
                averageLengths = DoubleArray(fields.size) { view?.avgLen(it) ?: 0.0 },
                seqCounter = seqCounter,
            ).encodeToByteArray()

    /** Replays one snapshot event (the caller holds the lock). */
    internal fun ingestVersion(
        docId: KdbUuid,
        seq: Long,
        commitHash: KdbHash,
        tokens: Array<FieldTokens?>,
    ) {
        seqCounter = maxOf(seqCounter, seq - 1)
        appendEventLocked(docId, commitHash, DocVersion(seqCounter + 1, tokens))
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

    internal fun eventsForSnapshot(): Map<KdbUuid, List<DocEvent>> = docs
}

private fun levenshtein(
    a: String,
    b: String,
): Int {
    if (a == b) return 0
    if (a.isEmpty()) return b.length
    if (b.isEmpty()) return a.length
    val prev = IntArray(b.length + 1) { it }
    val cur = IntArray(b.length + 1)
    for (i in a.indices) {
        cur[0] = i + 1
        for (j in b.indices) {
            val cost = if (a[i] == b[j]) 0 else 1
            cur[j + 1] = minOf(cur[j] + 1, prev[j + 1] + 1, prev[j] + cost)
        }
        for (j in prev.indices) prev[j] = cur[j]
    }
    return prev[b.length]
}

public fun fullTextIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
    flushEvery: Int = DEFAULT_FLUSH_EVERY,
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.FULLTEXT) {
            "FullTextIndexStoreFactory expected FULLTEXT, got ${descriptor.type}"
        }
        DefaultFullTextIndexStore(descriptor, dag, storage, blobs = blobs, flushEvery = flushEvery)
    }
