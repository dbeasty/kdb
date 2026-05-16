package dev.kdb.index.fulltext

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

public class FullTextQueryException(
    message: String,
    val query: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}

public data class FullTextQuery(
    val terms: List<String>,
    val phrase: String? = null,
    val fuzzy: Boolean = true,
)

public fun parseFullTextQuery(raw: String): FullTextQuery {
    val trimmed = raw.trim()
    if (trimmed.isEmpty()) throw FullTextQueryException("empty query", raw)
    var phrase: String? = null
    var rest = trimmed
    if (trimmed.startsWith('"')) {
        val end = trimmed.indexOf('"', startIndex = 1)
        if (end < 0) throw FullTextQueryException("unbalanced quotes", raw)
        phrase = trimmed.substring(1, end)
        rest = trimmed.substring(end + 1).trim()
    }
    val terms =
        rest.split(Regex("\\s+"))
            .map { it.trim().lowercase() }
            .filter { it.isNotEmpty() }
    return FullTextQuery(terms = terms, phrase = phrase)
}

public interface TextTokenizer {
    public fun tokenize(text: String): List<String>
}

public class UnicodeTokenizer : TextTokenizer {
    override fun tokenize(text: String): List<String> {
        val out = mutableListOf<String>()
        val sb = StringBuilder()
        fun flush() {
            if (sb.isNotEmpty()) {
                val t = sb.toString().lowercase()
                if (t.length <= 64) out.add(t)
                sb.clear()
            }
        }
        for (ch in text) {
            if (ch.isLetterOrDigit()) {
                sb.append(ch)
            } else {
                flush()
            }
        }
        flush()
        return out
    }
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

private data class Posting(
    val docId: KdbUuid,
    val commitHash: KdbHash,
    val text: String,
)

public class DefaultFullTextIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    private val tokenizer: TextTokenizer = UnicodeTokenizer(),
    private val fuzzyConfig: FuzzyMatchConfig = FuzzyMatchConfig.DEFAULT,
) : IndexStore {

    private val mutex = Mutex()
    private val dictionary = mutableMapOf<String, MutableList<Posting>>()
    private var seq = 0L


    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        mutex.withLock {
            removeDocLocked(docId)
            seq++
        }
    }

    override suspend fun put(entry: IndexEntry) {
        mutex.withLock { putLocked(entry) }
    }

    override suspend fun bulkLoad(entries: List<IndexEntry>) {
        mutex.withLock {
            dictionary.clear()
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
            "lookup not on FULLTEXT",
            descriptor.fieldName,
            IndexType.HASH,
            IndexType.FULLTEXT,
        )

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> =
        throw IndexTypeMismatchException(
            "range not on FULLTEXT",
            descriptor.fieldName,
            IndexType.BTREE,
            IndexType.FULLTEXT,
        )

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<KdbUuid> {
        val parsed = parseFullTextQuery(query)
        val cutoff = atCommit ?: dag.head()
        val termSets =
            parsed.terms.map { term ->
                resolveTerm(term, parsed.fuzzy).toSet()
            }
        if (termSets.isEmpty() && parsed.phrase == null) return emptyList()
        val candidates =
            mutex.withLock {
                if (termSets.isEmpty()) {
                    dictionary.values.flatten()
                } else {
                    termSets
                        .map { terms ->
                            terms.flatMap { dictionary[it] ?: emptyList() }
                        }.reduce { acc, list -> acc.filter { p -> list.any { it.docId == p.docId } } }
                }
            }
        val filtered =
            candidates
                .filter { dag.isAncestor(it.commitHash, cutoff) }
                .distinctBy { it.docId }
                .filter { posting ->
                    if (parsed.phrase == null) return@filter true
                    tokenizer.tokenize(posting.text).joinToString(" ").contains(parsed.phrase.lowercase())
                }
        return filtered.take(limit).map { it.docId }
    }

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> =
        throw IndexTypeMismatchException(
            "VECTOR not on FULLTEXT",
            descriptor.fieldName,
            IndexType.VECTOR,
            IndexType.FULLTEXT,
        )

    override suspend fun clear() {
        mutex.withLock { dictionary.clear() }
    }

    override suspend fun isValid(atCommit: KdbHash): Boolean = dag.hasCommit(atCommit)

    override suspend fun snapshot(): ByteArray =
        mutex.withLock {
            dictionary.entries
                .flatMap { (term, posts) ->
                    posts.map { p -> "$term|${p.docId}|${p.commitHash}|${escape(p.text)}" }
                }.joinToString("\n")
                .encodeToByteArray()
        }

    override suspend fun restoreSnapshot(data: ByteArray) {
        mutex.withLock {
            dictionary.clear()
            for (line in data.decodeToString().lines().filter { it.isNotBlank() }) {
                val parts = line.split('|', limit = 4)
                val term = parts[0]
                val docId = KdbUuid.fromString(parts[1])
                val commit = KdbHash.fromHex(parts[2])
                val text = unescape(parts[3])
                dictionary.getOrPut(term) { mutableListOf() }.add(Posting(docId, commit, text))
            }
        }
    }

    private fun putLocked(entry: IndexEntry) {
        val key =
            entry.key as? IndexKey.StringKey
                ?: throw IndexTypeMismatchException(
                    "FULLTEXT requires StringKey",
                    descriptor.fieldName,
                    IndexType.FULLTEXT,
                    descriptor.type,
                )
        removeDocLocked(entry.docId)
        for (term in tokenizer.tokenize(key.value).distinct()) {
            dictionary.getOrPut(term) { mutableListOf() }
                .add(Posting(entry.docId, entry.commitHash, key.value))
        }
    }

    private fun removeDocLocked(docId: KdbUuid) {
        for ((term, list) in dictionary) {
            list.removeAll { it.docId == docId }
            if (list.isEmpty()) dictionary.remove(term)
        }
    }

    private fun resolveTerm(
        term: String,
        fuzzy: Boolean,
    ): List<String> {
        if (dictionary.containsKey(term)) return listOf(term)
        if (!fuzzy) return emptyList()
        val prefix = term.take(fuzzyConfig.prefixLength)
        val candidates =
            dictionary.keys
                .filter { it.startsWith(prefix) }
                .take(fuzzyConfig.maxCandidatesPerTerm)
        return candidates.filter { levenshtein(it, term) <= fuzzyConfig.maxEditDistance }
    }

    private fun escape(s: String): String = s.replace("\\", "\\\\").replace("|", "\\|")
    private fun unescape(s: String): String =
        buildString {
            var i = 0
            while (i < s.length) {
                if (s[i] == '\\' && i + 1 < s.length) {
                    append(s[i + 1])
                    i += 2
                } else {
                    append(s[i])
                    i++
                }
            }
        }
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
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.FULLTEXT) {
            "FullTextIndexStoreFactory expected FULLTEXT, got ${descriptor.type}"
        }
        DefaultFullTextIndexStore(descriptor, dag, storage)
    }
