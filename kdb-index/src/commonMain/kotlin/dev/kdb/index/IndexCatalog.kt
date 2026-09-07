package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/**
 * Persisted index descriptors for one namespace (§9.2): everything a runtime needs to recreate
 * its index stores after a restart. HASH/BTREE entries are rebuilt from the schema, FULLTEXT and
 * VECTOR entries from their snapshots or by scan. Stored under [indexCatalogBlobKey].
 */
public data class IndexCatalogEntry(
    val descriptor: IndexDescriptor,
    /** `CREATE INDEX` name when the index was declared via SQL; null for schema-derived indexes. */
    val sqlIndexName: String?,
)

public data class IndexCatalog(
    val namespaceId: String,
    val entries: List<IndexCatalogEntry>,
) {
    init {
        // The namespace is stored once, in the header, and every descriptor is read back with it.
        // A descriptor belonging to another namespace would be silently re-homed on load, so it is
        // rejected here instead — the registry always builds descriptors in its own namespace.
        for (entry in entries) {
            require(entry.descriptor.namespaceId == namespaceId) {
                "index catalog for $namespaceId cannot hold ${entry.descriptor.indexId} " +
                    "from namespace ${entry.descriptor.namespaceId}"
            }
        }
    }

    public fun encode(): ByteArray = IndexCatalogCodec.encode(this).encodeToByteArray()

    public suspend fun save(blobs: IndexBlobStore) {
        blobs.write(indexCatalogBlobKey(namespaceId), encode())
    }

    public companion object {
        public const val FORMAT_VERSION: Int = 1

        public fun decode(bytes: ByteArray): IndexCatalog = IndexCatalogCodec.decode(bytes.decodeToString())

        /** Null when no catalog has been saved for [namespaceId]. */
        public suspend fun load(
            blobs: IndexBlobStore,
            namespaceId: String,
        ): IndexCatalog? = blobs.read(indexCatalogBlobKey(namespaceId))?.let { decode(it) }
    }
}

/**
 * Line-oriented text format, one entry per line, `|`-separated, every free-text value escaped
 * with [CatalogEscape] so field paths, SQL names and option values may contain any character.
 */
internal object IndexCatalogCodec {
    private const val HEADER = "kdb-index-catalog"

    fun encode(catalog: IndexCatalog): String =
        buildString {
            append(HEADER).append(' ').append(IndexCatalog.FORMAT_VERSION).append('\n')
            append("ns|").append(CatalogEscape.escape(catalog.namespaceId)).append('\n')
            for (entry in catalog.entries) {
                val d = entry.descriptor
                append("ix|")
                append(d.indexId).append('|')
                append(CatalogEscape.escape(d.fieldName)).append('|')
                append(d.fields.joinToString(",") { CatalogEscape.escape(it) }).append('|')
                append(d.type.name).append('|')
                append(d.unique).append('|')
                append(d.schemaVersion).append('|')
                append(d.createdAtHash.toHex()).append('|')
                append(
                    d.options.entries
                        .sortedBy { it.key }
                        .joinToString(",") { (k, v) -> CatalogEscape.escape(k) + "=" + CatalogEscape.escape(v) },
                ).append('|')
                append(entry.sqlIndexName?.let { CatalogEscape.escape(it) } ?: "")
                append('\n')
            }
        }

    /** The `ns` header line must precede the `ix` lines, which are read back into that namespace. */
    fun decode(text: String): IndexCatalog {
        val lines = text.lines().filter { it.isNotBlank() }
        require(lines.isNotEmpty()) { "empty index catalog" }
        val header = lines[0].split(' ')
        require(header.size == 2 && header[0] == HEADER) { "not an index catalog: ${lines[0]}" }
        val version = header[1].toIntOrNull() ?: error("index catalog version corrupt")
        require(version == IndexCatalog.FORMAT_VERSION) { "unsupported index catalog version $version" }
        var namespaceId = ""
        val entries = mutableListOf<IndexCatalogEntry>()
        for (line in lines.drop(1)) {
            val parts = CatalogEscape.splitUnescaped(line, '|')
            when (parts[0]) {
                "ns" -> namespaceId = CatalogEscape.unescape(parts[1])
                "ix" -> {
                    require(parts.size == 10) { "index catalog line corrupt: $line" }
                    val fields = CatalogEscape.splitUnescaped(parts[3], ',').filter { it.isNotEmpty() }.map { CatalogEscape.unescape(it) }
                    val options = LinkedHashMap<String, String>()
                    for (pair in CatalogEscape.splitUnescaped(parts[8], ',')) {
                        if (pair.isEmpty()) continue
                        val kv = CatalogEscape.splitUnescaped(pair, '=')
                        options[CatalogEscape.unescape(kv[0])] = CatalogEscape.unescape(kv.getOrElse(1) { "" })
                    }
                    val descriptor =
                        IndexDescriptor(
                            indexId = KdbUuid.fromString(parts[1]),
                            namespaceId = namespaceId,
                            fieldName = CatalogEscape.unescape(parts[2]),
                            fields = fields,
                            type = IndexType.valueOf(parts[4]),
                            unique = parts[5].toBoolean(),
                            schemaVersion = parts[6].toInt(),
                            createdAtHash = KdbHash.fromHex(parts[7]),
                            options = options,
                        )
                    entries += IndexCatalogEntry(descriptor, parts[9].ifEmpty { null }?.let { CatalogEscape.unescape(it) })
                }
                else -> error("index catalog line corrupt: $line")
            }
        }
        return IndexCatalog(namespaceId, entries)
    }
}

/** Backslash escaping shared by the catalog and the snapshot codecs. */
public object CatalogEscape {
    private const val SPECIAL = "\\|,=\n\r "

    public fun escape(s: String): String =
        buildString(s.length) {
            for (ch in s) {
                when (ch) {
                    '\\' -> append("\\\\")
                    '|' -> append("\\p")
                    ',' -> append("\\c")
                    '=' -> append("\\e")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    ' ' -> append("\\s")
                    else -> append(ch)
                }
            }
        }

    public fun unescape(s: String): String =
        buildString(s.length) {
            var i = 0
            while (i < s.length) {
                val ch = s[i]
                if (ch == '\\' && i + 1 < s.length) {
                    when (s[i + 1]) {
                        '\\' -> append('\\')
                        'p' -> append('|')
                        'c' -> append(',')
                        'e' -> append('=')
                        'n' -> append('\n')
                        'r' -> append('\r')
                        's' -> append(' ')
                        else -> append(s[i + 1])
                    }
                    i += 2
                } else {
                    append(ch)
                    i++
                }
            }
        }

    /** Splits on [sep] outside escape sequences (an escaped separator never splits). */
    public fun splitUnescaped(
        s: String,
        sep: Char,
    ): List<String> {
        val out = mutableListOf<String>()
        val cur = StringBuilder()
        var i = 0
        while (i < s.length) {
            val ch = s[i]
            if (ch == '\\' && i + 1 < s.length) {
                cur.append(ch).append(s[i + 1])
                i += 2
                continue
            }
            if (ch == sep) {
                out += cur.toString()
                cur.clear()
            } else {
                cur.append(ch)
            }
            i++
        }
        out += cur.toString()
        return out
    }

    public fun needsEscaping(s: String): Boolean = s.any { it in SPECIAL }
}
