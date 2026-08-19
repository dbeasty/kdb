package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.toKdbTimestamp
import dev.kdb.codec.toKdbUuid
import dev.kdb.codec.toTimestampVal
import dev.kdb.codec.toUuidVal

/**
 * Materialised doc id → content hash map at a commit.
 *
 * trieRoot is intentionally a body property, not a primary-constructor
 * one: it backs incremental with/without updates (O(delta) via
 * DocumentTreeTrie.kt instead of O(entries.size)) but must NOT affect
 * this data class's generated equals/hashCode/copy/toString, which
 * existing code relies on comparing by treeHash/entries alone. Trees
 * built via build() from a flat map (e.g. wire decode) don't carry a
 * trieRoot - with/without fall back to a one-time O(n) trieBuild in that
 * case, then are incremental from then on. Always correct either way;
 * only the constant differs.
 */
public data class DocumentTree(
    public val treeHash: KdbHash,
    public val entries: Map<KdbUuid, KdbHash>,
) {
    internal var trieRoot: TrieNode? = null

    public val size: Int get() = entries.size

    public fun contains(docId: KdbUuid): Boolean = entries.containsKey(docId)

    public fun hashFor(docId: KdbUuid): KdbHash? = entries[docId]

    public fun with(
        docId: KdbUuid,
        contentHash: KdbHash,
    ): DocumentTree {
        val root = trieInsert(trieRootOrBuild(), docId, contentHash)
        val newEntries = entries + (docId to contentHash)
        return DocumentTree(trieTreeHash(root), newEntries).also { it.trieRoot = root }
    }

    public fun without(docId: KdbUuid): DocumentTree {
        val root = trieDelete(trieRootOrBuild(), docId)
        val newEntries = entries - docId
        return DocumentTree(trieTreeHash(root), newEntries).also { it.trieRoot = root }
    }

    private fun trieRootOrBuild(): TrieNode? {
        val cached = trieRoot
        if (cached != null || entries.isEmpty()) return cached
        return trieBuild(entries)
    }

    public companion object {
        public val EMPTY: DocumentTree = build(emptyMap())

        /**
         * Builds a tree with content-addressed tree hash from a flat map,
         * building a full trie from scratch (O(n) - see trieBuild). Used
         * for trees not derived incrementally via with/without (e.g. wire
         * decode); the result still carries a trieRoot, so any subsequent
         * with/without on it is O(delta).
         */
        public fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree {
            val root = trieBuild(entries)
            return DocumentTree(trieTreeHash(root), entries).also { it.trieRoot = root }
        }

        public fun fromKdbValue(value: KdbValue): DocumentTree {
            val arr = value as? KdbValue.ArrayVal ?: throw CommitDecodeException("DocumentTree: expected array")
            val m = linkedMapOf<KdbUuid, KdbHash>()
            for (e in arr.elements) {
                val r = e as? KdbValue.RecordVal ?: throw CommitDecodeException("DocumentTree: entry record")
                val id =
                    (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                        ?: throw CommitDecodeException("DocumentTree: docId")
                val fh =
                    (r.fields[2] as? KdbValue.FixedVal)?.v
                        ?: throw CommitDecodeException("DocumentTree: contentHash")
                m[id] = KdbHash.fromBytes(fh.copyOf())
            }
            return build(m)
        }
    }
}

private fun entriesToArrayValue(entries: Map<KdbUuid, KdbHash>): KdbValue {
    // Sort keys are computed once per entry rather than via sortedBy's
    // Comparator (which calls the selector on every comparison, i.e.
    // O(n log n) toString() calls instead of O(n)): at 2000 entries this
    // was the dominant cost of BuildDocumentTree by ~two orders of
    // magnitude over the actual hash/encode work - see the Phase 3 note
    // in docs/benchmarks/phase0-baseline.md. Sort order (and therefore
    // the resulting hash) is unchanged.
    val sorted =
        entries.keys
            .map { it to it.toString() }
            .sortedBy { it.second }
            .map { it.first }
    return KdbValue.ArrayVal(
        sorted.map { id ->
            KdbValue.RecordVal(
                mapOf(
                    1 to id.toUuidVal(),
                    2 to KdbValue.FixedVal(entries.getValue(id).bytes.copyOf()),
                ),
            )
        },
    )
}

public fun DocumentTree.toKdbValue(): KdbValue = entriesToArrayValue(entries)

/** Named branch pointer. */
public data class KdbBranch(
    val name: String,
    val namespaceId: String,
    val headHash: KdbHash,
    val createdAt: KdbTimestamp,
    val updatedAt: KdbTimestamp,
)

/** Named tag (immutable ref). */
public data class KdbTag(
    val name: String,
    val namespaceId: String,
    val commitHash: KdbHash,
    val createdAt: KdbTimestamp,
    val message: String = "",
)

/** DAG placeholder for ice-archived commits. */
public data class CommitStub(
    val originalHash: KdbHash,
    val archiveLocation: String,
    val stubbedAt: KdbTimestamp,
) {
    public fun toKdbValue(): KdbValue =
        KdbValue.RecordVal(
            mapOf(
                1 to KdbValue.FixedVal(originalHash.bytes.copyOf()),
                2 to KdbValue.StringVal(archiveLocation),
                3 to stubbedAt.toTimestampVal(),
            ),
        )

    public companion object {
        public fun fromKdbValue(value: KdbValue): CommitStub {
            val r = value as? KdbValue.RecordVal ?: throw CommitDecodeException("CommitStub: expected record")
            val oh =
                (r.fields[1] as? KdbValue.FixedVal)?.v?.copyOf()
                    ?: throw CommitDecodeException("CommitStub: originalHash")
            val loc =
                (r.fields[2] as? KdbValue.StringVal)?.v
                    ?: throw CommitDecodeException("CommitStub: archiveLocation")
            val ts =
                (r.fields[3] as? KdbValue.TimestampVal)?.toKdbTimestamp()
                    ?: throw CommitDecodeException("CommitStub: stubbedAt")
            return CommitStub(KdbHash.fromBytes(oh), loc, ts)
        }
    }
}
