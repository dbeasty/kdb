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
 * [with] and [without] are O(delta): both the tree hash and [entries] come from one persistent
 * trie (DocumentTreeTrie.kt) that shares every untouched subtree with the previous version.
 * [entries] is a read-only [Map] view over that trie rather than a copied map - copying was
 * O(entries.size) per insert, which made every committed put cost the size of its namespace.
 * Lookups walk the trie (a few levels; leaves are compressed), iteration is in UUID order, and
 * the view compares equal to any [Map] with the same contents, so this data class's
 * equals/hashCode/copy/toString are unchanged.
 *
 * A tree constructed directly from some other map (the primary constructor, or copy) builds its
 * trie once, O(n), on its first with/without; everything derived from it is incremental.
 */
public data class DocumentTree(
    public val treeHash: KdbHash,
    public val entries: Map<KdbUuid, KdbHash>,
) {
    // UNBUILT until a tree over a foreign map first needs its trie; a trie view carries its own.
    private var builtRoot: Any? = UNBUILT

    internal val trieRoot: TrieNode?
        get() {
            if (entries is TrieEntries) return entries.root
            val cached = builtRoot
            if (cached !== UNBUILT) return cached as TrieNode?
            return trieBuild(entries).also { builtRoot = it }
        }

    public val size: Int get() = entries.size

    public fun contains(docId: KdbUuid): Boolean = entries.containsKey(docId)

    public fun hashFor(docId: KdbUuid): KdbHash? = entries[docId]

    public fun with(
        docId: KdbUuid,
        contentHash: KdbHash,
    ): DocumentTree {
        val old = trieRoot
        val existing = trieGet(old, docId)
        if (existing == contentHash) return this
        val root = trieInsert(old, docId, contentHash)
        return DocumentTree(trieTreeHash(root), TrieEntries(root, if (existing == null) size + 1 else size))
    }

    public fun without(docId: KdbUuid): DocumentTree {
        val old = trieRoot
        if (trieGet(old, docId) == null) return this
        val root = trieDelete(old, docId)
        return DocumentTree(trieTreeHash(root), TrieEntries(root, size - 1))
    }

    public companion object {
        // Ahead of EMPTY: constructing EMPTY reads it.
        private val UNBUILT = Any()

        public val EMPTY: DocumentTree = build(emptyMap())

        /**
         * Builds a tree with content-addressed tree hash from a flat map,
         * building a full trie from scratch (O(n) - see trieBuild). Used
         * for trees not derived incrementally via with/without (e.g. wire
         * decode). The result's entries are the trie view, not [entries]
         * itself: the map is not retained, so later changes to it do not
         * show through, and any subsequent with/without is O(delta).
         */
        public fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree {
            val root = trieBuild(entries)
            return DocumentTree(trieTreeHash(root), TrieEntries(root, entries.size))
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

/**
 * [DocumentTree.entries]: a read-only map over a trie, O(1) to make from the trie it views. The
 * root hash names the contents, so two views compare in O(1); against any other map, AbstractMap's
 * entry-by-entry comparison applies.
 */
internal class TrieEntries(
    val root: TrieNode?,
    override val size: Int,
) : AbstractMap<KdbUuid, KdbHash>() {
    // 0 doubles as "not computed": one field, so a racing reader at worst computes it again.
    private var hash = 0

    override fun get(key: KdbUuid): KdbHash? = trieGet(root, key)

    override fun containsKey(key: KdbUuid): Boolean = trieGet(root, key) != null

    override fun isEmpty(): Boolean = root == null

    override val entries: Set<Map.Entry<KdbUuid, KdbHash>>
        get() = object : AbstractSet<Map.Entry<KdbUuid, KdbHash>>() {
            override val size: Int get() = this@TrieEntries.size

            override fun iterator(): Iterator<Map.Entry<KdbUuid, KdbHash>> = TrieLeafIterator(root)

            override fun contains(element: Map.Entry<KdbUuid, KdbHash>): Boolean = trieGet(root, element.key) == element.value
        }

    override fun equals(other: Any?): Boolean {
        if (other is TrieEntries) {
            return size == other.size && (root?.hash ?: EMPTY_HASH).contentEquals(other.root?.hash ?: EMPTY_HASH)
        }
        return super.equals(other)
    }

    override fun hashCode(): Int {
        // Map's contract fixes this to the sum over every entry, O(n); immutable, so pay it once.
        var h = hash
        if (h == 0) {
            h = super.hashCode()
            hash = h
        }
        return h
    }

    private companion object {
        val EMPTY_HASH = ByteArray(0)
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
