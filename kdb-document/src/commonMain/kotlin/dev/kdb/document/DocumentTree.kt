package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.toKdbTimestamp
import dev.kdb.codec.toKdbUuid
import dev.kdb.codec.toTimestampVal
import dev.kdb.codec.toUuidVal
import dev.kdb.document.internal.sha256Digest

/**
 * Materialised doc id → content hash map at a commit.
 */
public data class DocumentTree(
    public val treeHash: KdbHash,
    public val entries: Map<KdbUuid, KdbHash>,
) {
    public val size: Int get() = entries.size

    public fun contains(docId: KdbUuid): Boolean = entries.containsKey(docId)

    public fun hashFor(docId: KdbUuid): KdbHash? = entries[docId]

    public fun with(
        docId: KdbUuid,
        contentHash: KdbHash,
    ): DocumentTree = build(entries + (docId to contentHash))

    public fun without(docId: KdbUuid): DocumentTree = build(entries - docId)

    public companion object {
        public val EMPTY: DocumentTree = build(emptyMap())

        public fun build(entries: Map<KdbUuid, KdbHash>): DocumentTree {
            val treeVal = entriesToArrayValue(entries)
            val reg = KdbDocumentWireRegistry()
            val bytes = treeVal.encodeToBytes(DocumentTreeWireType, reg)
            return DocumentTree(KdbHash.fromBytes(sha256Digest(bytes)), entries)
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
    val sorted = entries.keys.sortedBy { it.toString() }
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
