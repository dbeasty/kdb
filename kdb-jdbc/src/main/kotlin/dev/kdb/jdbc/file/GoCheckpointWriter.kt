package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.toTimestampVal
import dev.kdb.codec.toUuidVal
import dev.kdb.dag.CheckpointCommit
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbTag
import dev.kdb.document.kdbSha256
import dev.kdb.storage.PlatformIoShim
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Writes Go's namespace checkpoint (go/kdb/embed/checkpoint.go writeCheckpoint, encodeLegacyCheckpoint,
 * encodeCheckpointWith; checkpoint_section.go writeSectionedCheckpoint), byte for byte what Go writes
 * for the same state - pinned by go/kdb/embed/kotlin_checkpoint_golden_test.go. A data directory
 * Kotlin checkpoints is then one Go opens, and [GoCheckpoint] reads it back.
 *
 * As Go: a namespace of at most [LEGACY_MAX_ENTRIES] live documents plus commits, with no
 * attachments, is version 2 (commits and the live tree as codec records, what every release since
 * v0.6.2 reads); anything else is version 6 (the live tree and commits packed in a section snapshot
 * the record names, written first).
 */
internal object GoCheckpointWriter {
    /** Go's legacyCheckpointMaxEntries. */
    const val LEGACY_MAX_ENTRIES = 20_000

    /** Go's namespaceCheckpoint, for the parts Kotlin writes (no graph segments). */
    class State(
        val namespaceId: String,
        val throughSequence: Long,
        val floorSequence: Long,
        /** In the order the checkpoint lists them. */
        val commits: List<CheckpointCommit>,
        val branches: List<KdbBranch>,
        val tags: List<KdbTag>,
        /** The live tree's entries (any order: written in ascending id order). */
        val liveTree: Map<KdbUuid, KdbHash>,
        val segments: List<GoCheckpoint.Fingerprint>,
        /** Canonical payloads of the commits whose operations open must have (shallow roots). */
        val headCommits: List<ByteArray>,
        /** Commits may carry attachment roots: never version 2, and [attachmentTables] goes in field 15. */
        val hasAttachments: Boolean = false,
        val attachmentTables: ByteArray = ByteArray(0),
    )

    /** What a checkpoint writes: its record, and for version 6 its section, by snapshot key. */
    class Encoded(
        val record: ByteArray,
        val sectionKey: String?,
        val section: ByteArray?,
    )

    fun legacyFits(s: State): Boolean = !s.hasAttachments && s.liveTree.size + s.commits.size <= LEGACY_MAX_ENTRIES

    fun encode(s: State): Encoded = if (legacyFits(s)) Encoded(encodeLegacy(s), null, null) else encodeSectioned(s)

    /**
     * Writes [s] as Go's writeCheckpoint does: a version 6 section before the record that names it,
     * so a crash leaves the old record with its own section or the new one with its, and then the
     * section the previous record named is deleted.
     */
    suspend fun write(
        shim: PlatformIoShim,
        s: State,
    ) {
        val previous = runCatching { GoCheckpoint.read(shim, s.namespaceId) }.getOrNull()
        val previousSection = previous?.let { sectionKeyOf(shim, s.namespaceId) }
        val e = encode(s)
        if (e.sectionKey != null) shim.writeSnapshot(e.sectionKey, e.section!!)
        shim.writeSnapshot(GoCheckpoint.key(s.namespaceId), e.record)
        if (previousSection != null && previousSection != e.sectionKey) runCatching { shim.deleteSnapshot(previousSection) }
    }

    private suspend fun sectionKeyOf(
        shim: PlatformIoShim,
        namespaceId: String,
    ): String? {
        val raw = shim.readSnapshot(GoCheckpoint.key(namespaceId)) ?: return null
        val rec =
            runCatching { KdbValue.decodeFromBytes(raw, KdbType.Ref(GoCheckpoint.FQN_CHECKPOINT), GoCheckpoint.registry) as? KdbValue.RecordVal }
                .getOrNull() ?: return null
        return (rec.fields[16] as? KdbValue.StringVal)?.v?.takeIf { it.isNotEmpty() }
    }

    private fun hashVal(h: KdbHash): KdbValue = KdbValue.FixedVal(h.bytes.copyOf())

    private fun branches(s: State): KdbValue =
        KdbValue.ArrayVal(
            s.branches.map {
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(it.name),
                        2 to hashVal(it.headHash),
                        3 to it.createdAt.toTimestampVal(),
                        4 to it.updatedAt.toTimestampVal(),
                    ),
                )
            },
        )

    private fun tags(s: State): KdbValue =
        KdbValue.ArrayVal(
            s.tags.map {
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(it.name),
                        2 to hashVal(it.commitHash),
                        3 to it.createdAt.toTimestampVal(),
                        4 to KdbValue.StringVal(it.message),
                    ),
                )
            },
        )

    private fun segments(s: State): KdbValue =
        KdbValue.ArrayVal(
            s.segments.map {
                val f =
                    mutableMapOf<Int, KdbValue>(
                        1 to KdbValue.Int64Val(it.sequence),
                        2 to KdbValue.Int64Val(it.sizeBytes),
                        3 to hashVal(it.lastCommitHash),
                    )
                // Without a stamp, the defaults (-1, 0), which the encoder leaves out as Go does.
                val stamped = it.stampSize != null && it.stampModTimeNanos != null
                f[4] = KdbValue.Int64Val(if (stamped) it.stampSize!! else -1)
                f[5] = KdbValue.Int64Val(if (stamped) it.stampModTimeNanos!! else 0)
                KdbValue.RecordVal(f)
            },
        )

    private fun heads(s: State): KdbValue = KdbValue.ArrayVal(s.headCommits.map { KdbValue.BytesVal(it) })

    /** The live tree in ascending id order, compared unsigned - Go's AppendPacked and walk order. */
    private fun sortedTree(s: State): List<Pair<KdbUuid, KdbHash>> =
        s.liveTree.entries.map { it.key to it.value }.sortedWith { a, b -> compareIds(a.first, b.first) }

    private fun compareIds(
        a: KdbUuid,
        b: KdbUuid,
    ): Int {
        val c = java.lang.Long.compareUnsigned(a.msb, b.msb)
        return if (c != 0) c else java.lang.Long.compareUnsigned(a.lsb, b.lsb)
    }

    /** Fields 11-21 at their defaults: the encoder needs every field and leaves a default out of the
     * bytes, as Go leaves out a field it does not set. */
    private val defaults: Map<Int, KdbValue> =
        mapOf(
            11 to KdbValue.ArrayVal(emptyList()),
            12 to KdbValue.ArrayVal(emptyList()),
            13 to KdbValue.BytesVal(ByteArray(0)),
            14 to KdbValue.BytesVal(ByteArray(0)),
            15 to KdbValue.BytesVal(ByteArray(0)),
            16 to KdbValue.StringVal(""),
            17 to KdbValue.FixedVal(ByteArray(32)),
            18 to KdbValue.Int64Val(0),
            19 to KdbValue.Int64Val(0),
            20 to KdbValue.Int64Val(0),
            21 to KdbValue.Bool(false),
        )

    private fun encodeRecord(fields: Map<Int, KdbValue>): ByteArray =
        KdbValue.RecordVal(defaults + fields).encodeToBytes(KdbType.Ref(GoCheckpoint.FQN_CHECKPOINT), GoCheckpoint.registry)

    private fun encodeLegacy(s: State): ByteArray {
        val commits =
            s.commits.map { cc ->
                val c = cc.commit
                val f =
                    mutableMapOf<Int, KdbValue>(
                        1 to hashVal(c.hash),
                        2 to KdbValue.ArrayVal(c.parentHashes.map(::hashVal)),
                        3 to c.transactionId.toUuidVal(),
                        4 to c.timestamp.toTimestampVal(),
                        5 to c.authorNodeId.toUuidVal(),
                        6 to hashVal(c.documentTreeHash),
                        8 to KdbValue.StringVal(c.message),
                        9 to KdbValue.Int32Val(cc.operationCount),
                    )
                f[7] = c.schemaHash?.let(::hashVal) ?: KdbValue.Null
                KdbValue.RecordVal(f)
            }
        val entries = sortedTree(s).map { (id, h) -> KdbValue.RecordVal(mapOf(1 to id.toUuidVal(), 2 to hashVal(h))) }
        return encodeRecord(
            mapOf(
                1 to KdbValue.Int32Val(GoCheckpoint.VERSION_LEGACY),
                2 to KdbValue.StringVal(s.namespaceId),
                3 to KdbValue.Int64Val(s.throughSequence),
                4 to KdbValue.ArrayVal(commits),
                5 to branches(s),
                6 to tags(s),
                7 to KdbValue.ArrayVal(entries),
                8 to segments(s),
                9 to heads(s),
                10 to KdbValue.Int64Val(s.floorSequence),
            ),
        )
    }

    private fun encodeSectioned(s: State): Encoded {
        val tree = packTree(sortedTree(s))
        val commits = packCommits(s.commits)
        val treeDigest = kdbSha256(tree)
        val commitsDigest = kdbSha256(commits)
        val id = ByteArray(16) { (treeDigest[it].toInt() xor commitsDigest[it].toInt()).toByte() }
        val key = "kdb:checkpoint-section:${s.namespaceId}:" + id.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }
        val header =
            ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN).put("KDBCPS01".encodeToByteArray())
                .putLong(tree.size.toLong()).putLong(commits.size.toLong()).array()
        val body = header + tree + commits
        val crc = CRC32().apply { update(body) }.value.toInt()
        val section = body + ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(crc).array()
        val f =
            mutableMapOf(
                1 to KdbValue.Int32Val(GoCheckpoint.VERSION_SECTION),
                2 to KdbValue.StringVal(s.namespaceId),
                3 to KdbValue.Int64Val(s.throughSequence),
                4 to KdbValue.ArrayVal(emptyList()),
                5 to branches(s),
                6 to tags(s),
                7 to KdbValue.ArrayVal(emptyList()),
                8 to segments(s),
                9 to heads(s),
                10 to KdbValue.Int64Val(s.floorSequence),
                16 to KdbValue.StringVal(key),
                17 to KdbValue.FixedVal(treeDigest),
                18 to KdbValue.Int64Val(s.liveTree.size.toLong()),
                19 to KdbValue.Int64Val(tree.size.toLong()),
                20 to KdbValue.Int64Val(commits.size.toLong()),
                21 to KdbValue.Bool(s.hasAttachments),
            )
        if (s.hasAttachments) f[15] = KdbValue.BytesVal(s.attachmentTables)
        return Encoded(encodeRecord(f), key, section)
    }

    /** Go's DocumentTree.AppendPacked: 16-byte id (big-endian halves), 32-byte content hash. */
    private fun packTree(entries: List<Pair<KdbUuid, KdbHash>>): ByteArray {
        val b = ByteBuffer.allocate(entries.size * GoCheckpoint.PACKED_TREE_ENTRY)
        for ((id, h) in entries) b.putLong(id.msb).putLong(id.lsb).put(h.bytes)
        return b.array()
    }

    /** Go's dag.PackCheckpointCommits (checkpoint_packed.go), the layout [GoCheckpoint.packedCommitSequence] reads. */
    internal fun packCommits(commits: List<CheckpointCommit>): ByteArray {
        val parts =
            commits.map { cc ->
                val c = cc.commit
                val msg = c.message.encodeToByteArray()
                val b =
                    ByteBuffer.allocate(
                        116 + 32 * c.parentHashes.size + (if (c.schemaHash != null) 32 else 0) +
                            (if (c.attachmentTreeHash != null) 32 else 0) + msg.size,
                    ).order(ByteOrder.LITTLE_ENDIAN)
                b.put(c.hash.bytes).put(c.documentTreeHash.bytes)
                b.putLong(c.transactionId.msb).putLong(c.transactionId.lsb).putLong(c.authorNodeId.msb).putLong(c.authorNodeId.lsb)
                b.putLong(c.timestamp.epochMillis).putShort(c.timestamp.microRemainder.toShort())
                b.put(((if (c.schemaHash != null) 1 else 0) or (if (c.attachmentTreeHash != null) 2 else 0)).toByte())
                b.put(c.parentHashes.size.toByte()).putInt(cc.operationCount).putInt(msg.size)
                c.parentHashes.forEach { b.put(it.bytes) }
                c.schemaHash?.let { b.put(it.bytes) }
                c.attachmentTreeHash?.let { b.put(it.bytes) }
                b.put(msg)
                b.array()
            }
        val out = ByteArray(parts.sumOf { it.size })
        var off = 0
        for (p in parts) {
            p.copyInto(out, off)
            off += p.size
        }
        return out
    }
}
