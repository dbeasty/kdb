package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.schema.FieldSchema
import dev.kdb.codec.schema.FixedSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema
import dev.kdb.codec.toKdbTimestamp
import dev.kdb.codec.toKdbUuid
import dev.kdb.dag.CheckpointCommit
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTag
import dev.kdb.document.kdbSha256
import dev.kdb.storage.PlatformIoShim
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Go's namespace checkpoint (go/kdb/embed/checkpoint.go, checkpoint_section.go), read-only.
 *
 * Go owns the format and writes it; this reads what Go writes so a Kotlin open can skip replaying
 * the segments a checkpoint accounts for. Versions:
 *
 *  - 2: commits (field 4) and the live tree (7) as codec records.
 *  - 3: version 2 plus graph segment references (11, 12) - read, then refused (see [graphRefs]).
 *  - 4: the live tree (13) and commits (14) packed; 5 adds attachment tables (15).
 *  - 6: 4's packed tree and commits moved to a section snapshot the record names (16-21).
 *
 * Every failure here is "not usable", never an answer: the caller replays the log instead.
 */
internal class GoCheckpoint(
    val version: Int,
    val namespaceId: String,
    val throughSequence: Long,
    val floorSequence: Long,
    /** Commits as records (version 2/3) - decoded already. */
    val recordCommits: List<CheckpointCommit>,
    /** Commits packed (version 4+): decoded lazily by [commits]. */
    val packedCommits: ByteArray,
    val branches: List<KdbBranch>,
    val tags: List<KdbTag>,
    val liveTree: DocumentTree,
    val segments: List<Fingerprint>,
    val headCommitPayloads: List<ByteArray>,
    /** Graph segments holding commits the checkpoint leaves out (history=full with graph.file). */
    val graphRefs: Int,
    val attachmentTables: ByteArray?,
) {
    /** One delta segment the checkpoint covers, as Go fingerprinted it (checkpointSegment). */
    data class Fingerprint(
        val sequence: Long,
        /** Where the segment's intact frames ended. */
        val sizeBytes: Long,
        val lastCommitHash: KdbHash,
        /** File size and modification time in nanoseconds when Go wrote the checkpoint, if it could. */
        val stampSize: Long?,
        val stampModTimeNanos: Long?,
    )

    /** Every commit the checkpoint carries, without operations. */
    fun commits(): Sequence<CheckpointCommit> =
        recordCommits.asSequence() + packedCommitSequence(packedCommits, namespaceId)

    companion object {
        const val VERSION_LEGACY = 2
        const val VERSION_GRAPH = 3
        const val VERSION_PACKED = 4
        const val VERSION_ATTACH = 5
        const val VERSION_SECTION = 6

        /** Bytes per packed live-tree entry: 16-byte document id, 32-byte content hash. */
        const val PACKED_TREE_ENTRY = 16 + 32

        private const val SECTION_HEADER = 8 + 8 + 8
        private val SECTION_MAGIC = "KDBCPS01".encodeToByteArray()

        fun key(namespaceId: String): String = "kdb:checkpoint:$namespaceId"

        /**
         * Reads and decodes the checkpoint for [namespaceId] through [shim], with its section for
         * version 6. Returns null when there is none; throws [UnusableCheckpointException] when
         * there is one this build cannot use.
         */
        suspend fun read(
            shim: PlatformIoShim,
            namespaceId: String,
        ): GoCheckpoint? {
            val raw = shim.readSnapshot(key(namespaceId))
            if (raw == null || raw.isEmpty()) return null
            return decode(namespaceId, raw) { k -> shim.readSnapshot(k) }
        }

        /** Decodes [raw], the checkpoint record; [readSection] fetches a version 6 record's section. */
        suspend fun decode(
            namespaceId: String,
            raw: ByteArray,
            readSection: suspend (String) -> ByteArray?,
        ): GoCheckpoint {
            val rec =
                try {
                    KdbValue.decodeFromBytes(raw, KdbType.Ref(FQN_CHECKPOINT), registry) as? KdbValue.RecordVal
                } catch (e: Exception) {
                    throw UnusableCheckpointException("it does not decode (${e.message})", e)
                } ?: throw UnusableCheckpointException("its root is not a record")
            val version = (rec.fields[1] as? KdbValue.Int32Val)?.v ?: throw UnusableCheckpointException("it has no format version")
            if (version !in VERSION_LEGACY..VERSION_SECTION) {
                throw UnusableCheckpointException(
                    "it is format version $version, and this build reads $VERSION_LEGACY-$VERSION_SECTION",
                    foreign = true,
                )
            }
            val ns = str(rec.fields[2], "namespaceId")
            if (ns != namespaceId) {
                throw UnusableCheckpointException("it describes namespace \"$ns\"")
            }
            val through = (rec.fields[3] as? KdbValue.Int64Val)?.v ?: throw UnusableCheckpointException("it has no throughSequence")
            val floor = (rec.fields[10] as? KdbValue.Int64Val)?.v ?: 0L

            val recordCommits = arr(rec.fields[4]).map { recordCommit(it, ns) }
            val branches =
                arr(rec.fields[5]).map {
                    val r = record(it, "branch")
                    KdbBranch(str(r.fields[1], "branch name"), ns, hash(r.fields[2]), ts(r.fields[3]), ts(r.fields[4]))
                }
            val tags =
                arr(rec.fields[6]).map {
                    val r = record(it, "tag")
                    KdbTag(str(r.fields[1], "tag name"), ns, hash(r.fields[2]), ts(r.fields[3]), str(r.fields[4], "tag message"))
                }
            val liveRecords = arr(rec.fields[7])
            val segments =
                arr(rec.fields[8]).map {
                    val r = record(it, "segment")
                    val seq = (r.fields[1] as? KdbValue.Int64Val)?.v ?: throw UnusableCheckpointException("a segment entry has no sequence")
                    val size = (r.fields[2] as? KdbValue.Int64Val)?.v ?: throw UnusableCheckpointException("a segment entry has no size")
                    val stampSize = (r.fields[4] as? KdbValue.Int64Val)?.v?.takeIf { s -> s >= 0 }
                    val stampMod = if (stampSize != null) (r.fields[5] as? KdbValue.Int64Val)?.v else null
                    Fingerprint(seq, size, hash(r.fields[3]), stampSize.takeIf { stampMod != null }, stampMod)
                }
            val heads =
                arr(rec.fields[9]).map {
                    (it as? KdbValue.BytesVal)?.v ?: throw UnusableCheckpointException("a head commit payload is malformed")
                }
            val graphRefs = if (version >= VERSION_GRAPH) arr(rec.fields[11]).size else 0
            if (version == VERSION_GRAPH && graphRefs == 0) {
                throw UnusableCheckpointException("it is version 3 with no graph segments")
            }

            var packedCommits = ByteArray(0)
            var attachmentTables: ByteArray? = null
            val liveTree: DocumentTree
            when {
                version == VERSION_SECTION -> {
                    if (liveRecords.isNotEmpty() || recordCommits.isNotEmpty()) {
                        throw UnusableCheckpointException("version 6 lists commits or live documents as records")
                    }
                    val section = sectionFromRecord(rec, ns)
                    if ((rec.fields[21] as? KdbValue.Bool)?.v == true) {
                        attachmentTables = (rec.fields[15] as? KdbValue.BytesVal)?.v
                    }
                    val bytes = readSection(section.key) ?: throw UnusableCheckpointException("its section ${section.key} is missing")
                    val (tree, commits) = verifySection(section, bytes)
                    liveTree = packedTree(tree)
                    packedCommits = commits
                }
                version >= VERSION_PACKED -> {
                    if (liveRecords.isNotEmpty()) throw UnusableCheckpointException("the live tree is stored both packed and as records")
                    liveTree = packedTree((rec.fields[13] as? KdbValue.BytesVal)?.v ?: ByteArray(0))
                    packedCommits = (rec.fields[14] as? KdbValue.BytesVal)?.v ?: ByteArray(0)
                    if (version >= VERSION_ATTACH) attachmentTables = (rec.fields[15] as? KdbValue.BytesVal)?.v
                }
                else -> {
                    val live = LinkedHashMap<KdbUuid, KdbHash>(liveRecords.size)
                    for (e in liveRecords) {
                        val r = record(e, "tree entry")
                        live[uuid(r.fields[1])] = hash(r.fields[2])
                    }
                    liveTree = DocumentTree.build(live)
                }
            }
            return GoCheckpoint(
                version = version,
                namespaceId = ns,
                throughSequence = through,
                floorSequence = floor,
                recordCommits = recordCommits,
                packedCommits = packedCommits,
                branches = branches,
                tags = tags,
                liveTree = liveTree,
                segments = segments,
                headCommitPayloads = heads,
                graphRefs = graphRefs,
                attachmentTables = attachmentTables?.takeIf { it.isNotEmpty() },
            )
        }

        private class Section(
            val key: String,
            val treeDigest: ByteArray,
            val entries: Long,
            val treeLen: Long,
            val commitsLen: Long,
        )

        private fun sectionFromRecord(
            rec: KdbValue.RecordVal,
            ns: String,
        ): Section {
            val key = (rec.fields[16] as? KdbValue.StringVal)?.v?.takeIf { it.isNotEmpty() }
                ?: throw UnusableCheckpointException("version 6 without a section key")
            val digest = (rec.fields[17] as? KdbValue.FixedVal)?.v ?: throw UnusableCheckpointException("version 6 without a tree digest")
            val entries = (rec.fields[18] as? KdbValue.Int64Val)?.v
            val treeLen = (rec.fields[19] as? KdbValue.Int64Val)?.v
            val commitsLen = (rec.fields[20] as? KdbValue.Int64Val)?.v
            if (entries == null || treeLen == null || commitsLen == null || entries < 0 || treeLen < 0 || commitsLen < 0 ||
                treeLen != entries * PACKED_TREE_ENTRY || !key.startsWith("kdb:checkpoint-section:$ns:")
            ) {
                throw UnusableCheckpointException("version 6 with a malformed section description")
            }
            return Section(key, digest, entries, treeLen, commitsLen)
        }

        /**
         * Go's readCheckpointSection checks: the length the record implies, magic, both lengths,
         * the CRC-32 over everything before the trailer, and SHA-256 of the packed tree against the
         * record's digest - plus that the key is the one those digests name, so a section can only
         * be the one written for this record.
         */
        private fun verifySection(
            s: Section,
            raw: ByteArray,
        ): Pair<ByteArray, ByteArray> {
            val want = SECTION_HEADER + s.treeLen + s.commitsLen + 4
            if (raw.size.toLong() != want) {
                throw UnusableCheckpointException("its section ${s.key} is ${raw.size} bytes, and the record says $want")
            }
            val le = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
            if (!raw.copyOfRange(0, 8).contentEquals(SECTION_MAGIC) || le.getLong(8) != s.treeLen || le.getLong(16) != s.commitsLen) {
                throw UnusableCheckpointException("its section ${s.key} does not match the record")
            }
            val crc = CRC32().apply { update(raw, 0, raw.size - 4) }.value.toInt()
            if (crc != le.getInt(raw.size - 4)) {
                throw UnusableCheckpointException("its section ${s.key} fails its checksum")
            }
            val tree = raw.copyOfRange(SECTION_HEADER, SECTION_HEADER + s.treeLen.toInt())
            val commits = raw.copyOfRange(SECTION_HEADER + s.treeLen.toInt(), raw.size - 4)
            val treeDigest = kdbSha256(tree)
            if (!treeDigest.contentEquals(s.treeDigest)) {
                throw UnusableCheckpointException("its section ${s.key} holds a different tree than the record names")
            }
            val commitsDigest = kdbSha256(commits)
            val id = ByteArray(16) { (treeDigest[it].toInt() xor commitsDigest[it].toInt()).toByte() }
            val ns = s.key.removePrefix("kdb:checkpoint-section:").substringBeforeLast(':')
            if (s.key != "kdb:checkpoint-section:$ns:${id.toHexString()}") {
                throw UnusableCheckpointException("its section ${s.key} is not named after what it holds")
            }
            return tree to commits
        }

        /** A packed live tree (Go AppendPacked): entries in strictly ascending id order. */
        internal fun packedTree(packed: ByteArray): DocumentTree {
            if (packed.size % PACKED_TREE_ENTRY != 0) {
                throw UnusableCheckpointException("the packed live tree is not a whole number of entries")
            }
            val n = packed.size / PACKED_TREE_ENTRY
            val entries = LinkedHashMap<KdbUuid, KdbHash>(n * 4 / 3 + 1)
            var prev: ByteArray? = null
            for (i in 0 until n) {
                val off = i * PACKED_TREE_ENTRY
                val id = packed.copyOfRange(off, off + 16)
                if (prev != null && compareUnsigned(prev, id) >= 0) {
                    throw UnusableCheckpointException("the packed live tree is out of order at entry $i")
                }
                prev = id
                entries[KdbUuid.fromBytes(id)] = KdbHash.fromBytes(packed.copyOfRange(off + 16, off + PACKED_TREE_ENTRY))
            }
            return DocumentTree.build(entries)
        }

        /** Packed checkpoint commits (go/kdb/dag/checkpoint_packed.go), decoded one at a time. */
        internal fun packedCommitSequence(
            packed: ByteArray,
            namespaceId: String,
        ): Sequence<CheckpointCommit> =
            sequence {
                val b = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN)
                var off = 0
                while (off < packed.size) {
                    if (packed.size - off < PACKED_COMMIT_FIXED) {
                        throw UnusableCheckpointException("a packed commit is truncated at $off")
                    }
                    val hash = KdbHash.fromBytes(packed.copyOfRange(off, off + 32))
                    val tree = KdbHash.fromBytes(packed.copyOfRange(off + 32, off + 64))
                    val tx = KdbUuid(b.getLong(off + 64), b.getLong(off + 72))
                    val author = KdbUuid(b.getLong(off + 80), b.getLong(off + 88))
                    val millis = b.getLong(off + 96)
                    val micro = b.getShort(off + 104).toInt() and 0xFFFF
                    val flags = packed[off + 106].toInt()
                    val nParents = packed[off + 107].toInt() and 0xFF
                    val opCount = b.getInt(off + 108).toLong() and 0xFFFFFFFFL
                    val msgLen = b.getInt(off + 112).toLong() and 0xFFFFFFFFL
                    off += PACKED_COMMIT_FIXED
                    val hasSchema = flags and 1 != 0
                    val hasAttach = flags and 2 != 0
                    val need = nParents * 32L + msgLen + (if (hasSchema) 32 else 0) + (if (hasAttach) 32 else 0)
                    if (packed.size - off < need || micro > 999) {
                        throw UnusableCheckpointException("packed commit ${hash.toHex()} is malformed")
                    }
                    val parents = List(nParents) { i -> KdbHash.fromBytes(packed.copyOfRange(off + i * 32, off + i * 32 + 32)) }
                    off += nParents * 32
                    val schema = if (hasSchema) KdbHash.fromBytes(packed.copyOfRange(off, off + 32)).also { off += 32 } else null
                    val attach = if (hasAttach) KdbHash.fromBytes(packed.copyOfRange(off, off + 32)).also { off += 32 } else null
                    val message = packed.decodeToString(off, off + msgLen.toInt())
                    off += msgLen.toInt()
                    val c =
                        KdbCommit(
                            hash = hash,
                            parentHashes = parents,
                            namespaceId = namespaceId,
                            transactionId = tx,
                            timestamp = KdbTimestamp(millis, micro),
                            authorNodeId = author,
                            operations = emptyList(),
                            documentTreeHash = tree,
                            schemaHash = schema,
                            message = message,
                            attachmentTreeHash = attach,
                        )
                    yield(CheckpointCommit(c, opCount.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()))
                }
            }

        private const val PACKED_COMMIT_FIXED = 32 + 32 + 16 + 16 + 8 + 2 + 1 + 1 + 4 + 4

        private fun recordCommit(
            v: KdbValue,
            ns: String,
        ): CheckpointCommit {
            val r = record(v, "commit")
            val schema =
                when (val sf = r.fields[7]) {
                    null, KdbValue.Null -> null
                    else -> hash(sf)
                }
            val count = (r.fields[9] as? KdbValue.Int32Val)?.v ?: throw UnusableCheckpointException("a commit has no operation count")
            val c =
                KdbCommit(
                    hash = hash(r.fields[1]),
                    parentHashes = arr(r.fields[2]).map { hash(it) },
                    namespaceId = ns,
                    transactionId = uuid(r.fields[3]),
                    timestamp = ts(r.fields[4]),
                    authorNodeId = uuid(r.fields[5]),
                    operations = emptyList(),
                    documentTreeHash = hash(r.fields[6]),
                    schemaHash = schema,
                    message = str(r.fields[8], "commit message"),
                )
            return CheckpointCommit(c, count)
        }

        private fun record(
            v: KdbValue?,
            what: String,
        ): KdbValue.RecordVal = v as? KdbValue.RecordVal ?: throw UnusableCheckpointException("a $what entry is malformed")

        private fun arr(v: KdbValue?): List<KdbValue> = (v as? KdbValue.ArrayVal)?.elements ?: emptyList()

        private fun str(
            v: KdbValue?,
            what: String,
        ): String = (v as? KdbValue.StringVal)?.v ?: throw UnusableCheckpointException("$what is malformed")

        private fun hash(v: KdbValue?): KdbHash {
            val b = (v as? KdbValue.FixedVal)?.v ?: throw UnusableCheckpointException("a hash is malformed")
            if (b.size != 32) throw UnusableCheckpointException("a hash is ${b.size} bytes")
            return KdbHash.fromBytes(b)
        }

        private fun uuid(v: KdbValue?): KdbUuid = (v as? KdbValue.UuidVal)?.toKdbUuid() ?: throw UnusableCheckpointException("a uuid is malformed")

        private fun ts(v: KdbValue?): KdbTimestamp =
            (v as? KdbValue.TimestampVal)?.toKdbTimestamp() ?: throw UnusableCheckpointException("a timestamp is malformed")

        private fun compareUnsigned(
            a: ByteArray,
            b: ByteArray,
        ): Int {
            for (i in a.indices) {
                val d = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
                if (d != 0) return d
            }
            return 0
        }

        private fun ByteArray.toHexString(): String = joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

        private const val NS = "dev.kdb.checkpoint"
        internal const val FQN_CHECKPOINT = "$NS.Checkpoint"

        /** Go's checkpointRegistry, field for field: a type mismatch fails the decode, never misreads. */
        internal val registry: KdbTypeRegistry by lazy {
            val r = KdbTypeRegistry.create()
            val uuidT = KdbType.Primitive(PhysicalKind.FIXED, LogicalAnnotation.Uuid)
            val hashT = KdbType.Ref("$NS.Hash32")
            val tsT = KdbType.Primitive(PhysicalKind.INT64, LogicalAnnotation.TimestampMicros(null))
            val strT = KdbType.Primitive(PhysicalKind.STRING)
            val i64 = KdbType.Primitive(PhysicalKind.INT64)
            val i32 = KdbType.Primitive(PhysicalKind.INT32)
            val bytesT = KdbType.Primitive(PhysicalKind.BYTES)
            fun rec(
                name: String,
                vararg fields: FieldSchema,
            ) = r.registerRecord(RecordSchema(name = name, namespace = NS, fields = fields.toList()))
            r.registerFixed(FixedSchema(name = "Hash32", namespace = NS, size = 32))
            rec("CheckpointTreeEntry", FieldSchema(1, "docId", uuidT), FieldSchema(2, "contentHash", hashT))
            rec(
                "CheckpointCommit",
                FieldSchema(1, "hash", hashT),
                FieldSchema(2, "parentHashes", KdbType.Array(hashT)),
                FieldSchema(3, "transactionId", uuidT),
                FieldSchema(4, "timestamp", tsT),
                FieldSchema(5, "authorNodeId", uuidT),
                FieldSchema(6, "documentTreeHash", hashT),
                FieldSchema(7, "schemaHash", KdbType.Nullable(hashT), KdbValue.Null),
                FieldSchema(8, "message", strT),
                FieldSchema(9, "operationCount", i32),
            )
            rec(
                "CheckpointBranch",
                FieldSchema(1, "name", strT),
                FieldSchema(2, "headHash", hashT),
                FieldSchema(3, "createdAt", tsT),
                FieldSchema(4, "updatedAt", tsT),
            )
            rec(
                "CheckpointTag",
                FieldSchema(1, "name", strT),
                FieldSchema(2, "commitHash", hashT),
                FieldSchema(3, "createdAt", tsT),
                FieldSchema(4, "message", strT),
            )
            rec(
                "CheckpointSegment",
                FieldSchema(1, "sequence", i64),
                FieldSchema(2, "sizeBytes", i64),
                FieldSchema(3, "lastCommitHash", hashT),
                FieldSchema(4, "stampSize", i64, KdbValue.Int64Val(-1)),
                FieldSchema(5, "stampModTimeNanos", i64, KdbValue.Int64Val(0)),
            )
            rec(
                "CheckpointGraphSegment",
                FieldSchema(1, "sequence", i64),
                FieldSchema(2, "crc", i64),
                FieldSchema(3, "records", i64),
            )
            rec(
                "Checkpoint",
                FieldSchema(1, "formatVersion", i32),
                FieldSchema(2, "namespaceId", strT),
                FieldSchema(3, "throughSequence", i64),
                FieldSchema(4, "commits", KdbType.Array(KdbType.Ref("$NS.CheckpointCommit"))),
                FieldSchema(5, "branches", KdbType.Array(KdbType.Ref("$NS.CheckpointBranch"))),
                FieldSchema(6, "tags", KdbType.Array(KdbType.Ref("$NS.CheckpointTag"))),
                FieldSchema(7, "liveTree", KdbType.Array(KdbType.Ref("$NS.CheckpointTreeEntry"))),
                FieldSchema(8, "segments", KdbType.Array(KdbType.Ref("$NS.CheckpointSegment"))),
                FieldSchema(9, "headCommits", KdbType.Array(bytesT)),
                FieldSchema(10, "floorSequence", i64),
                FieldSchema(11, "graphSegments", KdbType.Array(KdbType.Ref("$NS.CheckpointGraphSegment")), KdbValue.ArrayVal(emptyList())),
                FieldSchema(12, "graphHidden", KdbType.Array(hashT), KdbValue.ArrayVal(emptyList())),
                FieldSchema(13, "liveTreePacked", bytesT, KdbValue.BytesVal(ByteArray(0))),
                FieldSchema(14, "commitsPacked", bytesT, KdbValue.BytesVal(ByteArray(0))),
                FieldSchema(15, "attachmentTables", bytesT, KdbValue.BytesVal(ByteArray(0))),
                FieldSchema(16, "sectionKey", strT, KdbValue.StringVal("")),
                FieldSchema(17, "treeDigest", hashT, KdbValue.FixedVal(ByteArray(32))),
                FieldSchema(18, "treeEntries", i64, KdbValue.Int64Val(0)),
                FieldSchema(19, "treeBytes", i64, KdbValue.Int64Val(0)),
                FieldSchema(20, "commitsBytes", i64, KdbValue.Int64Val(0)),
                FieldSchema(21, "commitsHaveAttachments", KdbType.Primitive(PhysicalKind.BOOLEAN), KdbValue.Bool(false)),
            )
            r.freeze()
            r
        }
    }
}

/** A checkpoint this build cannot use; the open replays the log instead. */
internal class UnusableCheckpointException(
    reason: String,
    cause: Throwable? = null,
    /** Unusable because it is in a format or uses a feature this build does not read, not because it is
     * stale or damaged: a checkpoint Kotlin must not replace with its own (see [NamespaceCheckpointer]). */
    val foreign: Boolean = false,
) : IllegalStateException(reason, cause)
