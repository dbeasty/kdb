package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CheckpointRestorableDag
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.delta.DeltaSegmentScanner
import dev.kdb.storage.engine.ServerStorageEngine
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.sstable.readSsTableValues
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit

/**
 * Opens a namespace from Go's checkpoint instead of replaying its whole delta log - Go's
 * restoreNamespace (go/kdb/embed/checkpoint_open.go), read-only: Kotlin reads Go's checkpoints and
 * never writes one.
 *
 * The checkpoint is a cache, never an authority, and the rule here is Go's: anything wrong with it -
 * absent, undecodable, a version or feature this build cannot read, describing another namespace,
 * disagreeing with the log it was written against, or naming a document body nothing on disk
 * still holds - makes [open] return null, and the caller replays the log in full. A checkpoint
 * Kotlin cannot use costs a slow open, never a wrong answer.
 *
 * What it does with one it can use:
 *  1. Checks it against the log (Go's checkpointMatchesLog): every segment it fingerprinted at or
 *     above its floor is still there and unchanged - by file stamp when nothing has touched the
 *     segment since, else by rescanning it - and no segment it covers is newer than the log.
 *  2. Restores the commit graph (commits without operations, loaded lazily from the log when read),
 *     refs, the heads' operations and attachment tables, and the live tree.
 *  3. Replays only the segments after its ThroughSequence.
 *  4. Finds the body of every document the live tree names: Go keeps bodies in the delta log under
 *     history=full (with a location record per version in its blob store), and in the blob store
 *     under history=none and for snapshot or squash state. Every body is checked against the
 *     content hash the tree names before it is used.
 *
 * Not read: checkpoints that reference Go's graph segments (graph.file under history=full,
 * versions 3+ with fields 11/12) - those replay. Go's tree segments (tree.file) are only a cache of
 * the packed tree every checkpoint version carries itself, so they need no reader.
 */
internal class CheckpointOpen(
    private val dataRoot: String,
    private val namespaceId: String,
    private val io: PlatformIoShim,
    private val engine: ServerStorageEngine,
    private val reader: dev.kdb.storage.delta.DefaultDeltaSegmentReader,
    private val decisions: CrossNamespaceDecisions?,
    private val shallowRoots: Set<KdbHash>,
    private val graftRoots: Set<KdbHash>,
) {
    /** How the last [matchesLog] checked the log: by file stamps alone, or by rescanning segments. */
    private var checkedBy = ""

    /** Why the last [open] returned null. */
    var whyNot: String = ""
        private set

    /**
     * The namespace's DAG, opened from the checkpoint with the engine holding its live state, or
     * null (see [whyNot]) with the engine reset to empty, for the caller to replay the log.
     *
     * @throws Exception from the tail replay itself - damage after the checkpoint is refused as a
     *   full replay would refuse it, not hidden behind one.
     */
    suspend fun open(): CommitDag? {
        val cp =
            try {
                GoCheckpoint.read(io, namespaceId)
            } catch (e: UnusableCheckpointException) {
                return unusable("${e.message}")
            } ?: return unusable("there is none")
        if (cp.graphRefs > 0) {
            return unusable(
                "it references ${cp.graphRefs} Go graph segment(s) (graph.file) for commits it does not carry, which this build does not read",
            )
        }
        val sequences = deltaSequences()
        matchesLog(cp, sequences)?.let { return unusable(it) }
        val highest = sequences.lastOrNull() ?: -1L
        if (cp.throughSequence > highest) {
            return unusable("it covers delta segments through sequence ${cp.throughSequence} but the newest on disk is $highest")
        }

        val dag = inMemoryCommitDag(namespaceId)
        val restorable = dag as CheckpointRestorableDag
        val ops = LogCommitLoader()
        restorable.setOperationsLoader { ops.load(it) }
        try {
            restorable.restoreCheckpoint(cp.commits(), cp.branches, cp.tags)
            cp.attachmentTables?.let { dag.seedPackedAttachmentTables(it) }
            restorable.restoreCommitOperations(cp.headCommitPayloads.map { KdbCommit.fromPayloadBytes(it) })
            // Branch heads with their operations resident, as Go's HydrateBranchHeads: a head the
            // checkpoint carried no operations for is loaded from the log now, or this fails here.
            for (b in dag.listBranches()) dag.getCommitOrThrow(b.headHash)
        } catch (e: Exception) {
            return unusable("its commit graph could not be restored (${e.message})")
        }
        val head = dag.getCommitOrThrow(dag.head())
        if (head.documentTreeHash != cp.liveTree.treeHash) {
            return unusable(
                "its live tree (${cp.liveTree.treeHash.toHex()}) is not main's (${head.documentTreeHash.toHex()})",
            )
        }
        engine.restoreLiveTree(cp.liveTree)
        dag.putDocumentTree(cp.liveTree)

        // Only the tail: everything up to throughSequence is in the checkpoint.
        val tail = reader.listSegmentsAfter(cp.throughSequence).size
        DeltaNamespaceReplayer.replay(dag, engine, reader, decisions, cp.throughSequence, shallowRoots + graftRoots)

        val found = BodyLoader(cp.throughSequence).loadAll()
        val stillMissing = engine.documentsWithoutBody()
        if (stillMissing.isNotEmpty()) {
            val (id, h) = stillMissing.entries.first()
            return unusable(
                "${stillMissing.size} live document version(s) are in no blob store table and no delta segment on disk " +
                    "(first: $id at ${h.toHex()})",
            )
        }
        System.err.println(
            "kdb: namespace $namespaceId: opened from its checkpoint (format v${cp.version}, through delta segment " +
                "${cp.throughSequence}; $checkedBy); replayed $tail later segment(s); ${cp.liveTree.size} live document(s), bodies from " +
                "the blob store ${found.blobStore}, log locations ${found.located}, segment scan ${found.scanned}",
        )
        return dag
    }

    private suspend fun unusable(why: String): CommitDag? {
        whyNot = why
        engine.restoreLiveTree(DocumentTree.EMPTY)
        return null
    }

    // --- the log ------------------------------------------------------------------------------

    private val deltaPrefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"

    /** Sequence numbers of the delta segments on disk, ascending - from names alone. */
    private suspend fun deltaSequences(): List<Long> =
        io.listSegments(namespaceId)
            .filter { it.startsWith(deltaPrefix) }
            .mapNotNull { SegmentNameBuilder.parseDeltaSequencedFileName(it.removePrefix(deltaPrefix)) }
            .sorted()

    private fun segmentFile(seq: Long): File = File(dataRoot, SegmentNameBuilder.deltaSequenced(namespaceId, seq))

    /** Go's SegmentStamp: the file's size and modification time in nanoseconds. */
    private fun stamp(seq: Long): Pair<Long, Long>? =
        try {
            val a = Files.readAttributes(segmentFile(seq).toPath(), BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
            a.size() to a.lastModifiedTime().to(TimeUnit.NANOSECONDS)
        } catch (_: Exception) {
            null
        }

    private suspend fun segmentBytes(seq: Long): ByteArray =
        io.readFromSegment(SegmentNameBuilder.deltaSequenced(namespaceId, seq), 0, Int.MAX_VALUE)

    /**
     * Null when the log still says what it said when [cp] was written, else why not - Go's
     * checkpointMatchesLog, with its stamp fast path (unchangedSinceCheckpoint). A segment below
     * the checkpoint's floor was truncated on purpose and is expected to be gone; one missing at
     * or above it is damage.
     */
    private suspend fun matchesLog(
        cp: GoCheckpoint,
        onDisk: List<Long>,
    ): String? {
        if (unchangedSinceCheckpoint(cp, onDisk)) {
            checkedBy = "log checked by file stamps, no covered segment read"
            return null
        }
        checkedBy = "log checked by rescanning ${cp.segments.count { it.sequence >= cp.floorSequence }} covered segment(s)"
        val present = onDisk.toSet()
        val damaged = mutableListOf<String>()
        for (want in cp.segments) {
            if (want.sequence < cp.floorSequence) continue
            if (want.sequence !in present) return "delta segment ${want.sequence} is missing"
            val bytes = segmentBytes(want.sequence)
            val scan = DeltaSegmentScanner.scanSegment(bytes)
            val end = scan.tail.offset.toLong()
            val last = scan.commits.lastOrNull()?.commitHash ?: ZERO_HASH
            if (end == want.sizeBytes && last == want.lastCommitHash) continue
            if (damagedInPlace(bytes, scan, want)) {
                damaged += "delta segment ${want.sequence} has a damaged frame at offset ${scan.tail.offset}"
                continue
            }
            if (end != want.sizeBytes) {
                return "delta segment ${want.sequence} is $end bytes but was ${want.sizeBytes} when the checkpoint was written"
            }
            return "delta segment ${want.sequence} ends at a different commit than it did"
        }
        if (damaged.isNotEmpty()) {
            System.err.println(
                "kdb: namespace $namespaceId: ${damaged.joinToString("; ")} - opening from the checkpoint, which holds what " +
                    "they recorded",
            )
        }
        return null
    }

    /**
     * True when every fingerprinted segment still has the stamp it had then and no segment the
     * checkpoint covers has appeared since: nothing need be read. Any doubt is false.
     */
    private fun unchangedSinceCheckpoint(
        cp: GoCheckpoint,
        onDisk: List<Long>,
    ): Boolean {
        val wanted = HashSet<Long>()
        for (want in cp.segments) {
            wanted += want.sequence
            if (want.sequence < cp.floorSequence) continue
            val size = want.stampSize ?: return false
            val mod = want.stampModTimeNanos ?: return false
            if (stamp(want.sequence) != size to mod) return false
        }
        return onDisk.none { it <= cp.throughSequence && it >= cp.floorSequence && it !in wanted }
    }

    /**
     * Go's damagedInPlace, narrowed to the case that cannot be a different file: the same length
     * on disk as the checkpoint recorded, with damage in the middle, and the intact frames read
     * past it still ending at the commit the checkpoint recorded. Anything less certain (a damaged
     * last frame, a preallocated file) is treated as a changed log.
     */
    private fun damagedInPlace(
        bytes: ByteArray,
        scan: DeltaSegmentScanner.SegmentScan,
        want: GoCheckpoint.Fingerprint,
    ): Boolean {
        if (scan.tail.kind != DeltaSegmentScanner.TailKind.DAMAGED) return false
        if (bytes.size.toLong() != want.sizeBytes) return false
        val after = DeltaSegmentScanner.intactCommitsAfter(bytes, scan.tail.offset)
        return after.lastOrNull()?.commitHash == want.lastCommitHash
    }

    /**
     * Commits by hash out of the whole log, for operations a checkpoint left out. Indexes every
     * segment the first time it is asked - the cost of reading history, paid only by what reads it.
     */
    private inner class LogCommitLoader {
        private var index: Map<KdbHash, Long>? = null
        private var cachedSeq = -1L
        private var cached: Map<KdbHash, KdbCommit> = emptyMap()

        suspend fun load(hash: KdbHash): KdbCommit? {
            val idx = index ?: buildIndex().also { index = it }
            val seq = idx[hash] ?: return null
            if (seq != cachedSeq) {
                cached = commitsIn(seq).associateBy { it.hash }
                cachedSeq = seq
            }
            return cached[hash]
        }

        private suspend fun buildIndex(): Map<KdbHash, Long> {
            val out = HashMap<KdbHash, Long>()
            for (seq in deltaSequences()) for (c in commitsIn(seq)) out.putIfAbsent(c.hash, seq)
            return out
        }
    }

    /** The commits in a segment's intact frames, including any past a damaged one. */
    private suspend fun commitsIn(seq: Long): List<KdbCommit> = scannedIn(seq).map { it.commit }

    private suspend fun scannedIn(seq: Long): List<DeltaSegmentScanner.ScannedCommit> {
        val bytes = segmentBytes(seq)
        val scan = DeltaSegmentScanner.scanSegment(bytes)
        if (scan.tail.kind != DeltaSegmentScanner.TailKind.DAMAGED) return scan.commits
        return scan.commits + DeltaSegmentScanner.intactCommitsAfter(bytes, scan.tail.offset)
    }

    // --- document bodies ----------------------------------------------------------------------

    data class Found(
        var blobStore: Int = 0,
        var located: Int = 0,
        var scanned: Int = 0,
    )

    /**
     * Supplies the engine with the body of every live-tree version it lacks, verified by content
     * hash: from the WAL-recovered memtable and Go's SSTables (raw bodies, or Go's 17-byte location
     * records pointing at the frame that wrote the version), then by scanning the segments the
     * checkpoint covers, newest first, for whatever is left.
     */
    private inner class BodyLoader(
        private val through: Long,
    ) {
        private val found = Found()

        suspend fun loadAll(): Found {
            var missing = engine.documentsWithoutBody()
            if (missing.isEmpty()) return found
            val locations = HashMap<KdbHash, Pair<Long, Long>>()
            val byHash = missing.entries.groupBy({ it.value }, { it.key })

            // The memtable: Go's WAL is the same format, and was recovered into it at open.
            install(
                byHash.mapNotNull { (h, ids) -> engine.readBlob(h)?.let { classify(h, ids, it, locations) } }.flatten(),
            ) { found.blobStore += it }

            missing = engine.documentsWithoutBody()
            if (missing.isNotEmpty()) {
                val want = missing.values.toMutableSet()
                val tables = io.listSegments(namespaceId).filter { it.startsWith(sstablePrefix) }
                for (t in tables.asReversed()) {
                    if (want.isEmpty()) break
                    val values =
                        try {
                            readSsTableValues(io, t, want)
                        } catch (e: Exception) {
                            System.err.println("kdb: namespace $namespaceId: skipping unreadable sstable $t (${e.message})")
                            continue
                        }
                    val docs =
                        values.flatMap { (h, v) ->
                            classify(h, missing.filterValues { it == h }.keys, v, locations)
                        }
                    install(docs) { found.blobStore += it }
                    want.removeAll(docs.map { it.contentHash }.toSet())
                }
            }

            missing = engine.documentsWithoutBody()
            if (missing.isNotEmpty() && locations.isNotEmpty()) {
                val docs = mutableListOf<KdbDocument>()
                for ((seq, group) in locations.entries.filter { (h, _) -> h in missing.values }.groupBy { it.value.first }) {
                    val frames = runCatching { scannedIn(seq) }.getOrNull() ?: continue
                    val byOffset = frames.associateBy { it.frameOffset.toLong() }
                    for ((h, loc) in group) {
                        val commit = byOffset[loc.second]?.commit ?: continue
                        docs += versionsIn(commit, missing.filterValues { it == h })
                    }
                }
                install(docs) { found.located += it }
            }

            missing = engine.documentsWithoutBody()
            if (missing.isNotEmpty()) {
                // No blob and no location: the frame that wrote it is in a segment the checkpoint
                // covers. Newest first, since a live version is usually a recent one.
                val seqs = deltaSequences().filter { it <= through }.asReversed()
                for (seq in seqs) {
                    if (missing.isEmpty()) break
                    val docs = scannedIn(seq).flatMap { versionsIn(it.commit, missing) }
                    install(docs) { found.scanned += it }
                    missing = engine.documentsWithoutBody()
                }
            }
            return found
        }

        private val sstablePrefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "sstable/"

        /** A blob-store value: a location record (filed in [locations]) or a body, checked. */
        private fun classify(
            h: KdbHash,
            ids: Collection<KdbUuid>,
            value: ByteArray,
            locations: MutableMap<KdbHash, Pair<Long, Long>>,
        ): List<KdbDocument> {
            if (value.size == LOCATION_BYTES && value[0] == LOCATION_TAG) {
                val b = java.nio.ByteBuffer.wrap(value)
                locations[h] = b.getLong(1) to b.getLong(9)
                return emptyList()
            }
            val text = value.decodeToString()
            return ids.mapNotNull { id -> verified(id, text, h) }
        }

        /** The versions [commit] writes that [wanted] (doc to content hash) names. */
        private fun versionsIn(
            commit: KdbCommit,
            wanted: Map<KdbUuid, KdbHash>,
        ): List<KdbDocument> =
            commit.operations.mapNotNull { op ->
                if (op !is KdbOp.Write) return@mapNotNull null
                val h = wanted[op.docId] ?: return@mapNotNull null
                verified(op.docId, op.patch, h)
            }

        private suspend fun install(
            docs: List<KdbDocument>,
            count: (Int) -> Unit,
        ) {
            val unique = docs.distinctBy { it.id }
            if (unique.isEmpty()) return
            engine.installBodies(unique)
            count(unique.size)
        }
    }

    private companion object {
        val ZERO_HASH: KdbHash = KdbHash.fromBytes(ByteArray(32))

        /** Go's documentLocationTag / documentLocationBytes (storage/engine/tree_objects.go). */
        const val LOCATION_TAG: Byte = 2
        const val LOCATION_BYTES = 1 + 8 + 8

        /**
         * The document [text] is for [id], if it is the version [want] names: verbatim, as Go
         * keeps it (documentFromPatch), or as replay parses it.
         */
        fun verified(
            id: KdbUuid,
            text: String,
            want: KdbHash,
        ): KdbDocument? {
            val raw = KdbDocument(id, text)
            if (raw.contentHash == want) return raw
            val parsed = runCatching { KdbDocument.fromJson(id, text) }.getOrNull() ?: return null
            return parsed.takeIf { it.contentHash == want }
        }
    }
}
