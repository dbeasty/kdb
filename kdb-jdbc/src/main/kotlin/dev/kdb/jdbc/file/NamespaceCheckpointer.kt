package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CheckpointRestorableDag
import dev.kdb.dag.CommitDag
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.delta.DeltaSegmentScanner
import dev.kdb.storage.engine.ServerStorageEngine
import dev.kdb.storage.io.SegmentNameBuilder
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit

/**
 * Writes a file runtime's namespace checkpoint in Go's format - Go's saveCheckpointThrough
 * (go/kdb/embed/checkpoint_open.go), the state gathered as Go gathers it and encoded by
 * [GoCheckpointWriter]. One format for both runtimes: Go opens what this writes, and
 * [CheckpointOpen] opens what Go writes.
 *
 * Written when Go writes one (wired in openFileRuntime):
 *  - after an open that replayed the whole log, through the segment below the writer's own (Go's
 *    checkpointAfterFullReplay), so a process that is killed does not replay the log again at
 *    every start;
 *  - at an orderly close, after the writer is sealed, through the newest segment on disk (Go's
 *    checkpointOnClose).
 * Kotlin keeps history=full and never truncates, so the floor is the lowest segment on disk.
 *
 * Best-effort, as Go's: a checkpoint is a cache, and failing to write one costs the next open a
 * replay, never data. It is not written - [save] returns why - when it could claim something this
 * runtime does not hold:
 *  - a segment it would cover ends at a commit this DAG lacks (another runtime in this process
 *    appending to the same namespace);
 *  - a segment it would cover is damaged in place;
 *  - main's tree is not the engine's live tree, or the DAG holds archived commit stubs;
 *  - the checkpoint on disk is one this build cannot read (a newer format, or Go's graph segment
 *    references): it says more than Kotlin could restate, so it is left as Go wrote it.
 */
internal class NamespaceCheckpointer(
    private val dataRoot: String,
    private val namespaceId: String,
    private val io: PlatformIoShim,
    private val engine: ServerStorageEngine,
    private val dag: CommitDag,
    /** The namespace's shallow roots now: snapshot installs add to them while the runtime is open. */
    private val shallowRoots: () -> Set<KdbHash>,
) {
    /** Fingerprints a previous checkpoint recorded, reused while a segment's stamp still matches -
     * Go's DeltaFingerprintMemo, so a checkpoint reads only the segments that changed since. */
    private val remembered = HashMap<Long, GoCheckpoint.Fingerprint>()

    /** The checkpoint on disk is one this build does not read; never replaced. */
    var foreignOnDisk: Boolean = false

    fun remember(fingerprints: List<GoCheckpoint.Fingerprint>) {
        for (f in fingerprints) if (f.stampSize != null && f.stampModTimeNanos != null) remembered[f.sequence] = f
    }

    /**
     * Writes a checkpoint claiming every delta segment through [through] (-1: none of them).
     * Returns null when it was written, else why it was not.
     */
    suspend fun save(through: Long): String? {
        if (through < -1) return "nothing to claim"
        if (foreignOnDisk) return "the checkpoint on disk is in a format this build does not read; it is left as it is"
        val restorable = dag as? CheckpointRestorableDag ?: return "this DAG cannot list its commits for a checkpoint"
        val commits = restorable.checkpointCommits() ?: return "the DAG holds archived commit stubs, which a checkpoint does not carry"
        val held = commits.mapTo(HashSet()) { it.commit.hash }
        val branches = dag.listBranches()
        val tags = dag.listTags()
        val main = dag.getCommitOrThrow(dag.head())
        val liveTree = engine.liveTree()
        if (main.documentTreeHash != liveTree.treeHash) {
            return "main's tree (${main.documentTreeHash.toHex()}) is not the live tree (${liveTree.treeHash.toHex()})"
        }

        val onDisk = deltaSequences()
        val segments = ArrayList<GoCheckpoint.Fingerprint>()
        for (seq in onDisk) {
            if (seq > through) break
            val fp = fingerprint(seq) ?: return "delta segment $seq is damaged in place"
            if (fp.lastCommitHash != ZERO_HASH && fp.lastCommitHash !in held) {
                return "delta segment $seq ends at commit ${fp.lastCommitHash.toHex()}, which this runtime does not hold"
            }
            segments += fp
        }

        val roots = shallowRoots()
        // Shallow roots' operations, whether or not a branch still names them (no segment holds
        // them), then every branch head's, as Go lists them.
        val heads = ArrayList<ByteArray>()
        for (h in roots) dag.getCommit(h)?.let { heads += it.toPayloadBytes() }
        for (b in branches) runCatching { dag.getCommitOrThrow(b.headHash) }.getOrNull()?.let { heads += it.toPayloadBytes() }

        val hasAttachments = commits.any { it.commit.attachmentTreeHash != null }
        val tables =
            if (hasAttachments) {
                dag.packAttachmentTables(branches.map { it.headHash } + tags.map { it.commitHash } + roots) ?: ByteArray(0)
            } else {
                ByteArray(0)
            }
        GoCheckpointWriter.write(
            io,
            GoCheckpointWriter.State(
                namespaceId = namespaceId,
                throughSequence = through,
                floorSequence = onDisk.firstOrNull() ?: 0,
                commits = commits,
                branches = branches,
                tags = tags,
                liveTree = liveTree.entries,
                segments = segments,
                headCommits = heads,
                hasAttachments = hasAttachments,
                attachmentTables = tables,
            ),
        )
        remember(segments)
        return null
    }

    private val deltaPrefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"

    private suspend fun deltaSequences(): List<Long> =
        io.listSegments(namespaceId)
            .filter { it.startsWith(deltaPrefix) }
            .mapNotNull { SegmentNameBuilder.parseDeltaSequencedFileName(it.removePrefix(deltaPrefix)) }
            .sorted()

    /**
     * Go's checkpointSegment for [seq]: where its intact frames end, the commit they end at, and the
     * file's stamp when the file is at least that long. Null when the segment is damaged in place
     * (an intact frame follows the damage): open handles that, and a checkpoint must not paper over it.
     */
    private suspend fun fingerprint(seq: Long): GoCheckpoint.Fingerprint? {
        val st = stamp(seq)
        remembered[seq]?.let { f -> if (st != null && f.stampSize == st.first && f.stampModTimeNanos == st.second) return f }
        val bytes = io.readFromSegment(SegmentNameBuilder.deltaSequenced(namespaceId, seq), 0, Int.MAX_VALUE)
        val scan = DeltaSegmentScanner.scanSegment(bytes)
        if (scan.tail.kind == DeltaSegmentScanner.TailKind.DAMAGED) return null
        val end = scan.tail.offset.toLong()
        val last = scan.commits.lastOrNull()?.commitHash ?: ZERO_HASH
        val stamped = st != null && st.first >= end
        return GoCheckpoint.Fingerprint(seq, end, last, if (stamped) st!!.first else null, if (stamped) st!!.second else null)
    }

    /** Go's SegmentStamp: the file's size and modification time in nanoseconds. */
    private fun stamp(seq: Long): Pair<Long, Long>? =
        try {
            val f = File(dataRoot, SegmentNameBuilder.deltaSequenced(namespaceId, seq)).toPath()
            val a = Files.readAttributes(f, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
            a.size() to a.lastModifiedTime().to(TimeUnit.NANOSECONDS)
        } catch (_: Exception) {
            null
        }

    private companion object {
        val ZERO_HASH: KdbHash = KdbHash.fromBytes(ByteArray(32))
    }
}
