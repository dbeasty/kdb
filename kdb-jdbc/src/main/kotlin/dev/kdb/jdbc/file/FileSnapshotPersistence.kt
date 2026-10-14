package dev.kdb.jdbc.file

import dev.kdb.dag.CheckpointCommit
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.embed.SnapshotPersistence
import dev.kdb.embed.SnapshotState
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.sstable.DefaultSsTableWriter
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * Go's PersistSnapshot (go/kdb/embed/snapshot_install.go) for a file runtime: what makes a namespace
 * installed from a peer's snapshot survive a restart - and open in Go as well as here.
 *
 *  1. The bodies, as SSTables keyed by content hash (what [CheckpointOpen]'s body loader and Go's
 *     DiscoverTables find). There is no history to replay them from.
 *  2. A checkpoint ([GoCheckpointWriter]) through sequence -1 - nothing of the log is in it - with
 *     main at the root, the live tree, and the root's payload as a head commit, since the log holds
 *     none of the root's operations. Version 6, with the root's attachment table, when it has one.
 *  3. The namespace marker (meta.json) naming the root as a shallow root, and bodiesExternal.
 *
 * In that order: before the marker names the root, an open replays the log and finds the namespace
 * still fresh (an install can simply run again); after, the checkpoint and bodies it needs are there.
 */
internal class FileSnapshotPersistence(
    private val dataRoot: String,
    private val io: PlatformIoShim,
) : SnapshotPersistence {
    override suspend fun persist(
        namespaceId: String,
        genesis: KdbCommit,
        state: SnapshotState,
    ) {
        writeBodies(namespaceId, state)
        val root = state.root
        val hasAttachments = root.attachmentTreeHash != null
        val now = root.timestamp
        GoCheckpointWriter.write(
            io,
            GoCheckpointWriter.State(
                namespaceId = namespaceId,
                throughSequence = -1,
                floorSequence = 0,
                commits = listOf(CheckpointCommit(genesis, genesis.operations.size), CheckpointCommit(root, root.operations.size)),
                branches = listOf(KdbBranch("main", namespaceId, root.hash, now, now)),
                tags = emptyList(),
                liveTree = state.documents.associate { it.id to it.contentHash },
                segments = emptyList(),
                headCommits = listOf(root.toPayloadBytes()),
                hasAttachments = hasAttachments,
                attachmentTables = if (hasAttachments) state.attachmentTables ?: ByteArray(0) else ByteArray(0),
            ),
        )
        markShallowRoot(namespaceId, root.hash.toHex())
    }

    /** Bodies in tables of at most [TABLE_BYTES] each: one writer holds its entries until finish. */
    private suspend fun writeBodies(
        namespaceId: String,
        state: SnapshotState,
    ) {
        var writer: DefaultSsTableWriter? = null
        var bytes = 0L
        for (d in state.documents) {
            val w = writer ?: DefaultSsTableWriter(io, namespaceId, 0).also { writer = it }
            val body = d.json.encodeToByteArray()
            w.put(d.contentHash, body)
            bytes += body.size + 32
            if (bytes >= TABLE_BYTES) {
                w.finish()
                writer = null
                bytes = 0
            }
        }
        writer?.finish()
    }

    /** Adds [rootHex] to the marker's shallowRoots and sets bodiesExternal, keeping every other
     * field; written as Go writes it (temp file, fsync, rename, directory fsync). */
    private fun markShallowRoot(
        namespaceId: String,
        rootHex: String,
    ) {
        val path = NamespacePaths.metaFile(dataRoot, namespaceId)
        Files.createDirectories(path.parent)
        val old =
            if (Files.isRegularFile(path)) {
                runCatching { Json.parseToJsonElement(Files.readString(path)) as? JsonObject }.getOrNull()
                    ?: throw IllegalStateException("kdb: namespace marker $path does not parse; refusing to overwrite it")
            } else {
                null
            }
        val fields = LinkedHashMap<String, kotlinx.serialization.json.JsonElement>(old ?: emptyMap())
        fields.putIfAbsent("namespaceId", JsonPrimitive(namespaceId))
        val roots = (fields["shallowRoots"] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.content }.orEmpty()
        fields["shallowRoots"] = JsonArray((roots + rootHex).distinct().map { JsonPrimitive(it) })
        fields["bodiesExternal"] = JsonPrimitive(true)
        val tmp = path.resolveSibling(path.fileName.toString() + ".tmp")
        FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE).use { ch ->
            ch.write(java.nio.ByteBuffer.wrap(JsonObject(fields).toString().encodeToByteArray()))
            ch.force(true)
        }
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        runCatching { FileChannel.open(path.parent, StandardOpenOption.READ).use { it.force(true) } }
    }

    private companion object {
        const val TABLE_BYTES = 32L shl 20
    }
}
