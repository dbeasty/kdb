package dev.kdb.jdbc.file

import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.embed.SquashPersistence
import dev.kdb.storage.PlatformIoShim

/**
 * Go's PersistSquash (go/kdb/embed/snapshot_install.go) for a file runtime: what makes a squash
 * catch-up survive a restart - and open in Go as well as here. The squash is already applied in
 * memory (installSquash); this writes
 *
 *  1. the bodies the squash brought, as SSTables keyed by content hash: no log frame holds them;
 *  2. a checkpoint through the log as it stands, with main at the root - the segment the writer
 *     still has open replays on top at the next open, where the commits the checkpoint already
 *     holds are skipped;
 *  3. the namespace marker naming the root as a shallow root, with bodiesExternal.
 *
 * In that order: until the checkpoint is written the log alone rebuilds the head before the
 * squash, which the next sync squashes again; once it is, open must use it, which the marker then
 * requires.
 */
internal class FileSquashPersistence(
    private val dataRoot: String,
    private val io: PlatformIoShim,
    private val checkpointer: NamespaceCheckpointer,
    /** The writer's own segment: the checkpoint claims the ones below it. */
    private val writerSequence: Long,
) : SquashPersistence {
    override suspend fun persist(
        namespaceId: String,
        root: KdbCommit,
        changed: List<KdbDocument>,
    ) {
        writeBodies(io, namespaceId, changed)
        checkpointer.save(writerSequence - 1)?.let { why ->
            throw IllegalStateException("kdb: namespace $namespaceId: the squash to ${root.hash.toHex()} cannot be made durable: $why")
        }
        markShallowRoot(dataRoot, namespaceId, root.hash.toHex())
    }
}
