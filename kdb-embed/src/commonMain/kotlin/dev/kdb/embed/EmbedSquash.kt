package dev.kdb.embed

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CheckpointRestorableDag
import dev.kdb.document.AttachmentTable
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument

/**
 * A squash catch-up (docs/kdb-memory-cassandra-plan.md C7; Go's peersync.InstallSquash): main jumps
 * to [root], a peer's head held without the history between this namespace's head and it, by
 * applying only what differs - [changed] bodies and [deleted] ids. [attachmentTable] is the root's
 * whole attachment table when it has one, since no history here derives it.
 */
public class SquashState(
    public val root: KdbCommit,
    public val changed: List<KdbDocument>,
    public val deleted: List<KdbUuid>,
    public val attachmentTable: AttachmentTable? = null,
)

/**
 * Makes an applied squash durable - for a file runtime, Go's PersistSquash: the changed bodies to
 * the blob store (no log frame holds them), a checkpoint through the log as it stands with main at
 * the root, then the namespace marker naming the root as a shallow root. Until that checkpoint is
 * written the log alone still rebuilds the head before the squash, which the next sync squashes
 * again.
 */
public fun interface SquashPersistence {
    public suspend fun persist(
        namespaceId: String,
        root: KdbCommit,
        changed: List<KdbDocument>,
    )
}

/** A squash this runtime cannot install: the state does not hash to the root, or it cannot be kept. */
public class SquashInstallException(
    message: String,
) : IllegalStateException(message)

/**
 * Moves [namespaceId]'s main to [state]'s root, held as a shallow root, with its documents live and
 * its attachment table seeded, and makes that durable ([squashPersistence]). Everything is checked
 * first - main's documents with the changes applied must hash to the root's tree, and the table must
 * be the root's - so a refusal changes nothing.
 */
public suspend fun EmbeddedKdbRuntime.installSquash(
    namespaceId: String,
    state: SquashState,
) {
    val root = state.root
    val head = dag.getCommitOrThrow(dag.head())
    val base = dag.getDocumentTreeOrThrow(head.documentTreeHash)
    val entries = HashMap(base.entries)
    for (d in state.changed) entries[d.id] = d.contentHash
    for (id in state.deleted) entries.remove(id)
    val tree = dev.kdb.document.DocumentTree.build(entries)
    if (tree.treeHash != root.documentTreeHash) {
        throw SquashInstallException("the squash's changes build tree ${tree.treeHash.toHex()}, not commit ${root.hash.toHex()}'s")
    }
    val table = state.attachmentTable ?: AttachmentTable.EMPTY
    if (table.root != root.attachmentTreeHash) {
        throw SquashInstallException("the squash's attachment table is not commit ${root.hash.toHex()}'s")
    }
    val restorable =
        dag as? CheckpointRestorableDag
            ?: throw SquashInstallException("this runtime's history cannot hold a commit without its parents")

    for (d in state.changed) storage.putDocument(namespaceId, d)
    for (id in state.deleted) storage.deleteDocument(namespaceId, id)
    val built = storage.commitTree(namespaceId, base.treeHash)
    if (built.treeHash != root.documentTreeHash) {
        throw SquashInstallException("the applied squash built tree ${built.treeHash.toHex()}, not the root's")
    }
    dag.putDocumentTree(built)
    if (table.root != null) dag.seedAttachmentTable(table)
    restorable.putShallowCommit(root)
    dag.setHead("main", root.hash)
    shallowRoots.getOrPut(namespaceId) { mutableSetOf() } += root.hash
    val registry = indexManager.registryFor(namespaceId)
    indexManager.writer.rebuildAll(root.hash, dag, registry, storage, schema, null)
    squashPersistence?.persist(namespaceId, root, state.changed)
}
