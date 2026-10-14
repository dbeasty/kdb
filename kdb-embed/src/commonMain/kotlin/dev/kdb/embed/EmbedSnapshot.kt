package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CheckpointRestorableDag
import dev.kdb.document.AttachmentTable
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument

/**
 * A namespace's state at one commit, without the history behind it: what a peer's snapshot
 * installs (go/kdb/peersync/snapshot.go InstallSnapshot). [root] is held without its parents - a
 * shallow root - and [attachmentTables] (Go's packed form) holds its attachment table when it has
 * one, since there is no history to derive it from.
 */
public class SnapshotState(
    public val root: KdbCommit,
    public val documents: List<KdbDocument>,
    public val attachmentTables: ByteArray? = null,
)

/**
 * Makes a snapshot install durable before the runtime applies it - for a file runtime, Go's
 * PersistSnapshot: the bodies, then a checkpoint holding the root's state, then the namespace
 * marker naming the root. [genesis] is the commit the namespace held before (the checkpoint lists
 * it, as Go's does).
 */
public fun interface SnapshotPersistence {
    public suspend fun persist(
        namespaceId: String,
        genesis: KdbCommit,
        state: SnapshotState,
    )
}

/** A snapshot this runtime cannot install: the namespace is not fresh, or the state is inconsistent. */
public class SnapshotInstallException(
    message: String,
) : IllegalStateException(message)

/** Whether [namespaceId] is fresh - main at its genesis, no other ref - so a snapshot may be installed. */
public suspend fun EmbeddedKdbRuntime.canInstallSnapshot(namespaceId: String = defaultNamespace): Boolean {
    if (namespaceId != dag.namespaceId) return false
    val head = dag.getCommit(dag.head()) ?: return false
    return head.parentHashes.isEmpty() && head.operations.isEmpty() && dag.listBranches().size == 1 && dag.listTags().isEmpty()
}

/**
 * Installs [state] as [namespaceId]'s state: main moves to its root, held without parents, with its
 * documents live and its attachment table seeded. The namespace must be fresh
 * ([canInstallSnapshot]). Everything is checked before anything is written - the documents must
 * hash to the root's tree, the tables must hold the root's attachment table - and a file runtime
 * persists the state ([snapshotPersistence]) before it is applied, so a crash leaves either the
 * fresh namespace or the installed one.
 */
public suspend fun EmbeddedKdbRuntime.installSnapshot(
    namespaceId: String,
    state: SnapshotState,
) {
    if (!canInstallSnapshot(namespaceId)) {
        throw SnapshotInstallException("namespace $namespaceId is not fresh: a snapshot is installed only into an empty namespace")
    }
    val root = state.root
    val tree = DocumentTree.build(state.documents.associate { it.id to it.contentHash })
    if (tree.size != state.documents.size) throw SnapshotInstallException("the snapshot of ${root.hash.toHex()} lists a document twice")
    if (tree.treeHash != root.documentTreeHash) {
        throw SnapshotInstallException("the snapshot's documents do not hash to commit ${root.hash.toHex()}'s tree")
    }
    val attachRoot = root.attachmentTreeHash
    if (attachRoot != null) {
        val tables = state.attachmentTables?.let { AttachmentTable.parsePacked(it) }.orEmpty()
        if (tables.none { it.root == attachRoot }) {
            throw SnapshotInstallException("the snapshot of ${root.hash.toHex()} has attachments but not their table")
        }
    }
    val restorable =
        dag as? CheckpointRestorableDag
            ?: throw SnapshotInstallException("this runtime's history cannot hold a commit without its parents")
    val genesis = dag.getCommitOrThrow(dag.head())
    snapshotPersistence?.persist(namespaceId, genesis, state)

    for (d in state.documents) storage.putDocument(namespaceId, d)
    val built = storage.commitTree(namespaceId, genesis.documentTreeHash)
    if (built.treeHash != root.documentTreeHash) {
        throw SnapshotInstallException("the installed documents built tree ${built.treeHash.toHex()}, not the root's")
    }
    dag.putDocumentTree(built)
    state.attachmentTables?.let { dag.seedPackedAttachmentTables(it) }
    restorable.putShallowCommit(root)
    dag.setHead("main", root.hash)
    shallowRoots.getOrPut(namespaceId) { mutableSetOf() } += root.hash
    // Indexes follow the documents: rebuilt from the new head, as an open after a restart would.
    val registry = indexManager.registryFor(namespaceId)
    indexManager.writer.rebuildAll(root.hash, dag, registry, storage, schema, null)
}

/** [namespaceId]'s shallow roots, as a peer advertises them. */
public fun EmbeddedKdbRuntime.shallowRootsOf(namespaceId: String = defaultNamespace): List<KdbHash> =
    shallowRoots[namespaceId]?.toList().orEmpty()
