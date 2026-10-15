package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.AttachmentException
import dev.kdb.document.AttachmentTable
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.SnapshotState
import dev.kdb.embed.SquashState
import dev.kdb.embed.attachmentTableAt
import dev.kdb.embed.canInstallSnapshot
import dev.kdb.embed.installSnapshot
import dev.kdb.embed.installSquash
import dev.kdb.jdbc.file.DataDirectoryLockRegistry
import dev.kdb.jdbc.file.NamespacePaths
import dev.kdb.jdbc.file.openFileRuntime
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * Full-mode namespaces kept in a Kotlin file runtime under [dataDir] - the replica's on-disk store
 * (plan §3.3, D12): Go's data-directory layout, delta log and checkpoint format, so Go tools and the
 * Kotlin CLI open a replica's directory as they open a node's. Pass it as
 * [ReplicaOptions.fullNamespaces].
 */
public fun fileNamespaces(dataDir: String): suspend (name: String, nodeId: String) -> ReplicaNamespace =
    { name, nodeId -> FileReplicaNamespace.open(dataDir, name, nodeId) }

/**
 * One full-mode namespace held in a Kotlin file runtime ([openFileRuntime]): the same engine rules
 * as the store-backed namespace ([ReplicaNamespace.open]) over a node's on-disk format.
 *
 *  - Fast-forwards log the host's real commits - the replica receives them whole - so a reopen
 *    replays them like any node's log, from the runtime's checkpoint.
 *  - A snapshot bootstrap is the runtime's durable [installSnapshot] (bodies, a Go checkpoint, the
 *    marker naming the root); a squash is [installSquash], made durable as Go's PersistSquash makes
 *    it. A squashed replica therefore reopens from its checkpoint, the only record of the root.
 *  - Local commits are real commits on main, logged before they are acknowledged.
 *  - What a node does not hold - the commits still to push, the host's main as last seen, a
 *    convergence in progress, the author id - is in `replica.json` beside the namespace's marker,
 *    written atomically.
 *
 * Unlike the store-backed namespace, a convergence's pages are held in memory until it finishes:
 * an interrupted snapshot bootstrap starts again from its first page, and a squash re-diffs.
 */
public class FileReplicaNamespace private constructor(
    private val dataDir: String,
    private val rt: EmbeddedKdbRuntime,
    private val metaPath: Path,
    @kotlin.concurrent.Volatile private var meta: NamespaceMeta,
) : ReplicaNamespace {
    override val name: String = meta.name
    private val id = rt.defaultNamespace
    private val lock = Mutex()
    private val genesisHash = genesisCommit(name).hash

    @kotlin.concurrent.Volatile private var headHex: String = meta.head!!

    /** A convergence's documents (null: deleted) and attachment sets, applied at [finishConverge]. */
    private val stagedDocs = LinkedHashMap<KdbUuid, String?>()
    private var stagedAttachments: AttachmentTable? = null
    private var stagedTree: DocumentTree? = null

    public companion object {
        private const val META_FILE = "replica.json"

        public suspend fun open(dataDir: String, name: String, nodeId: String): FileReplicaNamespace {
            val catalog = NamespacePaths.catalogFromNamespace(name)
            val rt = openFileRuntime(dataDir, catalog, name, lockHolder = "kdb-replica")
            val path = NamespacePaths.nsDir(dataDir, name).resolve(META_FILE)
            val head = rt.dag.head()
            val stored = if (Files.isRegularFile(path)) metaJson.decodeFromString(NamespaceMeta.serializer(), Files.readString(path)) else null
            // A commit is logged after the meta lists it as pending: a crash between the two leaves
            // a pending hash the log never got, which was never acknowledged either.
            val pending = stored?.pending.orEmpty().filter { rt.dag.hasCommit(KdbHash.fromHex(it)) }
            val meta =
                (stored ?: NamespaceMeta(name = name, nodeId = nodeId, head = null)).copy(
                    head = head.toHex(),
                    pending = pending,
                    // Pages of an interrupted convergence were not kept: it starts again.
                    target = stored?.target?.copy(after = ""),
                )
            val ns = FileReplicaNamespace(dataDir, rt, path, meta)
            if (stored == null || stored != meta) ns.writeMeta(meta)
            return ns
        }
    }

    override val head: String get() = headHex
    override val isEmpty: Boolean get() = headHex == genesisHash && meta.pending.isEmpty() && meta.target == null
    override val pending: List<String> get() = meta.pending
    override val target: ConvergeTarget? get() = meta.target
    override val remoteMain: String? get() = meta.remoteMain
    override val nodeId: String get() = meta.nodeId

    /** The file runtime behind this namespace. */
    public val runtime: EmbeddedKdbRuntime get() = rt

    private fun writeMeta(m: NamespaceMeta) {
        Files.createDirectories(metaPath.parent)
        val tmp = metaPath.resolveSibling("$META_FILE.tmp")
        FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE).use { ch ->
            ch.write(java.nio.ByteBuffer.wrap(metaJson.encodeToString(NamespaceMeta.serializer(), m).encodeToByteArray()))
            ch.force(true)
        }
        Files.move(tmp, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        runCatching { FileChannel.open(metaPath.parent, StandardOpenOption.READ).use { it.force(true) } }
        meta = m
    }

    private suspend fun mainTree(): DocumentTree = rt.dag.getDocumentTreeOrThrow(rt.dag.getCommitOrThrow(rt.dag.head()).documentTreeHash)

    /** The documents as the replica shows them: main's, with a convergence's pages laid over. */
    private suspend fun currentTree(): DocumentTree = stagedTree ?: mainTree()

    private suspend fun currentAttachments(): AttachmentTable = stagedAttachments ?: rt.attachmentTableAt(rt.dag.head())

    override suspend fun rootHash(): String = currentTree().treeHash.toHex()

    override suspend fun commit(hash: String): ReplicaCommit? =
        rt.dag.getCommit(KdbHash.fromHex(hash))?.let { ReplicaCommit.decode(it.toPayloadBytes()) }

    override suspend fun headCommit(): ReplicaCommit? = commit(headHex)

    override suspend fun get(id: String): String? {
        val u = parseUuid(id)
        if (stagedDocs.containsKey(u)) return stagedDocs[u]
        return rt.storage.getDocument(this.id, u, mainTree().treeHash)?.json
    }

    override suspend fun contentHashOf(id: String): String? = currentTree().hashFor(parseUuid(id))?.toHex()

    override suspend fun scan(after: String?, before: String?, limit: Int): List<ReplicaDocument> {
        val lo = after?.lowercase()
        val hi = before?.lowercase()
        val ids =
            currentTree().entries.keys.asSequence().map { it.toString() }
                .filter { (lo == null || it > lo) && (hi == null || it < hi) }
                .sorted().take(limit).toList()
        return ids.mapNotNull { i -> get(i)?.let { ReplicaDocument(i, it) } }
    }

    override suspend fun fastForward(commits: List<ReplicaCommit>, target: String): List<String> =
        lock.withLock {
            if (meta.pending.isNotEmpty()) {
                throw NotFastForwardException("kdb replica: local commits are not yet pushed; push before pulling")
            }
            if (meta.target != null) throw NotFastForwardException("kdb replica: finish converging before fast-forwarding")
            if (target == headHex) return@withLock emptyList()
            val byHash = commits.associateBy { it.hash }
            val tip = byHash[target] ?: throw NotFastForwardException("kdb replica: the range does not contain its target $target")
            checkReachesHead(byHash, target, headHex)
            val ordered = topoSort(commits)
            val effect = netEffect(ordered)
            // Everything is checked before anything is logged: replay moves main to what is logged.
            val base = mainTree()
            val entries = HashMap(base.entries)
            for (op in effect.values) {
                when (op) {
                    is KdbOp.Write -> entries[op.docId] = KdbDocument(op.docId, op.patch).contentHash
                    is KdbOp.Delete -> entries.remove(op.docId)
                    else -> {}
                }
            }
            val built = DocumentTree.build(entries).treeHash.toHex()
            if (built != tip.treeHash) throw TreeMismatchException(target, tip.treeHash, built)
            var table = rt.attachmentTableAt(rt.dag.head())
            val attRoot =
                try {
                    for (c in ordered) table = table.apply(c.ops)
                    table.root?.toHex()
                } catch (e: AttachmentException) {
                    throw AttachmentRootMismatchException(target, tip.attachmentRoot, "unappliable (${e.message})")
                }
            if (attRoot != tip.attachmentRoot) throw AttachmentRootMismatchException(target, tip.attachmentRoot, attRoot)

            for (c in ordered) rt.dag.putCommit(c.commit, requireParents = true)
            for ((docId, op) in effect) {
                when (op) {
                    is KdbOp.Write -> rt.storage.putDocument(id, KdbDocument(op.docId, op.patch))
                    is KdbOp.Delete -> rt.storage.deleteDocument(id, op.docId)
                    else -> {}
                }
            }
            val tree = rt.storage.commitTree(id, base.treeHash)
            check(tree.treeHash.toHex() == tip.treeHash) { "kdb replica: applying the range built ${tree.treeHash.toHex()}, not ${tip.treeHash}" }
            rt.dag.putDocumentTree(tree)
            rt.dag.setHead("main", tip.commit.hash)
            headHex = tip.hash
            writeMeta(meta.copy(head = tip.hash, lastMicros = maxOf(meta.lastMicros, ordered.maxOf { it.timestampMicros })))
            effect.keys.toList()
        }

    override suspend fun commitLocal(ops: List<KdbOp>, transactionId: KdbUuid, message: String, nowMicros: Long): ReplicaCommit =
        lock.withLock {
            if (meta.target != null) {
                throw ReplicaBusyException("kdb replica: $name is being brought up to date; retry once it has synced")
            }
            val base = mainTree()
            val entries = HashMap(base.entries)
            for (op in ops) {
                when (op) {
                    is KdbOp.Write -> entries[op.docId] = KdbDocument(op.docId, op.patch).contentHash
                    is KdbOp.Delete -> entries.remove(op.docId)
                    else -> {}
                }
            }
            val tree = DocumentTree.build(entries).treeHash.toHex()
            val attachmentRoot = rt.attachmentTableAt(rt.dag.head()).apply(ops).root?.toHex()
            val ts = if (nowMicros > meta.lastMicros) nowMicros else meta.lastMicros + 1
            val c =
                ReplicaCommit.build(
                    parents = listOf(headHex),
                    namespace = name,
                    transactionId = transactionId,
                    timestampMicros = ts,
                    authorNodeId = parseUuid(meta.nodeId),
                    ops = ops,
                    treeHash = tree,
                    message = message,
                    attachmentRoot = attachmentRoot,
                )
            writeMeta(meta.copy(pending = meta.pending + c.hash, lastMicros = ts))
            for (op in ops) {
                when (op) {
                    is KdbOp.Write -> rt.storage.putDocument(id, KdbDocument(op.docId, op.patch))
                    is KdbOp.Delete -> rt.storage.deleteDocument(id, op.docId)
                    else -> {}
                }
            }
            val built = rt.storage.commitTree(id, base.treeHash)
            rt.dag.putDocumentTree(built)
            rt.dag.putCommit(c.commit, requireParents = true)
            rt.dag.setHead("main", c.commit.hash)
            headHex = c.hash
            writeMeta(meta.copy(head = c.hash))
            c
        }

    override suspend fun setRemoteMain(hash: String?) {
        lock.withLock { writeMeta(meta.copy(remoteMain = hash)) }
    }

    override suspend fun beginConverge(target: ReplicaCommit, via: String) {
        lock.withLock {
            val t = meta.target
            if (t != null && t.hash == target.hash && t.via == via) return@withLock
            if (meta.pending.isNotEmpty()) {
                throw NotFastForwardException("kdb replica: local commits are not yet pushed; push before converging")
            }
            stagedDocs.clear()
            stagedAttachments = null
            stagedTree = null
            writeMeta(meta.copy(target = ConvergeTarget(target.hash, target.payload.toHex(), via, "")))
        }
    }

    override suspend fun applyConvergePage(docs: List<Pair<String, String?>>, after: String) {
        lock.withLock {
            val t = meta.target ?: error("kdb replica: not converging")
            val entries = HashMap(currentTree().entries)
            for ((docId, json) in docs) {
                val u = parseUuid(docId)
                stagedDocs[u] = json
                if (json == null) entries.remove(u) else entries[u] = KdbDocument(u, json).contentHash
            }
            stagedTree = DocumentTree.build(entries)
            // The cursor is not durable: the pages are not either.
            meta = meta.copy(target = t.copy(after = after))
        }
    }

    override suspend fun applyConvergeAttachments(sets: Map<String, List<ReplicaAttachment>>, whole: Boolean) {
        lock.withLock {
            if (meta.target == null) error("kdb replica: not converging")
            var table = if (whole) AttachmentTable.EMPTY else currentAttachments()
            for ((docId, set) in sets) {
                val u = parseUuid(docId)
                table = table.deleteAll(u)
                for (a in set) table = table.put(u, a.toDocument())
            }
            stagedAttachments = table
        }
    }

    override suspend fun attachmentRoot(): String? = currentAttachments().root?.toHex()

    override suspend fun attachments(id: String): List<ReplicaAttachment> =
        currentAttachments().list(parseUuid(id)).attachments.map { it.toReplica() }.let(::sortAttachments)

    override suspend fun finishConverge() {
        lock.withLock {
            val t = meta.target ?: return@withLock
            val tip = ReplicaCommit.decode(hexToBytes(t.payloadHex))
            val built = currentTree().treeHash.toHex()
            if (built != tip.treeHash) throw TreeMismatchException(tip.hash, tip.treeHash, built)
            val attRoot = currentAttachments().root?.toHex()
            if (attRoot != tip.attachmentRoot) throw AttachmentRootMismatchException(tip.hash, tip.attachmentRoot, attRoot)
            val table = currentAttachments()
            if (rt.canInstallSnapshot(id)) {
                // Nothing here yet: the target's documents are all staged.
                val docs = stagedDocs.mapNotNull { (u, json) -> json?.let { KdbDocument(u, it) } }
                val packed = if (table.root != null) AttachmentTable.pack(listOf(table)) else null
                rt.installSnapshot(id, SnapshotState(tip.commit, docs, packed))
            } else {
                val changed = stagedDocs.mapNotNull { (u, json) -> json?.let { KdbDocument(u, it) } }
                val deleted = stagedDocs.filterValues { it == null }.keys.toList()
                rt.installSquash(id, SquashState(tip.commit, changed, deleted, table.takeIf { it.root != null }))
            }
            stagedDocs.clear()
            stagedAttachments = null
            stagedTree = null
            headHex = tip.hash
            writeMeta(meta.copy(head = tip.hash, target = null, lastMicros = maxOf(meta.lastMicros, tip.timestampMicros)))
        }
    }

    override suspend fun markPushed(n: Int, remoteMain: String) {
        lock.withLock { writeMeta(meta.copy(pending = meta.pending.drop(n), remoteMain = remoteMain)) }
    }

    override suspend fun pendingCommits(): List<ReplicaCommit> =
        meta.pending.map { h -> commit(h) ?: error("kdb replica: pending commit $h is missing from the log") }

    override suspend fun treeNode(prefix: String, entryLimit: Int, tree: TreeKind): TreeNodeView {
        val t = if (tree == TreeKind.DOCUMENTS) currentTree() else currentAttachments().setTree
        val n = t.node(prefix, entryLimit)
        val hash = n.hash ?: return TreeNodeView(prefix, "", emptyList(), null)
        return TreeNodeView(
            prefix,
            hash.toHex(),
            n.children.map { it?.toHex() ?: "" },
            n.entries?.entries?.associate { (k, v) -> k.toString() to v.toHex() },
        )
    }

    override suspend fun walkTree(visit: suspend (id: String, contentHash: String) -> Unit) {
        for ((k, v) in currentTree().entries.entries.sortedBy { it.key.toString() }) visit(k.toString(), v.toHex())
    }

    /** Closes the runtime (sealing its log and writing its checkpoint) and releases the directory. */
    override suspend fun close() {
        try {
            rt.close()
        } finally {
            DataDirectoryLockRegistry.releaseBlocking(dataDir)
        }
    }
}
