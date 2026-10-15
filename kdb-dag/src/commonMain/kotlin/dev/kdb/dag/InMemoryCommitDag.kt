package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.AttachmentException
import dev.kdb.document.AttachmentTable
import dev.kdb.document.CommitStub
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTag
import dev.kdb.document.KdbTransaction
import dev.kdb.document.computeCommitHash
import dev.kdb.document.touchesAttachments
import dev.kdb.error.IceStorageException
import dev.kdb.error.VersionNotFoundException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

private const val MAIN_BRANCH = "main"

internal class InMemoryCommitDag(
    override val namespaceId: String,
) : CommitDag,
    CheckpointRestorableDag {
    private val mutex = Mutex()

    private val commits = LinkedHashMap<KdbHash, KdbCommit>()
    private val stubs = LinkedHashMap<KdbHash, CommitStub>()
    private val trees = LinkedHashMap<KdbHash, DocumentTree>()
    private val branches = LinkedHashMap<String, KdbBranch>()
    private val tags = LinkedHashMap<String, KdbTag>()
    /** Sorted lowercase full hex strings for prefix scans. */
    private val hexSorted = mutableListOf<String>()
    /** Maps a transaction id to the commit it produced, so idempotent-retry detection
     * (getCommitByTransactionId, used by DefaultTransactionEngine.findExistingCommit) is O(1)
     * instead of walking history - see getCommitByTransactionId's own doc comment. */
    private val txIndex = LinkedHashMap<KdbUuid, KdbHash>()
    /** Live retention pins, keyed by commit hash, ref-counted. See [CommitDag.pin]. */
    private val pins = LinkedHashMap<KdbHash, Int>()
    /** Attachment tables by root (see AttachmentTables.kt). */
    private val attachments = AttachmentTableCache()
    /** Commits restored from a checkpoint whose operations are still only in the log - see
     * [CheckpointRestorableDag]. Held with an empty operation list until [hydrateLocked] loads it;
     * the value is how many operations it has, which a checkpoint written now must record. */
    private val opsMissing = HashMap<KdbHash, Int>()
    private var opsLoader: (suspend (KdbHash) -> KdbCommit?)? = null

    init {
        trees[DocumentTree.EMPTY.treeHash] = DocumentTree.EMPTY
        val genesisTx = KdbUuid.fromString("00000000-0000-4000-8000-000000000001")
        val genesisAuthor = KdbUuid.fromString("00000000-0000-4000-8000-000000000002")
        val genesisTs = KdbTimestamp(0, 0)
        val genesis =
            KdbCommit.build(
                parentHashes = emptyList(),
                namespaceId = namespaceId,
                transactionId = genesisTx,
                timestamp = genesisTs,
                authorNodeId = genesisAuthor,
                operations = emptyList(),
                documentTreeHash = DocumentTree.EMPTY.treeHash,
                schemaHash = null,
                message = "genesis",
            )
        commits[genesis.hash] = genesis
        insertHex(genesis.hash.toHex())
        txIndex[genesisTx] = genesis.hash
        val now = KdbTimestamp.now()
        branches[MAIN_BRANCH] =
            KdbBranch(
                name = MAIN_BRANCH,
                namespaceId = namespaceId,
                headHash = genesis.hash,
                createdAt = now,
                updatedAt = now,
            )
    }

    private fun insertHex(hexLower: String) {
        val hex = hexLower.lowercase()
        val ix = hexSorted.binarySearch(hex)
        if (ix >= 0) return
        hexSorted.add(-ix - 1, hex)
    }

    private fun removeHex(hexLower: String) {
        val hex = hexLower.lowercase()
        val ix = hexSorted.binarySearch(hex)
        if (ix >= 0) hexSorted.removeAt(ix)
    }

    override suspend fun lookupHashPrefix(hexPrefixLower: String): List<KdbHash> =
        mutex.withLock {
            val p = hexPrefixLower.lowercase()
            hexSorted.filter { it.startsWith(p) }.map { KdbHash.fromHex(it) }
        }

    override suspend fun getCommit(hash: KdbHash): KdbCommit? = mutex.withLock { commitLocked(hash) }

    /** commits[hash], with its operations loaded if a checkpoint restored it without them. */
    private suspend fun commitLocked(hash: KdbHash): KdbCommit? {
        val c = commits[hash] ?: return null
        return if (hash in opsMissing) hydrateLocked(c) else c
    }

    private suspend fun hydrateLocked(stripped: KdbCommit): KdbCommit {
        val loader =
            opsLoader
                ?: throw DagConsistencyException(
                    "the operations of commit ${stripped.hash.toHex()} were restored from a checkpoint without " +
                        "them, and nothing here can load them",
                    namespaceId,
                    stripped.hash,
                )
        val full =
            try {
                loader(stripped.hash)
            } catch (e: DagConsistencyException) {
                throw e
            } catch (e: Exception) {
                throw DagConsistencyException(
                    "loading the operations of commit ${stripped.hash.toHex()} from the delta log failed: ${e.message}",
                    namespaceId,
                    stripped.hash,
                    e,
                )
            } ?: throw DagConsistencyException(
                "the operations of commit ${stripped.hash.toHex()} are in no delta segment on disk",
                namespaceId,
                stripped.hash,
            )
        if (full.hash != stripped.hash || computeCommitHash(full) != stripped.hash) {
            throw DagConsistencyException("loaded operations do not reproduce their commit's hash", namespaceId, stripped.hash)
        }
        commits[stripped.hash] = full
        opsMissing.remove(stripped.hash)
        return full
    }

    override suspend fun restoreCheckpoint(
        commits: Sequence<CheckpointCommit>,
        branches: List<KdbBranch>,
        tags: List<KdbTag>,
    ): Unit =
        mutex.withLock {
            for (cc in commits) {
                val c = cc.commit
                if (this.commits.containsKey(c.hash)) continue
                this.commits[c.hash] = c
                insertHex(c.hash.toHex())
                if (!txIndex.containsKey(c.transactionId)) txIndex[c.transactionId] = c.hash
                if (cc.operationCount > 0 && c.operations.isEmpty()) opsMissing[c.hash] = cc.operationCount
            }
            for (b in branches) {
                requireCommitPresentLocked(b.headHash)
                this.branches[b.name] = b
            }
            for (t in tags) this.tags[t.name] = t
        }

    override suspend fun restoreCommitOperations(commits: List<KdbCommit>): Unit =
        mutex.withLock {
            for (c in commits) {
                if (computeCommitHash(c) != c.hash) {
                    throw DagConsistencyException("checkpointed operations do not reproduce their commit's hash", namespaceId, c.hash)
                }
                if (!this.commits.containsKey(c.hash)) continue
                this.commits[c.hash] = c
                opsMissing.remove(c.hash)
            }
        }

    override suspend fun putShallowCommit(commit: KdbCommit): Unit =
        mutex.withLock {
            putCommitLocked(commit, requireParents = false, verifyAttachments = false)
        }

    override fun setOperationsLoader(loader: (suspend (KdbHash) -> KdbCommit?)?) {
        opsLoader = loader
    }

    override suspend fun commitsWithoutOperations(): Int = mutex.withLock { opsMissing.size }

    override suspend fun checkpointCommits(): List<CheckpointCommit>? =
        mutex.withLock {
            if (stubs.isNotEmpty()) return@withLock null
            commits.values.map { CheckpointCommit(it, opsMissing[it.hash] ?: it.operations.size) }
        }

    override suspend fun getCommitOrThrow(hash: KdbHash): KdbCommit =
        mutex.withLock {
            stubs[hash]?.let {
                throw IceStorageException(
                    "commit archived",
                    namespaceId,
                    hash.toHex(),
                    it.archiveLocation,
                )
            }
            commitLocked(hash)
                ?: throw VersionNotFoundException(
                    "commit not found",
                    namespaceId,
                    hash.toHex(),
                )
        }

    override suspend fun getStub(hash: KdbHash): CommitStub? = mutex.withLock { stubs[hash] }

    override suspend fun hasCommit(hash: KdbHash): Boolean = mutex.withLock { commits.containsKey(hash) }

    override suspend fun hasStub(hash: KdbHash): Boolean = mutex.withLock { stubs.containsKey(hash) }

    override suspend fun putCommit(
        commit: KdbCommit,
        requireParents: Boolean,
    ): Unit =
        mutex.withLock {
            putCommitLocked(commit, requireParents)
        }

    private fun putCommitLocked(
        commit: KdbCommit,
        requireParents: Boolean,
        verifyAttachments: Boolean = true,
    ) {
        if (commits.containsKey(commit.hash)) return
        val recomputed = computeCommitHash(commit)
        if (recomputed != commit.hash) {
            throw DagConsistencyException(
                "commit hash mismatch",
                namespaceId,
                commit.hash,
            )
        }
        if (verifyAttachments) verifyAttachmentRootLocked(commit)
        if (requireParents && commit.parentHashes.isNotEmpty()) {
            for (p in commit.parentHashes) {
                if (!commits.containsKey(p) && !stubs.containsKey(p)) {
                    throw DagConsistencyException(
                        "missing parent ${p.toHex()}",
                        namespaceId,
                        commit.hash,
                    )
                }
            }
        }
        commits[commit.hash] = commit
        insertHex(commit.hash.toHex())
        // First commit for a given transaction id wins - a caller retrying the same transaction
        // always expects to find that original result, not a later, unrelated commit that
        // happens to reuse the id (which shouldn't legitimately happen, since ids are random
        // UUIDs minted fresh per transaction attempt, but "first wins" is the safer tie-break).
        // MutableMap.putIfAbsent is a JVM-only extension (backed by java.util.Map), not part of
        // Kotlin's common MutableMap - this whole function already runs under mutex.withLock
        // (see putCommit above), so a plain containsKey-then-set is just as atomic here and
        // compiles on every target.
        if (!txIndex.containsKey(commit.transactionId)) {
            txIndex[commit.transactionId] = commit.hash
        }
    }

    /**
     * Checks a received commit's attachmentTreeHash against its first parent's table with its
     * operations applied, when that parent is here and its table derivable; otherwise the check
     * happens when the table is first derived (attachmentTableAt). A commit whose recorded root
     * disagrees is refused.
     */
    private fun verifyAttachmentRootLocked(commit: KdbCommit) {
        val parentHash = commit.parentHashes.firstOrNull() ?: return
        val parent = commits[parentHash] ?: return
        val inherited = parent.attachmentTreeHash
        if (!touchesAttachments(commit.operations, inherited != null)) {
            if (commit.attachmentTreeHash != inherited) {
                throw DagConsistencyException(
                    "attachment root differs from the parent's, but the commit changes no attachment",
                    namespaceId,
                    commit.hash,
                )
            }
            return
        }
        val base =
            try {
                attachmentTableLocked(parentHash)
            } catch (_: DagConsistencyException) {
                return // the parent's own history cannot be derived here: verified lazily, if ever
            }
        val next =
            try {
                base.apply(commit.operations)
            } catch (e: AttachmentException) {
                throw DagConsistencyException("attachment table: ${e.message}", namespaceId, commit.hash, e)
            }
        if (next.root != commit.attachmentTreeHash) {
            throw DagConsistencyException(
                "attachment root does not match the parent's table with the commit's operations applied",
                namespaceId,
                commit.hash,
            )
        }
        next.root?.let { attachments.put(it, next) }
    }

    /**
     * Loads the operations of every commit [deriveAttachmentTable] will replay to build [start]'s
     * table: first parents back to the nearest commit whose table is cached (a checkpoint seeds
     * the heads') or has no root. A checkpoint restores commits without operations, and replaying
     * a table forward over those would be replaying nothing. Cheap when [start]'s table is cached.
     */
    private suspend fun hydrateAttachmentChainLocked(start: KdbHash) {
        if (opsMissing.isEmpty()) return
        var at = start
        while (true) {
            val c = commits[at] ?: return
            val root = c.attachmentTreeHash ?: return
            if (attachments.get(root) != null) return
            if (at in opsMissing) hydrateLocked(c)
            at = c.parentHashes.firstOrNull() ?: return
        }
    }

    private fun attachmentTableLocked(commit: KdbHash): AttachmentTable {
        if (stubs.containsKey(commit)) {
            throw DagConsistencyException("attachment table: commit archived", namespaceId, commit)
        }
        return deriveAttachmentTable(
            namespaceId,
            commit,
            { h ->
                val c = commits[h]
                val replayed = c?.attachmentTreeHash?.let { attachments.get(it) == null } ?: false
                if (h in opsMissing && replayed) {
                    // hydrateAttachmentChainLocked loads these first; a caller that did not is a
                    // bug, and replaying no operations would build a wrong table. (A commit that
                    // ends the walk - no root, or a cached table - is read but never replayed.)
                    throw DagConsistencyException(
                        "attachment table: commit ${h.toHex()} was restored from a checkpoint without its operations",
                        namespaceId,
                        h,
                    )
                }
                c
            },
            { attachments.get(it) },
            { root, t -> attachments.put(root, t) },
        )
    }

    /** The attachment root of a commit on [parents] carrying [operations]: the first parent's table
     * with them applied, or the parent's root unchanged when they touch no attachment. */
    private fun attachmentRootLocked(
        operations: List<dev.kdb.document.KdbOp>,
        parents: List<KdbHash>,
    ): KdbHash? {
        val inherited = parents.firstOrNull()?.let { commits[it]?.attachmentTreeHash }
        if (!touchesAttachments(operations, inherited != null)) return inherited
        val base = if (inherited != null) attachmentTableLocked(parents[0]) else AttachmentTable.EMPTY
        val next =
            try {
                base.apply(operations)
            } catch (e: AttachmentException) {
                throw DagConsistencyException("attachment table: ${e.message}", namespaceId, null, e)
            }
        next.root?.let { attachments.put(it, next) }
        return next.root
    }

    /** The attachment ops a two-parent commit needs beyond its own: see AttachmentMerge.kt. */
    private fun mergeAttachmentOpsLocked(
        ownOps: List<dev.kdb.document.KdbOp>,
        parents: List<KdbHash>,
        tree: DocumentTree,
    ): List<dev.kdb.document.KdbOp> {
        if (parents.size != 2) return emptyList()
        val oursRoot = commits[parents[0]]?.attachmentTreeHash
        val theirsRoot = commits[parents[1]]?.attachmentTreeHash
        if (oursRoot == theirsRoot) return emptyList()
        val ours = attachmentTableLocked(parents[0])
        val theirs = attachmentTableLocked(parents[1])
        val base = mergeBasesLocked(parents[0], parents[1]).firstOrNull()?.let { attachmentTableLocked(it) } ?: AttachmentTable.EMPTY
        return mergeAttachmentOps(ownOps, base, ours, theirs, tree)
    }

    /** The nearest common ancestors of [a] and [b], sorted by hash bytes - Go's MergeBases. A common
     * ancestor is redundant exactly when it is a parent of another common ancestor (every commit
     * between two common ancestors is itself one). */
    private fun mergeBasesLocked(
        a: KdbHash,
        b: KdbHash,
    ): List<KdbHash> {
        val common = ancestorClosureLocked(a) intersect ancestorClosureLocked(b)
        val redundant = mutableSetOf<KdbHash>()
        for (c in common) expandParentsLocked(c)?.forEach { if (it in common) redundant += it }
        return (common - redundant).filter { commits.containsKey(it) || stubs.containsKey(it) }.sortedBy { it.toHex() }
    }

    override suspend fun attachmentTableAt(commit: KdbHash): AttachmentTable =
        mutex.withLock {
            hydrateAttachmentChainLocked(commit)
            attachmentTableLocked(commit)
        }

    override suspend fun seedAttachmentTable(table: AttachmentTable) {
        val root = table.root ?: return
        mutex.withLock { attachments.put(root, table) }
    }

    override suspend fun getCommitByTransactionId(txId: KdbUuid): KdbCommit? =
        mutex.withLock {
            txIndex[txId]?.let { commitLocked(it) }
        }

    override suspend fun stubCommit(
        hash: KdbHash,
        archiveLocation: String,
    ): CommitStub =
        mutex.withLock {
            if (hash !in commits) {
                throw DagConsistencyException("cannot stub unknown commit", namespaceId, hash)
            }
            // Archiving is a reclamation like squashing: a stub satisfies requireCommitPresentLocked
            // but not getCommitOrThrow, so a reader pinned here would start failing. Same refusal.
            assertUnpinnedLocked(hash, "archive requested")
            val commit = commits.remove(hash)!!
            removeHex(hash.toHex())
            val stub =
                CommitStub(
                    originalHash = hash,
                    archiveLocation = archiveLocation,
                    stubbedAt = KdbTimestamp.now(),
                )
            stubs[hash] = stub
            stub
        }

    override suspend fun pin(hash: KdbHash): suspend () -> Unit {
        mutex.withLock { pins[hash] = (pins[hash] ?: 0) + 1 }
        // released is only ever touched while holding mutex (below), so - like the pins map
        // itself - it needs no separate synchronization of its own.
        var released = false
        return {
            mutex.withLock {
                if (!released) {
                    released = true
                    val n = pins[hash] ?: 0
                    if (n <= 1) pins.remove(hash) else pins[hash] = n - 1
                }
            }
        }
    }

    override suspend fun isPinned(hash: KdbHash): Boolean = mutex.withLock { pins.containsKey(hash) }

    override suspend fun pinnedCount(): Int = mutex.withLock { pins.size }

    /** Refuses a reclamation that would strand a live reader. Caller must hold [mutex]. */
    private fun assertUnpinnedLocked(hash: KdbHash, detail: String) {
        if (hash !in pins) return
        throw CompactionSafetyException("commit is pinned by a live reader", namespaceId, hash, detail)
    }

    override suspend fun getDocumentTree(treeHash: KdbHash): DocumentTree? = mutex.withLock { trees[treeHash] }

    override suspend fun getDocumentTreeOrThrow(treeHash: KdbHash): DocumentTree =
        mutex.withLock {
            trees[treeHash]
                ?: throw VersionNotFoundException(
                    "document tree not found",
                    namespaceId,
                    treeHash.toHex(),
                )
        }

    override suspend fun putDocumentTree(tree: DocumentTree): Unit =
        mutex.withLock {
            trees[tree.treeHash] = tree
        }

    override suspend fun head(): KdbHash =
        mutex.withLock {
            branches[MAIN_BRANCH]?.headHash
                ?: throw DagConsistencyException("missing default branch", namespaceId, null)
        }

    override suspend fun setHead(
        branchName: String,
        hash: KdbHash,
    ): Unit =
        mutex.withLock {
            val b =
                branches[branchName]
                    ?: throw BranchNotFoundException(
                        "branch not found",
                        namespaceId,
                        branchName,
                    )
            requireCommitPresentLocked(hash)
            val now = KdbTimestamp.now()
            branches[branchName] =
                b.copy(
                    headHash = hash,
                    updatedAt = now,
                )
        }

    override suspend fun getBranch(name: String): KdbBranch? = mutex.withLock { branches[name] }

    override suspend fun getBranchOrThrow(name: String): KdbBranch =
        mutex.withLock {
            branches[name]
                ?: throw BranchNotFoundException(
                    "branch not found",
                    namespaceId,
                    name,
                )
        }

    override suspend fun listBranches(): List<KdbBranch> = mutex.withLock { branches.values.toList() }

    override suspend fun createBranch(
        name: String,
        fromHash: KdbHash,
    ): KdbBranch =
        mutex.withLock {
            require(branches[name] == null) { "branch exists" }
            requireCommitPresentLocked(fromHash)
            val now = KdbTimestamp.now()
            val b =
                KdbBranch(
                    name = name,
                    namespaceId = namespaceId,
                    headHash = fromHash,
                    createdAt = now,
                    updatedAt = now,
                )
            branches[name] = b
            b
        }

    override suspend fun deleteBranch(name: String): Unit =
        mutex.withLock {
            if (name == MAIN_BRANCH) {
                throw BranchNotFoundException(
                    "cannot delete default branch",
                    namespaceId,
                    name,
                )
            }
            if (branches.remove(name) == null) {
                throw BranchNotFoundException(
                    "branch not found",
                    namespaceId,
                    name,
                )
            }
        }

    override suspend fun getTag(name: String): KdbTag? = mutex.withLock { tags[name] }

    override suspend fun getTagOrThrow(name: String): KdbTag =
        mutex.withLock {
            tags[name]
                ?: throw TagNotFoundException(
                    "tag not found",
                    namespaceId,
                    name,
                )
        }

    override suspend fun listTags(): List<KdbTag> = mutex.withLock { tags.values.toList() }

    override suspend fun createTag(
        name: String,
        commitHash: KdbHash,
        message: String,
    ): KdbTag =
        mutex.withLock {
            require(tags[name] == null) { "tag exists" }
            requireCommitPresentLocked(commitHash)
            val now = KdbTimestamp.now()
            val t =
                KdbTag(
                    name = name,
                    namespaceId = namespaceId,
                    commitHash = commitHash,
                    createdAt = now,
                    message = message,
                )
            tags[name] = t
            t
        }

    override suspend fun deleteTag(name: String): Unit =
        mutex.withLock {
            if (tags.remove(name) == null) {
                throw TagNotFoundException(
                    "tag not found",
                    namespaceId,
                    name,
                )
            }
        }

    override suspend fun walk(
        from: KdbHash,
        until: KdbHash?,
        limit: Int,
    ): List<TraversalEntry> =
        mutex.withLock {
            val frontier = mutableListOf<Pair<KdbHash, KdbTimestamp>>()
            fun enqueue(h: KdbHash) {
                val ts =
                    commits[h]?.timestamp
                        ?: stubs[h]?.stubbedAt
                        ?: KdbTimestamp(0, 0)
                frontier.add(h to ts)
            }
            enqueue(from)
            val visited = mutableSetOf<KdbHash>()
            val out = mutableListOf<TraversalEntry>()
            while (frontier.isNotEmpty() && out.size < limit) {
                val ix = frontier.indices.maxByOrNull { frontier[it].second } ?: break
                val (h, _) = frontier.removeAt(ix)
                if (until != null && h == until) break
                if (!visited.add(h)) continue
                val stub = stubs[h]
                if (stub != null) {
                    out.add(TraversalEntry.Stubbed(stub))
                    continue
                }
                val c = commitLocked(h) ?: continue
                out.add(TraversalEntry.Full(c))
                for (p in c.parentHashes) enqueue(p)
            }
            out
        }

    override suspend fun commitsSince(
        from: KdbHash,
        exclude: Set<KdbHash>,
    ): List<KdbHash> =
        mutex.withLock {
            val reachableFrom = ancestorClosureLocked(from)
            val excluded = exclude.flatMapTo(mutableSetOf()) { ancestorClosureLocked(it) }
            (reachableFrom - excluded).sortedBy { it.toHex() }
        }

    override suspend fun commonAncestor(
        hashA: KdbHash,
        hashB: KdbHash,
    ): KdbHash? =
        mutex.withLock {
            val sa = ancestorClosureLocked(hashA)
            val dq = ArrayDeque<KdbHash>()
            val seen = mutableSetOf<KdbHash>()
            dq.add(hashB)
            while (dq.isNotEmpty()) {
                val h = dq.removeFirst()
                if (!seen.add(h)) continue
                if (h in sa) return h
                expandParentsLocked(h)?.forEach { dq.add(it) }
            }
            null
        }

    override suspend fun isAncestor(
        ancestor: KdbHash,
        descendant: KdbHash,
    ): Boolean =
        mutex.withLock {
            ancestor in ancestorClosureLocked(descendant)
        }

    override suspend fun diff(
        fromHash: KdbHash,
        toHash: KdbHash,
    ): CommitDiff =
        mutex.withLock {
            if (fromHash == toHash) {
                return CommitDiff(fromHash, toHash, emptyList())
            }
            val fc =
                commits[fromHash]
                    ?: throw VersionNotFoundException(
                        "from commit missing",
                        namespaceId,
                        fromHash.toHex(),
                    )
            val tc =
                commits[toHash]
                    ?: throw VersionNotFoundException(
                        "to commit missing",
                        namespaceId,
                        toHash.toHex(),
                    )
            val fromTree =
                trees[fc.documentTreeHash]
                    ?: throw VersionNotFoundException(
                        "from tree missing",
                        namespaceId,
                        fc.documentTreeHash.toHex(),
                    )
            val toTree =
                trees[tc.documentTreeHash]
                    ?: throw VersionNotFoundException(
                        "to tree missing",
                        namespaceId,
                        tc.documentTreeHash.toHex(),
                    )
            val fe = fromTree.entries
            val te = toTree.entries
            val entries = mutableListOf<DiffEntry>()
            for ((id, h) in te) {
                val oh = fe[id]
                when {
                    oh == null -> entries.add(DiffEntry.Added(id, h))
                    oh != h -> entries.add(DiffEntry.Modified(id, oh, h))
                    else -> Unit
                }
            }
            for ((id, h) in fe) {
                if (id !in te) {
                    entries.add(DiffEntry.Removed(id, h))
                }
            }
            CommitDiff(fromHash, toHash, entries)
        }

    override suspend fun appendCommit(
        transaction: KdbTransaction,
        parentHash: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String,
    ): KdbCommit =
        mutex.withLock {
            if (touchesAttachments(transaction.operations, commits[parentHash]?.attachmentTreeHash != null)) {
                hydrateAttachmentChainLocked(parentHash)
            }
            appendCommitLocked(
                transaction,
                listOf(parentHash),
                newDocumentTree,
                schemaHash,
                message,
                MAIN_BRANCH,
            )
        }

    override suspend fun appendMergeCommit(
        transaction: KdbTransaction,
        primaryParent: KdbHash,
        mergedParent: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String,
    ): KdbCommit =
        mutex.withLock {
            // The tables mergeAttachmentOpsLocked merges: both parents' and their merge base's.
            if (commits[primaryParent]?.attachmentTreeHash != commits[mergedParent]?.attachmentTreeHash) {
                hydrateAttachmentChainLocked(primaryParent)
                hydrateAttachmentChainLocked(mergedParent)
                mergeBasesLocked(primaryParent, mergedParent).firstOrNull()?.let { hydrateAttachmentChainLocked(it) }
            } else if (touchesAttachments(transaction.operations, commits[primaryParent]?.attachmentTreeHash != null)) {
                hydrateAttachmentChainLocked(primaryParent)
            }
            appendCommitLocked(
                transaction,
                listOf(primaryParent, mergedParent),
                newDocumentTree,
                schemaHash,
                message,
                MAIN_BRANCH,
            )
        }

    private fun appendCommitLocked(
        transaction: KdbTransaction,
        parents: List<KdbHash>,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String,
        branchToAdvance: String,
    ): KdbCommit {
        for (p in parents) requireCommitPresentLocked(p)
        // A merge carries the attachment operations that merge its parents' tables (AttachmentMerge.kt).
        val operations = transaction.operations + mergeAttachmentOpsLocked(transaction.operations, parents, newDocumentTree)
        val attachRoot = attachmentRootLocked(operations, parents)
        trees[newDocumentTree.treeHash] = newDocumentTree
        val commit =
            KdbCommit.build(
                parentHashes = parents,
                namespaceId = namespaceId,
                transactionId = transaction.id,
                timestamp = transaction.timestamp,
                authorNodeId = transaction.authorNodeId,
                operations = operations,
                documentTreeHash = newDocumentTree.treeHash,
                schemaHash = schemaHash,
                message = message,
                attachmentTreeHash = attachRoot,
            )
        // The root was derived just now from the same parent and operations.
        putCommitLocked(commit, requireParents = true, verifyAttachments = false)
        val b =
            branches[branchToAdvance]
                ?: throw DagConsistencyException(
                    "branch missing",
                    namespaceId,
                    commit.hash,
                )
        val now = KdbTimestamp.now()
        branches[branchToAdvance] =
            b.copy(
                headHash = commit.hash,
                updatedAt = now,
            )
        return commit
    }

    override suspend fun compactableBefore(
        boundary: KdbHash,
        peerHeads: Set<KdbHash>,
    ): List<KdbHash> =
        mutex.withLock {
            val unsafe = peerHeads.toMutableSet()
            branches.values.forEach { unsafe.add(it.headHash) }
            tags.values.forEach { unsafe.add(it.commitHash) }
            val out = mutableListOf<KdbHash>()
            var cur =
                commits[boundary]
                    ?: throw VersionNotFoundException(
                        "boundary missing",
                        namespaceId,
                        boundary.toHex(),
                    )
            while (true) {
                if (cur.parentHashes.size != 1) break
                val p = cur.parentHashes.single()
                if (p in unsafe) break
                out.add(p)
                cur =
                    commits[p]
                        ?: throw VersionNotFoundException(
                            "parent missing during compact scan",
                            namespaceId,
                            p.toHex(),
                        )
            }
            out
        }

    override suspend fun squash(
        squashHashes: List<KdbHash>,
        boundary: KdbHash,
        syntheticTree: DocumentTree,
        syntheticSchemaHash: KdbHash?,
        message: String,
    ): KdbCommit =
        mutex.withLock {
            val squashSet = squashHashes.toSet()
            commits[boundary]
                ?: throw VersionNotFoundException(
                    "boundary missing",
                    namespaceId,
                    boundary.toHex(),
                )
            for (h in squashHashes) {
                val c =
                    commits[h]
                        ?: throw DagConsistencyException(
                            "squash target missing",
                            namespaceId,
                            h,
                        )
                // The synthetic commit has no parents and no operations, so an attachment table
                // could not be derived for it; a window carrying attachments is refused (as in Go).
                if (c.attachmentTreeHash != null) {
                    throw CompactionSafetyException(
                        "squash window holds commits with attachments",
                        namespaceId,
                        h,
                        "attachments",
                    )
                }
            }
            for (b in branches.values) {
                if (b.headHash in squashSet) {
                    throw CompactionSafetyException(
                        "branch head inside squash window",
                        namespaceId,
                        b.headHash,
                        "branch=${b.name}",
                    )
                }
            }
            // Readers are a retention root too. A branch head is the only pin the window check
            // used to know about, which left every open SNAPSHOT read and every queued commit's
            // base version free to be squashed out from under it - they are not branch heads,
            // and until pin() existed there was nowhere to look them up. See CommitDag.pin.
            for (h in squashHashes) {
                assertUnpinnedLocked(h, "inside squash window")
            }
            val syntheticTx = KdbUuid.fromString("00000000-0000-4000-8000-000000000003")
            val syntheticAuthor = KdbUuid.fromString("00000000-0000-4000-8000-000000000004")
            trees[syntheticTree.treeHash] = syntheticTree
            val synthetic =
                KdbCommit.build(
                    parentHashes = emptyList(),
                    namespaceId = namespaceId,
                    transactionId = syntheticTx,
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = syntheticAuthor,
                    operations = emptyList(),
                    documentTreeHash = syntheticTree.treeHash,
                    schemaHash = syntheticSchemaHash,
                    message = message,
                )
            for ((name, tag) in tags.toMap()) {
                if (tag.commitHash in squashSet) {
                    tags[name] =
                        tag.copy(
                            commitHash = synthetic.hash,
                        )
                }
            }
            for (h in squashHashes) {
                commits.remove(h)
                stubs.remove(h)
                removeHex(h.toHex())
            }
            commits[synthetic.hash] = synthetic
            insertHex(synthetic.hash.toHex())
            synthetic
        }

    private fun requireCommitPresentLocked(hash: KdbHash) {
        if (!commits.containsKey(hash) && !stubs.containsKey(hash)) {
            throw DagConsistencyException(
                "missing commit ${hash.toHex()}",
                namespaceId,
                hash,
            )
        }
    }

    /** Parents following full commits only (stubs terminate expansion). */
    private fun expandParentsLocked(hash: KdbHash): List<KdbHash>? {
        if (stubs.containsKey(hash)) return emptyList()
        return commits[hash]?.parentHashes
    }

    private fun ancestorClosureLocked(start: KdbHash): Set<KdbHash> {
        val acc = mutableSetOf<KdbHash>()
        val dq = ArrayDeque<KdbHash>()
        dq.add(start)
        while (dq.isNotEmpty()) {
            val h = dq.removeFirst()
            if (!acc.add(h)) continue
            val parents = expandParentsLocked(h) ?: continue
            for (p in parents) dq.add(p)
        }
        return acc
    }
}
