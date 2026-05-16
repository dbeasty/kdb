package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.CommitStub
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTag
import dev.kdb.document.KdbTransaction
import dev.kdb.document.computeCommitHash
import dev.kdb.error.IceStorageException
import dev.kdb.error.VersionNotFoundException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

private const val MAIN_BRANCH = "main"

internal class InMemoryCommitDag(
    override val namespaceId: String,
) : CommitDag {
    private val mutex = Mutex()

    private val commits = LinkedHashMap<KdbHash, KdbCommit>()
    private val stubs = LinkedHashMap<KdbHash, CommitStub>()
    private val trees = LinkedHashMap<KdbHash, DocumentTree>()
    private val branches = LinkedHashMap<String, KdbBranch>()
    private val tags = LinkedHashMap<String, KdbTag>()
    /** Sorted lowercase full hex strings for prefix scans. */
    private val hexSorted = mutableListOf<String>()

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

    override suspend fun getCommit(hash: KdbHash): KdbCommit? = mutex.withLock { commits[hash] }

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
            commits[hash]
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
    }

    override suspend fun stubCommit(
        hash: KdbHash,
        archiveLocation: String,
    ): CommitStub =
        mutex.withLock {
            val commit =
                commits.remove(hash)
                    ?: throw DagConsistencyException(
                        "cannot stub unknown commit",
                        namespaceId,
                        hash,
                    )
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
                val c = commits[h] ?: continue
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
        trees[newDocumentTree.treeHash] = newDocumentTree
        val commit =
            KdbCommit.build(
                parentHashes = parents,
                namespaceId = namespaceId,
                transactionId = transaction.id,
                timestamp = transaction.timestamp,
                authorNodeId = transaction.authorNodeId,
                operations = transaction.operations,
                documentTreeHash = newDocumentTree.treeHash,
                schemaHash = schemaHash,
                message = message,
            )
        putCommitLocked(commit, requireParents = true)
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
                if (!commits.containsKey(h)) {
                    throw DagConsistencyException(
                        "squash target missing",
                        namespaceId,
                        h,
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
