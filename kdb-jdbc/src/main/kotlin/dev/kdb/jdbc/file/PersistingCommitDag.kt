package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitDiff
import dev.kdb.dag.CommitRef
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.CommitStub
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTag
import dev.kdb.document.KdbTransaction

internal class PersistingCommitDag(
    private val delegate: CommitDag,
    private val persistence: DeltaCommitPersistence,
) : CommitDag {
    override val namespaceId: String get() = delegate.namespaceId

    override suspend fun lookupHashPrefix(hexPrefixLower: String): List<KdbHash> =
        delegate.lookupHashPrefix(hexPrefixLower)

    override suspend fun getCommit(hash: KdbHash): KdbCommit? = delegate.getCommit(hash)

    override suspend fun getCommitOrThrow(hash: KdbHash): KdbCommit = delegate.getCommitOrThrow(hash)

    override suspend fun getCommitByTransactionId(txId: dev.kdb.codec.KdbUuid): KdbCommit? =
        delegate.getCommitByTransactionId(txId)

    override suspend fun getStub(hash: KdbHash): CommitStub? = delegate.getStub(hash)

    override suspend fun hasCommit(hash: KdbHash): Boolean = delegate.hasCommit(hash)

    override suspend fun hasStub(hash: KdbHash): Boolean = delegate.hasStub(hash)

    override suspend fun putCommit(
        commit: KdbCommit,
        requireParents: Boolean,
    ) {
        val existed = delegate.hasCommit(commit.hash)
        delegate.putCommit(commit, requireParents)
        if (!existed && delegate.hasCommit(commit.hash)) {
            persistence.persist(commit)
        }
    }

    override suspend fun stubCommit(
        hash: KdbHash,
        archiveLocation: String,
    ): CommitStub = delegate.stubCommit(hash, archiveLocation)

    override suspend fun pin(hash: KdbHash): suspend () -> Unit = delegate.pin(hash)

    override suspend fun isPinned(hash: KdbHash): Boolean = delegate.isPinned(hash)

    override suspend fun pinnedCount(): Int = delegate.pinnedCount()

    override suspend fun getDocumentTree(treeHash: KdbHash): DocumentTree? =
        delegate.getDocumentTree(treeHash)

    override suspend fun getDocumentTreeOrThrow(treeHash: KdbHash): DocumentTree =
        delegate.getDocumentTreeOrThrow(treeHash)

    override suspend fun putDocumentTree(tree: DocumentTree) = delegate.putDocumentTree(tree)

    override suspend fun head(): KdbHash = delegate.head()

    override suspend fun setHead(branchName: String, hash: KdbHash) = delegate.setHead(branchName, hash)

    override suspend fun getBranch(name: String): KdbBranch? = delegate.getBranch(name)

    override suspend fun getBranchOrThrow(name: String): KdbBranch = delegate.getBranchOrThrow(name)

    override suspend fun listBranches(): List<KdbBranch> = delegate.listBranches()

    override suspend fun createBranch(name: String, fromHash: KdbHash): KdbBranch =
        delegate.createBranch(name, fromHash)

    override suspend fun deleteBranch(name: String) = delegate.deleteBranch(name)

    override suspend fun getTag(name: String): KdbTag? = delegate.getTag(name)

    override suspend fun getTagOrThrow(name: String): KdbTag = delegate.getTagOrThrow(name)

    override suspend fun listTags(): List<KdbTag> = delegate.listTags()

    override suspend fun createTag(
        name: String,
        commitHash: KdbHash,
        message: String,
    ): KdbTag = delegate.createTag(name, commitHash, message)

    override suspend fun deleteTag(name: String) = delegate.deleteTag(name)

    override suspend fun walk(
        from: KdbHash,
        until: KdbHash?,
        limit: Int,
    ): List<TraversalEntry> = delegate.walk(from, until, limit)

    override suspend fun commitsSince(
        from: KdbHash,
        exclude: Set<KdbHash>,
    ): List<KdbHash> = delegate.commitsSince(from, exclude)

    override suspend fun commonAncestor(hashA: KdbHash, hashB: KdbHash): KdbHash? =
        delegate.commonAncestor(hashA, hashB)

    override suspend fun isAncestor(ancestor: KdbHash, descendant: KdbHash): Boolean =
        delegate.isAncestor(ancestor, descendant)

    override suspend fun diff(fromHash: KdbHash, toHash: KdbHash): CommitDiff =
        delegate.diff(fromHash, toHash)

    override suspend fun appendCommit(
        transaction: KdbTransaction,
        parentHash: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String,
    ): KdbCommit {
        val commit =
            delegate.appendCommit(transaction, parentHash, newDocumentTree, schemaHash, message)
        persistence.persist(commit)
        return commit
    }

    override suspend fun appendMergeCommit(
        transaction: KdbTransaction,
        primaryParent: KdbHash,
        mergedParent: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String,
    ): KdbCommit {
        val commit =
            delegate.appendMergeCommit(
                transaction,
                primaryParent,
                mergedParent,
                newDocumentTree,
                schemaHash,
                message,
            )
        persistence.persist(commit)
        return commit
    }

    override suspend fun compactableBefore(
        boundary: KdbHash,
        peerHeads: Set<KdbHash>,
    ): List<KdbHash> = delegate.compactableBefore(boundary, peerHeads)

    override suspend fun squash(
        squashHashes: List<KdbHash>,
        boundary: KdbHash,
        syntheticTree: DocumentTree,
        syntheticSchemaHash: KdbHash?,
        message: String,
    ): KdbCommit =
        delegate.squash(squashHashes, boundary, syntheticTree, syntheticSchemaHash, message)
}
