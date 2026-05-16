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
import dev.kdb.error.VersionNotFoundException

/**
 * Primary interface for commit DAG operations within one namespace ([Layer 2 §6]).
 */
public interface CommitDag {
    public val namespaceId: String

    /**
     * Returns commits whose canonical lowercase hex encoding starts with [hexPrefixLower].
     */
    public suspend fun lookupHashPrefix(hexPrefixLower: String): List<KdbHash>

    public suspend fun getCommit(hash: KdbHash): KdbCommit?

    public suspend fun getCommitOrThrow(hash: KdbHash): KdbCommit

    public suspend fun getStub(hash: KdbHash): CommitStub?

    public suspend fun hasCommit(hash: KdbHash): Boolean

    public suspend fun hasStub(hash: KdbHash): Boolean

    public suspend fun putCommit(
        commit: KdbCommit,
        requireParents: Boolean = true,
    )

    public suspend fun stubCommit(
        hash: KdbHash,
        archiveLocation: String,
    ): CommitStub

    public suspend fun getDocumentTree(treeHash: KdbHash): DocumentTree?

    public suspend fun getDocumentTreeOrThrow(treeHash: KdbHash): DocumentTree

    public suspend fun putDocumentTree(tree: DocumentTree)

    public suspend fun head(): KdbHash

    public suspend fun setHead(
        branchName: String,
        hash: KdbHash,
    )

    public suspend fun getBranch(name: String): KdbBranch?

    public suspend fun getBranchOrThrow(name: String): KdbBranch

    public suspend fun listBranches(): List<KdbBranch>

    public suspend fun createBranch(
        name: String,
        fromHash: KdbHash,
    ): KdbBranch

    public suspend fun deleteBranch(name: String)

    public suspend fun getTag(name: String): KdbTag?

    public suspend fun getTagOrThrow(name: String): KdbTag

    public suspend fun listTags(): List<KdbTag>

    public suspend fun createTag(
        name: String,
        commitHash: KdbHash,
        message: String = "",
    ): KdbTag

    public suspend fun deleteTag(name: String)

    public suspend fun walk(
        from: KdbHash,
        until: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<TraversalEntry>

    public suspend fun commitsSince(
        from: KdbHash,
        exclude: Set<KdbHash>,
    ): List<KdbHash>

    public suspend fun commonAncestor(
        hashA: KdbHash,
        hashB: KdbHash,
    ): KdbHash?

    public suspend fun isAncestor(
        ancestor: KdbHash,
        descendant: KdbHash,
    ): Boolean

    public suspend fun diff(
        fromHash: KdbHash,
        toHash: KdbHash,
    ): CommitDiff

    public suspend fun appendCommit(
        transaction: KdbTransaction,
        parentHash: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    public suspend fun appendMergeCommit(
        transaction: KdbTransaction,
        primaryParent: KdbHash,
        mergedParent: KdbHash,
        newDocumentTree: DocumentTree,
        schemaHash: KdbHash?,
        message: String = "",
    ): KdbCommit

    public suspend fun compactableBefore(
        boundary: KdbHash,
        peerHeads: Set<KdbHash>,
    ): List<KdbHash>

    public suspend fun squash(
        squashHashes: List<KdbHash>,
        boundary: KdbHash,
        syntheticTree: DocumentTree,
        syntheticSchemaHash: KdbHash?,
        message: String = "compaction",
    ): KdbCommit

    public suspend fun resolveRef(ref: CommitRef): KdbHash? {
        when (ref) {
            is CommitRef.ByHash -> {
                val hex = ref.hex.lowercase()
                require(hex.length >= 7) { "hash prefix must be at least 7 hex chars" }
                val candidates = lookupHashPrefix(hex)
                return when (candidates.size) {
                    0 -> null
                    1 -> candidates.single()
                    else ->
                        throw DagConsistencyException(
                            "ambiguous hash prefix $hex (${candidates.size} matches)",
                            namespaceId,
                            null,
                        )
                }
            }

            is CommitRef.ByBranch -> return getBranch(ref.name)?.headHash

            is CommitRef.ByTag -> return getTag(ref.name)?.commitHash

            is CommitRef.ByTime -> {
                val start = head()
                val walked =
                    walk(
                        from = start,
                        limit = Int.MAX_VALUE,
                    )
                for (e in walked) {
                    when (e) {
                        is TraversalEntry.Full -> {
                            if (e.commit.timestamp <= ref.timestamp) {
                                return e.commit.hash
                            }
                        }

                        else -> Unit
                    }
                }
                return null
            }
        }
    }

    public suspend fun resolveRefOrThrow(ref: CommitRef): KdbHash =
        resolveRef(ref)
            ?: throw VersionNotFoundException(
                "could not resolve ref",
                namespaceId,
                ref.toTraceString(),
            )
}

private fun CommitRef.toTraceString(): String =
    when (this) {
        is CommitRef.ByHash -> "hash:$hex"
        is CommitRef.ByBranch -> "branch:$name"
        is CommitRef.ByTag -> "tag:$name"
        is CommitRef.ByTime -> "time:${timestamp.toEpochMicros()}"
    }

/** Walk entry ([Layer 2 §6]). */
public sealed class TraversalEntry {
    public data class Full(
        val commit: KdbCommit,
    ) : TraversalEntry()

    public data class Stubbed(
        val stub: CommitStub,
    ) : TraversalEntry()
}

/** Document identity diff across two commits ([Layer 2 §6]). */
public data class CommitDiff(
    val fromHash: KdbHash,
    val toHash: KdbHash,
    val entries: List<DiffEntry>,
) {
    public val added: List<DiffEntry.Added> get() = entries.filterIsInstance<DiffEntry.Added>()

    public val removed: List<DiffEntry.Removed> get() = entries.filterIsInstance<DiffEntry.Removed>()

    public val modified: List<DiffEntry.Modified> get() = entries.filterIsInstance<DiffEntry.Modified>()

    public val isEmpty: Boolean get() = entries.isEmpty()
}

public sealed class DiffEntry {
    public data class Added(
        val docId: KdbUuid,
        val contentHash: KdbHash,
    ) : DiffEntry()

    public data class Removed(
        val docId: KdbUuid,
        val contentHash: KdbHash,
    ) : DiffEntry()

    public data class Modified(
        val docId: KdbUuid,
        val fromContentHash: KdbHash,
        val toContentHash: KdbHash,
    ) : DiffEntry()
}

/** User-facing symbolic revision ([Layer 2 §6]). */
public sealed class CommitRef {
    public data class ByHash(
        val hex: String,
    ) : CommitRef()

    public data class ByBranch(
        val name: String,
    ) : CommitRef()

    public data class ByTag(
        val name: String,
    ) : CommitRef()

    public data class ByTime(
        val timestamp: KdbTimestamp,
    ) : CommitRef()
}

/** In-memory DAG ([Layer 2 §6]). */
public fun inMemoryCommitDag(namespaceId: String): CommitDag = InMemoryCommitDag(namespaceId)
