package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitRef
import dev.kdb.error.VersionNotFoundException

public interface VersionResolver {
    public suspend fun resolve(
        dag: CommitDag,
        clause: VersionClause?,
        activeCheckout: CheckoutHandle?,
    ): KdbHash
}

public fun defaultVersionResolver(): VersionResolver = DefaultVersionResolver()

public class DefaultVersionResolver : VersionResolver {
    override suspend fun resolve(
        dag: CommitDag,
        clause: VersionClause?,
        activeCheckout: CheckoutHandle?,
    ): KdbHash {
        if (clause == null) {
            if (activeCheckout != null) return activeCheckout.commitHash
            return dag.head()
        }
        val ref =
            when (clause) {
                is VersionClause.AtTag -> CommitRef.ByTag(clause.tag)
                is VersionClause.AtCommit -> CommitRef.ByHash(clause.hex)
                is VersionClause.AtTime -> CommitRef.ByTime(KdbTimestamp.fromIso8601(clause.iso8601))
            }
        return dag.resolveRefOrThrow(ref)
    }
}

public fun VersionClause.toCommitRef(): CommitRef =
    when (this) {
        is VersionClause.AtTag -> CommitRef.ByTag(tag)
        is VersionClause.AtCommit -> CommitRef.ByHash(hex)
        is VersionClause.AtTime -> CommitRef.ByTime(KdbTimestamp.fromIso8601(iso8601))
    }
