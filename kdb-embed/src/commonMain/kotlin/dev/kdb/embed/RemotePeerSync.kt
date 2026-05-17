package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.peersync.PeerSession
import dev.kdb.peersync.PeerSyncResult
import dev.kdb.peersync.commitsToPush
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone

public data class RemotePushResult(
    val pushedCommits: Int,
    val localHead: KdbHash,
)

public suspend fun pushCommitsSinceRemoteHead(
    session: PeerSession,
    dag: CommitDag,
    remoteHead: KdbHash,
): RemotePushResult {
    val commits = commitsToPush(dag, dag.head(), remoteHead)
    val pushed = session.pushCommits(commits)
    return RemotePushResult(pushed, dag.head())
}

public suspend fun syncEmbeddedWithPeer(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    schema: KdbSchema = runtime.schema,
): PeerSyncResult {
    val result = session.syncBidirectional()
    materializeCommitHistory(runtime, namespaceId, schema)
    if (!schema.isNone) {
        syncEmbedSchema(runtime, namespaceId, schema)
    }
    return result
}
