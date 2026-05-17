package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.peersync.PeerSession
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone

/** Pull and materialize after a stream [DeltaCommit] notification. */
public suspend fun applyRemoteStreamDelta(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    commitHash: KdbHash,
    schema: KdbSchema = runtime.schema,
): Boolean {
    if (runtime.dag.hasCommit(commitHash)) {
        return false
    }
    session.pullMissing()
    materializeCommitHistory(runtime, namespaceId, schema)
    if (!schema.isNone) {
        syncEmbedSchema(runtime, namespaceId, schema)
    }
    return true
}
