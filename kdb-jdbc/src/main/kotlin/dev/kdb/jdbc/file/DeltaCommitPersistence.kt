package dev.kdb.jdbc.file

import dev.kdb.codec.KdbTimestamp
import dev.kdb.document.KdbCommit
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DeltaSegmentWriter

public class DeltaCommitPersistence(
    private val namespaceId: String,
    private val deltaWriter: DeltaSegmentWriter,
    private val principal: String = "embedded",
) {
    public suspend fun persist(commit: KdbCommit) {
        val record =
            DeltaRecord(
                commitHash = commit.hash,
                namespaceId = namespaceId,
                authorship =
                    DeltaAuthorshipEnvelope(
                        principal = principal,
                        timestamp = commit.timestamp,
                        rightsToken = "",
                        clientContext = "",
                    ),
                commitPayload = commit.toPayloadBytes(),
                documentPatches = emptyList(),
            )
        deltaWriter.append(record)
        deltaWriter.flush()
    }
}
