package dev.kdb.integrity

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit

/**
 * Reconstructs the well-known genesis commit hash for a namespace - the
 * same fixed transaction/author/timestamp/message
 * dev.kdb.dag.InMemoryCommitDag builds on every open. Genesis is
 * namespace-scoped (namespaceId is part of the hashed payload) but
 * otherwise identical every time, and by design is never written to the
 * delta log - a real commit's first parent legitimately points to it, and
 * L2 verification must not report that as a missing_parent.
 */
public fun genesisCommitHash(namespaceId: String): KdbHash {
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
    return genesis.hash
}
