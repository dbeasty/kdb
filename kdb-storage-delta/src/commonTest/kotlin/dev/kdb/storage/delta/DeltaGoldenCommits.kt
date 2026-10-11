package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp

/**
 * The two commits in the delta golden segments. Go builds the same ones (fixtureCommit and
 * secondFixtureCommit in go/kdb/interop/delta_physical_golden_test.go); keep them in lockstep.
 */
internal object DeltaGoldenCommits {
    const val NAMESPACE_ID = "fixture-ns"

    private fun hash(seed: Int): KdbHash = KdbHash.fromBytes(ByteArray(32) { (seed + it).toByte() })

    /** Leaves schemaHash null: where the Go and Kotlin encoders could most easily diverge (D8/D9). */
    fun first(): KdbCommit =
        KdbCommit.build(
            parentHashes = listOf(hash(1), hash(2)),
            namespaceId = NAMESPACE_ID,
            transactionId = KdbUuid.fromString("11111111-2222-3333-4444-555555555555"),
            timestamp = KdbTimestamp.fromEpochMicros(1_700_000_000_123_456L),
            authorNodeId = KdbUuid.fromString("66666666-7777-8888-9999-aaaaaaaaaaaa"),
            operations = emptyList<KdbOp>(),
            documentTreeHash = hash(3),
            schemaHash = null,
            message = "fixture commit 〰",
        )

    /** A child of [first]. */
    fun second(): KdbCommit =
        KdbCommit.build(
            parentHashes = listOf(first().hash),
            namespaceId = NAMESPACE_ID,
            transactionId = KdbUuid.fromString("11111111-2222-3333-4444-555555555556"),
            timestamp = KdbTimestamp.fromEpochMicros(1_700_000_000_123_457L),
            authorNodeId = KdbUuid.fromString("66666666-7777-8888-9999-aaaaaaaaaaaa"),
            operations = emptyList<KdbOp>(),
            documentTreeHash = hash(4),
            schemaHash = null,
            message = "second fixture commit",
        )
}
