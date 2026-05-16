package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.test.runTest

class DeltaSegmentReaderTest {
    @Test
    fun readAll_afterAppend() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config =
                StorageEngineConfig(
                    globalMemoryBudgetBytes = 2_000_000,
                    ioShim = shim,
                    compressionCodec = CompressionCodec.NONE,
                )
            val factory = DeltaSegmentFactory(config)
            val writer = factory.openWriter("ns1")
            val commit =
                KdbCommit.build(
                    parentHashes = emptyList(),
                    namespaceId = "ns1",
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":1}""")),
                    documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                    schemaHash = null,
                )
            writer.append(
                DeltaRecord(
                    commitHash = commit.hash,
                    namespaceId = "ns1",
                    authorship =
                        DeltaAuthorshipEnvelope(
                            principal = "p",
                            timestamp = commit.timestamp,
                            rightsToken = "",
                            clientContext = "",
                        ),
                    commitPayload = commit.toPayloadBytes(),
                    documentPatches = emptyList(),
                ),
            )
            val ref = writer.seal()
            val reader = factory.openReader("ns1")
            val records = reader.readAll(ref)
            assertEquals(1, records.size)
            assertEquals(commit.hash, records[0].commitHash)
        }
}
