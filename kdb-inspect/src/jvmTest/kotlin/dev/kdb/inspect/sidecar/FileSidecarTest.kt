package dev.kdb.inspect.sidecar

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.DebugSidecarConfig
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class FileSidecarTest {
    @Test
    fun deltaHook_writesJsonl() =
        runTest {
            val dir = Files.createTempDirectory("kdb-sidecar").toString()
            val config =
                DebugSidecarConfig(
                    enabled = true,
                    directory = dir,
                    logDelta = true,
                    logWire = false,
                )
            val hook = deltaDebugHook(config)
            val commit =
                KdbCommit.build(
                    parentHashes = emptyList(),
                    namespaceId = "app/sidecar",
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"a":1}""")),
                    documentTreeHash = KdbHash.fromHex("dd".repeat(32)),
                    schemaHash = null,
                )
            hook.onAppend(
                DeltaRecord(
                    commitHash = commit.hash,
                    namespaceId = "app/sidecar",
                    authorship =
                        DeltaAuthorshipEnvelope(
                            principal = "test",
                            timestamp = commit.timestamp,
                            rightsToken = "",
                            clientContext = "",
                        ),
                    commitPayload = commit.toPayloadBytes(),
                    documentPatches = emptyList(),
                ),
                KdbUuid.random(),
                0L,
            )
            val file = java.nio.file.Path.of(dir, "app/sidecar", "debug", "delta.jsonl")
            assertTrue(Files.exists(file))
            val text = Files.readString(file)
            assertTrue(text.contains("delta"))
            assertTrue(text.contains(commit.hash.toHex()))
        }
}
