package dev.kdb.inspect

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

public object BlobInspector {
    private val json = Json { prettyPrint = true }

    public fun dumpCommitPayload(bytes: ByteArray): String {
        val commit = KdbCommit.fromPayloadBytes(bytes)
        return json.encodeToString(InspectJson.commitDto(commit))
    }

  public fun dumpRawBlob(
        bytes: ByteArray,
        hash: KdbHash?,
    ): String {
        val preview =
            bytes.take(64).joinToString("") { b ->
                ((b.toInt() and 0xFF).toString(16)).padStart(2, '0')
            }
        return json.encodeToString(
            mapOf(
                "hash" to hash?.toHex(),
                "sizeBytes" to bytes.size,
                "hexPreview" to preview,
            ),
        )
    }
}
