package dev.kdb.inspect

import dev.kdb.storage.delta.DeltaSegmentScanner
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

public object DeltaSegmentDump {
    private val json = Json { prettyPrint = true }

    public fun dumpSegmentBytes(
        bytes: ByteArray,
        pretty: Boolean = true,
    ): String {
        val commits = DeltaSegmentScanner.scanSegmentBytes(bytes)
        val lines = commits.map { InspectJson.commitDto(it.commit) }
        val fmt = if (pretty) Json { prettyPrint = true } else json
        return fmt.encodeToString(lines)
    }
}
