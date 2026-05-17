package dev.kdb.jdbc

import dev.kdb.embed.EmbeddedKdbRuntime as EmbedRuntime
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.schema.KdbSchema

public typealias EmbeddedKdbRuntime = EmbedRuntime

public fun openMemoryRuntime(
    catalog: String,
    namespaceId: String,
    schema: KdbSchema = KdbSchema.NONE,
): EmbeddedKdbRuntime = openMemoryRuntimeBlocking(catalog, namespaceId, schema)
