package dev.kdb.embed

import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.runBlocking

/** Blocking convenience for JVM callers (JDBC, CLI). */
public fun openMemoryRuntimeBlocking(
    catalog: String,
    namespaceId: String,
    schema: KdbSchema = KdbSchema.NONE,
): EmbeddedKdbRuntime = runBlocking { openMemoryRuntime(catalog, namespaceId, schema) }
