package dev.kdb.jdbc.memory

import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.syncEmbedSchema
import dev.kdb.jdbc.KdbJdbcUrl
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import java.sql.SQLException
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking

internal data class MemoryRuntimeKey(
    val catalog: String,
    val namespaceId: String,
    val isolate: String,
)

/**
 * Process-wide shared in-memory engines keyed by JDBC URL identity.
 * Same `jdbc:kdb:memory:///catalog/namespace` ⇒ same [EmbeddedKdbRuntime] (pool-safe).
 */
internal object MemoryRuntimeRegistry {
    private val entries = mutableMapOf<MemoryRuntimeKey, Entry>()
    private val registryLock = ReentrantLock()

    fun acquireBlocking(
        url: KdbJdbcUrl,
        info: Properties?,
    ): MemoryRuntimeLease {
        val key = keyFor(url, info)
        val dropOnClose = dropOnClose(url, info)
        val schema = schemaFromProperties(info)
        return registryLock.withLock {
            val entry =
                entries.getOrPut(key) {
                    createEntry(url, schema)
                }
            if (!schema.isNone) {
                entry.applySchema(schema)
            }
            entry.refCount++
            MemoryRuntimeLease(
                runtime = entry.runtime,
                access = entry.access,
                schemaRef = entry.schemaRef,
                onRelease = {
                    registryLock.withLock {
                        releaseLocked(key, dropOnClose)
                    }
                },
            )
        }
    }

    /** Test hook: drop all shared memory databases. */
    fun clearAllBlocking() {
        registryLock.withLock {
            entries.clear()
        }
    }

    private fun releaseLocked(
        key: MemoryRuntimeKey,
        dropOnClose: Boolean,
    ) {
        val entry = entries[key] ?: return
        entry.refCount--
        if (entry.refCount <= 0) {
            if (dropOnClose) {
                entries.remove(key)
            } else {
                entry.refCount = 0
            }
        }
    }

    private fun createEntry(
        url: KdbJdbcUrl,
        schema: KdbSchema,
    ): Entry {
        val runtime =
            openMemoryRuntimeBlocking(
                catalog = url.catalog,
                namespaceId = url.namespaceId,
                schema = schema,
            )
        if (!schema.isNone) {
            runBlocking {
                syncEmbedSchema(runtime, url.namespaceId, schema)
            }
        }
        return Entry(
            runtime = runtime,
            access = ReentrantLock(),
            schemaRef = AtomicReference(schema.takeUnless { it.isNone } ?: runtime.schema),
            refCount = 0,
        )
    }

    private fun keyFor(
        url: KdbJdbcUrl,
        info: Properties?,
    ): MemoryRuntimeKey {
        val unique =
            info?.getProperty("unique")?.toBooleanStrictOrNull() == true ||
                url.memoryParams["unique"]?.toBooleanStrictOrNull() == true
        val isolate =
            when {
                unique -> UUID.randomUUID().toString()
                else -> url.memoryParams["isolate"] ?: ""
            }
        return MemoryRuntimeKey(url.catalog, url.namespaceId, isolate)
    }

    private fun dropOnClose(
        url: KdbJdbcUrl,
        info: Properties?,
    ): Boolean =
        info?.getProperty("dropOnClose")?.toBooleanStrictOrNull() == true ||
            url.memoryParams["dropOnClose"]?.toBooleanStrictOrNull() == true

    private fun schemaFromProperties(info: Properties?): KdbSchema {
        info?.getProperty("schema") ?: return KdbSchema.NONE
        return KdbSchema.NONE
    }

    private class Entry(
        val runtime: EmbeddedKdbRuntime,
        val access: ReentrantLock,
        val schemaRef: AtomicReference<KdbSchema>,
        var refCount: Int,
    ) {
        fun applySchema(schema: KdbSchema) {
            val current = schemaRef.get()
            if (!current.isNone && !schemasCompatible(current, schema)) {
                throw SQLException(
                    "schema mismatch for shared memory database",
                )
            }
            if (current.isNone && !schema.isNone) {
                schemaRef.set(schema)
                runBlocking {
                    syncEmbedSchema(runtime, runtime.defaultNamespace, schema)
                }
            }
        }
    }
}

public class MemoryRuntimeLease internal constructor(
    val runtime: EmbeddedKdbRuntime,
    private val access: ReentrantLock,
    private val schemaRef: AtomicReference<KdbSchema>,
    private val onRelease: () -> Unit,
) {
    fun currentSchema(): KdbSchema = schemaRef.get()

    fun registerSchemaBlocking(schema: KdbSchema) {
        if (schema.isNone) return
        access.withLock {
            val current = schemaRef.get()
            if (!current.isNone && !schemasCompatible(current, schema)) {
                throw SQLException("schema mismatch for shared memory database")
            }
            if (current.isNone) {
                schemaRef.set(schema)
                runBlocking(Dispatchers.Default) {
                    syncEmbedSchema(runtime, runtime.defaultNamespace, schema)
                }
            }
        }
    }

    /** Sets the shared schema identity without re-syncing indexes (caller already materialized data). */
    fun publishSchema(schema: KdbSchema) {
        if (schema.isNone) return
        access.withLock {
            val current = schemaRef.get()
            if (!current.isNone && !schemasCompatible(current, schema)) {
                throw SQLException("schema mismatch for shared memory database")
            }
            schemaRef.set(schema)
        }
    }

    fun release() {
        onRelease()
    }

    fun <T> withAccess(block: () -> T): T = access.withLock(block)
}

private fun schemasCompatible(
    registered: KdbSchema,
    requested: KdbSchema,
): Boolean = registered.fields == requested.fields
